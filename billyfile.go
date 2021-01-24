package billyfs

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/go-git/go-billy/v5"
	"io"
	"os"
	"path/filepath"
)

// FoundationDbFile represents a file in foundation db
type FoundationDbFile struct {
	fs              *FoundationDbFs
	sp              *directory.DirectorySubspace
	protocolVersion int8
	data            *filedata
}

type filedata struct {
	pos int64
	len int64
}

var _ billy.File = &FoundationDbFile{}

// NewFile creates a struct
func NewFile(fs *FoundationDbFs, path string, flag int, perm os.FileMode) (*FoundationDbFile, error) {
	// allocates new logical file
	file, err := fs.db.ReadTransact(func(t fdb.ReadTransaction) (interface{}, error) {
		node, err := nodeOrRoot(t, fs.split(path))
		if err != nil {
			return nil, err
		}

		if sp, ok := node.(directory.DirectorySubspace); !ok {
			return nil, fmt.Errorf("can_not_open_root")
		} else {
			return &FoundationDbFile{fs: fs, sp: &sp, data: &filedata{}}, nil
		}

	})

	if err != nil {
		return nil, err
	}

	return file.(*FoundationDbFile), nil
}

// Open does nothing
//does nothing. Need to initialise attrs
//func (file FoundationDbFile) Open(flag int, perm os.FileMode) (billy.File, error) {
//	return file, nil
//}

func (f *FoundationDbFile) Read(p []byte) (n int, err error) {
	// difference between readat and read is one of multiple stupid brain farts of go.
	// no consistency of the language.
	n, err = f.ReadAt(p, f.data.pos)
	if n > 0 {
		f.data.pos += int64(n)
	}
	return n, err
}

const rEADSIZE int64 = 1024

// Write writes bytes in write position. Stateful!
func (f *FoundationDbFile) Write(p []byte) (int, error) {

	written, err := f.WriteAt(p, f.data.pos)
	if written > 0 {
		f.data.pos += int64(written)
	}

	return written, err

}

type writeOp struct {
	what     []byte
	key      tuple.Tuple
	offset   int
	pageSize int
}

//this function splits given byte slice into number of write operations
func AsWriteOps(p []byte, off int64, writeSize int) (stream []writeOp) {

	shift := int(off % int64(writeSize))
	times := (len(p) + shift) / writeSize
	reminder := (len(p) + shift) % writeSize
	if reminder > 0 {
		times += 1
	}
	stream = make([]writeOp, times)
	var writeOpStart = 0

	for i := range stream {

		key, _, start := findPosition(off, int64(writeSize))
		toWrite := writeSize
		if i == 0 && shift > 0 {
			toWrite -= shift
		}

		writeOpEnd := writeOpStart + toWrite
		if writeOpEnd > len(p) {
			writeOpEnd = len(p)
		}
		data := p[writeOpStart:writeOpEnd]
		writeOpStart = writeOpEnd
		off += int64(len(data))
		stream[i] = writeOp{data, key, start, writeSize}
	}

	return stream
}

func (f *FoundationDbFile) WriteAt(p []byte, off int64) (int, error) {

	//unfortunately if off misses exact bucket start, we incur penalty of read-before-write, since we
	// have to set only changed bytes in a target bucket
	// alternatively, slice p[] with offset off can be represented as a stream of slices ,
	//  which will have bucket key, offset and length to write less or equal than bucket size
	var written int = 0
	var err error = nil

	stream := AsWriteOps(p, off, int(rEADSIZE))
	for i := range stream {
		currWritten, err := f.doWrite(stream[i])
		if currWritten < len(stream[i].what) {
			err = io.ErrShortWrite
		}
		written += currWritten
		if err != nil {
			break
		}
	}

	return written, err
}

//in theory this function is much more testable as doWrite since it does not need to be part of file.
// it needs just a key and write op data
func asWrite(key fdb.Key, op writeOp) func(fdb.Transaction) (interface{}, error) {
	return func(tx fdb.Transaction) (ret interface{}, err error) {
		return WriteBlock(tx, &NarrowGetterCast{tx}, key, op)
	}
}

func (f *FoundationDbFile) doWrite(op writeOp) (int, error) {
	//writes exactly writeOp

	written, err := f.fs.db.Transact(asWrite((*f.sp).Pack(op.key), op))

	return written.(int), err
}

func findPosition(off int64, readSz int64) (key tuple.Tuple, upperBound tuple.Tuple, bucketStart int) {
	var startBucket = off / readSz
	var bucketOffset = int(off % readSz)

	return tuple.Tuple{0xFD, 0x00, startBucket}, tuple.Tuple{0xFD, 0x01}, bucketOffset
}

type readOp struct {
	slice   fdb.KeyValue
	hasMore bool
}

// ReadAt function that is directly compatible with stateless NFS
func (f *FoundationDbFile) ReadAt(p []byte, off int64) (d int, e error) {
	var tuPack, up, slice = findPosition(off, rEADSIZE)
	key := (*f.sp).Pack(tuPack)
	upper := (*f.sp).Pack(up)

	read, err := f.fs.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		rr := tx.GetRange(
			fdb.KeyRange{key, upper},
			fdb.RangeOptions{2, fdb.StreamingModeExact, false})
		kv, err := rr.GetSliceWithError()
		if err != nil {
			return nil, err
		}

		//given semantics of readrange we don't need to store file size
		// on fs. TO obtain file size we'll get firs end-most bucket and get its length.
		// then file size will be lastBucket * rEADSIZE + len(lastBucket)
		return readOp{slice: kv[0], hasMore: len(kv) > 1}, nil
	})

	bytes := read.(readOp).slice.Value
	//in case passed offset off does not hit start of the bucket, we have to read from position
	bytes = bytes[slice:]

	//todo return EOF. funky!
	d = copy(p, bytes)
	if err == nil {
		//check for EOF condition
		// if we fully transferred bytes from last available bucket
		if d == len(bytes) && !read.(readOp).hasMore {
			err = io.EOF
		}
	}

	return d, err
}

// Seek is not compatible with NFSv3 Only makes sense in context of writing because Write is stateful
func (*FoundationDbFile) Seek(offset int64, i int) (int64, error) {
	return 0, nil
}

// Truncate truncates file
func (f *FoundationDbFile) Truncate(size int64) error {

	//truncate operation is 2-fold. if we are not on exact range, then drop keys from next bucket and
	// do bitwise and with reduced length of current bucket.
	return nil
}

// Close have no meaning in NFSv3
func (*FoundationDbFile) Close() error {
	//does nothing

	return nil
}

// Lock is not supported
func (*FoundationDbFile) Lock() error {
	return nil
}

// Unlock is not supported
func (*FoundationDbFile) Unlock() error {
	return nil
}

// Name returns file name
func (f *FoundationDbFile) Name() string {
	return filepath.Join((*f.sp).GetPath()...)
}
