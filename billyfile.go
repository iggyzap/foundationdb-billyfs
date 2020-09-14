package billyfs

import (
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-billy/v5"
)

// FoundationDbFile represents a file in foundation db
type FoundationDbFile struct {
	fs              *FoundationDbFs
	path            string
	protocolVersion int8
}

var _ billy.File = FoundationDbFile{}

// NewFile creates a struct
func NewFile(fs FoundationDbFs, path string) (FoundationDbFile, error) {
	// allocates new logical file
	return FoundationDbFile{&fs, path, 0}, nil
}

// Open does nothing
//does nothing. Need to initialise attrs
func (file FoundationDbFile) Open(flag int, perm os.FileMode) (billy.File, error) {
	return file, nil
}

func (FoundationDbFile) Read(p []byte) (n int, err error) {
	//not supported for nfs
	return 0, nil
}

const rEADSIZE int16 = 4096

func findDataKey(path string, offset int64) string {
	return ""
}

// Write writes bytes in write postion. Stateful!
func (FoundationDbFile) Write(p []byte) (int, error) {

	return 0, nil
}

// ReadAt function that is directly compatible with stateless NFS
func (file FoundationDbFile) ReadAt(p []byte, off int64) (d int, e error) {

	tmp, error := file.fs.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		return tx.Get(fdb.Key(findDataKey(file.path, off))).MustGet(), nil
	})

	if error != nil {
		return 0, error
	}

	if tmp != nil {
		return copy(tmp.([]byte), p), nil
	}

	return 0, nil
}

// Seek is not compatible with NFSv3 Only makes sense in context of writing because Write is stateful
func (FoundationDbFile) Seek(offset int64, i int) (int64, error) {
	return 0, nil
}

// Truncate truncates file
func (FoundationDbFile) Truncate(size int64) error {
	return nil
}

// Close have no meaning in NFSv3
func (FoundationDbFile) Close() error {
	//does nothing

	return nil
}

// Lock is not supported
func (FoundationDbFile) Lock() error {
	return nil
}

// Unlock is not supported
func (FoundationDbFile) Unlock() error {
	return nil
}

// Name returns file name
func (file FoundationDbFile) Name() string {
	return file.path
}
