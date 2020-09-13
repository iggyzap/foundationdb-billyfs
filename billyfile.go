package billyfs

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-billy/v5"
)

type FoundationDbFile struct {
	fs              *FoundationDbFs
	path            string
	protocolVersion int8
}

var _ billy.File = FoundationDbFile{}

func NewFile(fs FoundationDbFs, path string) (FoundationDbFile, error) {
	// allocates new logical file
	return FoundationDbFile{&fs, path, 0}, nil
}

func (FoundationDbFile) Read(p []byte) (n int, err error) {
	//not supported for nfs
	return 0, nil
}

const READ_SIZE int8 = 4096


func (file FoundationDbFile) ReadAt(p []byte, off int64) (n int, err error) {


	return file.fs.db.ReadTransact( func(tx fdb.ReadTransaction) {
		tx.GetRange()

	
	)
}

func (FoundationDbFile) Close() error {
	//does nothing

	return nil
}

func (FoundationDbFile) Lock() error {
	return nil
}

func (FoundationDbFile) Unlock() error {
	return nil
}

func (file FoundationDbFile) Name() string {
	return file.path
}
