package billyfs

import (
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-billy/v5"
)

type FoundationDbFs struct {
	db fdb.Database
}

// ensure that FoundationDbFs fulfills interfaces
var _ billy.Basic = FoundationDbFs{}
var _ billy.Dir = FoundationDbFs{}
var _ billy.Capable = FoundationDbFs{}

// Creates new FoundationDBFs
func NewFoundationDbFs(clusterFile string) (FoundationDbFs, error) {
	//fdb.setAPIVersion
	db, error := fdb.OpenDatabase(clusterFile)
	if error != nil {
		return FoundationDbFs{}, error
	}

	return FoundationDbFs{db}, nil

}

//billy.Dir methods
func (FoundationDbFs) MkdirAll(path string, perm os.FileMode) error {
	return nil
}

func (FoundationDbFs) ReadDir(path string) ([]os.FileInfo, error) {
	return nil, nil
}

//billy.Basic methods
func (fs FoundationDbFs) Open(path string) (billy.File, error) {
	return fs.OpenFile(path, os.O_RDONLY, 0666)
}

func (fs FoundationDbFs) Create(path string) (billy.File, error) {

	return fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
}

func (fs FoundationDbFs) OpenFile(path string, flag int, perm os.FileMode) (billy.File, error) {
	return NewFile(fs, path).Open(flag, perm)
}

func (FoundationDbFs) Remove(path string) error {
	return nil
}

func (FoundationDbFs) Rename(from string, to string) error {
	return nil
}

func (FoundationDbFs) Stat(path string) (os.FileInfo, error) {
	return nil, nil
}

func (FoundationDbFs) Join(arr ...string) string {
	return ""
}

//billy.Capable methods
func (FoundationDbFs) Capabilities() billy.Capability {
	return billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability

}
