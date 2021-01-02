package billyfs

import (
	"os"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/go-git/go-billy/v5"
)

// FoundationDbFs representds a billy filesystem over FoundationDb KV store
type FoundationDbFs struct {
	db fdb.Database
}

// ensure that FoundationDbFs fulfills interfaces
var _ billy.Basic = FoundationDbFs{}
var _ billy.Dir = FoundationDbFs{}
var _ billy.Capable = FoundationDbFs{}

func init() {
	fdb.APIVersion(620)
}

// NewFoundationDbFs Creates new FoundationDBFs
func NewFoundationDbFs(clusterFile string) (FoundationDbFs, error) {
	//fdb.setAPIVersion
	db, error := fdb.OpenDatabase(clusterFile)
	if error != nil {
		return FoundationDbFs{}, error
	}

	return FoundationDbFs{db}, nil

}

//billy.Dir methods

// MkdirAll creates full path
func (FoundationDbFs) MkdirAll(path string, perm os.FileMode) error {
	return nil
}

// ReadDir returns all file entries in a pth
func (FoundationDbFs) ReadDir(path string) ([]os.FileInfo, error) {
	return nil, nil
}

//billy.Basic methods

// Open  a file
func (fs FoundationDbFs) Open(path string) (billy.File, error) {
	return fs.OpenFile(path, os.O_RDONLY, 0666)
}

// Create creates a file
func (fs FoundationDbFs) Create(path string) (billy.File, error) {

	return fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
}

// OpenFile full fledged call
func (fs FoundationDbFs) OpenFile(path string, flag int, perm os.FileMode) (billy.File, error) {

	file, error := NewFile(fs, path)
	if error == nil {
		return file.Open(flag, perm)
	}

	return file, error
}

// Remove deletes path
func (FoundationDbFs) Remove(path string) error {
	return nil
}

// Rename renames path
func (FoundationDbFs) Rename(from string, to string) error {
	return nil
}

// Stat obtains file meta
func (FoundationDbFs) Stat(path string) (os.FileInfo, error) {
	return nil, nil
}

// Join joins path
func (FoundationDbFs) Join(arr ...string) string {
	return ""
}

//billy.Capable methods

// Capabilities what fs can do
func (FoundationDbFs) Capabilities() billy.Capability {
	return billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability

}
