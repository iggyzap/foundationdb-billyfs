package billyfs

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

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
func (fs FoundationDbFs) MkdirAll(path string, perm os.FileMode) error {
	fsPath := fs.split(path)

	//TODO : add meta key to preserve file info
	_, err := fs.db.Transact(func(w fdb.Transaction) (interface{}, error) {

		return directory.CreateOrOpen(w, fsPath, nil)
	})

	return err
}

// ReadDir returns all file entries in a pth
func (fs FoundationDbFs) ReadDir(path string) ([]os.FileInfo, error) {
	fsPath := fs.split(path)
	list, err := fs.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		entries, err := directory.List(r, fsPath)
		if err != nil {
			return nil, err
		}

		result := make([]os.FileInfo, len(entries))
		for i := range entries {
			result[i] = dirFileInfo{entries[i]}
		}

		return result, nil
	})

	if err != nil {
		return nil, err
	}

	slice, ok := list.([]os.FileInfo)
	if !ok {
		return nil, fmt.Errorf("Failed converting to slice %v", list)
	}

	return slice, nil
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

func (fs FoundationDbFs) split(in string) []string {
	//need to add normalisation
	clean := path.Clean(in)
	return fs.norm(strings.Split(clean, "/"))
}

func (FoundationDbFs) norm(in []string) []string {

	//we don't want to have empty strings in path array
	if in == nil {
		return nil
	}

	result := []string{}

	for i := range in {
		if in[i] != "" {
			result = append(result, in[i])
		}
	}

	return result
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
	return path.Join(arr...)
}

//billy.Capable methods

// Capabilities what fs can do
func (FoundationDbFs) Capabilities() billy.Capability {
	return billy.ReadAndWriteCapability | billy.SeekCapability | billy.TruncateCapability

}
