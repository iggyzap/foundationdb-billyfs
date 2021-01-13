package billyfs

import (
	"encoding/binary"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	pkg_errors "github.com/pkg/errors"
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

	//TODO : add meta key to preserve file info
	_, err := fs.createOrGet(path, &fileModeApplicator{perm, nil})

	if err != nil {
		return pkg_errors.Wrap(err, "failed_on_mkdirall")
	}

	return err
}

type fileModeApplicator struct {
	perm       os.FileMode
	permAsByte []byte
}

func (p *fileModeApplicator) visit(w fdb.Transaction, step *opResult) {
	if step.wasCreated {
		if p.permAsByte == nil {
			p.permAsByte = make([]byte, 4)
			binary.LittleEndian.PutUint32(p.permAsByte, uint32(p.perm))
		}
		w.Set(step.Pack(tuple.Tuple{0xFC, 0x00}), p.permAsByte)
	}
}

type opResult struct {
	subspace.Subspace
	wasCreated bool
}

type SpaceVisitor interface {
	visit(w fdb.Transaction, result *opResult)
}

func (fs *FoundationDbFs) createOrGet(path string, txSpaceVisitor SpaceVisitor) (*opResult, error) {

	fsPath := fs.split(path)

	var out interface{}
	var err error

	for i := range fsPath {
		out, err = fs.db.Transact(func(w fdb.Transaction) (interface{}, error) {
			path := fsPath[0 : i+1]
			once, err := directory.Exists(w, path)
			if err != nil {
				return nil, err
			}
			created, err := directory.CreateOrOpen(w, path, nil)
			if err != nil {
				return nil, err
			}

			dang := &opResult{
				Subspace:   created,
				wasCreated: !once,
			}

			//in reality this visitor have to be called for every node in path, since we need to
			// apply txSpaceVisitor per subspace
			if txSpaceVisitor != nil {
				txSpaceVisitor.visit(w, dang)
			}

			return dang, nil
		})

		if err != nil {
			return nil, pkg_errors.WithMessagef(err, "Unable to obtain subspace %s", path)
		}

	}

	return out.(*opResult), nil
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
		//below is bad, since we have to read all entries for path!
		// it's not that bad, since entries are last element, so we need to just construct subspace and unpack
		node, err := nodeOrRoot(r, fsPath)
		for i := range entries {
			result[i], err = stat(r, node, entries[i])

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

func nodeOrRoot(r fdb.ReadTransaction, p []string) (directory.Directory, error) {
	var node directory.Directory
	var err error
	if len(p) == 0 {
		node = directory.Root()
	} else {
		node, err = directory.Open(r, p, nil)
		if err != nil {
			return nil, err
		}
	}

	return node, nil

}

func stat(r fdb.ReadTransaction, p directory.Directory, n string) (os.FileInfo, error) {
	entry, err := p.Open(r, []string{n}, nil)
	if err != nil {
		return nil, err
	}

	bytes := r.Get(entry.Pack(tuple.Tuple{0xFC, 0x00})).MustGet()

	return dirFileInfo{n, os.FileMode(binary.LittleEndian.Uint32(bytes))}, nil
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

func (fs *FoundationDbFs) split(in string) []string {
	//need to add normalisation
	clean := path.Clean(in)
	return fs.norm(strings.Split(clean, "/"))
}

func (FoundationDbFs) norm(in []string) []string {

	//we don't want to have empty strings in path array
	if in == nil {
		return nil
	}

	var result []string

	for i := range in {
		if in[i] != "" {
			result = append(result, in[i])
		}
	}

	return result
}

// OpenFile full fledged call
func (fs FoundationDbFs) OpenFile(path string, flag int, perm os.FileMode) (billy.File, error) {

	return NewFile(&fs, path, flag, perm)
}

// Remove deletes path
func (fs FoundationDbFs) Remove(path string) error {

	fsPath := fs.split(path)

	_, err := fs.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		return directory.Root().Remove(tx, fsPath)
	})

	return err
}

// Rename renames path
func (FoundationDbFs) Rename(from string, to string) error {
	return nil
}

// Stat obtains file meta
func (fs FoundationDbFs) Stat(path string) (os.FileInfo, error) {
	fsPath := fs.split(path)

	if len(fsPath) == 0 {
		return dirFileInfo{name: "/", mode: os.ModeDir | os.ModePerm}, nil
	}

	stat, err := fs.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {

		ind := len(fsPath) - 1
		node, err := nodeOrRoot(r, fsPath[0:ind])

		if err != nil {
			return nil, err
		}

		return stat(r, node, fsPath[ind])

	})
	if err != nil {
		return nil, err
	}
	return stat.(os.FileInfo), nil
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
