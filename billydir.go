package billyfs

import (
	"os"
	"time"
)

type dirFileInfo struct {
	name string
}

func (dirFileInfo) IsDir() bool {
	return true
}
func (dirFileInfo) ModTime() time.Time {
	return time.Now()
}
func (dirFileInfo) Mode() os.FileMode {
	return os.ModeDir | os.ModePerm
}
func (d dirFileInfo) Name() string {
	return d.name
}
func (dirFileInfo) Size() int64 {
	return 0
}
func (dirFileInfo) Sys() interface{} {
	return nil
}
