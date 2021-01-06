package billyfs

import (
	"os"
	"time"
)

type dirFileInfo struct {
	name string
	mode os.FileMode
}

func (dirFileInfo) IsDir() bool {
	return true
}
func (dirFileInfo) ModTime() time.Time {
	return time.Now()
}
func (d dirFileInfo) Mode() os.FileMode {
	return d.mode
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
