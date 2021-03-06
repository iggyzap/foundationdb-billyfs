package billyfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	docker "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	pkg_errors "github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type FsTestSuite struct {
	suite.Suite
	fdbfs *FoundationDbFs
	t     *testing.T
}

func (s *FsTestSuite) SetupSuite() {
	s.t = s.T()
	defer func() {
		handleError(s.T())
	}()

	var filePath, err = startFdb(s.t)
	checkError(err, "Failed starting fdb for %s", filePath)
	s.t.Cleanup(func() {
		os.Remove(filePath)
	})

	var fdbFs, error = NewFoundationDbFs(filePath)
	checkError(error, "Failed creating Fs %s for %s", error, filePath)

	s.fdbfs = &fdbFs
}

func (s *FsTestSuite) TearDownSuite() {

}

func TestEntry(t *testing.T) {
	suite.Run(t, new(FsTestSuite))
}

func (s *FsTestSuite) TestFsCreated() {
	s.Require().NotEmpty(s.fdbfs, "File system should be created")
}

func (s *FsTestSuite) TestByDefaultNoDirs() {

	files, err := s.fdbfs.ReadDir("/")
	s.Empty(err, "Should return no error")
	s.Empty(files, "No directories present")
}

func (s *FsTestSuite) TestCanCreateDirAndSeesIt() {
	err := s.fdbfs.MkdirAll("/foo", os.ModeDir|os.ModePerm)
	s.Assert().Empty(err, "Mkdir success")
	files, err := s.fdbfs.ReadDir("/")
	s.Empty(err, "Should return no error")
	s.Len(files, 1, "Should contain just %s dirs", 1)
	s.Condition(func() bool {
		return files[0].Name() == "foo" && files[0].IsDir()
	}, "should contain dir")

}

func (s *FsTestSuite) TestCreateFile() {

	s.fdbfs.MkdirAll("/foo/bar", os.ModeDir|os.ModePerm)
	//in reality, mkdir all for full path should not be needed. We should need to have /foo prefix in place
	file, err := s.fdbfs.Create("/foo/bar")
	s.Assert().Empty(err, "No Errors")
	toWrite := []byte{0xff, 0x00, 0x20}
	n, err := file.Write(toWrite)
	s.Assert().Empty(err, "No errors")
	s.Assert().Equal(len(toWrite), n, "Expected len bytes written")
	reads := make([]byte, 1024)
	n, err = file.ReadAt(reads, 0)
	s.Assert().Equal(toWrite, reads[0:n], "Expected same data")

}

func (s *FsTestSuite) TestReadWriteFullFile() {

	//let's try 65k
	rndContent := make([]byte, 65536)

	rand.Read(rndContent)
	s.fdbfs.MkdirAll("/foo/bar", os.ModeDir|os.ModePerm)
	file, err := s.fdbfs.Create("/foo/bar")
	s.Assert().Empty(err, "No Errors")

	w, err := io.Copy(file, bytes.NewReader(rndContent))
	s.Assert().Equal(int64(len(rndContent)), w, "All bytes are written to file")

	file.Seek(0, io.SeekStart)

	read, err := ioutil.ReadAll(file)

	s.Assert().Equal(len(rndContent), len(read), "Number of bytes read")
	s.Assert().Equal(rndContent, read, "Number of bytes read")

}

func (s *FsTestSuite) TestFewNestedDirs() {
	s.fdbfs.MkdirAll("/foo/bar", os.ModeDir|os.ModePerm)
	s.fdbfs.MkdirAll("/foo/baz", os.ModeDir|os.ModePerm)

	files, err := s.fdbfs.ReadDir("/foo")
	s.Empty(err, "Should return no error")

	names := make([]string, len(files))
	for i := range files {
		names[i] = files[i].Name()
	}

	s.ElementsMatch(names, []string{"bar", "baz"})
}

func (s *FsTestSuite) TestDeleteDir() {
	s.fdbfs.MkdirAll("/foo/bar", os.ModeDir|os.ModePerm)
	err := s.fdbfs.Remove("/foo/bar")
	s.Empty(err, "No Error")

	files, err := s.fdbfs.ReadDir("/foo")
	s.Empty(err, "Should return no error")

	s.Equal(0, len(files), "No files in dir /foo")
}

func (s *FsTestSuite) TestPathDeep() {
	err := s.fdbfs.MkdirAll("/foo/bar/baz", os.ModeDir|os.ModePerm)
	s.Assert().Empty(err, "Mkdir success")

	info, err := s.fdbfs.Stat("/foo/bar/baz")

	s.Assert().Equal(os.ModeDir|os.ModePerm, info.Mode())
	s.Assert().Equal("baz", info.Name())

}

//this function catches panic and signals to testing framework that test have failed
func handleError(t *testing.T) {

	if r := recover(); r != nil {

		if err, ok := r.(error); ok {
			if t == nil {
				panic(pkg_errors.WithMessage(err, "Testing pointer is nil"))
			}

			t.Error(err)

		} else {
			panic(r)
		}
	}
}

func checkError(err error, s string, args ...interface{}) {
	if err != nil {
		panic(pkg_errors.Wrapf(err, s, args...))
	}
}

const FOUNDATION_DB_CONTAINER string = "foundationdb/foundationdb:6.2.25"

// Starts foundation db container for specific test run. Defers container removal when test is finished.
// WORK in progress
func startFdb(t *testing.T) (string, error) {
	//github actions only support version 1.40
	var cli, err = docker.NewClientWithOpts(client.WithVersion("1.40"))
	checkError(err, "Failed to create docker client")

	pullImage(FOUNDATION_DB_CONTAINER, cli, t)

	dbDef := "test:test@%s:%s"

	conf := container.Config{
		Image:        FOUNDATION_DB_CONTAINER,
		ExposedPorts: nat.PortSet{"4500/tcp": {}},
		Env: []string{fmt.Sprintf("%s=%s", "FDB_CLUSTER_FILE_CONTENTS", fmt.Sprintf(dbDef, "127.0.0.1", "4500")),
			"FDB_NETWORKING_MODE=host"}}

	cnt, err := cli.ContainerCreate(context.TODO(),
		&conf,
		&container.HostConfig{PortBindings: nat.PortMap{"4500/tcp": {{HostPort: "4500"}}}},
		&network.NetworkingConfig{},
		nil,
		t.Name())

	checkError(err, "Failed to create docker container")
	t.Logf("Created container with config %+v\n", conf)
	t.Cleanup(func() {
		err := cli.ContainerRemove(context.TODO(), cnt.ID, types.ContainerRemoveOptions{RemoveVolumes: true, RemoveLinks: false, Force: true})
		checkError(err, "Unable to kill container %s", cnt.ID)
	})

	err = cli.ContainerStart(context.TODO(), cnt.ID, types.ContainerStartOptions{})
	checkError(err, "Failed to start container %s", cnt.ID)

	//run DB init with file. optionally supply different entry point.
	id, err := cli.ContainerExecCreate(context.TODO(), cnt.ID, types.ExecConfig{Cmd: []string{"fdbcli", "--exec", "configure new single ssd ; status"}})
	checkError(err, "Unable to create execution to init DB")
	err = cli.ContainerExecStart(context.TODO(), id.ID, types.ExecStartCheck{})
	checkError(err, "Unable to init db")

	json, err := cli.ContainerInspect(context.TODO(), cnt.ID)
	checkError(err, "Failed to inspect container %s", cnt.ID)

	t.Logf("Started container info: %+v\n", json)

	if !json.State.Running {
		checkError(fmt.Errorf("Container %s is not running. State: %s", cnt.ID, json.State.Status), "")
	}

	ports := json.NetworkSettings.Ports["4500/tcp"]
	file, err := ioutil.TempFile(t.TempDir(), "fdb-cluster-*.conf")
	checkError(err, "Failed to create fdb file")
	str := fmt.Sprintf(dbDef, "127.0.0.1", ports[0].HostPort)
	written, err := file.WriteString(str)
	checkError(err, "Failed to write to cluster file %s content '%s'", file.Name(), str)
	if len(str) != written {
		checkError(fmt.Errorf("Wrote wrong number of bytes, expected %d wrote %d", len(str), written), "")
	}

	return file.Name(), nil
}

type Loggly interface {
	Log(args ...interface{})
	Logf(fmt string, args ...interface{})
}

type Imagy interface {
	ImagePull(ctx context.Context, imageDef string, options types.ImagePullOptions) (io.ReadCloser, error)
	ImageList(ctx context.Context, options types.ImageListOptions) ([]types.ImageSummary, error)
}

//this func will panic if pulling fails, and wait a bit if image is too large to be pulled quickly
func pullImage(imageDef string, cli Imagy, t Loggly) {
	//for large images image pull returns too quickly

	t.Logf("Pulling image %s", imageDef)
	resp, err := cli.ImagePull(context.TODO(), imageDef, types.ImagePullOptions{})
	checkError(err, "Failed to pull image %s", imageDef)
	defer resp.Close()
	args := filters.NewArgs(filters.KeyValuePair{Key: "reference", Value: imageDef})
	firstTime := false

	//TODO: there is alternative to listen to docker events for image, then wait by millis is not required.
	// LATER.
	for {
		list, err := cli.ImageList(
			context.TODO(),
			types.ImageListOptions{
				All:     false,
				Filters: args})
		checkError(err, "Failed listing images")
		if len(list) > 0 {
			break
		}
		if firstTime {
			firstTime = false
			t.Logf("Waiting for image %s to be pulled ", imageDef)
		} else {
			t.Log(".")
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("Image %s pulled", imageDef)
}

func killFdb() {

}
