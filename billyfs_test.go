package billyfs

import (
	"context"
	"os"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	docker "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	pkg_errors "github.com/pkg/errors"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestOpenFs(t *testing.T) {
	//defer below fails under linux
	defer func() {
		handleError(t)
	}()

	var dir = t.TempDir()
	var filePath, err = startFdb(dir, t)
	checkError(err, "Failed starting fdb for %s", filePath)

	//var _, error = NewFoundationDbFs(filePath)
	//checkError(error, "Failed creating Fs %s for %s", error, filePath)

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

// Starts foundation db container for specific test run. Defers container removal when test is finished.
// WORK in progress
func startFdb(s string, t *testing.T) (string, error) {
	var cli, err = docker.NewClientWithOpts()
	checkError(err, "Failed to create docker client")

	resp, err := cli.ImagePull(context.TODO(), "foundationdb/foundationdb:6.2.25", types.ImagePullOptions{})
	defer resp.Close()
	checkError(err, "Failed to pull image")

	conf := container.Config{Image: "foundationdb/foundationdb:6.2.25", ExposedPorts: nat.PortSet{"4500/tcp": {}}}

	cnt, err := cli.ContainerCreate(context.TODO(),
		&conf,
		&container.HostConfig{},
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

	return "nil", nil
}

func killFdb() {

}
