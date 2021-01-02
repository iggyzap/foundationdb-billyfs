package billyfs

import (
	"context"
	"fmt"
	"io"
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
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestOpenFs(t *testing.T) {
	//defer below fails under linux
	defer func() {
		handleError(t)
	}()

	var filePath, err = startFdb(t)
	checkError(err, "Failed starting fdb for %s", filePath)

	//TODO: container starts, need to write cluster file to local FS so client can read it.
	// or change API to pass file inline.
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

const FOUNDATION_DB_CONTAINER string = "foundationdb/foundationdb:6.2.25"

// Starts foundation db container for specific test run. Defers container removal when test is finished.
// WORK in progress
func startFdb(t *testing.T) (string, error) {
	//github actions only support version 1.40
	var cli, err = docker.NewClientWithOpts(client.WithVersion("1.40"))
	checkError(err, "Failed to create docker client")

	//for large images image pull returns too quickly
	pullImage(FOUNDATION_DB_CONTAINER, cli, t)

	dbDef := "test:test@%s:%s"

	conf := container.Config{
		Image:        FOUNDATION_DB_CONTAINER,
		ExposedPorts: nat.PortSet{"4500/tcp": {}},
		Env: []string{fmt.Sprintf("%s=%s", "FDB_CLUSTER_FILE_CONTENTS", fmt.Sprintf(dbDef, "127.0.0.1", "4500")),
			"FDB_NETWORKING_MODE=host"}}

	cnt, err := cli.ContainerCreate(context.TODO(),
		&conf,
		&container.HostConfig{PortBindings: nat.PortMap{"4500/tcp": {{HostPort: ""}}}},
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

	json, err := cli.ContainerInspect(context.TODO(), cnt.ID)
	checkError(err, "Failed to inspect container %s", cnt.ID)

	t.Logf("Started container info: %+v\n", json)

	if !json.State.Running {
		checkError(fmt.Errorf("Container %s is not running. State: %s", cnt.ID, json.State.Status), "")
	}

	ports := json.NetworkSettings.Ports["4500/tcp"]

	return fmt.Sprintf(dbDef, "127.0.0.1", ports[0].HostPort), nil
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
