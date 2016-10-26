package rbddriver

import (
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
)

type RbdDriver struct {
	volume.Driver
}

func NewRbdDriver(ds string) (*RbdDriver, error) {
	log.SetLevel(log.DebugLevel)
	log.Debug("Creating new RbdDriver.")

	return &RbdDriver{rds: rds}, err
}

func (rd *RbdDriver) Create(req volume.Request) volume.Response {

	dsName := rd.rds.Name + "/" + req.Name

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) List(req volume.Request) volume.Response {
	log.WithField("Requst", req).Debug("List")
	var vols []*volume.Volume

	dsl, err := rd.rds.DatasetList()
	if err != nil {
		return volume.Response{Err: err.Error()}
	}

	errStr := ""
	for _, ds := range dsl {
		mp, err := ds.GetMountpoint()
		if err != nil {
			errStr += "Failed to get mountpoint of dsl: " + ds.Name + " Error: " + err.Error() + "\n"
		}

		vols = append(vols, &volume.Volume{Name: volNameFromDsName(ds.Name), Mountpoint: mp})
	}

	return volume.Response{Volumes: vols, Err: errStr}
}

func (rd *RbdDriver) Get(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Get")
	dsName := rd.rds.Name + "/" + req.Name

	return volume.Response{Volume: &volume.Volume{Name: volNameFromDsName(ds.Name), Mountpoint: mp}, Err: ""}
}

func (rd *RbdDriver) Remove(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Remove")
	dsName := rd.rds.Name + "/" + req.Name

	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Path(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Path")
	res := rd.Get(req)

	if res.Err != "" {
		return res
	}

	return volume.Response{Mountpoint: res.Volume.Mountpoint, Err: ""}
}

func (rd *RbdDriver) Mount(req volume.MountRequest) volume.Response {
	log.WithField("Request", req).Debug("Mount")

	return rd.Path(volume.Request{Name: req.Name})
}

func (rd *RbdDriver) Unmount(req volume.UnmountRequest) volume.Response {
	log.WithField("Request", req).Debug("Unmount")
	return volume.Response{Err: ""}
}

func (rd *RbdDriver) Capabilities(req volume.Request) volume.Response {
	log.WithField("Request", req).Debug("Capabilites")
	return volume.Response{Capabilities: volume.Capability{Scope: "local"}}
}

func volNameFromDsName(dsName string) string {
	volArr := strings.Split(dsName, "/")

	return volArr[len(volArr)-1]
}
