package rbd

import (
	"encoding/json"
	"fmt"
	"io"
	"os/exec"

	"github.com/o1egl/fwencoder"
)

// DrDrpRbdBinPath is the path to the rbd binary
var rbdBin string
var fsFreezePath string

func init() {
	var err error
	rbdBin, err = exec.LookPath("rbd")
	if err != nil {
		panic(fmt.Errorf("unable to find rbd binary: %w", err))
	}
}

func cmdJSON(v interface{}, errMap map[int]error, args ...string) error {
	jsonDecode := func(v interface{}) func(io.Reader) error {
		return func(r io.Reader) error {
			return json.NewDecoder(r).Decode(v)
		}
	}
	args = append([]string{"--format", "json"}, args...)
	err := cmdDecode(jsonDecode(v), rbdBin, args...)
	return cmdMapErr(err, errMap)
}

func cmdColumns(v interface{}, errMap map[int]error, args ...string) error {
	colDecode := func(v interface{}) func(io.Reader) error {
		return func(r io.Reader) error {
			return fwencoder.UnmarshalReader(r, v)
		}
	}

	err := cmdDecode(colDecode(v), rbdBin, args...)
	return cmdMapErr(err, errMap)
}

func cmdOut(errMap map[int]error, args ...string) (string, error) {
	out, err := exec.Command(rbdBin, args...).Output()
	return string(out), cmdMapErr(err, errMap)
}

func cmdRun(errMap map[int]error, args ...string) error {
	err := exec.Command(rbdBin, args...).Run()
	return cmdMapErr(err, errMap)
}

func cmdDecode(decode func(io.Reader) error, name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error setting up stdout for cmd %v %v: %w", cmd, arg, err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error starting cmd %v %v: %w", cmd, arg, err)
	}
	if err := decode(stdOut); err != nil {
		return fmt.Errorf("error decoding cmd %v %v: %w", cmd, arg, err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("error waiting on cmd %v %v: %w", cmd, arg, err)
	}
	return nil
}

func cmdMapErr(err error, errMap map[int]error) error {
	if exitErr, isExitErr := err.(*exec.ExitError); isExitErr {
		if mappedErr, ok := errMap[exitErr.ExitCode()]; ok {
			return mappedErr
		}
	}
	return err
}

type mappedNBD struct {
	Pid      int    `column:"pid"`
	Pool     string `column:"pool"`
	Name     string `column:"image"`
	Snapshot string `column:"snap"`
	Device   string `column:"device"`
}

func mappedNBDs() ([]*mappedNBD, error) {
	var mapped []*mappedNBD
	err := cmdColumns(&mapped, nil, "nbd", "list")
	return mapped, err
}
