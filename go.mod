module github.com/TrilliumIT/docker-rbd-plugin

go 1.13

require (
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/coreos/go-systemd v0.0.0-00010101000000-000000000000
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-plugins-helpers v0.0.0-20200102110956-c9a8a2d92ccc
	github.com/google/martian v2.1.0+incompatible
	github.com/o1egl/fwencoder v0.0.1
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.4.2
	github.com/urfave/cli v1.22.2
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2 // indirect
)

replace github.com/coreos/go-systemd => github.com/coreos/go-systemd/v22 v22.0.0
