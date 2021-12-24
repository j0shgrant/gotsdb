# gotsdb

`gostdb` is a (soon to be timeseries) database written in Go.

## Getting Started

This application currently depends on Go 1.17.

There are a couple of ways you can run `gotsdb` locally:

### Run locally on your host

1. Build and install the binary to your `${GOROOT}/bin` (this automatically downloads any required dependencies).

```shell
$ go install .
```

2. Run the binary (the installation step should have built the binary to a location that is on your `PATH`, for any
   standard Go installation).

```shell
$ gotsdb
```

### Run as a Docker (or any other kind of OCI complaint) container
1. Build the image from the bundled `Dockerfile`.
```shell
$ docker build -t gotsdb .
```
2. Run the image, forwarding the default port that `gotsdb` listens for traffic on (8080).
```shell
$ docker run -p 8080:8080 gotsdb
```

## Authors

Josh Grant [<josh-grant@hotmail.co.uk>](mailto:josh-grant@hotmail.co.uk)
