# cloudops

[![Go Report Card](https://goreportcard.com/badge/github.com/libopenstorage/cloudops)](https://goreportcard.com/report/github.com/libopenstorage/cloudops)
[![Build Status](https://travis-ci.org/libopenstorage/cloudops.svg?branch=master)](https://travis-ci.org/libopenstorage/cloudops)


## Goal

1. Define abstraction layerproviding a common interface to perform cloud operations that include:
  * Provisioning and managing Instances
  * Provisioning and managing Storage
  * Provisioning and managing network resources. 

2. Provide implementation for variety of clouds.

Cloudops will provide a set of binaries for debugging but its main purpose is to be used a library.

## Building and running Cloudops

Cloudops expects GOLANG to be installed.  To build cloudops, simply run `make`:

```shell
make
```

### Vendoring

This repo uses [go dep](https://golang.github.io/dep/) for vendoring. The following make rule will update the vendor directory.

```shell
make vendor
```
