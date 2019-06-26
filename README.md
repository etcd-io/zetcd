# zetcd

[![Build Status](https://travis-ci.com/etcd-io/zetcd.svg?branch=master)](https://travis-ci.com/etcd-io/zetcd)

A ZooKeeper "personality" for etcd. Point a ZooKeeper client at zetcd to dispatch the operations on an etcd cluster.

Protocol encoding and decoding heavily based on [go-zookeeper](http://github.com/samuel/go-zookeeper/).

## Getting started

### Running zetcd

Forward ZooKeeper requests on `:2181` to an etcd server listening on `localhost:2379`:

```sh
go get github.com/etcd-io/zetcd/cmd/zetcd
zetcd --zkaddr 0.0.0.0:2181 --endpoints localhost:2379
```

Simple testing with `zkctl`:

```sh
go get github.com/etcd-io/zetcd/cmd/zkctl
zkctl watch / &
zkctl create /abc "foo"
```

### Running zetcd on Docker

Official docker images of tagged zetcd releases for containerized environments are hosted at [quay.io/coreos/zetcd](https://quay.io/coreos/zetcd). Use `docker run` to launch the zetcd container with the same configuration as the `go get` example:

```sh
docker run --net host -t quay.io/coreos/zetcd -endpoints localhost:2379
```

### Cross-checking

In cross-checking mode, zetcd dynamically tests a fresh isolated "candidate" zetcd cluster against a fresh isolated ZooKeeper "oracle" cluster for divergences. This mode dispatches requests to both zetcd and ZooKeeper, then compares the responses to check for equivalence. If the responses disagree, it is flagged in the logs. Use the flags `-zkbridge` to configure a ZooKeeper endpoint and `-oracle zk` to enable checking.

Cross-check zetcd's ZooKeeper emulation with a native ZooKeeper server endpoint at `localhost:2182` like so:

```sh
zetcd --zkaddr 0.0.0.0:2181 --endpoints localhost:2379 --debug-zkbridge localhost:2182  --debug-oracle zk --logtostderr -v 9
```

## Contact

- Mailing list: [etcd-dev](https://groups.google.com/forum/?hl=en#!forum/etcd-dev)
- IRC: #[etcd](irc://irc.freenode.org:6667/#etcd) on freenode.org
- Bugs: [issues](https://github.com/etcd-io/zetcd/issues)

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

### License

zetcd is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
