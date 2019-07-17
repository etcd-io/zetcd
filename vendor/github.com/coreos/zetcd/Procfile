etcd: etcd --name infra1 --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 --initial-cluster-token etcd-cluster-1 --initial-cluster 'infra1=http://127.0.0.1:2380' --initial-cluster-state new --enable-pprof

zketcd: ./cmd/zetcd/zetcd -endpoint http://localhost:2379 -zkaddr 127.0.0.1:2181 -logtostderr -v 9
