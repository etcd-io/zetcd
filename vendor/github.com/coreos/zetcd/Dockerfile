FROM alpine

ADD bin/zetcd-release /usr/local/bin/zetcd

EXPOSE 2181
ENTRYPOINT ["/usr/local/bin/zetcd", "-zkaddr", "0.0.0.0:2181"]
