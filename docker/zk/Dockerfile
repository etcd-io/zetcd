FROM java:openjdk-8-jre-alpine
# had to hack this a bit...
# MAINTAINER Justin Plock <justin@plock.net>

LABEL name="zookeeper" version="3.4.8"

RUN apk add --no-cache wget bash
RUN mkdir /opt
RUN wget -q -O - http://apache.mirrors.pair.com/zookeeper/zookeeper-3.4.8/zookeeper-3.4.8.tar.gz | tar -xzf - -C /opt
RUN mv /opt/zookeeper-3.4.8 /opt/zookeeper
RUN cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg
RUN mkdir -p /tmp/zookeeper
#RUN ln -s  /opt/zookeeper/conf/zoo.cfg /opt/zookeeper/conf/start
RUN ls -lah /opt/zookeeper/bin
EXPOSE 2181 2888 3888

WORKDIR /opt/zookeeper

VOLUME ["/opt/zookeeper/conf", "/tmp/zookeeper"]

ENTRYPOINT ["/opt/zookeeper/bin/zkServer.sh", "start-foreground", "/opt/zookeeper/conf/zoo.cfg"]
