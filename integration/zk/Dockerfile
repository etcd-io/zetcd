FROM java:openjdk-8-jre-alpine
LABEL name="zookeeper" version="3.4.10"
ENV ZKVERSION=3.4.10
RUN apk add --no-cache wget bash
RUN mkdir /opt
RUN wget -q -O - http://apache.mirrors.pair.com/zookeeper/zookeeper-$ZKVERSION/zookeeper-$ZKVERSION.tar.gz | tar -xzf - -C /opt
RUN mv /opt/zookeeper-$ZKVERSION /opt/zookeeper
RUN cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg
RUN mkdir -p /tmp/zookeeper
EXPOSE 2181 2888 3888
WORKDIR /opt/zookeeper
VOLUME ["/opt/zookeeper/conf", "/tmp/zookeeper"]
ENTRYPOINT ["/opt/zookeeper/bin/zkServer.sh", "start-foreground", "/opt/zookeeper/conf/zoo.cfg"]
