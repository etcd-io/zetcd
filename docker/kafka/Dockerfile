# Kafka

FROM java:openjdk-8-jre

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN apt-get install -y wget supervisor dnsutils

# Install Kafka, Zookeeper and other needed things
RUN rm -rf /var/lib/apt/lists/*
RUN apt-get clean

ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 0.8.2.1
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

RUN wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz
RUN tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt
RUN rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

ENV AUTO_CREATE_TOPICS true
ENV BROKER_ID 1
# 9092 is kafka port
EXPOSE 9092
ADD start-kafka.sh /usr/bin/start-kafka.sh
ENTRYPOINT ["/usr/bin/start-kafka.sh"]
