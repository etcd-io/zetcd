# Kafka

FROM java:openjdk-8-jre

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN apt-get install -y wget supervisor dnsutils
RUN rm -rf /var/lib/apt/lists/*; apt-get clean

ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 0.11.0.0
RUN wget -q http://www-us.apache.org/dist/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz
RUN tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && mv /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION" /kafka
# 9092 is kafka port
EXPOSE 9092

COPY kafka/ /kafka/config/
ADD kafka/run.sh /kafka/run.sh
ENTRYPOINT [ "/bin/bash", "/kafka/run.sh" ]
