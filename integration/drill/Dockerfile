FROM java:openjdk-8-jdk
# needs jdk to submit sql queries!?

ENV DRILL_VERSION 1.15.0

RUN	mkdir -p /opt/drill && \
	wget -q -O - http://www-us.apache.org/dist/drill/drill-${DRILL_VERSION}/apache-drill-${DRILL_VERSION}.tar.gz | tar -zxvf  - -C /opt/drill

EXPOSE 8047

ENV DRILL_HOME		/opt/drill/apache-drill-${DRILL_VERSION}
ENV DRILL_LOG_DIR	${DRILL_HOME}/log/
ENV DRILL_LOG_PREFIX	${DRILL_LOG_PATH}/drill

WORKDIR ${DRILL_HOME}

ADD drill/drill-override.conf ${DRILL_HOME}/conf/drill-override.conf

# drillbit.sh is helpfully totally broken
ENTRYPOINT ["java",\
	"-Xms4G",\
	"-Xmx4G",\
	"-XX:MaxDirectMemorySize=8G",\
	"-XX:ReservedCodeCacheSize=1G",\
	"-Ddrill.exec.enable-epoll=false",\
	"-XX:+CMSClassUnloadingEnabled",\
	"-XX:+UseG1GC",\
	"-Dlog.path=/opt/drill/apache-drill-${DRILL_VERSION}/log/drillbit.log",\
	"-Dlog.query.path=/opt/drill/apache-drill-${DRILL_VERSION}/log/drillbit_queries.json",\
	"-cp",\
	"/opt/drill/apache-drill-${DRILL_VERSION}/conf:/opt/drill/apache-drill-${DRILL_VERSION}/jars/*:/opt/drill/apache-drill-${DRILL_VERSION}/jars/ext/*:/opt/drill/apache-drill-${DRILL_VERSION}/jars/3rdparty/*:/opt/drill/apache-drill-${DRILL_VERSION}/jars/classb/*",\
	"org.apache.drill.exec.server.Drillbit"]
