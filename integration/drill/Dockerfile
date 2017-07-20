FROM java:openjdk-8-jdk
# needs jdk to submit sql queries!?

RUN	mkdir -p /opt/drill && \
	wget -q -O - http://www-us.apache.org/dist/drill/drill-1.10.0/apache-drill-1.10.0.tar.gz | tar -zxvf  - -C /opt/drill

EXPOSE 8047

ENV DRILL_HOME		/opt/drill/apache-drill-1.10.0
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
	"-Dlog.path=/opt/drill/apache-drill-1.10.0/log/drillbit.log",\
	"-Dlog.query.path=/opt/drill/apache-drill-1.10.0/log/drillbit_queries.json",\
	"-cp",\
	"/opt/drill/apache-drill-1.10.0/conf:/opt/drill/apache-drill-1.10.0/jars/*:/opt/drill/apache-drill-1.10.0/jars/ext/*:/opt/drill/apache-drill-1.10.0/jars/3rdparty/*:/opt/drill/apache-drill-1.10.0/jars/classb/*",\
	"org.apache.drill.exec.server.Drillbit"]
