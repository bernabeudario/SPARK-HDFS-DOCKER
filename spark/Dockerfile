FROM apache/spark:4.0.0

USER root

# Carpeta para los logs
RUN mkdir -p /opt/spark/work && chown -R spark:spark /opt/spark/work

# Copiar configuración de Hadoop Spark
COPY core-site.xml /opt/spark/conf/core-site.xml
COPY hdfs-site.xml /opt/spark/conf/hdfs-site.xml
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN chown spark:spark /opt/spark/conf/core-site.xml /opt/spark/conf/hdfs-site.xml /opt/spark/conf/spark-defaults.conf

# Variables de entorno
ENV HADOOP_CONF_DIR=/opt/spark/conf
ENV SPARK_DIST_CLASSPATH=$(/opt/spark/bin/spark-classpath)