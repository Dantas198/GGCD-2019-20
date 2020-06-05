FROM bde2020/spark-base
#:2.4.4-hadoop2.7
COPY target/trabalho-1.0-SNAPSHOT.jar /
#ENTRYPOINT ["/spark/bin/spark-submit", "--total-executor-cores", "8", "--executor-memory","2g","--driver-memory","10g", "--class", "Ex2", "--master", "spark://spark-master:7077", "/trabalho-1.0-SNAPSHOT.jar"]
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "Ex1", "--master", "spark://spark-master:7077", "/trabalho-1.0-SNAPSHOT.jar"]