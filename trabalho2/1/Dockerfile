FROM bde2020/spark-base
COPY target/trabalho-1.0-SNAPSHOT.jar /
ENTRYPOINT ["/spark/bin/spark-submit", "--class", "Ex1", "--master", "spark://spark-master:7077", "/trabalho-1.0-SNAPSHOT.jar"]