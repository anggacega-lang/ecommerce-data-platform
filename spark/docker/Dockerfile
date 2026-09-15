FROM apache/spark:3.5.7

USER root

RUN pip install --no-cache-dir delta-spark==3.3.2
RUN mkdir -p /home/spark/.ivy2
RUN chown -R spark:spark /home/spark
RUN usermod -d /home/spark spark

RUN proto=https && host=repo1.maven.org && path=maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar && curl -fL "$proto://$host/$path" -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar
RUN proto=https && host=repo1.maven.org && path=maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar && curl -fL "$proto://$host/$path" -o /opt/spark/jars/delta-storage-3.3.2.jar

RUN proto=https && host=repo1.maven.org && path=maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && curl -fL "$proto://$host/$path" -o /opt/spark/jars/hadoop-aws-3.3.4.jar
RUN proto=https && host=repo1.maven.org && path=maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && curl -fL "$proto://$host/$path" -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
