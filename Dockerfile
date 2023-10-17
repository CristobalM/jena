FROM debian:buster-slim

WORKDIR jenadir


ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-amd64"
ENV JENA_HOME "/jena-all/jena/apache-jena"
ENV PATH "$PATH:$JAVA_HOME:$JENA_HOME/bin"

COPY ./ /jena-all/

RUN apt-get update && apt-get install -y ca-certificates wget gnupg2 bash lsb-release software-properties-common
RUN mkdir -p /usr/share/man/man1
RUN apt-get update && \
    apt-get install -y \
    openjdk-11-jdk maven


RUN cd /jena-all && mvn clean install -DskipTests -Drat.skip=True -Dmaven.javadoc.skip=true

RUN cd /jena-all/apache-jena && \
mkdir -p lib && \
find ../jena* -type f -name "*.jar" -exec cp '{}' lib/ ';'
