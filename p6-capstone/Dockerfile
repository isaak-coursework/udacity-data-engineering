FROM python:3.7.12 AS glue

ARG USER_HOME_DIR="/root"
WORKDIR ${USER_HOME_DIR}

RUN apt-get update && \
    apt-get install -y openjdk-11 && \
    apt-get clean;

ARG MAVEN_URL="https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz"
RUN curl --silent --fail --show-error --location --output ./apache-maven.tar.gz ${MAVEN_URL} \
    && tar --extract --gzip --file ./apache-maven.tar.gz
    