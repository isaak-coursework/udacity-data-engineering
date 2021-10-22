FROM openjdk:8

ARG USER_HOME_DIR="/root"
WORKDIR ${USER_HOME_DIR}

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && chmod u+x Miniconda3-latest-Linux-x86_64.sh \
    && ./Miniconda3-latest-Linux-x86_64.sh -b -p /miniconda \
    && rm Miniconda3-latest-Linux-x86_64.sh \
    && /miniconda/condabin/conda install -y python=3.7 \
    && ln -sf /miniconda/bin/python3.7 /usr/local/bin/python

ARG MAVEN_URL="https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-common/apache-maven-3.6.0-bin.tar.gz"
ARG MAVEN_EXTRACT="./apache-maven-3.6.0.tar.gz"
RUN curl --silent --fail --show-error --location --output ${MAVEN_EXTRACT} ${MAVEN_URL} \
    && tar --extract --gzip --file ${MAVEN_EXTRACT} \
    && rm -f ${MAVEN_EXTRACT} \
    && ln -s ${USER_HOME_DIR}/apache-maven-3.6.0/bin/mvn /usr/bin/mvn

ARG SPARK_URL="https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-2.0/spark-2.4.3-bin-hadoop2.8.tgz"
ARG SPARK_EXTRACT="./spark-2.4.3.tgz"
RUN curl --silent --fail --show-error --location --output ${SPARK_EXTRACT} ${SPARK_URL} \
    && tar --extract --file ${SPARK_EXTRACT} \
    && rm -f ${SPARK_EXTRACT}
ENV SPARK_HOME=${USER_HOME_DIR}/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8

ARG GLUE_DIR=${USER_HOME_DIR}/aws-glue-libs
COPY ./aws-glue-libs ${GLUE_DIR}

RUN apt-get update && apt-get install -y zip unzip
RUN chmod u+x ./aws-glue-libs/bin/glue-setup.sh
RUN ./aws-glue-libs/bin/glue-setup.sh

WORKDIR ${USER_HOME_DIR}/aws-glue-libs
