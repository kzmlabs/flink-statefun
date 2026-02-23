# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM apache/flink:2.2.0-java21

# Logging library versions (downloaded from Maven Central)
ARG LOGBACK_VERSION=1.4.14
ARG LOGSTASH_LOGBACK_ENCODER_VERSION=7.4
ARG SLF4J_VERSION=2.0.9

ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

# StateFun JARs (copied from Maven build output)
ARG STATEFUN_CORE_JAR=flink/lib/statefun-flink-core.jar
ARG STATEFUN_DIST_JAR=flink/lib/statefun-flink-distribution.jar
ARG STATEFUN_RUNNER_JAR=statefun.jar

# Remove unnecessary Flink libs
RUN rm -rf ${FLINK_HOME}/lib/flink-table*jar \
    && rm -rf ${FLINK_HOME}/lib/log4j-slf4j-impl*jar

# Download logging JARs from Maven Central
RUN curl -fSL "${MAVEN_CENTRAL}/ch/qos/logback/logback-classic/${LOGBACK_VERSION}/logback-classic-${LOGBACK_VERSION}.jar" \
        -o ${FLINK_HOME}/lib/logback-classic.jar \
    && curl -fSL "${MAVEN_CENTRAL}/ch/qos/logback/logback-core/${LOGBACK_VERSION}/logback-core-${LOGBACK_VERSION}.jar" \
        -o ${FLINK_HOME}/lib/logback-core.jar \
    && curl -fSL "${MAVEN_CENTRAL}/net/logstash/logback/logstash-logback-encoder/${LOGSTASH_LOGBACK_ENCODER_VERSION}/logstash-logback-encoder-${LOGSTASH_LOGBACK_ENCODER_VERSION}.jar" \
        -o ${FLINK_HOME}/lib/logstash-logback-encoder.jar \
    && curl -fSL "${MAVEN_CENTRAL}/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar" \
        -o ${FLINK_HOME}/lib/slf4j-api.jar

# Copy StateFun JARs to Flink lib (parent classloader)
COPY ${STATEFUN_CORE_JAR} ${FLINK_HOME}/lib/
COPY ${STATEFUN_DIST_JAR} ${FLINK_HOME}/lib/

# Configure storage plugins
RUN mkdir -p ${FLINK_HOME}/plugins/s3-fs-presto \
    && mv ${FLINK_HOME}/opt/flink-s3-fs-presto-*.jar ${FLINK_HOME}/plugins/s3-fs-presto
RUN mkdir -p ${FLINK_HOME}/plugins/oss-fs-hadoop \
    && mv ${FLINK_HOME}/opt/flink-oss-fs-hadoop-*.jar ${FLINK_HOME}/plugins/oss-fs-hadoop
RUN mkdir -p ${FLINK_HOME}/plugins/azure-fs-hadoop \
    && mv ${FLINK_HOME}/opt/flink-azure-fs-hadoop-*.jar ${FLINK_HOME}/plugins/azure-fs-hadoop

# Move netty tcnative to lib
RUN mv ${FLINK_HOME}/opt/flink-shaded-netty-tcnative-dynamic-*.jar ${FLINK_HOME}/lib/

# Copy uber JAR as the StateFun job JAR
COPY ${STATEFUN_RUNNER_JAR} ${FLINK_HOME}/statefun.jar

USER flink

# Standard Flink entrypoint (K8s Operator compatible)
ENTRYPOINT ["/docker-entrypoint.sh"]
