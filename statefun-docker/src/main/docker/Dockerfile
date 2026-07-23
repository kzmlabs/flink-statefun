# SPDX-License-Identifier: Apache-2.0


# syntax=docker/dockerfile:1.7

ARG IMAGE_REGISTRY_PREFIX=
ARG FLINK_BASE_IMAGE=${IMAGE_REGISTRY_PREFIX}apache/flink:2.2.1-java21@sha256:256a3dde6a0d613fff973f3d86b04c22bbeddabee4acb3376f24b6ac1f1d6a58
FROM ${FLINK_BASE_IMAGE}

# Security: patch base-image OS packages that already have fixed versions
# published — openssl/libssl3t64 CVE-2026-45447 (High) plus the gnutls, krb5,
# sqlite, systemd/udev, nghttp2, gcrypt, gzip, perl-base and ncurses
# Mediums/Lows. Scoped --only-upgrade: touches only the flagged packages,
# never installs new ones. Base runs build steps as flink, so root for apt.
USER root
RUN apt-get update \
 && apt-get install -y --only-upgrade \
      openssl libssl3t64 libgnutls30t64 \
      libgssapi-krb5-2 libk5crypto3 libkrb5-3 libkrb5support0 \
      libsqlite3-0 libsystemd0 libudev1 libnghttp2-14 libgcrypt20 \
      gzip perl-base libncursesw6 libtinfo6 ncurses-base ncurses-bin \
 && rm -rf /var/lib/apt/lists/*
USER flink


# Logging library versions (downloaded from Maven Central)
ARG LOGBACK_VERSION=1.4.14
ARG LOGSTASH_LOGBACK_ENCODER_VERSION=7.4
ARG SLF4J_VERSION=2.0.9

ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

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
COPY flink/lib/statefun-flink-core.jar ${FLINK_HOME}/lib/
COPY flink/lib/statefun-flink-distribution.jar ${FLINK_HOME}/lib/

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
COPY statefun-flink-runner.jar ${FLINK_HOME}/statefun-flink-runner.jar

USER flink

# Standard Flink entrypoint (K8s Operator compatible)
ENTRYPOINT ["/docker-entrypoint.sh"]
