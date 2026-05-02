# SPDX-License-Identifier: Apache-2.0


ARG IMAGE_REGISTRY_PREFIX=
ARG JDK_BUILDER_IMAGE=${IMAGE_REGISTRY_PREFIX}eclipse-temurin:21-jdk-alpine
ARG RUNTIME_BASE_IMAGE=${IMAGE_REGISTRY_PREFIX}alpine:3.21

FROM ${JDK_BUILDER_IMAGE} AS jre-builder
RUN "${JAVA_HOME}/bin/jlink" \
        --add-modules java.base,java.compiler,java.desktop,java.instrument,java.logging,java.management,java.naming,java.net.http,java.security.jgss,java.security.sasl,java.sql,java.xml,jdk.crypto.ec,jdk.httpserver,jdk.unsupported \
        --strip-debug --no-man-pages --no-header-files --compress=2 \
        --output /opt/jre

FROM ${RUNTIME_BASE_IMAGE}
COPY --from=jre-builder /opt/jre /opt/jre
ENV PATH="/opt/jre/bin:${PATH}"

COPY target/statefun-e2e-k8s-native-remote-function-*.jar /opt/remote-function.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/opt/remote-function.jar"]
