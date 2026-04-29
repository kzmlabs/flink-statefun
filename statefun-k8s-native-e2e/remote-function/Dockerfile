# SPDX-License-Identifier: Apache-2.0


FROM eclipse-temurin:25-jre-alpine

COPY target/statefun-k8s-native-e2e-remote-function-*.jar /opt/remote-function.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/opt/remote-function.jar"]
