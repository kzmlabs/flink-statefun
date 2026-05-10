# SPDX-License-Identifier: Apache-2.0

# syntax=docker/dockerfile:1.7

FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q package

FROM eclipse-temurin:21-jre
WORKDIR /opt
COPY --from=builder /build/target/statefun-quickstart-greeter.jar /opt/greeter.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/opt/greeter.jar"]
