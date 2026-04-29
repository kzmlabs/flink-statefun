#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Maven Wrapper bootstrap. Resolves and runs Maven version specified in
# .mvn/wrapper/maven-wrapper.properties. Standard upstream maven-wrapper layout.
set -e
WRAPPER_JAR=".mvn/wrapper/maven-wrapper.jar"
PROPS=".mvn/wrapper/maven-wrapper.properties"
if [ ! -f "$WRAPPER_JAR" ]; then
    WRAPPER_URL=$(grep '^wrapperUrl=' "$PROPS" | cut -d'=' -f2-)
    echo "Downloading Maven wrapper from $WRAPPER_URL"
    curl -fsSL -o "$WRAPPER_JAR" "$WRAPPER_URL"
fi
exec "${JAVA_HOME:-}/bin/java" -classpath "$WRAPPER_JAR" \
    "-Dmaven.multiModuleProjectDirectory=$(pwd)" \
    org.apache.maven.wrapper.MavenWrapperMain "$@"
