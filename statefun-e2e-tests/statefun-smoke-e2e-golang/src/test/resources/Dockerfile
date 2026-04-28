# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation


FROM flink-statefun:latest

RUN mkdir -p /opt/statefun/modules/statefun-smoke-e2e
COPY statefun-smoke-e2e-driver.jar /opt/statefun/modules/statefun-smoke-e2e/
COPY remote-module/ /opt/statefun/modules/statefun-smoke-e2e/
COPY flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml

# Add the server's self seigned certificate to our trusted certificates.

ADD ssl/server.crt /tmp/server.crt
RUN keytool -importcert -file /tmp/server.crt -alias remote-function-host -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit -noprompt