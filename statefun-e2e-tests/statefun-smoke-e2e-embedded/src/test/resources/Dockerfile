# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation


FROM flink-statefun:latest

# Add embedded module. This includes both the embedded function and the driver components as a single jar.
RUN mkdir -p /opt/statefun/modules/statefun-smoke-e2e-embedded/
COPY statefun-smoke-e2e-embedded*.jar /opt/statefun/modules/statefun-smoke-e2e-embedded/

COPY flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml
