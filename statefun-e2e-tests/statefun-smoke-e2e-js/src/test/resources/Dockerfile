# SPDX-License-Identifier: Apache-2.0


FROM flink-statefun:latest

RUN mkdir -p /opt/statefun/modules/statefun-smoke-e2e
COPY statefun-smoke-e2e-driver.jar /opt/statefun/modules/statefun-smoke-e2e/
COPY remote-module/ /opt/statefun/modules/statefun-smoke-e2e/
COPY flink-conf.yaml $FLINK_HOME/conf/flink-conf.yaml
