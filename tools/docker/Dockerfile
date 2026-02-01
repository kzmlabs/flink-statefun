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

ENV ROLE=worker
ENV MASTER_HOST=localhost
ENV STATEFUN_HOME=/opt/statefun
ENV STATEFUN_MODULES=$STATEFUN_HOME/modules

# Switch to root for all modifications
USER root

# cleanup flink-lib
RUN rm -fr $FLINK_HOME/lib/flink-table*jar

# copy our distriubtion template
COPY flink/ $FLINK_HOME/

# fix CRLF line endings in shell scripts (for Windows builds)
RUN find $FLINK_HOME/bin -name "*.sh" -exec sed -i 's/\r$//' {} \;

# add user modules
RUN mkdir -p $STATEFUN_MODULES && \
    useradd --system --home-dir $STATEFUN_HOME --uid=9998 --gid=flink statefun && \
    chown -R statefun:flink $STATEFUN_HOME && \
    chmod -R g+rw $STATEFUN_HOME

# add filesystem plugins
RUN mkdir -p $FLINK_HOME/plugins/s3-fs-presto && \
    mv $FLINK_HOME/opt/flink-s3-fs-presto-*.jar $FLINK_HOME/plugins/s3-fs-presto
RUN mkdir -p $FLINK_HOME/plugins/oss-fs-hadoop && \
    mv $FLINK_HOME/opt/flink-oss-fs-hadoop-*.jar $FLINK_HOME/plugins/oss-fs-hadoop
RUN mkdir -p $FLINK_HOME/plugins/azure-fs-hadoop && \
    mv $FLINK_HOME/opt/flink-azure-fs-hadoop-*.jar $FLINK_HOME/plugins/azure-fs-hadoop

# add tcnative
RUN mv $FLINK_HOME/opt/flink-shaded-netty-tcnative-dynamic-*.jar  $FLINK_HOME/lib/

# entry point - convert CRLF to LF for Windows builds
ADD docker-entry-point.sh /tmp/docker-entry-point.sh
RUN sed -i 's/\r$//' /tmp/docker-entry-point.sh && \
    mv /tmp/docker-entry-point.sh /docker-entry-point.sh && \
    chmod +x /docker-entry-point.sh

ENTRYPOINT ["/docker-entry-point.sh"]
