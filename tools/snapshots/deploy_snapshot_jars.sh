#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation

#
# Deploys snapshot builds to Apache's snapshot repository.
#

# fail immediately
set -o errexit
set -o nounset

#
# Variables with defaults (if not overwritten by environment)
#
MVN=${MVN:-mvn}

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd "$PROJECT_ROOT"

CURRENT_STATEFUN_VERSION=`${MVN} org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^([0-9]+.[0-9]+(.[0-9]+)?(-[a-zA-Z0-9]+)?)$'`
echo "Detected current version as: '$CURRENT_STATEFUN_VERSION'"

if [[ ${CURRENT_STATEFUN_VERSION} == *SNAPSHOT* ]] ; then
    echo "Deploying to repository.apache.org/content/repositories/snapshots/"
    ${MVN} clean deploy -Papache-release -Dgpg.skip -Drat.skip=true -Drat.ignoreErrors=true -DskipTests -DretryFailedDeploymentCount=10
    exit 0
else
    echo "Snapshot deployments should only be done for snapshot versions"
    exit 1
fi
