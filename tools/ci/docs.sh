#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

HUGO_REPO=https://github.com/gohugoio/hugo/releases/download/v0.80.0/hugo_extended_0.80.0_Linux-64bit.tar.gz
HUGO_ARTIFACT=hugo_extended_0.80.0_Linux-64bit.tar.gz

if ! curl --fail -OL $HUGO_REPO ; then 
	echo "Failed to download Hugo binary"
	exit 1
fi

tar -zxvf $HUGO_ARTIFACT hugo

git submodule update --init --recursive
# generate docs into docs/target
./hugo -v --source docs --destination target

if [ $? -ne 0 ]; then
	echo "Error building the docs"
	exit 1
fi

