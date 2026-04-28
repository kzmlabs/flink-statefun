# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation


from setuptools import setup

import io
import os

this_directory = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='apache-flink-statefun',
    version='3.4-SNAPSHOT',
    packages=["statefun"],
    url='https://github.com/apache/flink-statefun',
    license='https://www.apache.org/licenses/LICENSE-2.0',
    license_files=["LICENSE", "NOTICE"],
    author='Apache Software Foundation',
    author_email='dev@flink.apache.org',
    description='Python SDK for Apache Flink Stateful functions',
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=['protobuf>=3.11.3,<4.0.0'],
    tests_require=['pytest'],
    python_requires='>=3.8',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7']
)
