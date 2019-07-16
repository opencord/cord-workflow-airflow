# Copyright 2019-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

from setuptools import setup


def readme():
    with open("README.rst") as f:
        return f.read()


def version():
    with open("VERSION") as f:
        return f.read().strip()


def parse_requirements(filename):
    # parse a requirements.txt file, allowing for blank lines and comments
    requirements = []
    for line in open(filename):
        if line and not line.startswith("#"):
            requirements.append(line)
    return requirements


setup(
    name="cord_workflow_airflow_extensions",
    version=version(),
    description="Airflow extensions for CORD Workflow Manager",
    long_description=readme(),
    author="Illyoung Choi",
    author_email="iychoi@opennetworking.org",
    classifiers=["License :: OSI Approved :: Apache Software License"],
    license="Apache v2",
    packages=[
        "cord_workflow_airflow_extensions"
    ],
    package_dir={
        "cord_workflow_airflow_extensions": "src/cord_workflow_airflow_extensions"
    },
    install_requires=parse_requirements("requirements.txt"),
    include_package_data=True,
)
