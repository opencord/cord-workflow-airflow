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

# set default shell
SHELL = bash -e -o pipefail

# Variables
VERSION                  ?= $(shell cat ./VERSION)

# Targets
all: test

# Create a virtualenv and install all the libraries
venv-cordworkflowairflow:
	virtualenv $@;\
    source ./$@/bin/activate ; set -u ;\
    pip install -r requirements.txt nose2 ;\
    pip install -e ./

# tests
test: unit-test

unit-test:
	tox

clean:
	find . -name '*.pyc' | xargs rm -f
	find . -name '__pycache__' | xargs rm -rf
	rm -rf \
    .coverage \
    coverage.xml \
    nose2-results.xml \
    venv-cordworkflowairflow \
    .tox \
    build \
    dist \
    *.egg-info \
    *results.xml
