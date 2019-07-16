#!/usr/bin/env python3

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

"""
Workflow Control CLI

This module kickstarts Airflow workflows for requests from Workflow Controller
"""

import json
import os.path
import argparse

from multistructlog import create_logger
from cord_workflow_controller_client.manager import Manager


log = create_logger()
progargs = {
    'controller_url': 'http://localhost:3030',
    'airflow_url': 'http://localhost:8080',
    'airflow_username': '',
    'airflow_password': '',
    'logging': None
}

DEFAULT_CONFIG_FILE_PATH = '/etc/cord_workflow_airflow_extensions/config.json'


def get_arg_parser():
    parser = argparse.ArgumentParser(description='CORD Workflow Control CLI.', prog='workflow_ctl')
    parser.add_argument('--config', help='locate a configuration file')
    parser.add_argument('--controller', help='CORD Workflow Controller URL')
    parser.add_argument('--airflow', help='Airflow REST URL')
    parser.add_argument('--airflow_user', help='User Name to access Airflow Web Interface')
    parser.add_argument('--airflow_passwd', help='Password to access Airlfow Web Interface')
    parser.add_argument('cmd', help='Command')
    parser.add_argument('cmd_args', help='Arguments for the command', nargs='*')
    return parser


def read_config(path):
    if os.path.exists(path):
        with open(path) as json_config_file:
            data = json.load(json_config_file)
            return data
    return {}


def read_json_file(filename):
    if filename:
        with open(filename, 'r') as f:
            return json.load(f)
    return None


def register_workflow(args):
    # expect args should be a list of essence files
    if not args:
        raise 'no essence file is given'

    log.info('Connecting to Workflow Controller (%s)...' % progargs['controller_url'])
    manager = Manager(logger=log)
    connected = False
    results = []

    try:
        manager.connect(progargs['controller_url'])
        connected = True

        for essence_file in args:
            if not os.path.exists(essence_file):
                log.error('cannot find the essence file (%s)' % essence_file)
                continue

            essence = read_json_file(essence_file)
            log.info('Registering an essence file (%s)...' % essence_file)
            result = manager.register_workflow_essence(essence)
            if result:
                log.inof('registered an essence file (%s)' % essence_file)
            else:
                log.error('cannot register an essence file (%s)' % essence_file)

            results.append(result)
    finally:
        if connected:
            manager.disconnect()

    return results


# for command-line execution
def main(args):
    # check if config path is set
    config_file_path = DEFAULT_CONFIG_FILE_PATH
    if args.config:
        config_file_path = args.config

    if os.path.exists(config_file_path):
        # read config
        config = read_config(config_file_path)
        if config:
            global progargs
            for k in progargs:
                # overwrite
                progargs[k] = config[k]

    global log
    log = create_logger(progargs["logging"])

    if args.airflow:
        progargs['airflow_url'] = args.airflow

    if args.controller:
        progargs['controller_url'] = args.controller

    if args.airflow_user:
        progargs['airflow_user'] = args.airflow_user

    if args.airflow_passwd:
        progargs['airflow_passwd'] = args.airflow_passwd

    if args.cmd:
        if args.cmd.strip().lower() in ['reg', 'register', 'register_workflow']:
            results = register_workflow(args.cmd_args)
            print(results)
        else:
            log.error('unknown command %s' % args.cmd)
            raise 'unknown command %s' % args.cmd


if __name__ == "__main__":
    parser = get_arg_parser()
    args = parser.parse_args()
    main(args)
