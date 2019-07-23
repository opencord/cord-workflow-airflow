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
import re

from multistructlog import create_logger
from cord_workflow_controller_client.manager import Manager
from cord_workflow_controller_client.probe import Probe


log = create_logger()
progargs = {
    'controller_url': 'http://localhost:3030',
    'logging': None
}

DEFAULT_CONFIG_FILE_PATH = '/etc/cord_workflow_airflow_extensions/config.json'


class InputError(Exception):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


def get_arg_parser():
    parser = argparse.ArgumentParser(description='CORD Workflow Control CLI.', prog='workflow_ctl')
    parser.add_argument('--config', help='locate a configuration file')
    parser.add_argument('--controller', help='CORD Workflow Controller URL')
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


def read_json_string(str):
    if str:
        try:
            return json.loads(str)
        except json.decoder.JSONDecodeError:
            return load_dirty_json(str)
    return None


def load_dirty_json(dirty_json):
    regex_replace = [
        (r"([ \{,:\[])(u)?'([^']+)'", r'\1"\3"'),
        (r" False([, \}\]])", r' false\1'),
        (r" True([, \}\]])", r' true\1')
    ]
    for r, s in regex_replace:
        dirty_json = re.sub(r, s, dirty_json)
    clean_json = json.loads(dirty_json)
    return clean_json


def register_workflow(args):
    # expect args should be a list of essence files
    if not args:
        raise InputError('no essence file is given')

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
                log.info('registered an essence file (%s)' % essence_file)
            else:
                log.error('cannot register an essence file (%s)' % essence_file)

            results.append(result)
    finally:
        if connected:
            manager.disconnect()

    return results


def emit_event(args):
    # expect args should be a json event
    if not args or len(args) != 2:
        raise InputError('parameter should be <topic> <message>')

    log.info('Connecting to Workflow Controller (%s)...' % progargs['controller_url'])
    probe = Probe(logger=log)
    connected = False

    try:
        probe.connect(progargs['controller_url'])
        connected = True

        topic = args[0]
        message = read_json_string(args[1])

        log.info('Emitting an event (%s - %s)...' % (topic, message))
        probe.emit_event(topic, message)
        log.info('Emitted an event (%s - %s)...' % (topic, message))
        return True
    finally:
        if connected:
            probe.disconnect()

    return False


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
                if k in config:
                    progargs[k] = config[k]

    global log
    log = create_logger(progargs["logging"])

    if args.controller:
        progargs['controller_url'] = args.controller

    if args.cmd:
        if args.cmd.strip().lower() in ['reg', 'register', 'register_workflow']:
            results = register_workflow(args.cmd_args)
            print(results)
        elif args.cmd.strip().lower() in ['emit', 'send', 'event', 'message']:
            results = emit_event(args.cmd_args)
            print(results)
        else:
            log.error('unknown command %s' % args.cmd)
            raise InputError('unknown command %s' % args.cmd)


if __name__ == "__main__":
    parser = get_arg_parser()
    args = parser.parse_args()
    main(args)
