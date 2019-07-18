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
Workflow Kickstarter

This module kickstarts Airflow workflows for requests from Workflow Controller
"""

import json
import os.path
import argparse
import pyfiglet
import traceback
import socket
import time

from multistructlog import create_logger
from cord_workflow_controller_client.manager import Manager
from importlib import import_module
from urlparse import urlparse
from airflow import configuration as AirflowConf
from airflow import api
from airflow.models import DagRun


log = create_logger()
manager = None
airflow_client = None

progargs = {
    'controller_url': 'http://localhost:3030',
    'logging': None
}

DEFAULT_CONFIG_FILE_PATH = '/etc/cord_workflow_airflow_extensions/config.json'
SOCKET_CONNECTION_TEST_TIMEOUT = 5
DEFAULT_CONNECTION_TEST_DELAY = 5
DEFAULT_CONNECTION_TEST_RETRY = 999999


def print_graffiti():
    result = pyfiglet.figlet_format("CORD\nWorkflow\nKickstarter", font="graffiti")
    print(result)


def get_arg_parser():
    parser = argparse.ArgumentParser(description='CORD Workflow Kickstarter Daemon.', prog='kickstarter')
    parser.add_argument('--config', help='locate a configuration file')
    parser.add_argument('--controller', help='CORD Workflow Controller URL')
    return parser


def read_config(path):
    if os.path.exists(path):
        with open(path) as json_config_file:
            data = json.load(json_config_file)
            return data
    return {}


def pretty_format_json(j):
    dumps = json.dumps(j, sort_keys=True, indent=4, separators=(',', ': '))
    return dumps


def is_port_open(url, timeout):
    o = urlparse(url)
    hostname = o.hostname
    port = o.port

    if (not port) or port <= 0:
        if o.scheme.lower() == 'http':
            port = 80
        elif o.scheme.lower() == 'https':
            port = 443

    succeed = False
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((hostname, int(port)))
        sock.shutdown(socket.SHUT_RDWR)
        succeed = True
    except BaseException:
        pass
    finally:
        sock.close()

    return succeed


def check_web_live(url,
                   retry=DEFAULT_CONNECTION_TEST_RETRY,
                   delay=DEFAULT_CONNECTION_TEST_DELAY,
                   timeout=SOCKET_CONNECTION_TEST_TIMEOUT):
    ipup = False
    for _ in range(retry):
        if is_port_open(url, timeout):
            ipup = True
            break
        else:
            time.sleep(delay)
    return ipup


def on_kickstart(workflow_id, workflow_run_id):
    if manager and airflow_client:
        try:
            log.info('> Kickstarting a workflow (%s) => workflow run (%s)' % (workflow_id, workflow_run_id))

            airflow_client.trigger_dag(dag_id=workflow_id, run_id=workflow_run_id)
            message = airflow_client.trigger_dag(
                dag_id=workflow_id,
                run_id=workflow_run_id
            )
            log.info('> Airflow Response: %s' % message)

            # let controller know that the new workflow run is created
            log.info('> Notifying a workflow (%s), a workflow run (%s)' % (workflow_id, workflow_run_id))
            manager.notify_new_workflow_run(workflow_id, workflow_run_id)
        except Exception as e:
            log.error('> Error : %s' % e)
            log.debug(traceback.format_exc())


def on_check_state(workflow_id, workflow_run_id):
    if manager and airflow_client:
        try:
            log.info('> Checking state of a workflow (%s) => workflow run (%s)' % (workflow_id, workflow_run_id))

            run = DagRun.find(dag_id=workflow_id, run_id=workflow_run_id)
            state = 'unknown'
            if run:
                # run is an array
                # this should be one of ['success', 'running', 'failed']
                state = run[0].state
            else:
                log.error(
                    'Cannot retrieve state of a workflow run (%s, %s)' %
                    (workflow_id, workflow_run_id)
                )
                state = 'unknown'

            log.info('> state : %s' % state)

            # let controller know the state of the workflow run
            log.info(
                '> Notifying update of state of a workflow (%s), a workflow run (%s) - state : %s' %
                (workflow_id, workflow_run_id, state)
            )
            manager.report_workflow_run_state(workflow_id, workflow_run_id, state)
        except Exception as e:
            log.error('> Error : %s' % e)
            log.debug(traceback.format_exc())


# for command-line execution
def main(args):
    print_graffiti()

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

    if args.controller:
        progargs['controller_url'] = args.controller

    print('=CONFIG=')
    config_json_string = pretty_format_json(progargs)
    print(config_json_string)
    print('\n')

    # checking controller and airflow web interface
    log.info('Checking if Workflow Controller (%s) is live...' % progargs['controller_url'])
    controller_live = check_web_live(progargs['controller_url'])
    if not controller_live:
        log.error('Controller (%s) appears to be down' % progargs['controller_url'])
        raise IOError('Controller (%s) appears to be down' % progargs['controller_url'])

    # connect to workflow controller
    log.info('Connecting to Workflow Controller (%s)...' % progargs['controller_url'])
    global manager
    manager = Manager(logger=log)
    manager.connect(progargs['controller_url'])
    manager.set_handlers({'kickstart': on_kickstart})

    # connect to airflow
    global airflow_client
    log.info('Connecting to Airflow...')

    api.load_auth()
    api_module = import_module(AirflowConf.get('cli', 'api_client'))
    airflow_client = api_module.Client(
        api_base_url=AirflowConf.get('cli', 'endpoint_url'),
        auth=api.api_auth.client_auth
    )

    log.info('Waiting for kickstart events from Workflow Controller...')
    try:
        manager.wait()
    finally:
        log.info('Terminating the program...')
        manager.disconnect()


if __name__ == "__main__":
    parser = get_arg_parser()
    args = parser.parse_args()
    main(args)
