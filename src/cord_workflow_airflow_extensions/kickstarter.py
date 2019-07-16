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
from airflow.api.client.json_client import Client as AirflowClient
from requests.auth import HTTPBasicAuth
from urlparse import urlparse


log = create_logger()
manager = None
airflow_client = None

progargs = {
    'controller_url': 'http://localhost:3030',
    'airflow_url': 'http://localhost:8080',
    'airflow_username': '',
    'airflow_password': '',
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
    parser.add_argument('--airflow', help='Airflow REST URL')
    parser.add_argument('--airflow_user', help='User Name to access Airflow Web Interface')
    parser.add_argument('--airflow_passwd', help='Password to access Airlfow Web Interface')
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

            # let controller know that the new workflow run is created
            log.info('> Notifying a workflow (%s), a workflow run (%s)' % (workflow_id, workflow_run_id))
            manager.notify_new_workflow_run(workflow_id, workflow_run_id)

            log.info('> OK')
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

    if args.airflow:
        progargs['airflow_url'] = args.airflow

    if args.controller:
        progargs['controller_url'] = args.controller

    if args.airflow_user:
        progargs['airflow_user'] = args.airflow_user

    if args.airflow_passwd:
        progargs['airflow_passwd'] = args.airflow_passwd

    print('=CONFIG=')
    config_json_string = pretty_format_json(progargs)
    print(config_json_string)
    print('\n')

    # checking controller and airflow web interface
    log.info('Checking if Workflow Controller (%s) is live...' % progargs['controller_url'])
    controller_live = check_web_live(progargs['controller_url'])
    if not controller_live:
        log.error('Controller (%s) appears to be down' % progargs['controller_url'])
        raise 'Controller (%s) appears to be down' % progargs['controller_url']

    log.info('Checking if Airflow (%s) is live...' % progargs['airflow_url'])
    airflow_live = check_web_live(progargs['airflow_url'])
    if not airflow_live:
        log.error('Airflow (%s) appears to be down' % progargs['airflow_url'])
        raise 'Airflow (%s) appears to be down' % progargs['airflow_url']

    # connect to workflow controller
    log.info('Connecting to Workflow Controller (%s)...' % progargs['controller_url'])
    global manager
    manager = Manager(logger=log)
    manager.connect(progargs['controller_url'])
    manager.set_handlers({'kickstart': on_kickstart})

    # connect to airflow
    global airflow_client
    log.info('Connecting to Airflow (%s)...' % progargs['airflow_url'])
    http_auth = None
    if progargs['airflow_user'] and progargs['airflow_passwd']:
        log.info('Using a username %s' % progargs['airflow_user'])
        http_auth = HTTPBasicAuth(progargs['airflow_user'], progargs['airflow_passwd'])

    airflow_client = AirflowClient(progargs['airflow_url'], auth=http_auth)

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
