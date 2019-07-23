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
import os
import os.path
import argparse
import pyfiglet
import traceback
import socket
import time
import subprocess

from multistructlog import create_logger
from cord_workflow_controller_client.manager import Manager

# We can't use experimental APIs for managing workflows/workflow runs of Airflow
# - REST API does not provide sufficient features at this version
# - API_Client does not work if a caller is not in main thread

# from importlib import import_module
# from airflow import configuration as AirflowConf
# from airflow import api
# from airflow.models import DagRun

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

log = create_logger()
manager = None
# airflow_client = None

airflow_bin = os.getenv('AIRFLOW_BIN', '/usr/local/bin')

progargs = {
    'controller_url': 'http://localhost:3030',
    'airflow_bin': airflow_bin,
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


def get_airflow_cli():
    return os.path.join(progargs['airflow_bin'], 'airflow')


def check_airflow_live():
    try:
        subprocess.check_call([get_airflow_cli(), 'list_dags'])
        return True
    except subprocess.CalledProcessError as e:
        log.error(e)
        return False


def on_kickstart(workflow_id, workflow_run_id):
    # if manager and airflow_client:
    if manager:
        try:
            created = False
            log.info('> Kickstarting a workflow (%s) => workflow run (%s)' % (workflow_id, workflow_run_id))
            # message = airflow_client.trigger_dag(
            #     dag_id=workflow_id,
            #     run_id=workflow_run_id
            # )
            # log.info('> Airflow Response: %s' % message)

            output = subprocess.Popen(
                [get_airflow_cli(), 'trigger_dag', '-r', workflow_run_id, workflow_id],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding='utf8'
            )

            for line in output.stdout.readlines():
                if 'Created <DagRun ' in line:
                    created = True
                    break

            if created:
                # let controller know that the new workflow run is created
                log.info('> Notifying a new workflow run (%s) for a workflow (%s)' % (workflow_run_id, workflow_id))
                manager.report_new_workflow_run(workflow_id, workflow_run_id)
        except subprocess.CalledProcessError as e:
            # when shell exited with non-zero code
            log.error('> Error : %s' % e)
        except Exception as e:
            log.error('> Error : %s' % e)
            log.debug(traceback.format_exc())


def on_check_status(workflow_id, workflow_run_id):
    # if manager and airflow_client:
    if manager:
        try:
            status = 'unknown'
            log.info('> Checking status of workflow run (%s)' % (workflow_run_id))

            # run = DagRun.find(dag_id=workflow_id, run_id=workflow_run_id)
            # status = 'unknown'
            # if run:
            #     # run is an array
            #     # this should be one of ['success', 'running', 'failed']
            #     status = run[0].state
            # else:
            #     log.error(
            #         'Cannot retrieve status of a workflow run (%s, %s)' %
            #         (workflow_id, workflow_run_id)
            #     )
            #     status = 'unknown'

            output = subprocess.Popen(
                [get_airflow_cli(), 'list_dag_runs', workflow_id],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                encoding='utf8'
            )

            title = False
            body = False
            for line in output.stdout.readlines():
                if 'DAG RUNS' in line:
                    title = True
                elif title and ('--------' in line):
                    body = True
                elif body:
                    # id  | run_id | state | execution_date | state_date |
                    if workflow_run_id in line:
                        # found the line
                        # 1 | ssss | running | 2019-07-25T21:35:06+00:00 |
                        # 2019-07-25T21:35:06.242130+00:00 |
                        fields = line.split('|')
                        status = fields[2].strip()
                        break

            log.info('> status : %s' % status)

            # let controller know the status of the workflow run
            log.info(
                '> Updating status of a workflow run (%s) - status : %s' %
                (workflow_run_id, status)
            )
            manager.report_workflow_run_status(workflow_id, workflow_run_id, status)
        except subprocess.CalledProcessError as e:
            # when shell exited with non-zero code
            log.error('> Error : %s' % e)
        except Exception as e:
            log.error('> Error : %s' % e)
            log.debug(traceback.format_exc())


def on_check_status_bulk(requests):
    # if manager and airflow_client:
    if requests:
        req = {}
        for req in requests:
            workflow_id = req['workflow_id']
            workflow_run_id = req['workflow_run_id']

            if workflow_id not in req:
                req[workflow_id] = []

            req[workflow_id].append(workflow_run_id)

        if manager:
            try:
                log.info('> Checking status of workflow runs')

                statuses = []
                for workflow_id in req:
                    workflow_run_ids = req[workflow_id]

                    output = subprocess.Popen(
                        [get_airflow_cli(), 'list_dag_runs', workflow_id],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        encoding='utf8'
                    )

                    title = False
                    body = False
                    for line in output.stdout.readlines():
                        if 'DAG RUNS' in line:
                            title = True
                        elif title and ('--------' in line):
                            body = True
                        elif body:
                            # id  | run_id | state | execution_date | state_date |
                            for workflow_run_id in workflow_run_ids:
                                if workflow_run_id in line:
                                    # found the line
                                    # 1 | ssss | running | 2019-07-25T21:35:06+00:00 |
                                    # 2019-07-25T21:35:06.242130+00:00 |
                                    fields = line.split('|')
                                    status = fields[2].strip()

                                    log.info('> status of a workflow run (%s) : %s' % (workflow_run_id, status))
                                    statuses.append({
                                        'workflow_id': workflow_id,
                                        'workflow_run_id': workflow_run_id,
                                        'status': status
                                    })

                # let controller know statuses of workflow runs
                log.info('> Updating status of workflow runs')
                manager.report_workflow_run_status_bulk(statuses)
            except subprocess.CalledProcessError as e:
                # when shell exited with non-zero code
                log.error('> Error : %s' % e)
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
                if k in config:
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

    airflow_live = check_airflow_live()
    if not airflow_live:
        log.error('Airflow appears to be down')
        raise IOError('Airflow appears to be down')

    # connect to workflow controller
    log.info('Connecting to Workflow Controller (%s)...' % progargs['controller_url'])
    global manager
    manager = Manager(logger=log)
    manager.connect(progargs['controller_url'])
    manager.set_handlers({
        'kickstart': on_kickstart,
        'check_status': on_check_status,
        'check_status_bulk': on_check_status_bulk
    })

    # connect to airflow
    # global airflow_client
    # log.info('Connecting to Airflow...')

    # api.load_auth()
    # api_module = import_module(AirflowConf.get('cli', 'api_client'))
    # airflow_client = api_module.Client(
    #     api_base_url=AirflowConf.get('cli', 'endpoint_url'),
    #     auth=api.api_auth.client_auth
    # )

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
