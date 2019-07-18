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
Airflow Hook
"""

from airflow.hooks.base_hook import BaseHook
from cord_workflow_controller_client.workflow_run import WorkflowRun


class CORDWorkflowControllerException(Exception):
    """
    Alias for Exception.
    """


class CORDWorkflowControllerHook(BaseHook):
    """
    Hook for accessing CORD Workflow Controller
    """

    def __init__(
            self,
            workflow_id,
            workflow_run_id,
            controller_conn_id='cord_controller_default'):
        super().__init__(source=None)
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.controller_conn_id = controller_conn_id

        self.workflow_run_client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.workflow_run_client is not None:
            self.close_conn()

    def get_conn(self):
        """
        Connect a Workflow Run client.
        """
        if self.workflow_run_client is None:
            # find connection info from database or environment
            # ENV: AIRFLOW_CONN_CORD_CONTROLLER_DEFAULT
            connection_params = self.get_connection(self.controller_conn_id)
            # connection_params have three fields
            # host
            # login - we don't use this yet
            # password - we don't use this yet
            try:
                self.workflow_run_client = WorkflowRun(self.workflow_id, self.workflow_run_id)
                self.workflow_run_client.connect(connection_params.host)
            except BaseException as ex:
                raise CORDWorkflowControllerException(ex)

        return self.workflow_run_client

    def close_conn(self):
        """
        Close the Workflow Run client
        """
        if self.workflow_run_client:
            try:
                self.workflow_run_client.disconnect()
            except BaseException as ex:
                raise CORDWorkflowControllerException(ex)

        self.workflow_run_client = None

    def update_status(self, task_id, status):
        """
        Update status of the workflow run.
        'state' should be one of ['begin', 'end']
        """
        client = self.get_conn()
        try:
            return client.update_status(task_id, status)
        except BaseException as ex:
            raise CORDWorkflowControllerException(ex)

    def count_events(self):
        """
        Count queued events for the workflow run.
        """
        client = self.get_conn()
        try:
            return client.count_events()
        except BaseException as ex:
            raise CORDWorkflowControllerException(ex)

    def fetch_event(self, task_id, topic):
        """
        Fetch an event for the workflow run.
        """
        client = self.get_conn()
        try:
            return client.fetch_event(task_id, topic)
        except BaseException as ex:
            raise CORDWorkflowControllerException(ex)
