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

from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from cord_workflow_controller_client.workflow_run import WorkflowRun


"""
Airflow Hook
"""


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
            # 'connection_params' has following fields
            # schema
            # host
            # port
            # login - we don't use this yet
            # password - we don't use this yet
            try:
                self.workflow_run_client = WorkflowRun(self.workflow_id, self.workflow_run_id)
                schema = connection_params.schema
                if not schema:
                    schema = 'http'

                host = connection_params.host
                if not host:
                    host = 'localhost'

                port = connection_params.port
                if (not port) or (port <= 0):
                    port = 3030

                url = '%s://%s:%s' % (schema, host, port)
                self.workflow_run_client.connect(url)
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


"""
Airflow Operators
"""


class CORDModelOperator(PythonOperator):
    """
    Calls a python function with model accessor.
    """

    # SCARLET
    # http://bootflat.github.io/color-picker.html
    ui_color = '#cf3a24'

    @apply_defaults
    def __init__(
        self,
        python_callable,
        cord_event_sensor_task_id=None,
        op_args=None,
        op_kwargs=None,
        provide_context=True,
        templates_dict=None,
        templates_exts=None,
        *args,
        **kwargs
    ):
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            provide_context=True,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            *args,
            **kwargs)
        self.cord_event_sensor_task_id = cord_event_sensor_task_id

    def execute_callable(self):
        # TODO
        model_accessor = None

        message = None
        if self.cord_event_sensor_task_id:
            message = self.op_kwargs['ti'].xcom_pull(task_ids=self.cord_event_sensor_task_id)

        new_op_kwargs = dict(self.op_kwargs, model_accessor=model_accessor, message=message)
        return self.python_callable(*self.op_args, **new_op_kwargs)


"""
Airflow Sensors
"""


class CORDEventSensor(BaseSensorOperator):
    # STEEL BLUE
    # http://bootflat.github.io/color-picker.html
    ui_color = '#4b77be'

    @apply_defaults
    def __init__(
            self,
            topic,
            key_field,
            controller_conn_id='cord_controller_default',
            *args,
            **kwargs):
        super().__init__(*args, **kwargs)

        self.log.debug('Initializing CORD EventSensor for topic %s' % topic)

        self.topic = topic
        self.key_field = key_field
        self.controller_conn_id = controller_conn_id
        self.message = None
        self.hook = None

    def __create_hook(self, context):
        """
        Return connection hook.
        """
        self.log.debug('Creating a hook for run_id %s' % context['dag_run'].run_id)
        return CORDWorkflowControllerHook(self.dag_id, context['dag_run'].run_id, self.controller_conn_id)

    def execute(self, context):
        """
        Overridden to allow messages to be passed to next tasks via XCOM
        """
        self.log.debug('Executing a task %s for run_id %s' % (self.task_id, context['dag_run'].run_id))

        if self.hook is None:
            self.hook = self.__create_hook(context)

        super().execute(context)

        self.hook.close_conn()
        self.hook = None
        return self.message

    def poke(self, context):
        # we need to use notification to immediately react at event
        # https://github.com/apache/airflow/blob/master/airflow/sensors/base_sensor_operator.py#L122
        self.log.info('Poking : trying to fetch a message with a topic %s', self.topic)

        event = self.hook.fetch_event(self.task_id, self.topic)
        if event:
            self.message = event
            return True
        return False


class CORDModelSensor(CORDEventSensor):
    # SISKIN SPROUT YELLOW
    # http://bootflat.github.io/color-picker.html
    ui_color = '#7a942e'

    @apply_defaults
    def __init__(
            self,
            model_name,
            key_field,
            controller_conn_id='cord_controller_default',
            *args,
            **kwargs):
        topic = 'datamodel.%s' % model_name
        super().__init__(topic=topic, key_field=key_field, controller_conn_id=controller_conn_id, *args, **kwargs)


"""
Airflow Plugin Definition
"""


# Defining the plugin class
class CORD_Workflow_Airflow_Plugin(AirflowPlugin):
    name = "cord_workflow_plugin"
    operators = [CORDModelOperator]
    sensors = [CORDEventSensor, CORDModelSensor]
    hooks = [CORDWorkflowControllerHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
