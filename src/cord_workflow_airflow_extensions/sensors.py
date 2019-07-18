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
Airflow Sensors
"""

from .hook import CORDWorkflowControllerHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


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

        self.topic = topic
        self.key_field = key_field
        self.controller_conn_id = controller_conn_id
        self.message = None
        self.hook = None

    def __create_hook(self, context):
        """
        Return connection hook.
        """
        return CORDWorkflowControllerHook(self.dag_id, context['dag_run'].run_id, self.controller_conn_id)

    def execute(self, context):
        """
        Overridden to allow messages to be passed to next tasks via XCOM
        """
        if self.hook is None:
            self.hook = self.__create_hook(context)

        self.hook.update_status(self.task_id, 'begin')

        super().execute(context)

        self.hook.update_status(self.task_id, 'end')
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
        super().__init__(topic=topic, *args, **kwargs)
