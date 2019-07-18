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
Airflow Operators
"""

from airflow.operators import PythonOperator
from airflow.utils.decorators import apply_defaults


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
