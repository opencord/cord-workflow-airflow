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
Example parallel workflow
"""
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.cord_workflow_plugin import CORDEventSensor, CORDModelSensor
from airflow.operators.cord_workflow_plugin import CORDModelOperator


log = logging.getLogger(__name__)
args = {
    # hard coded date
    'start_date': datetime(2019, 1, 1),
    'owner': 'iychoi'
}

dag_parallel_cord = DAG(
    dag_id='parallel_cord_workflow',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None
)
dag_parallel_cord.doc_md = __doc__

dag_parallel_cord_admin = DAG(
    dag_id='parallel_cord_workflow_admin',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None
)
dag_parallel_cord_admin.doc_md = __doc__


def on_onu_event(model_accessor, message, **kwargs):
    log.info('onu.events: received an event - %s' % message)


def on_auth_event(model_accessor, message, **kwargs):
    log.info('authentication.events: received an event - %s' % message)


def on_dhcp_event(model_accessor, message, **kwargs):
    log.info('dhcp.events: received an event - %s' % message)


def on_model_event(model_accessor, message, **kwargs):
    log.info('model event: received an event - %s' % message)


onu_event_sensor = CORDEventSensor(
    task_id='onu_event_sensor',
    topic='onu.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_parallel_cord
)

onu_event_handler = CORDModelOperator(
    task_id='onu_event_handler',
    python_callable=on_onu_event,
    cord_event_sensor_task_id='onu_event_sensor',
    dag=dag_parallel_cord
)

auth_event_sensor = CORDEventSensor(
    task_id='auth_event_sensor',
    topic='authentication.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_parallel_cord
)

auth_event_handler = CORDModelOperator(
    task_id='auth_event_handler',
    python_callable=on_auth_event,
    cord_event_sensor_task_id='auth_event_sensor',
    dag=dag_parallel_cord
)

dhcp_event_sensor = CORDEventSensor(
    task_id='dhcp_event_sensor',
    topic='dhcp.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_parallel_cord
)

dhcp_event_handler = CORDModelOperator(
    task_id='dhcp_event_handler',
    python_callable=on_dhcp_event,
    cord_event_sensor_task_id='dhcp_event_sensor',
    dag=dag_parallel_cord
)

join = DummyOperator(
    task_id='join',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag_parallel_cord
)

att_model_event_sensor = CORDModelSensor(
    task_id='att_model_event_sensor',
    model_name='AttWorkflowDriverServiceInstance',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_parallel_cord_admin
)

att_model_event_handler = CORDModelOperator(
    task_id='att_model_event_handler',
    python_callable=on_model_event,
    cord_event_sensor_task_id='att_model_event_sensor',
    dag=dag_parallel_cord_admin
)

# handle standard flow
onu_event_sensor >> onu_event_handler >> join
auth_event_sensor >> auth_event_handler >> join
dhcp_event_sensor >> dhcp_event_handler >> join

# handle admin flow
att_model_event_sensor >> att_model_event_handler

