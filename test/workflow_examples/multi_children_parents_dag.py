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
Example AT&T workflow using Airflow
"""
import json
import logging
import airflow
from datetime import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators import PythonOperator
from cord_workflow_airflow_extensions.sensors import CORDEventSensor, CORDModelSensor
from cord_workflow_airflow_extensions.operators import CORDModelOperator

from att_helpers import *
from att_service_instance_funcs import *


args = {
    'start_date': datetime.utcnow(),
    'owner': 'ATT',
}

dag_att = DAG(
    dag_id='att_workflow_onu',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None,
)

dag_att.doc_md = __doc__


def ONU_event(model_accessor, message, **kwargs):
    #context = kwargs
    #run_id = context['dag_run'].run_id

    logging.info('onu.events: received event', message=message)

    si = find_or_create_att_si(model_accessor, logging, message)
    if message['status'] == 'activated':
        logging.info('onu.events: activated onu', message=message)
        si.no_sync = False
        si.uni_port_id = long(message['portNumber'])
        si.of_dpid = message['deviceId']
        si.oper_onu_status = 'ENABLED'
        si.save_changed_fields(always_update_timestamp=True)
    elif message['status'] == 'disabled':
        logging.info('onu.events: disabled onu, resetting the subscriber', value=message)
        si.oper_onu_status = 'DISABLED'
        si.save_changed_fields(always_update_timestamp=True)
    else:
        logging.warn('onu.events: Unknown status value: %s' % message['status'], value=message)
        raise AirflowException('onu.events: Unknown status value: %s' % message['status'], value=message)


def AUTH_event(model_accessor, message, **kwargs):
    #context = kwargs
    #run_id = context['dag_run'].run_id

    logging.info('authentication.events: Got event for subscriber', message=message)

    si = find_or_create_att_si(model_accessor, logging, message)
    logging.debug('authentication.events: Updating service instance', si=si)
    si.authentication_state = message['authenticationState']
    si.save_changed_fields(always_update_timestamp=True)


def DHCP_event(model_accessor, message, **kwargs):
    #context = kwargs
    #run_id = context['dag_run'].run_id

    logging.info('dhcp.events: Got event for subscriber', message=message)

    si = find_or_create_att_si(model_accessor, logging, message)
    logging.debug('dhcp.events: Updating service instance', si=si)
    si.dhcp_state = message['messageType']
    si.ip_address = message['ipAddress']
    si.mac_address = message['macAddress']
    si.save_changed_fields(always_update_timestamp=True)


def DriverService_event(model_accessor, message, si, **kwargs):
    #context = kwargs
    #run_id = context['dag_run'].run_id

    event_type = message['event_type']

    go = False
    if event_type == 'create':
        logging.debug('MODEL_POLICY: handle_create for AttWorkflowDriverServiceInstance %s ' % si.id)
        go = True
    elif event_type == 'update':
        logging.debug('MODEL_POLICY: handle_update for AttWorkflowDriverServiceInstance %s ' %
                          (si.id), onu_state=si.admin_onu_state, authentication_state=si.authentication_state)
        go = True
    elif event_type == 'delete':
        pass
    else:
        pass

    if not go:
        return

    # handle only create & update events

    # Changing ONU state can change auth state
    # Changing auth state can change DHCP state
    # So need to process in this order
    process_onu_state(model_accessor, si)
    process_auth_state(si)
    process_dhcp_state(si)

    validate_states(si)

    # handling the subscriber status
    # It's a combination of all the other states
    subscriber = get_subscriber(model_accessor, si.serial_number)
    if subscriber:
        update_subscriber(model_accessor, subscriber, si)

    si.save_changed_fields()


onu_event_sensor = CORDEventSensor(
    task_id='onu_event_sensor',
    topic='onu.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_att
)

onu_event_handler = CORDModelOperator(
    task_id='onu_event_handler',
    python_callable=ONU_event,
    cord_event_sensor_task_id='onu_event_sensor',
    dag=dag_att
)

auth_event_sensor = CORDEventSensor(
    task_id='auth_event_sensor',
    topic='authentication.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_att
)

auth_event_handler = CORDModelOperator(
    task_id='auth_event_handler',
    python_callable=AUTH_event,
    cord_event_sensor_task_id='auth_event_sensor',
    dag=dag_att
)

dhcp_event_sensor = CORDEventSensor(
    task_id='dhcp_event_sensor',
    topic='dhcp.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_att
)

dhcp_event_handler = CORDModelOperator(
    task_id='dhcp_event_handler',
    python_callable=DHCP_event,
    cord_event_sensor_task_id='dhcp_event_sensor',
    dag=dag_att
)

att_model_event_sensor1 = CORDModelSensor(
    task_id='att_model_event_sensor1',
    model_name='AttWorkflowDriverServiceInstance',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_att
)

att_model_event_handler1 = CORDModelOperator(
    task_id='att_model_event_handler1',
    python_callable=DriverService_event,
    cord_event_sensor_task_id='att_model_event_sensor1',
    dag=dag_att
)


onu_event_sensor >> onu_event_handler
auth_event_sensor >> auth_event_handler
dhcp_event_sensor >> dhcp_event_handler

[onu_event_handler, auth_event_handler, dhcp_event_handler] >> att_model_event_sensor1 >> att_model_event_handler1
