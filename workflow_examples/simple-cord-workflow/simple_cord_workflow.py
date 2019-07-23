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
Example CORD workflow using Airflow
"""


import logging
from datetime import datetime
from airflow import DAG
from airflow.sensors.cord_workflow_plugin import CORDEventSensor, CORDModelSensor
from airflow.operators.cord_workflow_plugin import CORDModelOperator


log = logging.getLogger(__name__)

args = {
    # hard coded date
    'start_date': datetime(2019, 1, 1),
    'owner': 'iychoi'
}

dag_cord = DAG(
    dag_id='simple_cord_workflow',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None
)

dag_cord.doc_md = __doc__


def ONU_event(model_accessor, message, **kwargs):
    log.info('onu.events: received an event - %s' % message)

    """
    si = find_or_create_cord_si(model_accessor, logging, message)
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
    """


def AUTH_event(model_accessor, message, **kwargs):
    log.info('authentication.events: received an event - %s' % message)

    """
    si = find_or_create_cord_si(model_accessor, logging, message)
    logging.debug('authentication.events: Updating service instance', si=si)
    si.authentication_state = message['authenticationState']
    si.save_changed_fields(always_update_timestamp=True)
    """


def DHCP_event(model_accessor, message, **kwargs):
    log.info('dhcp.events: received an event - %s' % message)

    """
    si = find_or_create_cord_si(model_accessor, logging, message)
    logging.debug('dhcp.events: Updating service instance', si=si)
    si.dhcp_state = message['messageType']
    si.ip_address = message['ipAddress']
    si.mac_address = message['macAddress']
    si.save_changed_fields(always_update_timestamp=True)
    """


def DriverService_event(model_accessor, message, **kwargs):
    log.info('model event: received an event - %s' % message)

    """
    event_type = message['event_type']

    go = False
    si = find_or_create_cord_si(model_accessor, logging, message)

    if event_type == 'create':
        logging.debug('MODEL_POLICY: handle_create for cordWorkflowDriverServiceInstance %s ' % si.id)
        go = True
    elif event_type == 'update':
        logging.debug('MODEL_POLICY: handle_update for cordWorkflowDriverServiceInstance %s ' %
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
    """


onu_event_sensor = CORDEventSensor(
    task_id='onu_event_sensor',
    topic='onu.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_cord
)

onu_event_handler = CORDModelOperator(
    task_id='onu_event_handler',
    python_callable=ONU_event,
    cord_event_sensor_task_id='onu_event_sensor',
    dag=dag_cord
)

auth_event_sensor = CORDEventSensor(
    task_id='auth_event_sensor',
    topic='authentication.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_cord
)

auth_event_handler = CORDModelOperator(
    task_id='auth_event_handler',
    python_callable=AUTH_event,
    cord_event_sensor_task_id='auth_event_sensor',
    dag=dag_cord
)

dhcp_event_sensor = CORDEventSensor(
    task_id='dhcp_event_sensor',
    topic='dhcp.events',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_cord
)

dhcp_event_handler = CORDModelOperator(
    task_id='dhcp_event_handler',
    python_callable=DHCP_event,
    cord_event_sensor_task_id='dhcp_event_sensor',
    dag=dag_cord
)

cord_model_event_sensor1 = CORDModelSensor(
    task_id='cord_model_event_sensor1',
    model_name='cordWorkflowDriverServiceInstance',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_cord
)

cord_model_event_handler1 = CORDModelOperator(
    task_id='cord_model_event_handler1',
    python_callable=DriverService_event,
    cord_event_sensor_task_id='cord_model_event_sensor1',
    dag=dag_cord
)

cord_model_event_sensor2 = CORDModelSensor(
    task_id='cord_model_event_sensor2',
    model_name='cordWorkflowDriverServiceInstance',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_cord
)

cord_model_event_handler2 = CORDModelOperator(
    task_id='cord_model_event_handler2',
    python_callable=DriverService_event,
    cord_event_sensor_task_id='cord_model_event_sensor2',
    dag=dag_cord
)

cord_model_event_sensor3 = CORDModelSensor(
    task_id='cord_model_event_sensor3',
    model_name='cordWorkflowDriverServiceInstance',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_cord
)

cord_model_event_handler3 = CORDModelOperator(
    task_id='cord_model_event_handler3',
    python_callable=DriverService_event,
    cord_event_sensor_task_id='cord_model_event_sensor3',
    dag=dag_cord
)


onu_event_sensor >> onu_event_handler >> cord_model_event_sensor1 >> cord_model_event_handler1 >> \
    auth_event_sensor >> auth_event_handler >> cord_model_event_sensor2 >> cord_model_event_handler2 >> \
    dhcp_event_sensor >> dhcp_event_handler >> cord_model_event_sensor3 >> cord_model_event_handler3
