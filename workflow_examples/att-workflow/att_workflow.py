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
from datetime import datetime
from airflow import DAG
from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.cord_workflow_plugin import CORDEventSensor, CORDModelSensor
from airflow.operators.cord_workflow_plugin import CORDModelOperator
from xossynchronizer.steps.syncstep import DeferredException

log = logging.getLogger(__name__)
args = {
    # hard coded date
    'start_date': datetime(2019, 1, 1),
    'owner': 'ATT'
}

dag_att = DAG(
    dag_id='att_workflow',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None
)
dag_att.doc_md = __doc__

dag_att_admin = DAG(
    dag_id='att_admin_workflow',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None
)
dag_att_admin.doc_md = __doc__

def find_or_create_att_si(model_accessor, event):
    try:
        att_si = model_accessor.AttWorkflowDriverServiceInstance.objects.get(
            serial_number=event["serialNumber"]
        )
        log.debug("Found existing AttWorkflowDriverServiceInstance - si = %s" % att_si)
    except IndexError:
        # create an AttWorkflowDriverServiceInstance, the validation will be
        # triggered in the corresponding sync step
        att_si = model_accessor.AttWorkflowDriverServiceInstance(
            serial_number=event["serialNumber"],
            of_dpid=event["deviceId"],
            uni_port_id=long(event["portNumber"]),
            # we assume there is only one AttWorkflowDriverService
            owner=model_accessor.AttWorkflowDriverService.objects.first()
        )
        log.debug("Created new AttWorkflowDriverServiceInstance - si = %s" % att_si)
    return att_si


def validate_onu(model_accessor, si):
    """
    This method validate an ONU against the whitelist and set the appropriate state.
    It's expected that the deferred exception is managed in the caller method,
    for example a model_policy or a sync_step.

    :param si: AttWorkflowDriverServiceInstance
    :return: [boolean, string]
    """

    oss_service = si.owner.leaf_model

    # See if there is a matching entry in the whitelist.
    matching_entries = model_accessor.AttWorkflowDriverWhiteListEntry.objects.filter(
        owner_id=oss_service.id,
    )
    matching_entries = [e for e in matching_entries if e.serial_number.lower() == si.serial_number.lower()]

    if len(matching_entries) == 0:
        log.warn("ONU not found in whitelist - serial_number = %s" % si.serial_number)
        return [False, "ONU not found in whitelist"]

    whitelisted = matching_entries[0]
    try:
        onu = model_accessor.ONUDevice.objects.get(serial_number=si.serial_number)
        pon_port = onu.pon_port
    except IndexError:
        raise DeferredException("ONU device %s is not know to XOS yet" % si.serial_number)

    if onu.admin_state == "ADMIN_DISABLED":
        return [False, "ONU has been manually disabled"]

    if pon_port.port_no != whitelisted.pon_port_id or si.of_dpid != whitelisted.device_id:
        log.warn("ONU disable as location don't match - serial_number = %s, device_id= %s" % (si.serial_number, si.of_dpid))
        return [False, "ONU activated in wrong location"]

    return [True, "ONU has been validated"]


def update_onu(model_accessor, serial_number, admin_state):
    onu = [onu for onu in model_accessor.ONUDevice.objects.all() if onu.serial_number.lower()
            == serial_number.lower()][0]
    if onu.admin_state == "ADMIN_DISABLED":
        log.debug(
            "MODEL_POLICY: ONUDevice [%s] has been manually disabled, not changing state to %s" %
            (serial_number, admin_state))
        return
    if onu.admin_state == admin_state:
        log.debug(
            "MODEL_POLICY: ONUDevice [%s] already has admin_state to %s" %
            (serial_number, admin_state))
    else:
        log.debug(
            "MODEL_POLICY: setting ONUDevice [%s] admin_state to %s" %
            (serial_number, admin_state))
        onu.admin_state = admin_state
        onu.save_changed_fields(always_update_timestamp=True)


def process_onu_state(model_accessor, si):
    """
    Check the whitelist to see if the ONU is valid.  If it is, make sure that it's enabled.
    """

    [valid, message] = validate_onu(model_accessor, si)
    si.status_message = message
    if valid:
        si.admin_onu_state = "ENABLED"
        update_onu(model_accessor, si.serial_number, "ENABLED")
    else:
        si.admin_onu_state = "DISABLED"
        update_onu(model_accessor, si.serial_number, "DISABLED")


def process_auth_state(si):
    """
    If the ONU has been disabled then we force re-authentication when it
    is re-enabled.
    Setting si.authentication_state = AWAITING:
        -> subscriber status = "awaiting_auth"
        -> service chain deleted
        -> need authentication to restore connectivity after ONU enabled
    """

    auth_msgs = {
        "AWAITING": " - Awaiting Authentication",
        "REQUESTED": " - Authentication requested",
        "STARTED": " - Authentication started",
        "APPROVED": " - Authentication succeeded",
        "DENIED": " - Authentication denied"
    }
    if si.admin_onu_state == "DISABLED" or si.oper_onu_status == "DISABLED":
        si.authentication_state = "AWAITING"
    else:
        si.status_message += auth_msgs[si.authentication_state]


def process_dhcp_state(si):
    """
    The DhcpL2Relay ONOS app generates events that update the fields below.
    It only sends events when it processes DHCP packets.  It keeps no internal state.
    We reset dhcp_state when:
    si.authentication_state in ["AWAITING", "REQUESTED", "STARTED"]
        -> subscriber status = "awaiting_auth"
        -> service chain not present
        -> subscriber's OLT flow rules, xconnect not present
        -> DHCP packets won't go through
    Note, however, that the DHCP state at the endpoints is not changed.
    A previously issued DHCP lease may still be valid.
    """

    if si.authentication_state in ["AWAITING", "REQUESTED", "STARTED"]:
        si.ip_address = ""
        si.mac_address = ""
        si.dhcp_state = "AWAITING"


def validate_states(si):
    """
    Make sure the object is in a legitimate state
    It should be after the above processing steps
    However this might still fail if an event has fired in the meantime
    Valid states:
    ONU       | Auth     | DHCP
    ===============================
    AWAITING  | AWAITING | AWAITING
    ENABLED   | *        | AWAITING
    ENABLED   | APPROVED | *
    DISABLED  | AWAITING | AWAITING
    """

    if (si.admin_onu_state == "AWAITING" or si.admin_onu_state == "DISABLED") and \
        si.authentication_state == "AWAITING" and si.dhcp_state == "AWAITING":
        return

    if si.admin_onu_state == "ENABLED" and \
        (si.authentication_state == "APPROVED" or si.dhcp_state == "AWAITING"):
        return

    log.warning(
        "validate_states: invalid state combination - onu_state = %s, \
        auth_state = %s, dhcp_state = %s" %
        (si.admin_onu_state, si.authentication_state, si.dhcp_state)
    )


def get_subscriber(model_accessor, serial_number):
    try:
        return [s for s in model_accessor.RCORDSubscriber.objects.all() if s.onu_device.lower()
                == serial_number.lower()][0]
    except IndexError:
        # If the subscriber doesn't exist we don't do anything
        log.debug(
            "subscriber does not exists for this SI, doing nothing - onu_device = %s" %
            serial_number
        )
        return None


def update_subscriber_ip(model_accessor, subscriber, ip):
    # TODO check if the subscriber has an IP and update it,
    # or create a new one
    try:
        ip = model_accessor.RCORDIpAddress.objects.filter(
            subscriber_id=subscriber.id,
            ip=ip
        )[0]
        log.debug(
            "found existing RCORDIpAddress for subscriber",
            onu_device=subscriber.onu_device,
            subscriber_status=subscriber.status,
            ip=ip
        )
        ip.save_changed_fields()
    except IndexError:
        log.debug(
            "Creating new RCORDIpAddress for subscriber",
            onu_device=subscriber.onu_device,
            subscriber_status=subscriber.status,
            ip=ip)
        ip = model_accessor.RCORDIpAddress(
            subscriber_id=subscriber.id,
            ip=ip,
            description="DHCP Assigned IP Address"
        )
        ip.save()


def delete_subscriber_ip(model_accessor, subscriber, ip):
    try:
        ip = model_accessor.RCORDIpAddress.objects.filter(
            subscriber_id=subscriber.id,
            ip=ip
        )[0]
        log.debug(
            "MODEL_POLICY: delete RCORDIpAddress for subscriber",
            onu_device=subscriber.onu_device,
            subscriber_status=subscriber.status,
            ip=ip)
        ip.delete()
    except BaseException:
        log.warning("MODEL_POLICY: no RCORDIpAddress object found, cannot delete", ip=ip)


def update_subscriber(model_accessor, subscriber, si):
    cur_status = subscriber.status
    # Don't change state if someone has disabled the subscriber
    if subscriber.status != "disabled":
        if si.authentication_state in ["AWAITING", "REQUESTED", "STARTED"]:
            subscriber.status = "awaiting-auth"
        elif si.authentication_state == "APPROVED":
            subscriber.status = "enabled"
        elif si.authentication_state == "DENIED":
            subscriber.status = "auth-failed"

        # NOTE we save the subscriber only if:
        # - the status has changed
        # - we get a DHCPACK event
        if cur_status != subscriber.status or si.dhcp_state == "DHCPACK":
            log.debug(
                "updating subscriber",
                onu_device=subscriber.onu_device,
                authentication_state=si.authentication_state,
                subscriber_status=subscriber.status
            )

            if subscriber.status == "awaiting-auth":
                delete_subscriber_ip(model_accessor, subscriber, si.ip_address)
                subscriber.mac_address = ""
            elif si.ip_address and si.mac_address:
                update_subscriber_ip(model_accessor, subscriber, si.ip_address)
                subscriber.mac_address = si.mac_address
            subscriber.save_changed_fields(always_update_timestamp=True)
        else:
            log.debug(
                "subscriber status has not changed",
                onu_device=subscriber.onu_device,
                authentication_state=si.authentication_state,
                subscriber_status=subscriber.status
            )


def update_model(model_accessor, si):
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

    si.save_changed_fields(always_update_timestamp=True)


def on_onu_event(model_accessor, message, **kwargs):
    log.info('onu.events: received an event - message = %s' % message)

    si = find_or_create_att_si(model_accessor, message)
    if message['status'] == 'activated':
        log.info('onu.events: activated onu')
        si.no_sync = False
        si.uni_port_id = long(message['portNumber'])
        si.of_dpid = message['deviceId']
        si.oper_onu_status = 'ENABLED'
    elif message['status'] == 'disabled':
        log.info('onu.events: disabled onu, resetting the subscriber')
        si.oper_onu_status = 'DISABLED'
    else:
        log.error('onu.events: Unknown status value: %s' % message['status'])
        raise AirflowException('onu.events: Unknown status value: %s' % message['status'])

    update_model(model_accessor, si)

def on_auth_event(model_accessor, message, **kwargs):
    log.info('authentication.events: received an event - message = %s' % message)

    si = find_or_create_att_si(model_accessor, message)
    log.debug('authentication.events: Updating service instance')
    si.authentication_state = message['authenticationState']
    update_model(model_accessor, si)


def on_dhcp_event(model_accessor, message, **kwargs):
    log.info('dhcp.events: received an event - message = %s' % message)

    si = find_or_create_att_si(model_accessor, message)
    log.debug('dhcp.events: Updating service instance')
    si.dhcp_state = message['messageType']
    si.ip_address = message['ipAddress']
    si.mac_address = message['macAddress']
    update_model(model_accessor, si)


def DriverService_event(model_accessor, message, **kwargs):
    log.info('model event: received an event - %s' % message)

    # handle only create & update events
    event_type = message['event_type']
    if event_type is None or event_type.lower() not in ['create', 'update']:
        log.error('can not handle an event type - %s' % event_type)
        return

    si = find_or_create_att_si(model_accessor, message)
    update_model(model_accessor, si)


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
    python_callable=on_onu_event,
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
    python_callable=on_auth_event,
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
    python_callable=on_dhcp_event,
    cord_event_sensor_task_id='dhcp_event_sensor',
    dag=dag_att
)

att_model_event_sensor = CORDModelSensor(
    task_id='att_model_event_sensor',
    model_name='AttWorkflowDriverServiceInstance',
    key_field='serialNumber',
    controller_conn_id='local_cord_controller',
    poke_interval=5,
    dag=dag_att_admin
)

att_model_event_handler = CORDModelOperator(
    task_id='att_model_event_handler',
    python_callable=DriverService_event,
    cord_event_sensor_task_id='att_model_event_sensor',
    dag=dag_att_admin
)

# handle standard flow
onu_event_sensor >> onu_event_handler >> auth_event_sensor >> auth_event_handler >> dhcp_event_sensor >> dhcp_event_handler

# handle admin flow
att_model_event_sensor >> att_model_event_handler

