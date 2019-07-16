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

import logging
from att_helpers import *

# Check the whitelist to see if the ONU is valid.  If it is, make sure that it's enabled.
def process_onu_state(model_accessor, si):
    [valid, message] = validate_onu(model_accessor, logging, si)
    si.status_message = message
    if valid:
        si.admin_onu_state = "ENABLED"
        update_onu(model_accessor, si.serial_number, "ENABLED")
    else:
        si.admin_onu_state = "DISABLED"
        update_onu(model_accessor, si.serial_number, "DISABLED")


# If the ONU has been disabled then we force re-authentication when it
# is re-enabled.
# Setting si.authentication_state = AWAITING:
#   -> subscriber status = "awaiting_auth"
#   -> service chain deleted
#   -> need authentication to restore connectivity after ONU enabled
def process_auth_state(si):
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


# The DhcpL2Relay ONOS app generates events that update the fields below.
# It only sends events when it processes DHCP packets.  It keeps no internal state.
# We reset dhcp_state when:
# si.authentication_state in ["AWAITING", "REQUESTED", "STARTED"]
#   -> subscriber status = "awaiting_auth"
#   -> service chain not present
#   -> subscriber's OLT flow rules, xconnect not present
#   -> DHCP packets won't go through
# Note, however, that the DHCP state at the endpoints is not changed.
# A previously issued DHCP lease may still be valid.
def process_dhcp_state(si):
    if si.authentication_state in ["AWAITING", "REQUESTED", "STARTED"]:
        si.ip_address = ""
        si.mac_address = ""
        si.dhcp_state = "AWAITING"


# Make sure the object is in a legitimate state
# It should be after the above processing steps
# However this might still fail if an event has fired in the meantime
# Valid states:
# ONU       | Auth     | DHCP
# ===============================
# AWAITING  | AWAITING | AWAITING
# ENABLED   | *        | AWAITING
# ENABLED   | APPROVED | *
# DISABLED  | AWAITING | AWAITING
def validate_states(si):
    if (si.admin_onu_state == "AWAITING" or si.admin_onu_state ==
            "DISABLED") and si.authentication_state == "AWAITING" and si.dhcp_state == "AWAITING":
        return
    if si.admin_onu_state == "ENABLED" and (si.authentication_state == "APPROVED" or si.dhcp_state == "AWAITING"):
        return
    logging.warning(
        "MODEL_POLICY (validate_states): invalid state combination",
        onu_state=si.admin_onu_state,
        auth_state=si.authentication_state,
        dhcp_state=si.dhcp_state)


def update_onu(model_accessor, serial_number, admin_state):
    onu = [onu for onu in model_accessor.ONUDevice.objects.all() if onu.serial_number.lower()
            == serial_number.lower()][0]
    if onu.admin_state == "ADMIN_DISABLED":
        logging.debug(
            "MODEL_POLICY: ONUDevice [%s] has been manually disabled, not changing state to %s" %
            (serial_number, admin_state))
        return
    if onu.admin_state == admin_state:
        logging.debug(
            "MODEL_POLICY: ONUDevice [%s] already has admin_state to %s" %
            (serial_number, admin_state))
    else:
        logging.debug("MODEL_POLICY: setting ONUDevice [%s] admin_state to %s" % (serial_number, admin_state))
        onu.admin_state = admin_state
        onu.save_changed_fields(always_update_timestamp=True)


def get_subscriber(model_accessor, serial_number):
    try:
        return [s for s in model_accessor.RCORDSubscriber.objects.all() if s.onu_device.lower()
                == serial_number.lower()][0]
    except IndexError:
        # If the subscriber doesn't exist we don't do anything
        logging.debug(
            "MODEL_POLICY: subscriber does not exists for this SI, doing nothing",
            onu_device=serial_number)
        return None


def update_subscriber_ip(model_accessor, subscriber, ip):
    # TODO check if the subscriber has an IP and update it,
    # or create a new one
    try:
        ip = model_accessor.RCORDIpAddress.objects.filter(
            subscriber_id=subscriber.id,
            ip=ip
        )[0]
        logging.debug("MODEL_POLICY: found existing RCORDIpAddress for subscriber",
                            onu_device=subscriber.onu_device, subscriber_status=subscriber.status, ip=ip)
        ip.save_changed_fields()
    except IndexError:
        logging.debug(
            "MODEL_POLICY: Creating new RCORDIpAddress for subscriber",
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
        logging.debug(
            "MODEL_POLICY: delete RCORDIpAddress for subscriber",
            onu_device=subscriber.onu_device,
            subscriber_status=subscriber.status,
            ip=ip)
        ip.delete()
    except BaseException:
        logging.warning("MODEL_POLICY: no RCORDIpAddress object found, cannot delete", ip=ip)


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
            logging.debug(
                "MODEL_POLICY: updating subscriber",
                onu_device=subscriber.onu_device,
                authentication_state=si.authentication_state,
                subscriber_status=subscriber.status)
            if subscriber.status == "awaiting-auth":
                delete_subscriber_ip(model_accessor, subscriber, si.ip_address)
                subscriber.mac_address = ""
            elif si.ip_address and si.mac_address:
                update_subscriber_ip(model_accessor, subscriber, si.ip_address)
                subscriber.mac_address = si.mac_address
            subscriber.save_changed_fields(always_update_timestamp=True)
        else:
            logging.debug("MODEL_POLICY: subscriber status has not changed", onu_device=subscriber.onu_device,
                              authentication_state=si.authentication_state, subscriber_status=subscriber.status)
