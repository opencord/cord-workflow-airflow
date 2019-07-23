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
Example workflow using Airflow
"""


import logging
from datetime import datetime
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

args = {
    # hard coded date
    'start_date': datetime(2019, 1, 1),
    'owner': 'iychoi'
}

dag = DAG(
    dag_id='simple_airflow_workflow',
    default_args=args,
    # this dag will be triggered by external systems
    schedule_interval=None
)

dag.doc_md = __doc__


def handler(ds, **kwargs):
    log.info('Handler is called!')
    print(ds)
    return


def check_http(response):
    content = response.text
    if len(content) > 0:
        log.info('the server responsed http content - %s' % content)
        return True

    log.info('the server did not respond')
    return False


sensor = HttpSensor(
    task_id='http_sensor',
    # 'http_default' goes to https://google.com
    http_conn_id='http_default',
    endpoint='',
    request_params={},
    response_check=check_http,
    poke_interval=5,
    dag=dag,
)

handler = PythonOperator(
    task_id='python_operator',
    provide_context=True,
    python_callable=handler,
    dag=dag
)


sensor >> handler
