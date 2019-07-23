#!/usr/bin/env bash

# Copyright Matthieu "Puckel_" Roisil (https://github.com/puckel)
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

POSTGRES_HOST="postgres"
POSTGRES_PORT=5432
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow"
POSTGRES_DB="airflow"

export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; fk = Fernet.generate_key().decode(); print(fk)")
echo "export AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY}" >> ~/.bashrc
export AIRFLOW__CORE__LOAD_EXAMPLES=False
echo "export AIRFLOW__CORE__LOAD_EXAMPLES=${AIRFLOW__CORE__LOAD_EXAMPLES}" >> ~/.bashrc
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
echo "export AIRFLOW__CORE__SQL_ALCHEMY_CONN=${AIRFLOW__CORE__SQL_ALCHEMY_CONN}" >> ~/.bashrc

TRY=20

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY"
    sleep 5
  done
}

wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

airflow initdb
airflow scheduler &
airflow webserver
