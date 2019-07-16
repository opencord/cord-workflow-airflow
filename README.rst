Airflow Extensions for CORD Workflow
====================================
This library provides airflow extensions and tools for CORD Workflow.
Three packages are provided as below:

Essence Extractor
-----------------
A tool to extract **essence** from Airflow workflow code. Extracted essence is
used to register the workflow to CORD Workflow Controller.

Workflow Kickstarter
--------------------
A daemon that monitors kickstart events from CORD Workflow Controller, and
instantiate Airflow Workflows.

Airflow Extensions
------------------
A set of Airflow extensions that provide Python library to access CORD data
models and Airflow operators/sensors.



Note
----
Installing Airflow 1.10 has an issue of PEP 517 when installing it with pip 19.1.
PEP 517 makes installing pendulum 1.4.4 which is required by Airflow failed.
Higher version of pendulum is not compatible to Airflow.