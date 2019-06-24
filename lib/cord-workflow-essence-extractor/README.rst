CORD Workflow Essence Extractor
===============================

Extract workflow essence from Airflow workflow code. The essence information
will be later passed to Workflow Controller for control.

The essence extractor extracts following information from Airflow workflow code written in python:
- DAG information (DAG ID)
- Operators (class name, event and other parameters)
- Task dependencies (parents and children)

