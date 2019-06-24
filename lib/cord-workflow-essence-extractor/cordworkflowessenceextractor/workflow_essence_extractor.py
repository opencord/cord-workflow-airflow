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
Workflow Essence Extractor

This module extracts essence of airflow workflows
Following information will be extracted from workflow code
- DAG info
- Operator info
    - XOS-related operators
    - Airflow operators
- Dependency info
"""

import ast
import sys
import json
import os.path


def classname(cls):
    return cls.__class__.__name__


def jsonify_ast(node, level=0):
    fields = {}
    for k in node._fields:
        fields[k] = '...'
        v = getattr(node, k)
        if isinstance(v, ast.AST):
            if v._fields:
                fields[k] = jsonify_ast(v)
            else:
                fields[k] = classname(v)

        elif isinstance(v, list):
            fields[k] = []
            for e in v:
                fields[k].append(jsonify_ast(e))

        elif isinstance(v, str):
            fields[k] = v

        elif isinstance(v, int) or isinstance(v, float):
            fields[k] = v

        elif v is None:
            fields[k] = None

        else:
            fields[k] = 'unrecognized'

    ret = {
        classname(node): fields
    }
    return ret


def parse(code):
    lines = code.split("\n")
    if len(lines) == 1:
        if code.endswith(".py") and os.path.exists(code):
            return parse_codefile(code)
    return parse_code(code)


def parse_code(code):
    tree = ast.parse(code)
    return jsonify_ast(tree)


def parse_codefile(code_filepath):
    code = None
    with open(code_filepath, "r") as f:
        code = f.read()
    tree = ast.parse(code, code_filepath)
    return jsonify_ast(tree)


def pretty_print_json(j):
    dumps = json.dumps(j, sort_keys=True, indent=4, separators=(',', ': '))
    print(dumps)


def recursively_find_elements(tree, elem):
    """
    traverse AST and find elements
    """
    for e in tree:
        obj = None
        if isinstance(tree, list):
            obj = e
        elif isinstance(tree, dict):
            obj = tree[e]

        if e == elem:
            yield obj

        if obj and (isinstance(obj, list) or isinstance(obj, dict)):
            for y in recursively_find_elements(obj, elem):
                yield y


def extract_func_calls(tree, func_name):
    """
    extract function calls with assignment
    """
    assigns = recursively_find_elements(tree, "Assign")
    if assigns:
        for assign in assigns:
            found = False

            calls = recursively_find_elements(assign, "Call")
            if calls:
                for call in calls:
                    funcs = recursively_find_elements(call, "func")
                    if funcs:
                        for func in funcs:
                            if "Name" in func:
                                name = func["Name"]
                                if "ctx" in name and "id" in name:
                                    # found function
                                    if name["id"] == func_name:
                                        found = True

            if found:
                yield assign


def extract_func_calls_airflow_operators(tree):
    """
    extract only airflow operators which end with "*Operator" or "*Sensor"
    """
    assigns = recursively_find_elements(tree, "Assign")
    if assigns:
        for assign in assigns:
            found = False

            calls = recursively_find_elements(assign, "Call")
            if calls:
                for call in calls:
                    funcs = recursively_find_elements(call, "func")
                    if funcs:
                        for func in funcs:
                            if "Name" in func:
                                name = func["Name"]
                                if "ctx" in name and "id" in name:
                                    # found function
                                    if name["id"].endswith(("Operator", "Sensor")):
                                        found = True

            if found:
                yield assign


def extract_bin_op(tree, op_name):
    """
    extract binary operation such as >>, <<
    """
    ops = recursively_find_elements(tree, "BinOp")
    if ops:
        for op in ops:
            if op["op"] == op_name:
                yield op


def take_string_or_tree(tree):
    if "Str" in tree:
        return tree["Str"]["s"]
    return tree


def take_num_or_tree(tree):
    if "Num" in tree:
        return tree["Num"]["n"]
    return tree


def take_id_or_tree(tree):
    if "Name" in tree:
        return tree["Name"]["id"]
    return tree


def take_name_constant_or_tree(tree):
    if "NameConstant" in tree:
        return tree["NameConstant"]["value"]
    return tree


def take_value_or_tree(tree):
    if "Str" in tree:
        return tree["Str"]["s"]
    elif "Num" in tree:
        return tree["Num"]["n"]
    elif "Name" in tree:
        val = tree["Name"]["id"]
        if val in ["True", "False"]:
            return bool(val)
        elif val == "None":
            return None
        return val
    elif "NameConstant" in tree:
        val = tree["NameConstant"]["value"]
        if val in ["True", "False"]:
            return bool(val)
        elif val == "None":
            return None
        return val
    elif "List" in tree:
        vals = []
        if "elts" in tree["List"]:
            elts = tree["List"]["elts"]
            for elt in elts:
                val = take_value_or_tree(elt)
                vals.append(val)
        return vals
    return tree


def make_dag(tree):
    loc_val = None
    dag_id = None

    if "targets" in tree:
        targets = tree["targets"]
        loc_val = take_id_or_tree(targets[0])

    if "value" in tree:
        value = tree["value"]
        if "Call" in value:
            call = value["Call"]
            if "keywords" in call:
                keywords = call["keywords"]
                for keyword in keywords:
                    if "keyword" in keyword:
                        k = keyword["keyword"]
                        if k["arg"] == "dag_id":
                            dag_id = take_string_or_tree(k["value"])

    return {
        'local_variable': loc_val,
        'dag_id': dag_id
    }


def make_airflow_operator(tree):
    airflow_operator = {}

    if "targets" in tree:
        targets = tree["targets"]
        loc_val = take_id_or_tree(targets[0])
        airflow_operator["local_variable"] = loc_val

    if "value" in tree:
        value = tree["value"]
        if "Call" in value:
            call = value["Call"]
            if "func" in call:
                class_name = take_id_or_tree(call["func"])
                airflow_operator["class"] = class_name

            if "keywords" in call:
                keywords = call["keywords"]
                for keyword in keywords:
                    if "keyword" in keyword:
                        k = keyword["keyword"]
                        arg = k["arg"]
                        airflow_operator[arg] = take_value_or_tree(k["value"])

    return airflow_operator


def make_dependencies_bin_op(tree, dependencies):
    children = []
    parents = []
    child = None
    parent = None

    if tree["op"] == "RShift":
        child = take_id_or_tree(tree["right"])
        parent = take_id_or_tree(tree["left"])
    elif tree["op"] == "LShift":
        child = take_id_or_tree(tree["left"])
        parent = take_id_or_tree(tree["right"])

    if child:
        if isinstance(child, dict):
            if "List" in child:
                for c in child["List"]["elts"]:
                    children.append(take_id_or_tree(c))
            elif "BinOp" in child:
                deps = make_dependencies_bin_op(child["BinOp"], dependencies)
                for dep in deps:
                    children.append(dep)
            else:
                children.append(take_id_or_tree(child))
        else:
            children.append(child)

    if parent:
        if isinstance(parent, dict):
            if "List" in parent:
                for p in parent["List"]["elts"]:
                    parents.append(take_id_or_tree(p))
            elif "BinOp" in parent:
                deps = make_dependencies_bin_op(parent["BinOp"], dependencies)
                for dep in deps:
                    parents.append(dep)
            else:
                parents.append(take_id_or_tree(parent))
        else:
            parents.append(parent)

    if len(parents) > 0 and len(children) > 0:
        # make all-vs-all combinations
        for p in parents:
            for c in children:
                dep = {
                    'parent': p,
                    'child': c
                }
                dependencies.append(dep)

    if tree["op"] == "RShift":
        return children
    elif tree["op"] == "LShift":
        return parents
    return children


def extract_dep_operations(tree):
    # extract dependency definition using ">>"
    ops = extract_bin_op(tree, "RShift")
    if ops:
        for op in ops:
            deps = []
            make_dependencies_bin_op(op, deps)
            for dep in deps:
                yield dep

    # extract dependency definition using "<<"
    ops = extract_bin_op(tree, "LShift")
    if ops:
        for op in ops:
            deps = []
            make_dependencies_bin_op(op, deps)
            for dep in deps:
                yield dep


def extract_dags(tree):
    dags = {}
    calls = extract_func_calls(tree, "DAG")
    if calls:
        for call in calls:
            dag = make_dag(call)
            dagid = dag["dag_id"]
            dags[dagid] = dag
    return dags


def extract_XOS_event_sensors(tree):
    operators = {}
    calls = extract_func_calls(tree, "XOSEventSensor")
    if calls:
        for call in calls:
            operator = make_airflow_operator(call)
            operatorid = operator["task_id"]
            operators[operatorid] = operator
    return operators


def extract_XOS_model_sensors(tree):
    operators = {}
    calls = extract_func_calls(tree, "XOSModelSensor")
    if calls:
        for call in calls:
            operator = make_airflow_operator(call)
            operatorid = operator["task_id"]
            operators[operatorid] = operator
    return operators


def extract_airflow_operators(tree):
    operators = {}
    calls = extract_func_calls_airflow_operators(tree)
    if calls:
        for call in calls:
            operator = make_airflow_operator(call)
            operatorid = operator["task_id"]
            operators[operatorid] = operator
    return operators


def extract_all_operators(tree):
    operators = {}
    event_sensors = extract_XOS_event_sensors(tree)
    if event_sensors:
        for event_sensor in event_sensors:
            operators[event_sensor] = event_sensors[event_sensor]

    model_sensors = extract_XOS_model_sensors(tree)
    if model_sensors:
        for model_sensor in model_sensors:
            operators[model_sensor] = model_sensors[model_sensor]

    airflow_operators = extract_airflow_operators(tree)
    if airflow_operators:
        for airflow_operator in airflow_operators:
            operators[airflow_operator] = airflow_operators[airflow_operator]

    return operators


def extract_dependencies(tree):
    """
    Build N-N dependencies from fragmented parent-child relations
    A node can have multiple parents and multiple children
    """
    dependencies = {}
    ops = extract_dep_operations(tree)
    if ops:
        for op in ops:
            p = op["parent"]
            c = op["child"]

            if p in dependencies:
                # append to an existing list
                node_p = dependencies[p]
                if "children" in node_p:
                    # prevent duplicates
                    if c not in node_p["children"]:
                        node_p["children"].append(c)
                else:
                    node_p["children"] = [c]
            else:
                # create a new
                node_p = {
                    'children': [c]
                }
                dependencies[p] = node_p

            if c in dependencies:
                # append to an existing list
                node_c = dependencies[c]
                if "parents" in node_c:
                    # prevent duplicates
                    if p not in node_c["parents"]:
                        node_c["parents"].append(p)
                else:
                    node_c["parents"] = [p]
            else:
                # create a new
                node_c = {
                    'parents': [p]
                }
                dependencies[c] = node_c

    return dependencies


def extract_all(tree):
    """
    Build highlevel information of workflows dag, operators and dependencies refers to each other
    """
    dags = extract_dags(tree)
    operators = extract_all_operators(tree)
    dependencies = extract_dependencies(tree)

    dag_dict = {}
    for dag_id in dags:
        dag = dags[dag_id]
        dag_var = dag["local_variable"]

        # filter operators that do not belong to the dag
        my_operators = {}
        my_operators_var = {}
        for task_id in operators:
            operator = operators[task_id]
            if operator["dag"] == dag_var:
                # set dag_id
                operator["dag_id"] = dag_id
                my_operators[task_id] = operator

                # this is to help fast search while working with dependencies
                operator_local_var = operator["local_variable"]
                my_operators_var[operator_local_var] = operator

        # filter dependencies that do not belong to the dag
        my_dependencies = {}
        for task_var in dependencies:
            if task_var in my_operators_var:
                dependency = dependencies[task_var]
                task_id = my_operators_var[task_var]["task_id"]

                # convert dependency task_var to task_id
                dep = {}
                if "children" in dependency:
                    dep["children"] = []
                    for child in dependency["children"]:
                        if child in my_operators_var:
                            child_task_id = my_operators_var[child]["task_id"]
                            dep["children"].append(child_task_id)

                if "parents" in dependency:
                    dep["parents"] = []
                    for parent in dependency["parents"]:
                        if parent in my_operators_var:
                            parent_task_id = my_operators_var[parent]["task_id"]
                            dep["parents"].append(parent_task_id)

                my_dependencies[task_id] = dep

        d = {
            'dag': dag,
            'tasks': my_operators,
            'dependencies': my_dependencies
        }
        dag_dict[dag_id] = d

    return dag_dict


# for command-line execution
def main(argv):
    if len(argv) < 1:
        sys.exit("Error: Need a filepath")

    code_filepath = argv[0]

    tree = parse_codefile(code_filepath)
    all = extract_all(tree)
    pretty_print_json(all)


if __name__ == "__main__":
    main(sys.argv[1:])
