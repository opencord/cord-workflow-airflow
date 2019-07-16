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
import json
import os.path
import argparse
import pyfiglet

from multistructlog import create_logger


progargs = {
    'logging': None
}

DEFAULT_CONFIG_FILE_PATH = '/etc/cord_workflow_airflow_extensions/config.json'


class NoopLogger(object):
    def __init__(self):
        pass

    def info(self, *args):
        pass

    def debug(self, *args):
        pass

    def error(self, *args):
        pass

    def warn(self, *args):
        pass


class EssenceExtractor(object):
    def __init__(self, logger=None):
        if logger:
            self.logger = logger
        else:
            self.logger = NoopLogger()

        self.tree = None

    def set_logger(self, logger):
        self.logger = logger

    def get_logger(self):
        return self.logger

    def get_ast(self):
        return self.tree

    def parse_code(self, code):
        tree = ast.parse(code)
        self.tree = self.__jsonify_ast(tree)

    def parse_codefile(self, filepath):
        code = None
        with open(filepath, "r") as f:
            code = f.read()
            tree = ast.parse(code, filepath)
            self.tree = self.__jsonify_ast(tree)

    def __classname(self, cls):
        return cls.__class__.__name__

    def __jsonify_ast(self, node, level=0):
        fields = {}
        for k in node._fields:
            fields[k] = '...'
            v = getattr(node, k)
            if isinstance(v, ast.AST):
                if v._fields:
                    fields[k] = self.__jsonify_ast(v)
                else:
                    fields[k] = self.__classname(v)

            elif isinstance(v, list):
                fields[k] = []
                for e in v:
                    fields[k].append(self.__jsonify_ast(e))

            elif isinstance(v, str):
                fields[k] = v

            elif isinstance(v, int) or isinstance(v, float):
                fields[k] = v

            elif v is None:
                fields[k] = None

            else:
                fields[k] = 'unrecognized'

        ret = {
            self.__classname(node): fields
        }
        return ret

    def __recursively_find_elements(self, tree, elem):
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
                for y in self.__recursively_find_elements(obj, elem):
                    yield y

    def __extract_func_calls(self, tree, func_name):
        """
        extract function calls with assignment
        """
        assigns = self.__recursively_find_elements(tree, "Assign")
        if assigns:
            for assign in assigns:
                found = False

                calls = self.__recursively_find_elements(assign, "Call")
                if calls:
                    for call in calls:
                        funcs = self.__recursively_find_elements(call, "func")
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

    def __extract_func_calls_airflow_operators(self, tree):
        """
        extract only airflow operators which end with "*Operator" or "*Sensor"
        """
        assigns = self.__recursively_find_elements(tree, "Assign")
        if assigns:
            for assign in assigns:
                found = False

                calls = self.__recursively_find_elements(assign, "Call")
                if calls:
                    for call in calls:
                        funcs = self.__recursively_find_elements(call, "func")
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

    def __extract_bin_op(self, tree, op_name):
        """
        extract binary operation such as >>, <<
        """
        ops = self.__recursively_find_elements(tree, "BinOp")
        if ops:
            for op in ops:
                if op["op"] == op_name:
                    yield op

    def __take_string_or_tree(self, tree):
        if "Str" in tree:
            return tree["Str"]["s"]
        return tree

    def __take_num_or_tree(self, tree):
        if "Num" in tree:
            return tree["Num"]["n"]
        return tree

    def __take_id_or_tree(self, tree):
        if "Name" in tree:
            return tree["Name"]["id"]
        return tree

    def __take_name_constant_or_tree(self, tree):
        if "NameConstant" in tree:
            return tree["NameConstant"]["value"]
        return tree

    def __take_value_or_tree(self, tree):
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
                    val = self.__take_value_or_tree(elt)
                    vals.append(val)
            return vals
        return tree

    def __make_dag(self, tree):
        loc_val = None
        dag_id = None

        if "targets" in tree:
            targets = tree["targets"]
            loc_val = self.__take_id_or_tree(targets[0])

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
                                dag_id = self.__take_string_or_tree(k["value"])

        return {
            'local_variable': loc_val,
            'dag_id': dag_id
        }

    def __make_airflow_operator(self, tree):
        airflow_operator = {}

        if "targets" in tree:
            targets = tree["targets"]
            loc_val = self.__take_id_or_tree(targets[0])
            airflow_operator["local_variable"] = loc_val

        if "value" in tree:
            value = tree["value"]
            if "Call" in value:
                call = value["Call"]
                if "func" in call:
                    class_name = self.__take_id_or_tree(call["func"])
                    airflow_operator["class"] = class_name

                if "keywords" in call:
                    keywords = call["keywords"]
                    for keyword in keywords:
                        if "keyword" in keyword:
                            k = keyword["keyword"]
                            arg = k["arg"]
                            airflow_operator[arg] = self.__take_value_or_tree(k["value"])

        return airflow_operator

    def __make_dependencies_bin_op(self, tree, dependencies):
        children = []
        parents = []
        child = None
        parent = None

        if tree["op"] == "RShift":
            child = self.__take_id_or_tree(tree["right"])
            parent = self.__take_id_or_tree(tree["left"])
        elif tree["op"] == "LShift":
            child = self.__take_id_or_tree(tree["left"])
            parent = self.__take_id_or_tree(tree["right"])

        if child:
            if isinstance(child, dict):
                if "List" in child:
                    for c in child["List"]["elts"]:
                        children.append(self.__take_id_or_tree(c))
                elif "BinOp" in child:
                    deps = self.__make_dependencies_bin_op(child["BinOp"], dependencies)
                    for dep in deps:
                        children.append(dep)
                else:
                    children.append(self.__take_id_or_tree(child))
            else:
                children.append(child)

        if parent:
            if isinstance(parent, dict):
                if "List" in parent:
                    for p in parent["List"]["elts"]:
                        parents.append(self.__take_id_or_tree(p))
                elif "BinOp" in parent:
                    deps = self.__make_dependencies_bin_op(parent["BinOp"], dependencies)
                    for dep in deps:
                        parents.append(dep)
                else:
                    parents.append(self.__take_id_or_tree(parent))
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

    def __extract_dep_operations(self, tree):
        # extract dependency definition using ">>"
        ops = self.__extract_bin_op(tree, "RShift")
        if ops:
            for op in ops:
                deps = []
                self.__make_dependencies_bin_op(op, deps)
                for dep in deps:
                    yield dep

        # extract dependency definition using "<<"
        ops = self.__extract_bin_op(tree, "LShift")
        if ops:
            for op in ops:
                deps = []
                self.__make_dependencies_bin_op(op, deps)
                for dep in deps:
                    yield dep

    def __extract_dags(self, tree):
        dags = {}
        calls = self.__extract_func_calls(tree, "DAG")
        if calls:
            for call in calls:
                dag = self.__make_dag(call)
                dagid = dag["dag_id"]
                dags[dagid] = dag
        return dags

    def __extract_XOS_event_sensors(self, tree):
        operators = {}
        calls = self.__extract_func_calls(tree, "XOSEventSensor")
        if calls:
            for call in calls:
                operator = self.__make_airflow_operator(call)
                operatorid = operator["task_id"]
                operators[operatorid] = operator
        return operators

    def __extract_XOS_model_sensors(self, tree):
        operators = {}
        calls = self.__extract_func_calls(tree, "XOSModelSensor")
        if calls:
            for call in calls:
                operator = self.__make_airflow_operator(call)
                operatorid = operator["task_id"]
                operators[operatorid] = operator
        return operators

    def __extract_airflow_operators(self, tree):
        operators = {}
        calls = self.__extract_func_calls_airflow_operators(tree)
        if calls:
            for call in calls:
                operator = self.__make_airflow_operator(call)
                operatorid = operator["task_id"]
                operators[operatorid] = operator
        return operators

    def __extract_all_operators(self, tree):
        operators = {}
        event_sensors = self.__extract_XOS_event_sensors(tree)
        if event_sensors:
            for event_sensor in event_sensors:
                operators[event_sensor] = event_sensors[event_sensor]

        model_sensors = self.__extract_XOS_model_sensors(tree)
        if model_sensors:
            for model_sensor in model_sensors:
                operators[model_sensor] = model_sensors[model_sensor]

        airflow_operators = self.__extract_airflow_operators(tree)
        if airflow_operators:
            for airflow_operator in airflow_operators:
                operators[airflow_operator] = airflow_operators[airflow_operator]

        return operators

    def __extract_dependencies(self, tree):
        """
        Build N-N dependencies from fragmented parent-child relations
        A node can have multiple parents and multiple children
        """
        dependencies = {}
        ops = self.__extract_dep_operations(tree)
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

    def extract(self):
        """
        Build highlevel information of workflows dag, operators and dependencies refers to each other
        """
        if self.tree:
            dags = self.__extract_dags(self.tree)
            operators = self.__extract_all_operators(self.tree)
            dependencies = self.__extract_dependencies(self.tree)

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
        else:
            return None


"""
Command-line tool
"""


def print_graffiti():
    result = pyfiglet.figlet_format("CORD\nWorkflow\nEssence\nExtractor", font="graffiti")
    print(result)


def get_arg_parser():
    parser = argparse.ArgumentParser(description='CORD Workflow Essence Extractor.', prog='essence_extractor')
    parser.add_argument('--config', help='locate a configuration file')
    parser.add_argument('-o', '--output', help='output file path')
    parser.add_argument('-c', '--stdout', action='store_true', help='output to console (STDOUT)')
    parser.add_argument('input_file', help='input airflow dag source file')
    return parser


def read_config(path):
    if os.path.exists(path):
        with open(path) as json_config_file:
            data = json.load(json_config_file)
            return data
    return {}


def pretty_format_json(j):
    dumps = json.dumps(j, sort_keys=True, indent=4, separators=(',', ': '))
    return dumps


# for command-line execution
def main(args):
    print_graffiti()

    # check if config path is set
    config_file_path = DEFAULT_CONFIG_FILE_PATH
    if args.config:
        config_file_path = args.config

    if os.path.exists(config_file_path):
        # read config
        config = read_config(config_file_path)
        if config:
            global progargs
            for k in progargs:
                # overwrite
                progargs[k] = config[k]

    log = create_logger(progargs["logging"])

    code_filepath = args.input_file
    if os.path.exists(code_filepath):
        raise 'cannot find an input file - %s' % code_filepath

    extractor = EssenceExtractor(logger=log)
    extractor.parse_codefile(code_filepath)
    essence = extractor.extract()

    output_filepath = './essence.json'
    if args.output:
        output_filepath = args.output

    json_string = pretty_format_json(essence)
    if args.stdout or output_filepath == '-':
        print(json_string)
    else:
        with open(output_filepath, 'w') as f:
            f.write(json_string)


if __name__ == "__main__":
    parser = get_arg_parser()
    args = parser.parse_args()
    main(args)
