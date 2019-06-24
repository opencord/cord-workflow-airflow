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

from __future__ import absolute_import
import unittest
import json
import cordworkflowessenceextractor.workflow_essence_extractor as extractor

import os
import collections

test_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
examples_dir = os.path.join(test_path, "workflow-examples")
extension_expected_result = ".expected.json"

try:
    basestring
except NameError:
    basestring = str


def convert(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        v = {}
        for item in data:
            v[convert(item)] = convert(data[item])
        return v
    elif isinstance(data, collections.Iterable):
        v = []
        for item in data:
            v.append(convert(item))
        return v
    else:
        return data


class TestParse(unittest.TestCase):

    """
    Try parse all examples under workflow-examples dir.
    Then compares results with expected solution.
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def isDagFile(self, filepath):
        _, file_extension = os.path.splitext(filepath)
        if file_extension == ".py":
            return True
        return False

    def test_parse(self):
        dags = [f for f in os.listdir(examples_dir) if self.isDagFile(f)]

        for dag in dags:
            dag_path = os.path.join(examples_dir, dag)
            tree = extractor.parse_codefile(dag_path)
            workflow_info = extractor.extract_all(tree)

            # check if its expected solution fil
            expected_result_file = dag_path + extension_expected_result
            self.assertTrue(os.path.exists(expected_result_file))

            # compare content
            with open(dag_path + extension_expected_result) as json_file:
                # this builds a dict with unicode strings
                expected_workflow_info_uni = json.load(json_file)
                expected_workflow_info = convert(expected_workflow_info_uni)
                if workflow_info != expected_workflow_info:
                    print("Expected")
                    print(expected_workflow_info)

                    print("We got")
                    print(workflow_info)
                    self.fail("produced result is different")


if __name__ == "__main__":
    unittest.main()
