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

from cord_workflow_airflow_extensions.workflow_ctl import register_workflow


class TestWorkflowCtl(unittest.TestCase):
    """
    Check if some private functions work.
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testRegisterWorkflow(self):
        failed = False
        try:
            register_workflow(None)
        except BaseException:
            failed = True

        self.assertTrue(failed, 'invalid args should fail')


if __name__ == "__main__":
    unittest.main()
