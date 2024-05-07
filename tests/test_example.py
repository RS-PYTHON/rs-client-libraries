# Copyright 2024 CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for example prefect flow"""
import pytest

from rs_workflows.example import hello_world, print_hello


@pytest.mark.unit
def test_print_hello(capsys):
    """Unit test for the print_hello prefect task function.

    This test validates the behavior of the print_hello function when printing a greeting message.

    Args:
        capsys: Pytest fixture for capturing stdout and stderr.

    Raises:
        AssertionError: If the printed message does not match the expected output.

    Returns:
        None: This test does not return any value.
    """

    print_hello.fn("Test")
    captured = capsys.readouterr()
    assert captured.out == "Hello Test!\n"


@pytest.mark.unit
def test_hello_world(capsys):
    """Unit test for the hello_world prefect flow function.

    This test validates the behavior of the hello_world function when printing multiple greeting messages.

    Args:
        capsys: Pytest fixture for capturing stdout and stderr.

    Raises:
        AssertionError: If the printed messages do not match the expected output.

    Returns:
        None: This test does not return any value.
    """
    hello_world(name="Test", tasks_number=2)
    captured = capsys.readouterr()
    assert captured.out == "Hello Test_0!\nHello Test_1!\n"
