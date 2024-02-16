"""Docstring here"""
from prefect import Flow, Task
from unittest.mock import patch
import pytest
from rs_workflows.example import (
    print_hello, 
    hello_world
)

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
    print("captured = {}".format(captured))
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
    print("captured = {}".format(captured))
    assert captured.out == "Hello Test_0!\nHello Test_1!\n"
        