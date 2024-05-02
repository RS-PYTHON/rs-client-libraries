"""Prefect flow example for rs-client-libraries"""
import pytest
from prefect import flow, task


@task
def print_hello(name: str):
    """Example task to be used in a prefect flow

    This prefect task may be used as start point in creating your own prefect tasks

    Args:
        name (str): Username to be printed

    Returns:
        None

    Raises:
        None
    """
    print(f"Hello {name}!")


@flow(name="Hello RS-Server flow ")
def hello_world(name="COPERNICUS", tasks_number=2):
    """Example flow that can be use in a COPERNICUS chain

    This prefect flow may be used as start point in creating your own prefect flows. It runs in parallel
    tasks_number of print_hello

    Args:
        name (str): Username to be printed. Default COPERNICUS
        tasks_number (int): Number of tasks to be run. Default 2

    Returns:
        None

    Raises:
        None
    """
    for idx in range(0, tasks_number):
        print_hello(name + "_" + str(idx))


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
