"""Prefect flow example for rs-server-libararies"""
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
