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

"""Prefect flow example for rs-client-libraries"""
from prefect import flow, task


@task
def print_hello(name: str):
    """Example task to be used in a prefect flow

    This prefect task may be used as start point in creating your own prefect tasks

    Args:
        name (str): Username to be printed
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
    """
    for idx in range(0, tasks_number):
        print_hello(name + "_" + str(idx))
