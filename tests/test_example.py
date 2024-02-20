"""Example for running a prefect flow example"""
import pytest

from rs_workflows.example import hello_world


@pytest.mark.unit
def test_hello_world():
    """Main function

    This script executes the hello_world prefect flow
    from the rs_workflows.example module when the script is run directly as the main program.
    """
    hello_world()
