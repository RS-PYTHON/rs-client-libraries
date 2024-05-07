"""All tests for the Stac Client."""

from rs_client.stac_client import StacClient


def test_create_object_stac_client():  # pylint: disable=missing-function-docstring
    catalog = StacClient.open(
        "http://localhost:8003/catalog/",
        rs_server_api_key="9856a269-3b7a-40b3-851c-aba7cf8eb8be",
        rs_server_href="test",
        owner_id="toto",
    )
    collection = catalog.get_collection("jgaucher:S1_L1")
    print("hello")
