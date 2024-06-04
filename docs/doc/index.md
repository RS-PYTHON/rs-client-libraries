# RS Client Libraries overview

The RS-Client libraries allows the user to call all the RS-Server services. It is composed mainly from the base class RsClient and the
classes which extend this base class: AuxipClient, CadipClient and StacClient.

![Python Components](../images/python-components.png)

The **RsClient** class is a base class for interacting with the RS-Server. It provides methods for managing and retrieving files about various resources and stations available on the server. The class includes methods for checking file download statuses, staging files for a future processing, and searching for files within specified time ranges. It also provides methods for creating instances of its child classes: AuxipClient, CadipClient, and StacClient.

The **AuxipClient** class extends the RsClient class to provide specific functionality for interfacing with the ADGS (Auxiliary Data Generation System) endpoints runnin on the RS-Server. This class overrides the necessary properties to define the ADGS-specific endpoints for searching, staging, and checking the status of file downloads.

The **CadipClient** class extends the RsClient class to provide specific functionality for interfacing with the CADIP (CADU Ingestion Platform) endpoints running on the RS-Server. This class overrides the necessary properties to define the CADIP-specific endpoints for searching sessions, staging files, and checking the status of file downloads.

The **StacClient** class extends the RsClient class and the [PySTAC](https://pystac-client.readthedocs.io/en/stable/) to provide specific functionality for interfacing with the STAC (SpatioTemporal Asset Catalog) endpoints on the RS-Server. This class overrides the necessary properties to define the STAC-specific endpoints. Please check also the PyStac client [usage documentation](https://pystac-client.readthedocs.io/en/stable/usage.html#client) for how to use the read catalog methods.

![Python RS Client](../images/python-rs-client.png)
