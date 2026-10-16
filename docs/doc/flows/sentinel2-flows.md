# Sentinel-2 flows

No [Sentinel-2](https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2) processing flow is delivered yet.

The task table of the Sentinel-2 MSI Level-1 and Level-2 processor (`TaskTable_S2_L1L2MSI.json`) is available in the
[rs-dpr-service config](https://github.com/RS-PYTHON/rs-dpr-service/tree/develop/config) folder, to prepare its
integration, which follows the [DPR processor integration](https://github.com/RS-PYTHON/rs-dpr-service/blob/develop/docs/doc/processor-integration.md)
procedure. The Sentinel-2 sessions and products can already be searched and staged with the
[common flows](common-flows.md), but the Level-0 flow `process-l0` doesn't process them yet.
