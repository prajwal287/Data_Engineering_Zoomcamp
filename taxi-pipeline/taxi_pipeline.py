"""
NYC Yellow Taxi trip data pipeline (Data Engineering Zoomcamp 2026 - dlt homework).
Loads paginated JSON from the custom API into DuckDB.
"""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

# For a *quick test* (pipeline works, ~1 min): use 5000.
# For *homework answers* (full dataset): use None. Full run can take 10–30+ min.
MAXIMUM_OFFSET = 5000


@dlt.source
def taxi_pipeline_rest_api_source(maximum_offset=None):
    """REST API source for NYC Yellow Taxi trip data.
    API: paginated JSON, 1000 records per page, stop when empty page.
    """
    if maximum_offset is None:
        maximum_offset = MAXIMUM_OFFSET
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "params": {},
                    "data_selector": "$",
                    "paginator": {
                        "type": "offset",
                        "limit": 1000,
                        "offset_param": "offset",
                        "limit_param": "limit",
                        "total_path": None,
                        "stop_after_empty_page": True,
                        "maximum_offset": maximum_offset,
                    },
                },
            },
        ],
    }
    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)
