from dagster import Definitions, load_assets_from_modules, io_manager, StringSource, EnvVar, resource

from . import assets
from .r2_io_managers import R2Config, R2GeoParquetManager
from .resources import ArcGISFeatureServerResource, CloudflareR2DataStore

all_assets = load_assets_from_modules([assets])

@io_manager(config_schema={
    "endpoint_url": StringSource,
    "access_key_id": StringSource,
    "secret_access_key": StringSource,
    "bucket_name": StringSource
})
def r2_geo_parquet_io_manager(init_context):
    config = R2Config(
        endpoint_url=init_context.resource_config["endpoint_url"],
        access_key_id=init_context.resource_config["access_key_id"],
        secret_access_key=init_context.resource_config["secret_access_key"],
        bucket_name=init_context.resource_config["bucket_name"]
    )
    return R2GeoParquetManager(config)

defs = Definitions(
    assets=all_assets,
    resources={
    "r2_geo_parquet_io_manager": r2_geo_parquet_io_manager.configured({
        "endpoint_url": {"env": "R2_ENDPOINT_URL"},
        "access_key_id": {"env": "R2_ACCESS_KEY_ID"},
        "secret_access_key": {"env": "R2_SECRET_ACCESS_KEY"},
        "bucket_name": {"env": "R2_BUCKET_PRIMARY"}
    }),
    "r2_datastore": CloudflareR2DataStore(
            endpoint_url=EnvVar("R2_ENDPOINT_URL"),
            access_key_id=EnvVar("R2_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("R2_SECRET_ACCESS_KEY"),
            bucket_name=EnvVar("R2_BUCKET_PRIMARY")
        ),
    "feature_server": ArcGISFeatureServerResource(),

    }
)

