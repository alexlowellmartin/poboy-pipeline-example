from dagster import Definitions, load_assets_from_modules, EnvVar

from . import assets
from .resources import ArcGISFeatureServerResource, CloudflareR2DataStore

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
    "r2_datastore": CloudflareR2DataStore(
            endpoint_url=EnvVar("R2_ENDPOINT_URL"),
            access_key_id=EnvVar("R2_ACCESS_KEY_ID"),
            secret_access_key=EnvVar("R2_SECRET_ACCESS_KEY"),
            bucket_name=EnvVar("R2_BUCKET_PRIMARY")
        ),
    "feature_server": ArcGISFeatureServerResource(),

    }
)

