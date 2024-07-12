from dagster import Config, UPathIOManager, InputContext, OutputContext, TimeWindow
import boto3
import io
from datetime import datetime, timezone
import geopandas as gpd
import pandas as pd
from upath import UPath
from typing import Union, Optional, Any, Dict


from dagster import ConfigurableIOManager, InputContext, OutputContext, MetadataValue, AssetExecutionContext
from dagster._core.storage.upath_io_manager import UPathIOManager
from upath import UPath
import boto3
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from io import BytesIO



class R2Config(Config):
    """
    Configuration class for Cloudflare R2 storage.

    Attributes:
        endpoint_url (str): The endpoint URL for the Cloudflare R2 service.
        access_key_id (str): The access key ID for authentication.
        secret_access_key (str): The secret access key for authentication.
        bucket_name (str): The name of the default bucket to use.
    """
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str


class R2GeoParquetManager(UPathIOManager):
    """
    Custom IO Manager for handling GeoParquet files stored in Cloudflare R2.
    """
    extension = ".geoparquet"

    def __init__(self, config: R2Config):
        self.config = config
        super().__init__(base_path=UPath(f"https://{self.config.endpoint_url}/{self.config.bucket_name}"))
        self.r2 = boto3.client(
            's3',
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            endpoint_url=self.config.endpoint_url
        )

    def _get_r2_key(self, context: Union[InputContext, OutputContext], extension: str) -> str:
        if isinstance(context, InputContext) and context.upstream_output:
            metadata = context.upstream_output.metadata
            asset_key = context.upstream_output.asset_key
        else:
            metadata = context.metadata
            asset_key = context.asset_key

        tier = metadata.get("tier", "default_tier")
        source = metadata.get("source", "default_source")
        asset_name = asset_key.path[-1]

        if context.has_asset_partitions:
            partition_key = self._format_partition_key(context.asset_partition_key)
            r2_key = f'{tier}/{source}/{asset_name}/{partition_key}{extension}'
        else:
            r2_key = f'{tier}/{source}/{asset_name}/{asset_name}{extension}'

        context.log.info(f"Generated R2 key: {r2_key}")
        return r2_key

    def _format_partition_key(self, partition_key: Any) -> str:
        if isinstance(partition_key, str):
            return partition_key
        elif isinstance(partition_key, TimeWindow):
            return f"{partition_key.start.isoformat()}_{partition_key.end.isoformat()}"
        elif isinstance(partition_key, dict):
            return "/".join(f"{k}={v}" for k, v in sorted(partition_key.items()))
        elif partition_key is None:
            return ""
        else:
            return str(partition_key)

    def dump_to_path(self, context: OutputContext, obj: gpd.GeoDataFrame, path: UPath):
        try:
            object_key = self._get_r2_key(context, self.extension)
            bucket = self.config.bucket_name
            context.log.info(f"Preparing to upload to bucket: {bucket}, object_key: {object_key}")

            buffer = io.BytesIO()
            obj.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
            buffer.seek(0)
            self.r2.upload_fileobj(buffer, bucket, object_key)
            context.log.info(f"Uploaded file to {bucket}/{object_key}")
        except Exception as e:
            context.log.error(f"Failed to upload file: {e}")
            raise

    def load_from_path(self, context: InputContext, path: UPath) -> gpd.GeoDataFrame:
        try:
            object_key = self._get_r2_key(context, self.extension)
            bucket = self.config.bucket_name
            context.log.info(f"Attempting to download from bucket: {bucket}, object_key: {object_key}")

            try:
                self.r2.head_object(Bucket=bucket, Key=object_key)
            except self.r2.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    context.log.warning(f"No data available for {object_key} in bucket {bucket}")
                    return gpd.GeoDataFrame()
                else:
                    raise

            buffer = io.BytesIO()
            self.r2.download_fileobj(bucket, object_key, buffer)
            buffer.seek(0)
            context.log.info(f"Successfully downloaded file from {bucket}/{object_key}")
            gdf = gpd.read_parquet(buffer)
            context.log.info(f"Loaded GeoDataFrame with {len(gdf)} records")
            return gdf
        except Exception as e:
            context.log.error(f"Failed to download file: {e}")
            raise

    def get_path(self, context: Union[InputContext, OutputContext]) -> UPath:
        r2_key = self._get_r2_key(context, self.extension)
        return self._base_path / r2_key

    def handle_output(self, context: OutputContext, obj: gpd.GeoDataFrame):
        if obj is not None and not obj.empty:
            super().handle_output(context, obj)
        else:
            context.log.info(f"Skipping output for empty or None GeoDataFrame in partition {context.asset_partition_key}")

    def load_input(self, context: InputContext) -> gpd.GeoDataFrame:
        # Check if the asset itself is partitioned
        if context.has_asset_partitions:
            # If the asset is partitioned, load all partitions and concatenate them
            if len(context.asset_partition_keys) == 0:
                return gpd.GeoDataFrame()
            else:
                gdfs = [
                    self.load_from_path(
                        context,
                        self.get_path(context)
                    )
                    for partition_key in context.asset_partition_keys
                ]
                return pd.concat(gdfs, ignore_index=True) if gdfs else gpd.GeoDataFrame()
        else:
            # If the asset is not partitioned, check if it depends on partitioned upstream assets
            upstream_partition_keys = context.asset_partition_keys_for_input(context.name)
            if upstream_partition_keys:
                # Load all partitions from the upstream asset and concatenate them
                gdfs = []
                for partition_key in upstream_partition_keys:
                    partition_context = context  # Use the current context as a base
                    path = self.get_path(partition_context)
                    gdf = self.load_from_path(partition_context, path)
                    if not gdf.empty:
                        gdfs.append(gdf)
                return pd.concat(gdfs, ignore_index=True) if gdfs else gpd.GeoDataFrame()
            else:
                # Regular unpartitioned asset
                path = self.get_path(context)
                return self.load_from_path(context, path)
    """
    def get_metadata(self, context: OutputContext, obj: gpd.GeoDataFrame) -> Dict[str, MetadataValue]:
        metadata = super().get_metadata(context, obj)
        
        # Add custom metadata
        metadata.update({
            "num_rows": MetadataValue.int(len(obj)),
            "num_columns": MetadataValue.int(len(obj.columns)),
            "crs": MetadataValue.text(str(obj.crs)),
            "geometry_type": MetadataValue.text(obj.geometry.type.name),
            "r2_key": MetadataValue.text(self._get_r2_key(context, self.extension)),
            "file_size": MetadataValue.size(obj.memory_usage(deep=True).sum()),
        })

        return metadata
    """