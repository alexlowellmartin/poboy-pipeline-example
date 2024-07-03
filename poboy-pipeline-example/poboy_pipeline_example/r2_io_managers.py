from dagster import Config, UPathIOManager, InputContext, OutputContext
import boto3
import io
from datetime import datetime, timezone
import geopandas as gpd
from upath import UPath
from typing import Union

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

    Attributes:
        extension (str): The file extension for GeoParquet files.
    """
    extension = ".geoparquet"

    def __init__(self, config: R2Config):
        """
        Initialize the R2GeoParquetManager.

        Args:
            config (R2Config): The configuration object for R2 storage.
        """
        self.config = config
        super().__init__(base_path=UPath(f"https://{self.config.endpoint_url}/{self.config.bucket_name}"))
        self.r2 = boto3.client(
            's3',
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            endpoint_url=self.config.endpoint_url
        )
    
    def _get_r2_key(self, context, extension: str, timestamp: bool = False) -> str:
        """
        Generate the R2 key for storing or retrieving an object.

        Args:
            context (Union[InputContext, OutputContext]): The context for the operation.
            extension (str): The file extension.
            timestamp (bool): Whether to include a timestamp in the key.

        Returns:
            str: The generated R2 key.
        """
        metadata = context.metadata
        tier = metadata.get("tier", "default_tier")
        source = metadata.get("source", "default_source")
        asset_name = context.asset_key.path[-1]
        ts = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f') if timestamp else ""
        context.log.info("R2 keys fetched.")
        return f'{tier}/{source}/{asset_name}/{asset_name}-{ts}{extension}'
    
    def dump_to_path(self, context: OutputContext, obj: gpd.GeoDataFrame, path: UPath):
        """
        Save a GeoDataFrame to Cloudflare R2.

        Args:
            context (OutputContext): The context for the operation.
            obj (gpd.GeoDataFrame): The GeoDataFrame to save.
            path (UPath): The path where the object will be saved.
        """
        try:
            object_key = self._get_r2_key(context, self.extension, timestamp=True)
            bucket = self.config.bucket_name
            context.log.info(f"Preparing to upload to bucket: {bucket}, object_key: {object_key}")
            
            # Convert GeoDataFrame to an in-memory buffer
            buffer = io.BytesIO()
            obj.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)  # Reset buffer position to the beginning
            context.log.info("GeoDataFrame converted to in-memory buffer")
            
            # Upload the buffer to Cloudflare R2
            self.r2.upload_fileobj(buffer, bucket, object_key)
            context.log.info(f"Uploaded file to {bucket}/{object_key}")
            
        except Exception as e:
            context.log.error(f"Failed to upload file: {e}")
            raise
    
    def load_from_path(self, context: InputContext, path: UPath) -> gpd.GeoDataFrame:
        """
        Load a GeoDataFrame from Cloudflare R2.

        Args:
            context (InputContext): The context for the operation.
            path (UPath): The path from which the object will be loaded.

        Returns:
            gpd.GeoDataFrame: The loaded GeoDataFrame.
        """
        try:
            object_key = self._get_r2_key(context, self.extension)
            bucket = self.config.bucket_name
            context.log.info(f"Preparing to download from bucket: {bucket}, object_key: {object_key}")
            
            # Download the file to an in-memory buffer
            buffer = io.BytesIO()
            self.r2.download_fileobj(bucket, object_key, buffer)
            buffer.seek(0)  # Reset buffer position to the beginning
            context.log.info("Downloaded file into in-memory buffer")
            
            # Read the buffer into a GeoDataFrame and return
            gdf = gpd.read_parquet(buffer)
            context.log.info(f"Loaded GeoDataFrame with {len(gdf)} records")
            return gdf
        
        except Exception as e:
            context.log.error(f"Failed to download file: {e}")
            raise

    def get_path(self, context: Union[InputContext, OutputContext]) -> UPath:
        """
        Generate the full path for an object in Cloudflare R2.

        Args:
            context (Union[InputContext, OutputContext]): The context for the operation.

        Returns:
            UPath: The full path for the object.
        """
        r2_key = self._get_r2_key(context, self.extension)
        return self._base_path / r2_key
