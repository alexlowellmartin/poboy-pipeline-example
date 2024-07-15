from dagster import ConfigurableResource, AssetExecutionContext, AssetKey, MetadataValue, AssetMaterialization, Output, AssetObservation
import requests
import json
import boto3
import io
import geopandas as gpd
import pandas as pd
from upath import UPath
from pydantic import PrivateAttr
from datetime import datetime

def format_size(size_bytes):
    return f"{size_bytes / (1024 * 1024):.2f} MB"

class CloudflareR2DataStore(ConfigurableResource):
    """
    Custom IO Manager for handling GeoParquet files stored in Cloudflare R2.

    This manager supports various partition types and handles cases where data might be missing.

    Attributes:
        endpoint_url (str): The endpoint URL for Cloudflare R2.
        access_key_id (str): The access key ID for authentication.
        secret_access_key (str): The secret access key for authentication.
        bucket_name (str): The name of the bucket to use.
    """
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str

    _base_path: UPath = PrivateAttr()
    _r2: boto3.client = PrivateAttr()

    def setup_for_execution(self, context):
        """
        Initialize the Cloudflare R2 client for execution.

        Args:
            context: The execution context.
        """
        self._base_path = UPath(f"https://{self.endpoint_url}/{self.bucket_name}")
        self._r2 = boto3.client(
            's3',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            endpoint_url=self.endpoint_url
        )
        context.log.info(f"Initialized Cloudflare R2 client for bucket: {self.bucket_name}")

    def _get_r2_key(self, context: AssetExecutionContext) -> str:
        """
        Generate the R2 key for storing or retrieving an object.

        This method handles both partitioned and non-partitioned assets.

        Args:
            context (AssetExecutionContext): The context for the operation.

        Returns:
            str: The generated R2 key.
        """
        asset_key = context.asset_key
        metadata = context.assets_def.metadata_by_key[asset_key]
        layer = metadata.get("layer", "fallback_layer")
        source = metadata.get("source", "fallback_source")
        data_category = metadata.get("data_category", "fallback_category")
        asset_name = asset_key.path[-1]
        segmentation = metadata.get("segmentation", "fallback_segmentation")
        
        if context.has_partition_key:
            partition_key = context.partition_key
            r2_key = f'{layer}/{source}/{data_category}/{asset_name}/{segmentation}/{partition_key}.geoparquet'
        else:
            timestamp = datetime.now().isoformat()
            r2_key = f'{layer}/{source}/{data_category}/{asset_name}/{segmentation}/snapshot={timestamp}.geoparquet'
        
        return r2_key

    def write_gpq(self, context: AssetExecutionContext, gdf: gpd.GeoDataFrame):
        """
        Save a GeoDataFrame to Cloudflare R2.

        Args:
            context (AssetExecutionContext): The context for the operation.
            gdf (gpd.GeoDataFrame): The GeoDataFrame to save.
        """
        if gdf is None or gdf.empty:
            context.log.info("No data available for this partition. Skipping write operation.")
            return

        try:
            object_key = self._get_r2_key(context)
            full_path = self._base_path / object_key
            context.log.info(f"Preparing to upload to path: {full_path}")

            buffer = io.BytesIO()
            gdf.to_parquet(buffer, engine='pyarrow', index=False, compression='snappy')
            buffer.seek(0)
            file_size = buffer.getbuffer().nbytes

            self._r2.upload_fileobj(buffer, self.bucket_name, object_key)
            context.log.info(f"Uploaded file to {full_path}")

            metadata = {
                "r2_write_location": MetadataValue.text(str(full_path)),
                "r2_file_size_written": MetadataValue.text(format_size(file_size)),
                "r2_num_records_written": MetadataValue.int(len(gdf)),
            }

            context.add_output_metadata(metadata)

        except Exception as e:
            context.log.error(f"Failed to upload file: {e}")
            raise

    def read_gpq_single_key(self, context: AssetExecutionContext, key: str) -> gpd.GeoDataFrame:
        """
        Load a GeoDataFrame from Cloudflare R2 using a specific key.

        Args:
            context (AssetExecutionContext): The context for the operation.
            key (str): The specific R2 key to load the object from.

        Returns:
            gpd.GeoDataFrame: The loaded GeoDataFrame, or an empty GeoDataFrame if no data is available.
        """
        try:
            full_path = self._base_path / key
            context.log.info(f"Attempting to download from path: {full_path}")

            buffer = io.BytesIO()
            self._r2.download_fileobj(self.bucket_name, key, buffer)
            buffer.seek(0)
            file_size = buffer.getbuffer().nbytes

            gdf = gpd.read_parquet(buffer)
            context.log.info(f"Successfully downloaded file from {full_path}")

            metadata = {
                "r2_read_location": MetadataValue.text(str(full_path)),
                "r2_file_size_read": MetadataValue.text(format_size(file_size)),
                "r2_num_records_read": MetadataValue.int(len(gdf)),
                "r2_columns_read": MetadataValue.json(list(gdf.columns)),
                "r2_object_preview": MetadataValue.md(self._get_head_preview(gdf)),
            }

            context.add_output_metadata(metadata)
            return gdf
        except Exception as e:
            context.log.error(f"Failed to download file: {e}")
            raise

    def read_gpq_all_partitions(self, context: AssetExecutionContext, key_pattern: str) -> gpd.GeoDataFrame:
        """
        Load a GeoDataFrame from Cloudflare R2 for all partitions matching a key pattern.

        Args:
            context (AssetExecutionContext): The context for the operation.
            key_pattern (str): The key pattern to match partitions.

        Returns:
            gpd.GeoDataFrame: The combined GeoDataFrame from all partitions.
        """
        try:
            paginator = self._r2.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=key_pattern)

            all_gdfs = []
            total_size = 0
            total_records = 0

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.geoparquet'):
                            gdf = self.read_gpq_single_key(context, key)
                            if not gdf.empty:
                                all_gdfs.append(gdf)
                                total_size += obj['Size']
                                total_records += len(gdf)

            if not all_gdfs:
                context.log.warning(f"No data found for key pattern: {key_pattern}")
                return gpd.GeoDataFrame()

            combined_gdf = pd.concat(all_gdfs, ignore_index=True)
            context.log.info(f"Combined GeoDataFrame with {len(combined_gdf)} total records")

            metadata = {
                "r2_key_pattern": MetadataValue.text(key_pattern),
                "r2_read_location": MetadataValue.text(str(self._base_path / key_pattern)),
                "r2_file_size_read": MetadataValue.text(format_size(total_size)),
                "r2_num_records_read": MetadataValue.int(total_records),
                "r2_columns_read": MetadataValue.json(list(combined_gdf.columns)),
                "r2_object_preview": MetadataValue.md(self._get_head_preview(combined_gdf)),
            }

            context.add_output_metadata(metadata)
            return combined_gdf
        except Exception as e:
            context.log.error(f"Failed to read all partitions: {e}")
            raise
        
    def read_gpq_latest_snapshot(self, context: AssetExecutionContext, key_pattern: str) -> gpd.GeoDataFrame:
        """
        Load the latest snapshot GeoDataFrame from Cloudflare R2 based on a key pattern.

        Args:
            context (AssetExecutionContext): The context for the operation.
            key_pattern (str): The key pattern to match snapshots.

        Returns:
            gpd.GeoDataFrame: The latest snapshot GeoDataFrame.
        """
        try:
            paginator = self._r2.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=key_pattern)

            latest_key = None
            latest_timestamp = None

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.geoparquet'):
                            # Extract timestamp from the key
                            timestamp_str = key.split('snapshot=')[-1].replace('.geoparquet', '')
                            timestamp = datetime.fromisoformat(timestamp_str)
                            if latest_timestamp is None or timestamp > latest_timestamp:
                                latest_timestamp = timestamp
                                latest_key = key

            if latest_key is None:
                context.log.warning(f"No snapshots found for key pattern: {key_pattern}")
                return gpd.GeoDataFrame()

            gdf = self.read_gpq_single_key(context, latest_key)
            context.log.info(f"Loaded latest snapshot from {latest_key}")

            metadata = {
                "r2_key_pattern": MetadataValue.text(key_pattern),
                "r2_read_location": MetadataValue.text(str(self._base_path / latest_key)),
                "r2_file_size_read": MetadataValue.text(format_size(obj['Size'])),
                "r2_num_records_read": MetadataValue.int(len(gdf)),
                "r2_columns_read": MetadataValue.json(list(gdf.columns)),
                "r2_object_preview": MetadataValue.md(self._get_head_preview(gdf)),
            }

            context.add_output_metadata(metadata)
            return gdf
        except Exception as e:
            context.log.error(f"Failed to read latest snapshot: {e}")
            raise

    def _get_head_preview(self, gdf: gpd.GeoDataFrame, n: int = 5) -> str:
        """
        Generate a preview of the GeoDataFrame, excluding the 'geometry' column if it exists.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to generate a preview for.
            n (int): The number of rows to include in the preview.

        Returns:
            str: The markdown representation of the preview.
        """
        preview_df = gdf.head(n).copy()
        if 'geometry' in preview_df.columns:
            preview_df = preview_df.drop(columns=['geometry'])
        return preview_df.to_markdown()

class ArcGISFeatureServerResource(ConfigurableResource):
    """
    Resource for querying an ArcGIS FeatureServer and returning data as a GeoDataFrame.
    """

    def fetch_data(self, url: str, params: dict, crs: str = 'EPSG:4326', context=None) -> gpd.GeoDataFrame:
        """
        Query an ArcGIS FeatureServer and return a GeoDataFrame with the specified CRS.

        Args:
        - url (str): The base URL of the FeatureServer endpoint.
        - params (dict): A dictionary of parameters to include in the query.
        - crs (str): The coordinate reference system to set for the GeoDataFrame (default: 'EPSG:4326').
        - context: The Dagster execution context (optional).

        Returns:
        - GeoDataFrame: A GeoDataFrame containing the features retrieved from the FeatureServer.
        """
        all_features = []
        params = params.copy()  # Avoid modifying the original params dictionary
        params['f'] = 'geojson'  # Ensure the response format is GeoJSON
        params['resultOffset'] = 0  # Start from the first record

        max_record_count = params.get('maxRecordCount', None)
        
        api_response = None
        
        while True:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                if api_response is None:
                    api_response = f"<Response [{response.status_code}]>"  # Store the response code

                data = response.json()

                if 'error' in data:
                    error_message = f"Error querying FeatureServer: {data['error']}"
                    if context:
                        context.log.error(error_message)
                    else:
                        print(error_message)
                    return gpd.GeoDataFrame()  # Return empty GeoDataFrame

                # Check if 'f' = 'geojson' is not supported
                if 'type' not in data or data['type'] != 'FeatureCollection':
                    error_message = "The server did not return GeoJSON data. Ensure the server supports GeoJSON format."
                    if context:
                        context.log.error(error_message)
                    else:
                        print(error_message)
                    return gpd.GeoDataFrame()  # Return empty GeoDataFrame

                features = data.get('features', [])
                all_features.extend(features)

                if max_record_count and len(features) < max_record_count:
                    break  # No more features to fetch

                # Update the resultOffset for the next iteration
                params['resultOffset'] += max_record_count if max_record_count else len(features)

                # If there are no more features, break the loop
                if not features:
                    break

            except requests.exceptions.RequestException as e:
                error_message = f"Request failed: {e}"
                if context:
                    context.log.error(error_message)
                else:
                    print(error_message)
                return gpd.GeoDataFrame()  # Return empty GeoDataFrame on error
            except json.JSONDecodeError:
                error_message = "Failed to decode JSON response."
                if context:
                    context.log.error(error_message)
                else:
                    print(error_message)
                return gpd.GeoDataFrame()  # Return empty GeoDataFrame on error

        # Convert all collected features to a GeoDataFrame
        if not all_features:
            return gpd.GeoDataFrame()  # Return empty GeoDataFrame if no features found

        gdf = gpd.GeoDataFrame.from_features(all_features, crs=crs)
        
        if context:
            context.log.info(f"Retrieved GeoDataFrame with {len(gdf)} features.")
        
            # Add metadata
            metadata = {
                "api_endpoint_url": MetadataValue.text(url),
                "api_query_params": MetadataValue.json(params),
                "api_response": MetadataValue.text(api_response),
                "api_num_features": MetadataValue.int(len(gdf)),
                "api_features_preview": MetadataValue.md(self._get_features_preview(gdf)),
                "api_columns": MetadataValue.json(list(gdf.columns))
            }
            context.add_output_metadata(metadata)
        
        return gdf

    def _get_features_preview(self, gdf: gpd.GeoDataFrame, n: int = 5) -> str:
        """
        Generate a preview of the GeoDataFrame, excluding the 'geometry' column if it exists.

        Args:
            gdf (gpd.GeoDataFrame): The GeoDataFrame to generate a preview for.
            n (int): The number of rows to include in the preview.

        Returns:
            str: The markdown representation of the preview.
        """
        preview_df = gdf.head(n).copy()
        if 'geometry' in preview_df.columns:
            preview_df = preview_df.drop(columns=['geometry'])
        return preview_df.to_markdown()