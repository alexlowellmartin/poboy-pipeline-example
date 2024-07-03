from dagster import ConfigurableResource
import geopandas as gpd
import requests
import json

class ArcGISFeatureServerResource(ConfigurableResource):

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

        while True:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
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
        else:
            print(f"Retrieved GeoDataFrame with {len(gdf)} features.")
        
        return gdf