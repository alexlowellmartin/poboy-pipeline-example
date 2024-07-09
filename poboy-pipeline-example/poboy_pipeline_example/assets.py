from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition, AssetIn
import geopandas as gpd
from datetime import datetime, timedelta
from .resources import ArcGISFeatureServerResource


# Define a daily partition
start_date = datetime(2023, 9, 1)
#end_date = datetime.now().date()
end_date = datetime(2023, 9, 3)
daily_partitions = DailyPartitionsDefinition(start_date=start_date, end_date=end_date)


@asset(
    group_name='txdot',
    metadata={"tier": "landing", "source":"txdot"},
    io_manager_key="r2_geo_parquet_io_manager",
    partitions_def=daily_partitions,
    )
def texas_trunk_system(context: AssetExecutionContext, feature_server: ArcGISFeatureServerResource):
    """ Fetches the TXDoT Texas Trunk System containing a network of divided highways intented to become >= 4 lanes."""
    
    # Declare partition key
    partition_key = context.partition_key
    context.log.info(f"Processing partition: {partition_key}")
    
    # Parse the partition key string into a datetime object
    partition_date = datetime.strptime(partition_key, "%Y-%m-%d")
    
    # Format partition key to pass to query
    formatted_partition_date = partition_date.strftime("%m-%d-%Y")
    
    # Define query
    url="https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TxDOT_Texas_Trunk_System/FeatureServer/0/query"
    params = {
        'where': f"EXT_DATE = '{formatted_partition_date}'",
        'outFields': '*',
        'f': 'geojson',
        }
    
    # Fetch data and return geodataframe
    gdf = feature_server.fetch_data(url=url, params=params, context=context)

    return gdf


@asset(
    group_name='txdot_processed',
    metadata={"tier": "processed", "source": "txdot"},
    io_manager_key="r2_geo_parquet_io_manager",
    partitions_def=daily_partitions,
    ins={"trunk_system": AssetIn("texas_trunk_system")}
)
def state_highways_trunk_system(context, trunk_system: gpd.GeoDataFrame):
    if trunk_system.empty:
        context.log.info(f"No data available for partition {context.partition_key}")
        return gpd.GeoDataFrame()
    
    # Process the data as usual
    state_highways = trunk_system[trunk_system['RTE_PRFX'] == 'SH']
    return state_highways

@asset(
    group_name='txdot',
    metadata={"tier": "landing", "source":"txdot"},
    io_manager_key="r2_geo_parquet_io_manager",
    )
def texas_county_boundaries(context: AssetExecutionContext, feature_server: ArcGISFeatureServerResource):
    """ Fetches the TXDoT polygon layer of the 254 Texas counties"""
    
    # Define query
    url="https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/Texas_County_Boundaries/FeatureServer/0/query?"
    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
        }
    
    # Fetch data and return geodataframe
    gdf = feature_server.fetch_data(url=url, params=params, context=context)

    return gdf

@asset(
    group_name='census_bureau',
    metadata={"tier": "landing", "source":"census_bureau"},
    io_manager_key="r2_geo_parquet_io_manager",
    )
def texas_acs_tract_median_household_income(context: AssetExecutionContext, feature_server: ArcGISFeatureServerResource):
    """ Fetches American Community Survey (ACS) about median household income by census tract in Texas."""
    
    # Envelope bbox that captures Texas
    envelope = "-106.645646, 25.837377, -93.508292, 36.500704"
    
    # Define query
    url="https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/ACS_Median_Income_by_Race_and_Age_Selp_Emp_Boundaries/FeatureServer/2/query?"
    params = {
        "geometryType": "esriGeometryEnvelope",
        "geometry": envelope, 
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "true", 
        "outSR": "4326",
        }
    
    # Fetch data and return geodataframe
    gdf = feature_server.fetch_data(url=url, params=params, context=context)

    return gdf

@asset(
    group_name='analytics',
    metadata={"tier": "enriched", "source":"analytics"},
    io_manager_key="r2_geo_parquet_io_manager",
)
def trunk_median_income(
    context: AssetExecutionContext, 
    texas_trunk_system: gpd.GeoDataFrame, 
    texas_acs_tract_median_household_income: gpd.GeoDataFrame
):
    """Combines the TXDoT Texas Trunk System data with ACS median household income data."""
    
    # Access the upstream data
    trunk_system_gdf = texas_trunk_system
    acs_tract_gdf = texas_acs_tract_median_household_income
    
    # Perform the necessary operations to combine or analyze the data
    # For example, spatial join or analysis
    # Example operation (assuming a spatial join):
    combined_gdf = gpd.sjoin(trunk_system_gdf, acs_tract_gdf, how="inner", op="intersects")
    
    # Return the combined geodataframe
    return combined_gdf