from dagster import asset, AssetExecutionContext, TimeWindowPartitionsDefinition
import geopandas as gpd
from datetime import datetime
from .resources import CloudflareR2DataStore, ArcGISFeatureServerResource


# Define yearly partitions starting from 2023
yearly_partitions = TimeWindowPartitionsDefinition(
    cron_schedule="@yearly",  # Run once a year
    start=datetime(2023, 1, 1),  # Start from January 1, 2023
    end_offset=1,  # Include the current year
    fmt="%Y"  # Format partition keys as YYYY
)

@asset(
    group_name='raw_data',
    metadata={"layer": "landing", "source": "txdot", "data_category": "vector", "segmentation": "partitions"},
    partitions_def=yearly_partitions,
    )
def texas_trunk_system(context: AssetExecutionContext, feature_server: ArcGISFeatureServerResource, r2_datastore: CloudflareR2DataStore):
    """ Fetches the TXDoT Texas Trunk System containing a network of divided highways intented to become >= 4 lanes."""
    
    # Define annual partition key
    year = context.partition_key
    context.log.info(f"Processing partition for year: {year}")
    
    # Define query
    url="https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TxDOT_Texas_Trunk_System/FeatureServer/0/query"
    params = {
        'where': f"EXTRACT(YEAR FROM EXT_DATE) = {year}",
        'outFields': '*',
        'f': 'geojson',
        }
    
    # Fetch data and return geodataframe
    gdf = feature_server.fetch_data(url=url, params=params, context=context)

    # Write the GeoDataFrame to R2
    r2_datastore.write_gpq(context, gdf)



@asset(
    group_name='raw_data',
    metadata={"layer": "landing", "source": "txdot", "data_category": "vector", "segmentation": "full_snapshots"},
)
def texas_county_boundaries(context: AssetExecutionContext, feature_server: ArcGISFeatureServerResource, r2_datastore: CloudflareR2DataStore):
    """Fetches the TXDoT polygon layer of the 254 Texas counties"""
    
    # Define query
    url = "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/Texas_County_Boundaries/FeatureServer/0/query?"
    params = {
        'where': '1=1',
        'outFields': '*',
        'f': 'geojson',
    }
    
    # Fetch data
    gdf = feature_server.fetch_data(url=url, params=params, context=context)
    
    # Write the GeoDataFrame to R2
    r2_datastore.write_gpq(context, gdf)
    
    return gdf


@asset(
    group_name='raw_data',
    metadata={"layer": "landing", "source": "census_bureau", "data_category": "vector", "segmentation": "full_snapshots"},
)
def tx_med_household_income(context: AssetExecutionContext, feature_server: ArcGISFeatureServerResource, r2_datastore: CloudflareR2DataStore):
    """Fetches American Community Survey (ACS) median household income by census tract in Texas."""
    
    # Define query
    url="https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/ACS_Median_Income_by_Race_and_Age_Selp_Emp_Boundaries/FeatureServer/2/query?"
    params = {
        'where': "State='Texas'",
        'outFields': '*',
        'f': 'geojson',
        "returnGeometry": "true", 
    }
    
    # Fetch data
    gdf = feature_server.fetch_data(url=url, params=params, context=context)
    
    # Write the GeoDataFrame to R2
    r2_datastore.write_gpq(context, gdf)
    
    return gdf

@asset(
    group_name='analytics',
    metadata={"layer": "enriched", "source": "analytics", "data_category": "vector", "segmentation": "full_snapshots"},
    deps = [texas_trunk_system, tx_med_household_income, texas_county_boundaries]
)
def trunk_median_income(context: AssetExecutionContext, r2_datastore: CloudflareR2DataStore):
    """Joins Texas trunk system to median household income. Filters by select counties."""

    # Fetch trunk system
    ts_key_pattern = "landing/txdot/vector/texas_trunk_system/partitions"
    trunk_system = r2_datastore.read_gpq_all_partitions(context, ts_key_pattern)
    
    # Fetch median income tracts
    med_income_tracts_key = "landing/census_bureau/vector/tx_med_household_income/full_snapshots/snapshot=2024-07-14T15:29:10.742535.geoparquet"
    med_income_tracts = r2_datastore.read_gpq_single_key(context, med_income_tracts_key)
    
    # Fetch counties
    tx_counties_pattern = 'landing/txdot/vector/texas_county_boundaries/full_snapshots'
    tx_counties = r2_datastore.read_gpq_latest_snapshot(context, tx_counties_pattern)
    
    # Filter counties
    county_list = ['Williamson','Travis', 'Hays', 'Bell','Milam', 'Lee', 'Bastrop', 'Caldwell', 'Guadalupe', 'Gonzales', 'Bexar', 'Comal', 'Fayette', 'Wilson']
    tx_counties = tx_counties[tx_counties['CNTY_NM'].isin(county_list)]

    # Join median income tracts to trunk system
    combined_gdf = gpd.sjoin(med_income_tracts, trunk_system, how="inner", predicate="intersects")
    
    # Clip combined gdf to only our desired county boundary areas
    combined_gdf = combined_gdf.clip(tx_counties)

    r2_datastore.write_gpq(context, combined_gdf)
    
    return combined_gdf