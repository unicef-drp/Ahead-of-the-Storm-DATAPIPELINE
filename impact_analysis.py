#!/usr/bin/env python3
"""
Impact Analysis Script

This script performs hurricane impact analysis by reading hurricane envelope data directly 
from Snowflake and creating impact analysis views for schools, health centers, population, 
and infrastructure. The analysis uses geospatial intersection calculations to determine 
potential impacts on critical infrastructure.

Key Features:
- Reads hurricane envelope data directly from Snowflake TC_ENVELOPES_COMBINED table
- Performs geospatial intersection analysis with infrastructure data
- Creates impact views for multiple wind speed thresholds
- Supports multiple countries and flexible storage backends (local/blob)
- Generates comprehensive impact statistics and visualizations

Usage:
    python impact_analysis.py --storm JERRY --date "20251010000000"
    python impact_analysis.py --storm JERRY --date "20251010000000" --countries DOM
"""

import os
import sys
import argparse
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box
from pyproj import CRS
from shapely import union_all
from shapely import wkt as shapely_wkt
import math

# Add the project root to Python path so we can import components
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import centralized configuration
from config import config

# Import GigaSpatial components
from gigaspatial.core.io import DataStore
from gigaspatial.handlers import AdminBoundaries, RWIHandler
from gigaspatial.processing import convert_to_geodataframe, buffer_geodataframe
from gigaspatial.handlers import GigaSchoolLocationFetcher
from gigaspatial.generators import GeometryBasedZonalViewGenerator, MercatorViewGenerator, AdminBoundariesViewGenerator
from gigaspatial.handlers.healthsites import HealthSitesFetcher
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.core.io.writers import write_dataset

# Import centralized data store utility
from data_store_utils import get_data_store

# Import only Snowflake data retrieval functions
from snowflake_utils import (
    get_envelopes_from_snowflake,
    convert_envelopes_to_geodataframe,
    get_snowflake_tracks
)

from reports import do_report, save_json_report

##### Constants #####
# oecs_countries = ['ATG','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB']
# countries = ['NIC','DOM','BLZ'] + oecs_countries
#default_countries = ['NIC']
data_cols = ['population', 'built_surface_m2', 'num_schools','school_age_population', 'infant_population','num_hcs','rwi','smod_class']
sum_cols = ["E_school_age_population", "E_infant_population", "E_built_surface_m2","E_population","E_num_schools","E_num_hcs"]
avg_cols = ["E_smod_class","E_rwi","probability"]
sum_cols_admin = ["school_age_population", "infant_population", "built_surface_m2","population","num_schools","num_hcs"]
avg_cols_admin = ["smod_class","rwi"]
sum_cols_cci = ['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_pop','E_CCI_pop']
#####################

##### Environment Variables #####
RESULTS_DIR = config.RESULTS_DIR
BBOX_FILE = config.BBOX_FILE
STORMS_FILE = config.STORMS_FILE
VIEWS_DIR = config.VIEWS_DIR
ROOT_DATA_DIR = config.ROOT_DATA_DIR
#################################

# Initialize data store using centralized utility
data_store = get_data_store()



# =============================================================================
# GEOSPATIAL PROCESSING FUNCTIONS
# =============================================================================

def rectangle_bbox_padded(geom, padding_km=0, crs_in=4326, proj_mode="utm"):
    """
    Smallest axis-aligned rectangle in meters that contains `geom`, padded by `padding_km`,
    returned as a lon/lat (EPSG:4326) polygon + its bounds tuple.

    geom: shapely (Multi)Polygon/Polygon
    padding_km: pad equally on all sides (km)
    crs_in: EPSG of `geom` (default 4326)
    proj_mode: "aeqd" (local Azimuthal Equidistant, good general choice) or "utm"
    """
    gs = gpd.GeoSeries([geom], crs=f"EPSG:{crs_in}")

    # choose a local metric CRS
    if proj_mode == "utm":
        proj = gs.estimate_utm_crs()
    else:  # AEQD centered on the geometry
        c = gs.unary_union.centroid
        proj = CRS.from_proj4(f"+proj=aeqd +lat_0={c.y} +lon_0={c.x} +datum=WGS84 +units=m +no_defs")

    gm = gs.to_crs(proj).iloc[0]
    minx, miny, maxx, maxy = gm.bounds
    pad = float(padding_km) * 1000.0

    rect_m = box(minx - pad, miny - pad, maxx + pad, maxy + pad)
    rect_ll = gpd.GeoSeries([rect_m], crs=proj).to_crs(crs_in).iloc[0]
    return rect_ll

def get_country_boundaries(countries):
    """
    countries: list of iso3 codes for countries of interest
    returns: list of admin 0 boundaries
    """
    country_boundaries = []
    for country in countries:
        admin_boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
        country_boundaries.append(admin_boundaries.to_geodataframe().geometry.iat[0])
    return country_boundaries

def get_padded_bounding_box_for_countries(countries, padding_km=1000):
    """
    countries: list of iso3 codes for countries of interest
    padding_km: padding to add on all sides (in kilometers)
    returns: bbox_polygon_lonlat
    """
    country_boundaries = get_country_boundaries(countries)
    gs = gpd.GeoSeries(country_boundaries, crs='EPSG:4326')
    u = union_all(gs)
    return rectangle_bbox_padded(u, padding_km)

def is_envelope_in_zone(bbox, df_envelopes, geometry_column='geometry'):
    """
    bbox: a polygon representing a bbox
    df_envelopes: DataFrame or GeoDataframe with the envelopes of the tracks
    returns: bool whether any of the envelopes intersects with bbox
    """
    if df_envelopes.empty:
        return False

    if geometry_column != 'geometry':
        df_envelopes = df_envelopes.rename(columns={geometry_column: 'geometry'})
    
    if isinstance(df_envelopes, pd.DataFrame):
        gdf_envelopes = convert_to_geodataframe(df_envelopes.dropna(subset=['geometry']))
    else:  # GeoDataFrame
        gdf_envelopes = df_envelopes.copy()

    mask = gdf_envelopes.intersects(bbox)
    return bool(mask.any())

def save_bounding_box(countries):
    """
    Save bounding box file so not to recalculate every time
    """
    filename = os.path.join(RESULTS_DIR, BBOX_FILE)
    if not data_store.file_exists(filename):
        bbox = get_padded_bounding_box_for_countries(countries)
        gbbox = gpd.GeoDataFrame(geometry=[bbox], crs='EPSG:4326')
        write_dataset(gbbox, data_store, filename)

def read_bounding_box():
    """
    Read bounding box file and return bbox geometry
    """
    # Use absolute path from project root
    #project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    bbox_path = os.path.join(RESULTS_DIR, BBOX_FILE)
    gbbox = read_dataset(data_store, bbox_path)
    return gbbox.geometry.iat[0]

def create_mercator_country_layer(country, zoom_level=15):
    """
    countries: list of iso3 codes of countries
    zoom_level: int - zoom level for mercator tiles
    returns a geodataframe with the mercator view
    """
    #### schools ####
    gdf_schools = GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
    # seems API is returning old column name...
    if 'giga_id_school' in gdf_schools.columns:
        gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
    #################
        
    #### health centers ####
    if hc_exist(country):
        gdf_hcs = load_hc_locations(country)
    else:
        try:
            gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
            if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
                print(f"    No health center data available for {country}, skipping...")
                # Create empty GeoDataFrame with proper geometry column for tracks processing
                gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
            else:
                gdf_hcs = gdf_hcs.set_crs(4326)
                save_hc_locations(gdf_hcs, country)
        except Exception as e:
            print(f"Error fetching health centers for {country}: {str(e)}")
            gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
    ########################

    tiles_viewer = MercatorViewGenerator(source=country, zoom_level=zoom_level, data_store=data_store)
    try:
        tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="school_age_population", school_age=True, project="age_structures", un_adjusted=False, sex='F_M')
    except:
        # this should be ready in the next release
        print("No school age available")
        try:
            tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="school_age_population", predicate='centroid_within', school_age=False, project="age_structures", un_adjusted=False,min_age=5, max_age=15)
        except:
            print("No age ranges either")
            school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
            tiles_viewer.add_variable_to_view(school_age, 'school_age_population')

    try:
        tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="infant_population", predicate='centroid_within', school_age=False, project="age_structures", un_adjusted=False,min_age=0, max_age=4)
    except:
        print("No infant age ranges")
        school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
        tiles_viewer.add_variable_to_view(school_age, 'infant_population')

    tiles_viewer.map_wp_pop(country=country, resolution=100)
    tiles_viewer.map_built_s()
    tiles_viewer.map_smod()
    
    ## schools
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")

    ## health centers
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")

    # should we do RWI?
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except:
        print("Relative Wealth Index will be empty")
        rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
    
    tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)
    
    return gdf_tiles


def save_mercator_view(gdf, country, zoom_level):
    """Save base mercator infrastructure view for country"""
    file_name = f"{country}_{zoom_level}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def admins_overlay(gdf_admins1,gdf_mercator):
    small_gdf = gdf_mercator.copy()
    large_gdf = gdf_admins1.copy()
    # Step 2: Overlay to get all intersections
    intersections = gpd.overlay(small_gdf, large_gdf, how="intersection")

    # Step 3: Compute intersection area
    intersections["intersection_area"] = intersections.geometry.area

    # Step 4: For each small polygon, keep the row with max intersection area
    max_area_idx = intersections.groupby("tile_id")["intersection_area"].idxmax()
    assigned = intersections.loc[max_area_idx, ["tile_id", "id"]]

    # Step 5: Merge back with original small_gdf to assign each small polygon
    result = small_gdf.merge(assigned, on="tile_id", how="left")

    # Step 6: Make sure it's a GeoDataFrame with correct geometry and CRS
    return gpd.GeoDataFrame(result, geometry="geometry", crs=small_gdf.crs)

def add_admin_ids(view,country,zoom_level,rewrite):
    gdf_admins1 = AdminBoundaries.create(country_code=country,admin_level=1).to_geodataframe()

    combined_view = admins_overlay(gdf_admins1,view)

    return combined_view, gdf_admins1

def save_mercator_and_admin_views(countries,zoom_level,rewrite):
    """
    Generates and saves all country mercator views and admin views
    """

    for country in countries:
        file_name = f"{country}_{zoom_level}.parquet"
        if not data_store.file_exists(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name)):
            view = create_mercator_country_layer(country, zoom_level)
            combined_view, gdf_admins1 = add_admin_ids(view,country,zoom_level,rewrite)
            d = gdf_admins1.set_index('id')['name'].to_dict()
            d_geo = gdf_admins1.set_index('id')['geometry'].to_dict()
            save_mercator_view(combined_view, country, zoom_level)
            #### Aggregate by admin ####
            
            # Define aggregation dictionary
            agg_dict = {col: "sum" for col in sum_cols_admin}
            agg_dict.update({col: "mean" for col in avg_cols_admin})

            # Group by large_id and aggregate
            agg = combined_view.groupby("id").agg(agg_dict).reset_index()
            admin_view = agg.rename(columns={'id':'tile_id'})
            ### add names ###
            admin_view['name'] = admin_view['tile_id'].map(d)
            admin_view['geometry'] = admin_view['tile_id'].map(d_geo)
            admin_view = convert_to_geodataframe(admin_view)
            save_admin_view(admin_view,country)
            ############################
        elif rewrite:
            view = load_mercator_view(country, zoom_level)
            if 'id' not in view.columns:
                combined_view, gdf_admins1 = add_admin_ids(view,country,zoom_level,rewrite)
                d = gdf_admins1.set_index('id')['name'].to_dict()
                d_geo = gdf_admins1.set_index('id')['geometry'].to_dict()
                save_mercator_view(combined_view, country, zoom_level)
                #### Aggregate by admin ####
            
                # Define aggregation dictionary
                agg_dict = {col: "sum" for col in sum_cols_admin}
                agg_dict.update({col: "mean" for col in avg_cols_admin})

                # Group by large_id and aggregate
                agg = combined_view.groupby("id").agg(agg_dict).reset_index()
                admin_view = agg.rename(columns={'id':'tile_id'})
                ### add names ###
                admin_view['name'] = admin_view['tile_id'].map(d)
                admin_view['geometry'] = admin_view['tile_id'].map(d_geo)
                admin_view = convert_to_geodataframe(admin_view)
                save_admin_view(admin_view,country)
                ############################
            else:
                gdf_admins1 = AdminBoundaries.create(country_code=country,admin_level=1).to_geodataframe()
                d = gdf_admins1.set_index('id')['name'].to_dict()
                d_geo = gdf_admins1.set_index('id')['geometry'].to_dict()
                #### Aggregate by admin ####
                combined_view = view
                # Define aggregation dictionary
                agg_dict = {col: "sum" for col in sum_cols_admin}
                agg_dict.update({col: "mean" for col in avg_cols_admin})

                # Group by large_id and aggregate
                agg = combined_view.groupby("id").agg(agg_dict).reset_index()
                admin_view = agg.rename(columns={'id':'tile_id'})
                ### add names ###
                admin_view['name'] = admin_view['tile_id'].map(d)
                admin_view['geometry'] = admin_view['tile_id'].map(d_geo)
                admin_view = convert_to_geodataframe(admin_view)
                save_admin_view(admin_view,country)
                ############################

def save_mercator_views(countries,zoom_level):
    """
    Generates and saves all country mercator views
    """

    for country in countries:
        view = create_mercator_country_layer(country, zoom_level)
        save_mercator_view(view, country, zoom_level)

def save_json_storms(d):
    """
    Save json file with processed storm,dates
    """
    filename = os.path.join(RESULTS_DIR, STORMS_FILE)
    write_dataset(d, data_store, filename)

def load_json_storms():
    """
    Read json file with saved storm,dates
    """
    filename = os.path.join(RESULTS_DIR, STORMS_FILE)
    if data_store.file_exists(filename):
        df = read_dataset(data_store, filename)
        column_name = df.columns[0]
        return {column_name: df[column_name].to_dict()}
    return {'storms':{}}


def load_mercator_view(country, zoom_level=15):
    """Load mercator view for country"""
    file_name = f"{country}_{zoom_level}.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def create_school_view_from_envelopes(gdf_schools, gdf_envelopes):
    """
    gdf_schools: a geodataframe with school data
    gdf_envelopes: a geodataframe with envelopes
    returns a dictionary of wind threshold - geodataframe with the corresponding school view
    """
    gdf_schools_buff = buffer_geodataframe(gdf_schools, buffer_distance_meters=150)
    wind_views = {}

    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            schools_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_schools_buff, zone_id_column='school_id_giga')
            try:
                new_col = schools_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in schools_viewer.view.zone_id.unique()}
            schools_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = schools_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes):
    """
    gdf_hcs: a geodataframe with school data
    gdf_envelopes: a geodataframe with envelopes
    returns a dictionary of wind threshold - geodataframe with the corresponding health center view
    """
    gdf_hcs_buff = buffer_geodataframe(gdf_hcs, buffer_distance_meters=150)
    wind_views = {}

    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            hcs_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_hcs_buff, zone_id_column='osm_id')
            try:
                new_col = hcs_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in hcs_viewer.view.zone_id.unique()}
            hcs_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = hcs_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes):
    """Create mercator tile impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in tiles_viewer.view.zone_id.unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                df_view[f"E_{col}"] = df_view[col]*df_view['probability']

            df_view = df_view.drop(columns=data_cols)

            wind_views[wind_th] = df_view


    return wind_views

def create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes):
    d = gdf_admin.set_index('tile_id')['name'].to_dict()
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in tiles_viewer.view.zone_id.unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            df_view = tiles_viewer.to_dataframe()
            for col in data_cols:
                df_view[f"E_{col}"] = df_view[col]*df_view['probability']

            df_view = df_view.drop(columns=data_cols)

            #### Aggregate by admin ####
            
            # Define aggregation dictionary
            agg_dict = {col: "sum" for col in sum_cols}
            agg_dict.update({col: "mean" for col in avg_cols})

            # Group by large_id and aggregate
            agg = df_view.groupby("id").agg(agg_dict).reset_index()
            df_view = agg.rename(columns={'id':'zone_id'})
            ### add names ###
            df_view['name'] = df_view['zone_id'].map(d)
            ############################

            wind_views[wind_th] = df_view


    return wind_views

def create_admin_view_from_envelopes(gdf_admin, gdf_envelopes):
    """Create admin tile impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            admin_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_admin, zone_id_column='tile_id')
            try:
                new_col = admin_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in admin_viewer.view.zone_id.unique()}
            admin_viewer.add_variable_to_view(probs, 'probability')

            df_view = admin_viewer.to_dataframe()
            for col in data_cols:
                df_view[f"E_{col}"] = df_view[col]*df_view['probability']

            df_view = df_view.drop(columns=data_cols)

            wind_views[wind_th] = df_view


    return wind_views

def create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member'):
    """Create tracks impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]

        tracks_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_envelopes_wth, zone_id_column=index_column)
        
        # Schools
        schools = tracks_viewer.map_points(points=gdf_schools)
        tracks_viewer.add_variable_to_view(schools, "severity_schools")

        # Health centers
        hcs = tracks_viewer.map_points(points=gdf_hcs)
        tracks_viewer.add_variable_to_view(hcs, "severity_hcs")

        # Tiles
        try:
            overlays = tracks_viewer.map_polygons(polygons=gdf_tiles, value_columns=["population","school_age_population","infant_population",'built_surface_m2'], aggregation="sum")
            tracks_viewer.add_variable_to_view(overlays['population'], "severity_population")
            tracks_viewer.add_variable_to_view(overlays['school_age_population'], "severity_school_age_population")
            tracks_viewer.add_variable_to_view(overlays['infant_population'], "severity_infant_population")
            tracks_viewer.add_variable_to_view(overlays['built_surface_m2'], "severity_built_surface_m2")
        except:
            zeros = {k: 0 for k in gdf_envelopes_wth[index_column].unique()}
            tracks_viewer.add_variable_to_view(zeros, "severity_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_school_age_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_infant_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_built_surface_m2")
        

        gdf_view = tracks_viewer.to_geodataframe()
        wind_views[wind_th] = gdf_view

    return wind_views

def save_school_view(gdf, country, storm, date, wind_th):
    """
    Saves school view
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name))

def save_hc_view(gdf, country, storm, date, wind_th):
    """
    Saves health center views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def save_hc_locations(gdf, country):
    """
    Saves health center locations
    """
    file_name = f"{country}_health_centers.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def load_hc_locations(country):
    """
    Loads health center locations
    """
    file_name = f"{country}_health_centers.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def hc_exist(country):
    file_name = f"{country}_health_centers.parquet"
    return data_store.file_exists(os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def create_admin_country_layer(country):
    """
    countries: list of iso3 codes of countries
    returns a geodataframe with the admin1 view
    """
    #### schools ####
    gdf_schools = GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
    # seems API is returning old column name...
    if 'giga_id_school' in gdf_schools.columns:
        gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
    #################
        
    #### health centers ####
    if hc_exist(country):
        gdf_hcs = load_hc_locations(country)
    else:
        try:
            gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
            if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
                print(f"    No health center data available for {country}, skipping...")
                # Create empty GeoDataFrame with proper geometry column for tracks processing
                gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
            else:
                gdf_hcs = gdf_hcs.set_crs(4326)
                save_hc_locations(gdf_hcs, country)
        except Exception as e:
            print(f"Error fetching health centers for {country}: {str(e)}")
            gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
    ########################

    tiles_viewer = AdminBoundariesViewGenerator(country=country,admin_level=1,data_store=data_store)
    try:
        tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="school_age_population", school_age=True, project="age_structures", un_adjusted=False, sex='F_M')
    except:
        # this should be ready in the next release
        print("No school age available")
        try:
            tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="school_age_population", predicate='centroid_within', school_age=False, project="age_structures", un_adjusted=False,min_age=5, max_age=15)
        except:
            print("No age ranges either")
            school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
            tiles_viewer.add_variable_to_view(school_age, 'school_age_population')

    try:
        tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="infant_population", predicate='centroid_within', school_age=False, project="age_structures", un_adjusted=False,min_age=0, max_age=4)
    except:
        print("No infant age ranges")
        school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
        tiles_viewer.add_variable_to_view(school_age, 'infant_population')

    tiles_viewer.map_wp_pop(country=country, resolution=100)
    tiles_viewer.map_built_s()
    tiles_viewer.map_smod()
    
    ## schools
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")

    ## health centers
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")

    # should we do RWI?
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except:
        print("Relative Wealth Index will be empty")
        rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
    
    tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)
    
    return gdf_tiles

def save_admin_view(gdf, country):
    """Save base admin1 infrastructure view for country"""
    file_name = f"{country}_admin1.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def save_admin_views(countries):
    """
    Generates and saves all country admin1 views
    """

    for country in countries:
        view = create_admin_country_layer(country)
        save_admin_view(view, country)

def save_tiles_view(gdf, country, storm, date, wind_th, zoom_level):
    """
    Saves tiles views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}_{zoom_level}.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def save_cci_tiles(gdf, country, storm, date, zoom_level):
    """
    Saves CCI tiles views
    """
    file_name = f"{country}_{storm}_{date}_{zoom_level}_cci.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def save_admin_tiles_view(gdf, country, storm, date, wind_th):
    """
    Saves admin tiles views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}_admin1.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def save_cci_admin(gdf, country, storm, date):
    """
    Saves CCI admin views
    """
    file_name = f"{country}_{storm}_{date}_admin1_cci.csv"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def load_admin_view(country):
    """Load admin view for country"""
    file_name = f"{country}_admin1.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'admin_views', file_name))

def save_tracks_view(gdf, country, storm, date, wind_th):
    """
    Saves tracks views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', file_name))


##################################################

cci_cols = ['CCI_children','CCI_pop','CCI_school_age','CCI_infants','E_CCI_children','E_CCI_pop','E_CCI_school_age','E_CCI_infants']    

def calculate_ccis(wind_tiles_views, gdf_tiles):
    d = gdf_tiles.set_index('tile_id')['id'].to_dict()
    winds = sorted(wind_tiles_views.keys())
    sorted_wind_views_indexed = []
    for wind in winds:
        df = wind_tiles_views[wind].copy().set_index('zone_id')
        sorted_wind_views_indexed.append(df)
    k = len(sorted_wind_views_indexed)
    gdf_tiles_index = gdf_tiles.rename(columns={'tile_id':'zone_id'}).set_index('zone_id')
    cci_tiles_view = pd.DataFrame(index=gdf_tiles_index.index)

    # Children cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'] + gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children']]

    # Children e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_school_age_population'] + sorted_wind_views_indexed[i]['E_infant_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'] + sorted_wind_views_indexed[i+1]['E_infant_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'] + sorted_wind_views_indexed[k-1]['E_infant_population'])
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_children'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children']]

    # school age cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['school_age_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_school_age'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age']]

    # school age e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_school_age_population']) - (sorted_wind_views_indexed[i+1]['E_school_age_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_school_age_population'])
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_school_age'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age']]

    # infant cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['infant_population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_infants'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants']]

    # infant e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_infant_population']) - (sorted_wind_views_indexed[i+1]['E_infant_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_infant_population'])
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_infants'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants']]

    # pop cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i]['probability']>0)  - (gdf_tiles_index['population'])*(sorted_wind_views_indexed[i+1]['probability']>0)
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (gdf_tiles_index['population'])*(sorted_wind_views_indexed[k-1]['probability']>0)
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_pop']]

    # pop e cci
    for i in range(k-1):
        wind = winds[i]
        cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[i]['E_population']) - (sorted_wind_views_indexed[i+1]['E_population'])
    wind = winds[-1]
    cci_tiles_view[f"{wind}"] = (sorted_wind_views_indexed[k-1]['E_population'])
    wcols = [cci_tiles_view[col]*math.pow(int(col),2)*math.pow(10,-6) for col in cci_tiles_view.columns if col not in cci_cols]
    cci_tiles_view['E_CCI_pop'] = sum(wcols)
    cci_tiles_view = cci_tiles_view[['CCI_children','E_CCI_children','CCI_school_age','E_CCI_school_age','CCI_infants','E_CCI_infants','CCI_pop','E_CCI_pop']]
    cci_tiles_view = cci_tiles_view.reset_index()

    cci_tiles_view['id'] = cci_tiles_view['zone_id'].map(d)
    
    return cci_tiles_view

##################################################

# =============================================================================

def create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes, zoom):
    """
    country: ios3 code of country
    storm: storm name
    date: date of the envelopes forecast
    gdf_envelopes: geodataframe with the forecasted envelopes
    **** Creates and saves the views
    """
    print(f"  Processing {country}...")
    
    # Schools
    print(f"    Processing schools...")
    gdf_schools = GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
    if 'giga_id_school' in gdf_schools.columns:
        gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
    
    wind_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes)
    for wind_th in wind_school_views:
        save_school_view(wind_school_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_school_views)} school views")

    # Health centers
    print(f"    Processing health centers...")
    if hc_exist(country):
        gdf_hcs = load_hc_locations(country)
        wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes)
        for wind_th in wind_hc_views:
            save_hc_view(wind_hc_views[wind_th], country, storm, date, wind_th)
    else:
        try:
            gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
            if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
                print(f"    No health center data available for {country}, skipping...")
                # Create empty GeoDataFrame with proper geometry column for tracks processing
                gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
                wind_hc_views = {}
            else:
                gdf_hcs = gdf_hcs.set_crs(4326)
                save_hc_locations(gdf_hcs, country)
                wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes)
                for wind_th in wind_hc_views:
                    save_hc_view(wind_hc_views[wind_th], country, storm, date, wind_th)
        except Exception as e:
            print(f"Error fetching health centers for {country}: {str(e)}")
            gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
            wind_hc_views = {}
    print(f"    Created {len(wind_hc_views)} health center views")

    # Tiles
    print(f"    Processing tiles...")
    try:
        gdf_tiles = load_mercator_view(country, zoom)
        print(f"    Loaded existing mercator tiles: {len(gdf_tiles)} tiles")
    except:
        print(f"    Creating base mercator tiles for {country}...")
        gdf_tiles = create_mercator_country_layer(country)
        save_mercator_view(gdf_tiles, country, zoom)
        print(f"    Created and saved base mercator tiles: {len(gdf_tiles)} tiles")
    
    wind_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes)
    for wind_th in wind_tiles_views:
        save_tiles_view(wind_tiles_views[wind_th], country, storm, date, wind_th, zoom)
    print(f"    Created {len(wind_tiles_views)} tile views")

    ### cci for tiles
    cci_tiles_view = calculate_ccis(wind_tiles_views, gdf_tiles)
    save_cci_tiles(cci_tiles_view, country, storm, date, zoom)
    #######

    # Admins
    print(f"    Processing admins...")
    try:
        gdf_admin = load_admin_view(country)
        print(f"    Loaded existing admin: {len(gdf_admin)} tiles")
    except:
        print(f"    Creating base admin for {country}...")
        gdf_admin = create_admin_country_layer(country)
        save_admin_view(gdf_admin, country)
        print(f"    Created and saved base admin tiles: {len(gdf_admin)} tiles")
    
    wind_admin_views = create_admin_view_from_envelopes_new(gdf_admin, gdf_tiles, gdf_envelopes)
    for wind_th in wind_admin_views:
        save_admin_tiles_view(wind_admin_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_admin_views)} admin views")

    ### cci for admin
    # Define aggregation dictionary
    agg_dict = {col: "sum" for col in sum_cols_cci}

    # Group by large_id and aggregate
    agg = cci_tiles_view.groupby("id").agg(agg_dict).reset_index()
    cci_admin_view = agg.rename(columns={'id':'zone_id'})
    ### add names ###
    #df_view['name'] = df_view['zone_id'].map(d)
    save_cci_admin(cci_admin_view, country, storm, date)
    #######

    # Tracks
    print(f"    Processing tracks...")
    wind_tracks_views = create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member')
    for wind_th in wind_tracks_views:
        save_tracks_view(wind_tracks_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_tracks_views)} track views")

    df_tracks = get_snowflake_tracks(date, storm)
    gdf_tracks = convert_to_geodataframe(df_tracks)

    json_report = do_report(wind_school_views, wind_hc_views, wind_tiles_views, wind_admin_views, cci_tiles_view, cci_admin_view, gdf_admin, gdf_tracks, country, storm, date)
    save_json_report(json_report, country, storm, date)

def load_envelopes_from_snowflake(storm, date):
    """Load envelope data directly from Snowflake"""
    # Convert date format if needed
    if len(date) == 14:  # YYYYMMDDHHMMSS format
        # Convert to datetime string format
        dt = pd.to_datetime(date, format="%Y%m%d%H%M%S")
        forecast_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        forecast_time = date
    
    try:
        # Get envelope data from Snowflake
        df_envelopes = get_envelopes_from_snowflake(storm, forecast_time)
        
        if df_envelopes.empty:
            print(f"Error: No envelope data found in Snowflake for {storm} at {forecast_time}")
            return pd.DataFrame()
        
        # Convert to GeoDataFrame
        gdf_envelopes = convert_envelopes_to_geodataframe(df_envelopes)
        return gdf_envelopes
        
    except Exception as e:
        print(f"Error loading envelopes from Snowflake: {str(e)}")
        return pd.DataFrame()