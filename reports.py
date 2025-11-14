import sys, os

import os
import sys
import argparse
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box, LineString
from pyproj import CRS
from shapely import union_all
from shapely import wkt as shapely_wkt
import math
from datetime import datetime,timedelta
import json

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

##### Environment Variables #####
# Application Configuration
RESULTS_DIR = os.getenv('RESULTS_DIR')
BBOX_FILE = os.getenv('BBOX_FILE')
STORMS_FILE = os.getenv('STORMS_FILE')
VIEWS_DIR = os.getenv('VIEWS_DIR')
ROOT_DATA_DIR = os.getenv('ROOT_DATA_DIR')
#################################

###### Dictionary for json files ####### -> we can move it to a config file
# storm_categories = {'34':'Tropical Storm','40':'Strong Tropical Storm','50':'Very Strong TS',
#                     '64':'Cat 1 Hurricane', '83':'Cat 2 Hurricane','96':'Cat 3 Hurricane',
#                     '113':'Cat 4 Hurricane', '137':'Cat 5 Hurricane'}
storm_categories = {34:'Tropical Storm',40:'Strong Tropical Storm',50:'Very Strong TS',
                    64:'Cat 1 Hurricane', 83:'Cat 2 Hurricane',96:'Cat 3 Hurricane',
                    113:'Cat 4 Hurricane', 137:'Cat 5 Hurricane'}
KEY_FOR_EXPECTED = 34
SEVERE = -1
POVERTY = -0.5


change_indicators = {'change_indicator_increase_large':"<span class='change-indicator change-increase-large'>+{change}</span></td>",
           'change_indicator_increase_medium':"<span class='change-indicator change-increase-medium'>+{change}</span></td>",
           'change_indicator_increase_small':"<span class='change-indicator change-increase-small'>+{change}</span></td>",
           'change_indicator_decrease_large':"<span class='change-indicator change-decrease-large'>{change}</span></td>",
           'change_indicator_decrease_medium':"<span class='change-indicator change-decrease-medium'>{change}</span></td>",
           'change_indicator_decrease_small':"<span class='change-indicator change-decrease-small'>{change}</span></td>",
}

row_admins_pop = """
            <tr>
                <td style="padding: 4px; font-weight: 600; max-width: 120px; word-wrap: break-word;">{admin_name}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_34} {change_pop_34}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_40} {change_pop_40}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_50} {change_pop_50}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_64} {change_pop_64}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_83} {change_pop_83}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_96} {change_pop_96}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_113} {change_pop_113}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_137} {change_pop_137}</td>
                <td style="text-align: center; padding: 4px; border-left: 2px solid #ccc;">{cci}</td>
            </tr>
"""

row_admins_pop_vulnerability = """
            <tr>
                <td style="padding: 4px; font-weight: 600; max-width: 120px; word-wrap: break-word;">{admin_name}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_poverty}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_severity}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_urban}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_rural}</td>
            </tr>
"""

row_poi_winds = """
            <tr>
                <td style="padding: 4px; font-weight: 600; max-width: 120px; word-wrap: break-word;">{admin_name}</td>
                <td style="text-align: center; padding: 4px;">{pois_34}</td>
                <td style="text-align: center; padding: 4px;">{pois_40}</td>
                <td style="text-align: center; padding: 4px;">{pois_50}</td>
                <td style="text-align: center; padding: 4px;">{pois_64}</td>
                <td style="text-align: center; padding: 4px;">{pois_83}</td>
                <td style="text-align: center; padding: 4px;">{pois_96}</td>
                <td style="text-align: center; padding: 4px;">{pois_113}</td>
                <td style="text-align: center; padding: 4px;">{pois_137}</td>
            </tr>
"""

d_report = {'storm':'', 'forecast_date':'', 'expected_landfall':'','storm_category':'','country':'', 'expected_children':0, 'expected_school_age':0,'expected_infants':0,
     'expected_schools':0,'expected_hcs':0,'children_change_direction':'','children_change':0,'children_change_perc':0,
     'expected_children_34':0,'change_children_34':'', 'expected_school_34':0,'change_school_34':'','expected_infant_34':0,'change_infant_34':'',
     'expected_children_40':0,'change_children_40':'', 'expected_school_40':0,'change_school_40':'','expected_infant_40':0,'change_infant_40':'',
     'expected_children_50':0,'change_children_50':'', 'expected_school_50':0,'change_school_50':'','expected_infant_50':0,'change_infant_50':'',
     'expected_children_64':0,'change_children_64':'', 'expected_school_64':0,'change_school_64':'','expected_infant_64':0,'change_infant_64':'',
     'expected_children_83':0,'change_children_83':'', 'expected_school_83':0,'change_school_83':'','expected_infant_83':0,'change_infant_83':'',
     'expected_children_96':0,'change_children_96':'', 'expected_school_96':0,'change_school_96':'','expected_infant_96':0,'change_infant_96':'',
     'expected_children_113':0,'change_children_113':'', 'expected_school_113':0,'change_school_113':'','expected_infant_113':0,'change_infant_113':'',
     'expected_children_137':0,'change_children_137':'', 'expected_school_137':0,'change_school_137':'','expected_infant_137':0,'change_infant_137':'',
     'rows_admins_pop_total':[],'rows_admins_school':[],'rows_admins_infant':[],
     'expected_pop_34':0,'expected_pop_40':0,'expected_pop_50':0,
     'expected_pop_64':0,'expected_pop_83':0,'expected_pop_96':0,'expected_pop_113':0,'expected_pop_137':0,'expected_cci_pop':0,
     'expected_cci_school':0,
     'expected_cci_infant':0,
     'expected_pop':0,
     'expected_pop_poverty':0,'expected_pop_severe':0,'expected_pop_urban':0,'expected_pop_rural':0,
     'expected_school_poverty':0,'expected_school_severe':0,'expected_school_urban':0,'expected_school_rural':0,
     'expected_infant_poverty':0,'expected_infant_severe':0,'expected_infant_urban':0,'expected_infant_rural':0,
     'expected_schools_34':0,'change_schools_34':'','expected_hcs_34':0,'change_hcs_34':'',
     'expected_schools_40':0,'change_schools_40':'','expected_hcs_40':0,'change_hcs_40':'',
     'expected_schools_50':0,'change_schools_50':'','expected_hcs_50':0,'change_hcs_50':'',
     'expected_schools_64':0,'change_schools_64':'','expected_hcs_64':0,'change_hcs_64':'',
     'expected_schools_83':0,'change_schools_83':'','expected_hcs_83':0,'change_hcs_83':'',
     'expected_schools_96':0,'change_schools_96':'','expected_hcs_96':0,'change_hcs_96':'',
     'expected_schools_113':0,'change_schools_113':'','expected_hcs_113':0,'change_hcs_113':'',
     'expected_schools_137':0,'change_schools_137':'','expected_hcs_137':0,'change_hcs_137':'',
     'rows_schools_winds':[],'rows_hcs_winds':[],
     'school_name_1':'','school_edulevel_1':'','school_prob_1':0,
     'school_name_2':'','school_edulevel_2':'','school_prob_2':0,'school_name_3':'','school_edulevel_3':'','school_prob_3':0,
     'school_name_4':'','school_edulevel_4':'','school_prob_4':0,'school_name_5':'','school_edulevel_5':'','school_prob_5':0,
     'hc_name_1':'','hc_type_1':'','hc_prob_1':0,
     'hc_name_2':'','hc_type_2':'','hc_prob_2':0,'hc_name_3':'','hc_type_3':'','hc_prob_3':0,
     'hc_name_4':'','hc_type_4':'','hc_prob_4':0,'hc_name_5':'','hc_type_5':'','hc_prob_5':0,
     'next_forecast_date':'','report_date':''     
     }
########################################

# Initialize data store using centralized utility
data_store = get_data_store()

def save_json_report(d, country, storm, date):
    """
    Save json file with report values
    """
    if len(d)==0: #if empty do not save file
        return
    
    file=f"{country}_{storm}_{date}.json"
    filename = os.path.join(RESULTS_DIR, 'jsons', file)
    write_dataset(d, data_store, filename)

def load_json_report(country, storm, date):
    """
    Read json file with saved storm,dates
    """
    file = f"{country}_{storm}_{date}.json"
    filename = os.path.join(RESULTS_DIR, 'jsons', file)
    if data_store.file_exists(filename):
        with data_store.open(filename, "r") as f:
            d = json.load(f)
        return d
    return {}

def get_previous_date(date):
    # Parse into datetime object
    dt = datetime.strptime(date, "%Y%m%d%H%M%S")

    # Subtract 6 hours
    dt_minus_6h = dt - timedelta(hours=6)

    # Format back to same string format
    return dt_minus_6h.strftime("%Y%m%d%H%M%S")

def get_future_date(date,delta):
    # Parse into datetime object
    dt = datetime.strptime(date, "%Y%m%d%H%M%S")

    # Subtract 6 hours
    dt_plus_delta = dt + timedelta(hours=delta)

    # Format back to same string format
    return dt_plus_delta.strftime("%B %d, %Y %H:%M UTC")    

def get_lines_from_points(gdf):
    lines = []

    for i in range(len(gdf) - 1):
        point1 = gdf.geometry.iloc[i]
        point2 = gdf.geometry.iloc[i + 1]
        
        # Create a LineString from point i to i+1
        line = LineString([point1, point2])
        
        # Copy attributes from row i
        row_data = gdf.iloc[i].drop("geometry").to_dict()
        row_data["geometry"] = line
        
        lines.append(row_data)

    # Convert to GeoDataFrame
    return gpd.GeoDataFrame(lines, crs=gdf.crs)

def get_expected_landfall(gdf_tracks,date,country):

    if gdf_tracks.empty:
        return "Unknown"
    
    gdf_det = gdf_tracks[gdf_tracks.ENSEMBLE_MEMBER==51]

    if gdf_det.empty:
        return "Unknown"
    
    boundary = AdminBoundaries.create(country_code=country,admin_level=0)

    polygon = boundary.to_geodataframe().geometry.iloc[0]

    inside_rows = gdf_det[gdf_det.within(polygon)]

    # Filter and get the first matching row
    if inside_rows.empty:
        #it could be that the points land outside the country because it moves inside and out by less than 6 hours
        gdf_det_lines = get_lines_from_points(gdf_det)
        inside_rows_lines = gdf_det_lines[gdf_det_lines.intersects(polygon)]
        if inside_rows_lines.empty:
            return "Unknown"
        
        lead_time = int(inside_rows_lines.iloc[0]["LEAD_TIME"])

        if lead_time == 0:
            return "Already landed"
        return get_future_date(date,lead_time)
    
    lead_time = int(inside_rows.iloc[0]["LEAD_TIME"])

    if lead_time == 0:
        return "Already landed"

    return get_future_date(date,lead_time)

def do_report(wind_school_views, wind_hc_views, wind_tiles_views, wind_admin_views, cci_tiles_view, cci_admin_view, gdf_admin, gdf_tracks, country, storm, date):
    # Don't do a report if nothing is impacted
    max_wind = 0
    for wind in storm_categories.keys():
        if wind not in wind_admin_views:
            continue  # Skip wind thresholds that don't exist in the views
        if wind_admin_views[wind].probability.sum()>0:
            max_wind = wind
        else:
            break #since they are ordered and envelopes overlap, the moment prob = 0 you already have the max wind
    if max_wind==0:
        return {}
    
    #Get previous report if it exists
    previous_date = get_previous_date(date)
    d_previous = load_json_report(country,storm,previous_date)

    # Use KEY_FOR_EXPECTED if available, otherwise use the minimum wind threshold that exists
    if not wind_tiles_views or len(wind_tiles_views) == 0:
        return {}  # No views available, return empty report
    if KEY_FOR_EXPECTED in wind_tiles_views:
        expected_wind = KEY_FOR_EXPECTED
    else:
        if len(wind_tiles_views.keys()) == 0:
            return {}  # No keys available
        expected_wind = min(wind_tiles_views.keys())

    # Start
    d = {'country':country, 'storm': storm, 'forecast_date':datetime.strptime(date, "%Y%m%d%H%M%S").strftime("%B %d, %Y %H:%M UTC"),'storm_category':storm_categories[max_wind]}
    d['expected_landfall'] = get_expected_landfall(gdf_tracks,date,country)
    d['next_forecast_date'] = get_future_date(date,6)
    d['report_date'] = datetime.today().date().strftime("%B %d, %Y %H:%M UTC")

    #expected totals
    d['expected_school_age'] = int(wind_tiles_views[expected_wind]['E_school_age_population'].sum())
    d['expected_infants'] = int(wind_tiles_views[expected_wind]['E_infant_population'].sum())
    d['expected_children'] = int(d['expected_school_age'] + d['expected_infants'])
    d['expected_pop'] = int(wind_tiles_views[expected_wind]['E_population'].sum())
    d['expected_schools'] = int(wind_tiles_views[expected_wind]['E_num_schools'].sum())
    d['expected_hcs'] = int(wind_tiles_views[expected_wind]['E_num_hcs'].sum())
    d['expected_cci_pop'] = int(cci_tiles_view['E_CCI_pop'].sum())
    d['expected_cci_school'] = int(cci_tiles_view['E_CCI_school_age'].sum())
    d['expected_cci_infant'] = int(cci_tiles_view['E_CCI_infants'].sum())
      
    # children change
    if len(d_previous)==0: #first 
        d['children_change_direction'] = 'increased'
        d['children_change'] = f"+{d['expected_children']}"
        d['children_change_perc'] = "-"
    else:
        new_expected_children = d['expected_children'] - d_previous['expected_children']
        if new_expected_children>0:
            d['children_change_direction'] = 'increased'
            d['children_change'] = f"+{new_expected_children}"
            d['children_change_perc'] = new_expected_children/d_previous['expected_children']*100
        else:
            d['children_change_direction'] = 'decreased'
            d['children_change'] = f"{new_expected_children}"
            d['children_change_perc'] = abs(new_expected_children)/d_previous['expected_children']*100

    #expected by wind
    for wind in storm_categories.keys():
        if wind not in wind_tiles_views:
            continue  # Skip wind thresholds that don't exist in the views
        d[f"expected_pop_{wind}"] = int(wind_tiles_views[wind]['E_population'].sum())
        d[f"expected_school_{wind}"] = int(wind_tiles_views[wind]['E_school_age_population'].sum())
        d[f"expected_infant_{wind}"] = int(wind_tiles_views[wind]['E_infant_population'].sum())
        d[f"expected_children_{wind}"] = int(d[f"expected_school_{wind}"] + d[f"expected_infant_{wind}"])
        d[f"expected_schools_{wind}"] = int(wind_tiles_views[wind]['E_num_schools'].sum())
        d[f"expected_hcs_{wind}"] = int(wind_tiles_views[wind]['E_num_hcs'].sum())
        if len(d_previous) == 0:
            d[f"change_school_{wind}"] = d[f"expected_school_{wind}"]
            d[f"change_infant_{wind}"] = d[f"expected_infant_{wind}"]
            d[f"change_children_{wind}"] = d[f"expected_children_{wind}"]
            d[f"change_schools_{wind}"] = d[f"expected_schools_{wind}"]
            d[f"change_hcs_{wind}"] = d[f"expected_hcs_{wind}"]
        else:
            d[f"change_school_{wind}"] = d[f"expected_school_{wind}"] - d_previous[f"expected_school_{wind}"]
            d[f"change_infant_{wind}"] = d[f"expected_infant_{wind}"] - d_previous[f"expected_infant_{wind}"]
            d[f"change_children_{wind}"] = d[f"expected_children_{wind}"] - d_previous[f"expected_children_{wind}"]
            d[f"change_schools_{wind}"] = d[f"expected_schools_{wind}"] - d_previous[f"expected_schools_{wind}"]
            d[f"change_hcs_{wind}"] = d[f"expected_hcs_{wind}"] - d_previous[f"expected_hcs_{wind}"]

    #Facilites at risk
    if not wind_school_views:
        top5_schools = pd.DataFrame()  # Empty dataframe if no school views
    else:
        expected_wind_schools = KEY_FOR_EXPECTED if KEY_FOR_EXPECTED in wind_school_views else min(wind_school_views.keys())
        top5_schools = wind_school_views[expected_wind_schools].sort_values(by="probability", ascending=False).head(5)
    i = 1
    for index, row in top5_schools.iterrows():
        d[f"school_name_{i}"] = row['school_name']
        d[f"school_edulevel_{i}"] = row['education_level']
        d[f"school_prob_{i}"] = row['probability']
        i+=1

    #Facilites at risk
    if not wind_hc_views:
        top5_hcs = pd.DataFrame()  # Empty dataframe if no HC views
    else:
        expected_wind_hcs = KEY_FOR_EXPECTED if KEY_FOR_EXPECTED in wind_hc_views else min(wind_hc_views.keys())
        top5_hcs = wind_hc_views[expected_wind_hcs].sort_values(by="probability", ascending=False).head(5)
    i = 1
    for index, row in top5_hcs.iterrows():
        d[f"hc_name_{i}"] = row['name']
        d[f"hc_type_{i}"] = row['healthcare']
        d[f"hc_prob_{i}"] = row['probability']
        i+=1

    # vulnerabilities
    tiles_for_smod = wind_tiles_views[expected_wind].dropna(subset=['E_smod_class'])
    if tiles_for_smod.empty:
        d['expected_pop_urban'] = 0
        d['expected_pop_rural'] = 0
        d['expected_school_urban'] = 0
        d['expected_school_rural'] = 0
        d['expected_infant_urban'] = 0
        d['expected_infant_rural'] = 0
    else:
        urban_tiles = tiles_for_smod[(tiles_for_smod.probability>0)&(tiles_for_smod.E_smod_class/tiles_for_smod.probability>=20)] #trick to not have to merge with gdf
        rural_tiles = tiles_for_smod[(tiles_for_smod.probability>0)&(tiles_for_smod.E_smod_class/tiles_for_smod.probability<20)]
        if not urban_tiles.empty:
            d['expected_pop_urban'] = int(urban_tiles['E_population'].sum())
            d['expected_school_urban'] = int(urban_tiles['E_school_age_population'].sum())
            d['expected_infant_urban'] = int(urban_tiles['E_infant_population'].sum())
        else:
            d['expected_pop_urban'] = 0
            d['expected_school_urban'] = 0
            d['expected_infant_urban'] = 0
        if not rural_tiles.empty:
            d['expected_pop_rural'] = int(rural_tiles['E_population'].sum())
            d['expected_school_rural'] = int(rural_tiles['E_school_age_population'].sum())
            d['expected_infant_rural'] = int(rural_tiles['E_infant_population'].sum())
        else:
            d['expected_pop_rural'] = 0
            d['expected_school_rural'] = 0
            d['expected_infant_rural'] = 0

    tiles_for_rwi = wind_tiles_views[expected_wind].dropna(subset=['E_rwi'])
    if tiles_for_rwi.empty:
        d['expected_pop_poverty'] = 0
        d['expected_pop_severe'] = 0
        d['expected_school_poverty'] = 0
        d['expected_school_severe'] = 0
        d['expected_infant_poverty'] = 0
        d['expected_infant_severe'] = 0
    else:
        poverty_tiles = tiles_for_rwi[(tiles_for_rwi.probability>0)&(tiles_for_rwi.E_rwi/tiles_for_rwi.probability>=SEVERE)&(tiles_for_rwi.E_rwi/tiles_for_rwi.probability<POVERTY)]
        severe_tiles = tiles_for_rwi[(tiles_for_rwi.probability>0)&(tiles_for_rwi.E_rwi/tiles_for_rwi.probability<SEVERE)]
        if not poverty_tiles.empty:
            d['expected_pop_poverty'] = int(poverty_tiles['E_population'].sum())
            d['expected_school_poverty'] = int(poverty_tiles['E_school_age_population'].sum())
            d['expected_infant_poverty'] = int(poverty_tiles['E_infant_population'].sum())
        else:
            d['expected_pop_poverty'] = 0
            d['expected_school_poverty'] = 0
            d['expected_infant_poverty'] = 0
        if not severe_tiles.empty:
            d['expected_pop_severe'] = int(severe_tiles['E_population'].sum())
            d['expected_school_severe'] = int(severe_tiles['E_school_age_population'].sum())
            d['expected_infant_severe'] = int(severe_tiles['E_infant_population'].sum())
        else:
            d['expected_pop_severe'] = 0
            d['expected_school_severe'] = 0
            d['expected_infant_severe'] = 0
    
    # row admins pop
    rows_admins_pop_total = []
    rows_admins_school = []
    rows_admins_infant = []
    rows_schools_winds = []
    rows_hcs_winds = []

    i = 0
    for _,row in gdf_admin.iterrows():
        d_rows_admins_pop_total = {'name':row['name']}
        d_rows_admins_school = {'name':row['name']}
        d_rows_admins_infant = {'name':row['name']}
        d_rows_schools_winds = {'name':row['name']}
        d_rows_hcs_winds = {'name':row['name']}

        admin_id = row['tile_id']
        for wind in storm_categories.keys():
            if wind not in wind_admin_views:
                # Include all wind thresholds, set to 0 if not present in views
                d_rows_admins_pop_total[f"{wind}"] = 0
                d_rows_admins_school[f"{wind}"] = 0
                d_rows_admins_infant[f"{wind}"] = 0
                d_rows_schools_winds[f"{wind}"] = 0
                d_rows_hcs_winds[f"{wind}"] = 0
            else:
                d_rows_admins_pop_total[f"{wind}"] = int(wind_admin_views[wind][wind_admin_views[wind].zone_id==admin_id]['E_population'].sum())
                d_rows_admins_school[f"{wind}"] = int(wind_admin_views[wind][wind_admin_views[wind].zone_id==admin_id]['E_school_age_population'].sum())
                d_rows_admins_infant[f"{wind}"] = int(wind_admin_views[wind][wind_admin_views[wind].zone_id==admin_id]['E_infant_population'].sum())
                d_rows_schools_winds[f"{wind}"] = int(wind_admin_views[wind][wind_admin_views[wind].zone_id==admin_id]['E_num_schools'].sum())
                d_rows_hcs_winds[f"{wind}"] = int(wind_admin_views[wind][wind_admin_views[wind].zone_id==admin_id]['E_num_schools'].sum())
            if len(d_previous) == 0:
                d_rows_admins_pop_total[f"change_{wind}"] = d_rows_admins_pop_total[f"{wind}"]
                d_rows_admins_school[f"change_{wind}"] = d_rows_admins_school[f"{wind}"]
                d_rows_admins_infant[f"change_{wind}"] = d_rows_admins_infant[f"{wind}"]
                #d_rows_schools_winds[f"change_{wind}"] = d_rows_schools_winds[f"{wind}"]
                #d_rows_hcs_winds[f"change_{wind}"] = d_rows_hcs_winds[f"{wind}"]
            else:
                prev_pop = d_previous['rows_admins_pop_total'][i].get(f"{wind}", 0) if i < len(d_previous['rows_admins_pop_total']) else 0
                prev_school = d_previous['rows_admins_school'][i].get(f"{wind}", 0) if i < len(d_previous['rows_admins_school']) else 0
                prev_infant = d_previous['rows_admins_infant'][i].get(f"{wind}", 0) if i < len(d_previous['rows_admins_infant']) else 0
                d_rows_admins_pop_total[f"change_{wind}"] = d_rows_admins_pop_total[f"{wind}"] - prev_pop
                d_rows_admins_school[f"change_{wind}"] = d_rows_admins_school[f"{wind}"] - prev_school
                d_rows_admins_infant[f"change_{wind}"] = d_rows_admins_infant[f"{wind}"] - prev_infant
                #d_rows_schools_winds[f"change_{wind}"] = d_rows_schools_winds[f"{wind}"] - d_previous['rows_schools_winds'][i][f"{wind}"]
                #d_rows_hcs_winds[f"change_{wind}"] = d_rows_hcs_winds[f"{wind}"] - d_previous['rows_hcs_winds'][i][f"{wind}"]
        
        d_rows_admins_pop_total["cci"] = int(cci_admin_view[cci_admin_view.zone_id==admin_id]['E_CCI_pop'].sum())
        d_rows_admins_school["cci"] = int(cci_admin_view[cci_admin_view.zone_id==admin_id]['E_CCI_school_age'].sum())
        d_rows_admins_infant["cci"] = int(cci_admin_view[cci_admin_view.zone_id==admin_id]['E_CCI_infants'].sum())

        rows_admins_pop_total.append(d_rows_admins_pop_total)
        rows_admins_school.append(d_rows_admins_school)
        rows_admins_infant.append(d_rows_admins_infant)
        rows_schools_winds.append(d_rows_schools_winds)
        rows_hcs_winds.append(d_rows_hcs_winds)
        i += 1
    
    d['rows_admins_pop_total'] = rows_admins_pop_total
    d['rows_admins_school'] = rows_admins_school
    d['rows_admins_infant'] = rows_admins_infant
    d['rows_schools_winds'] = rows_schools_winds
    d['rows_hcs_winds'] = rows_hcs_winds
            
   

    # validate
    for key in d_report.keys():
        if key not in d:
            print(f"{key} has not been calculated")

    for key in d.keys():
        if key not in d_report:
            print(f"{key} is not in template but has been calculated") 
    

    return d

