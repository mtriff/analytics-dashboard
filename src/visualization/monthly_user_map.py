import datetime
import json
import locale
import os
from typing import List, Tuple

import geopandas as gpd
import pandas as pd
from bokeh.io import output_file, output_notebook, show
from bokeh.models import (ColorBar, CustomJS, GeoJSONDataSource, HoverTool,
                          LinearColorMapper, Panel, RadioButtonGroup, Row,
                          Select, Tabs)
from bokeh.palettes import brewer
from bokeh.plotting import figure


def plot_map(user_counts: pd.DataFrame, month_to_show: int, color_palette: str) -> figure:
  '''
    Plots the number of users per country on a world map
    - user_counts contains the total number of users, per country, per month
    - month_to_show is the month of data from user_counts that should be plotted
    - color_palette is the color palette for drawing the user counts
  '''
  script_directory = os.path.dirname(__file__)
  shapefile = os.path.join(script_directory, '../../data/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp')

  gdf = gpd.read_file(shapefile)[['ADMIN', 'ISO_A2', 'geometry']]
  gdf.columns = ['country', 'country_code', 'geometry']
  gdf[gdf['country'] == 'Antarctica'].index
  gdf = gdf.drop(gdf[gdf['country'] == 'Antarctica'].index) # No data and clutters the plot

  user_month_data = user_counts[user_counts['month'] == month_to_show]
  data_gdf = gdf.merge(user_month_data, left_on = 'country_code', right_on = 'country', how = 'left')
  data_gdf['count'].fillna(0, inplace = True)

  json_data = json.dumps(json.loads(data_gdf.to_json()))
  geosource = GeoJSONDataSource(geojson = json_data)
  palette = brewer[color_palette][8]
  palette = palette[::-1] # More users = darker colours
  color_mapper = LinearColorMapper(palette = palette, low = 0, high = user_month_data['count'].max())

  hover = HoverTool(tooltips=[('Country', '@country_x'), ('Total User Count', '@count')])

  p = figure(plot_height = 600 , plot_width = 1150, toolbar_location = None, tools = [hover])
  p.patches('xs','ys', source = geosource,fill_color = {'field' :'count', 'transform' : color_mapper},
            line_color = 'black', line_width = 0.25, fill_alpha = 1)
  return p

def plot_all_month_maps(user_counts: pd.DataFrame, color_palette: str) -> [List[figure], List[Tuple[str, str]]]:
  '''
    Plot each month on an individual map
    - user_counts contains the total number of users, per country, per month
    - color_palette is the color palette for drawing the user counts
  '''
  maps = []
  map_labels = []
  for month in user_counts['month'].unique():
    month_name = datetime.date(1900, month, 1).strftime('%B')
    map_plot = plot_map(user_counts, month, color_palette)
    map_plot.visible = False
    maps.append(map_plot)
    map_labels.append((str(len(map_labels)), month_name))
  return [maps, map_labels]

def plot_total_monthly_users(total_monthly_users_by_country: pd.DataFrame) -> [List[figure], List[Tuple[str, str]]]:
  '''
    Helper function to draw the total monthly users by country with the correct palette
  '''
  return plot_all_month_maps(total_monthly_users_by_country, 'Blues')

def plot_total_new_monthly_users(total_new_monthly_users_by_country: pd.DataFrame) -> [List[figure], List[Tuple[str, str]]]:
  '''
    Helper function to draw the total new monthly users by country with the correct palette
  '''
  return plot_all_month_maps(total_new_monthly_users_by_country, 'Oranges')

def get_plot_widget_row(total_monthly_users_by_country_maps: List[figure], total_new_monthly_users_by_country_maps: List[figure], map_labels: List[Tuple[str, str]]) -> Row:
  '''
    Creates two widgets that jointly control which map plot is displayed.
    A dropdown menu selects which month of data will be shown.
    A radio button group selects which type of data will be shown (i.e. all users vs new users)
  '''
  active_maps = 0
  active_month = '0'

  callback = CustomJS(args = dict(
                                  active_maps = active_maps, 
                                  active_month = active_month, 
                                  total_maps = total_monthly_users_by_country_maps, 
                                  new_maps = total_new_monthly_users_by_country_maps), 
                                  code = """
                                          // Initial setup for shared vars
                                          // Need to use global vars to share state between multiple widgets
                                          if (!window.active_maps) {
                                            window.active_maps = active_maps
                                          }
                                          if (!window.active_month) {
                                            window.active_month = active_month
                                          }
                                          if (cb_obj.value !== undefined) {
                                            // Changing map type displayed
                                            if (window.active_maps == 0) {
                                              total_maps[window.active_month].visible = false;
                                              total_maps[cb_obj.value].visible = true;
                                            } else if (window.active_maps == 1) {
                                              new_maps[window.active_month].visible = false;
                                              new_maps[cb_obj.value].visible = true;
                                            }
                                            window.active_month = cb_obj.value;
                                          } else if (cb_obj.active !== undefined) {
                                            // Changing month displayed
                                            if (cb_obj.active == 0) {
                                              total_maps[window.active_month].visible = true
                                              new_maps[window.active_month].visible = false;
                                            } else if (cb_obj.active == 1) {
                                              new_maps[window.active_month].visible = true; 
                                              total_maps[window.active_month].visible = false;
                                            }
                                            window.active_maps = cb_obj.active;
                                          }
                                          """
                                  )

  radiogroup = RadioButtonGroup(labels = ["Total Users", "New Users"], active = active_maps)
  radiogroup.js_on_click(callback)

  select = Select(value = active_month, options=map_labels)
  select.js_on_change('value', callback)

  widget_row = Row(select, radiogroup)
  return widget_row

def plot_totals(total_monthly_users_by_country: pd.DataFrame, total_new_monthly_users_by_country: pd.DataFrame) -> [Row, Row]:
  '''
    Plot all users by country on a map and return layout components with the maps and their associated control widgets.
  '''
  [total_monthly_users_by_country_maps, map_labels] = plot_total_monthly_users(total_monthly_users_by_country)
  [total_new_monthly_users_by_country_maps, _] = plot_total_new_monthly_users(total_new_monthly_users_by_country)
  total_monthly_users_by_country_maps[0].visible = True

  map_row = Row(*total_monthly_users_by_country_maps, *total_new_monthly_users_by_country_maps)
  return [map_row, get_plot_widget_row(total_monthly_users_by_country_maps, total_new_monthly_users_by_country_maps, map_labels)]
