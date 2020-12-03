import pandas as pd
from bokeh.models import ColumnDataSource, HoverTool, SingleIntervalTicker
from bokeh.plotting import figure, output_file, show


def plot_totals(total_monthly_users: pd.DataFrame, total_returning_users: pd.DataFrame) -> figure:
  '''
    Plot all user totals by month and return the plot.
  '''
  p = figure(toolbar_location = None)
  p.xaxis.ticker = SingleIntervalTicker(interval=1)

  total_monthly_users_source = ColumnDataSource(total_monthly_users)
  bar_renderer = p.vbar(x = 'month', top = 'count', width = 0.5, legend_label = 'Total Users', source = total_monthly_users_source)
  bar_hover = HoverTool(tooltips=[('Total User Count', '@count')], renderers=[bar_renderer])
  p.add_tools(bar_hover)

  total_returning_users_source = ColumnDataSource(total_returning_users)
  p.line(x = 'month', y = 'count', color = 'black', source = total_returning_users_source)

  circle_renderer = p.circle(x = 'month', y = 'count', size=10, color = 'black', hover_color = 'red', legend_label = 'Returning Users', source = total_returning_users_source)
  circle_hover = HoverTool(tooltips=[('Returning User Count', '@count')], renderers=[circle_renderer])
  p.add_tools(circle_hover)
  return p
