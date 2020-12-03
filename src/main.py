from bokeh.models import Column, Div, Row
from bokeh.plotting import show

from data_processing import db_manager
from visualization import monthly_user_map, monthly_user_plot

engine = db_manager.get_sql_engine()

# Generate the map plot
total_monthly_users_by_country = db_manager.get_total_monthly_users_by_country(engine)
total_new_monthly_users_by_country = db_manager.get_total_new_monthly_users_by_country(engine)
[map_row, widget_row] = monthly_user_map.plot_totals(total_monthly_users_by_country, total_new_monthly_users_by_country)

# Generate the bar chart plot
total_monthly_users = db_manager.get_total_monthly_users(engine)
total_returning_users = db_manager.get_total_returning_monthly_users(engine)
monthly_bar_plot_figure = monthly_user_plot.plot_totals(total_monthly_users, total_returning_users)
monthly_bar_plot_figure.sizing_mode="stretch_width"

# Layout and display
show(Column(
      Row(Div(text='<strong>Users by Country</strong>')),
      widget_row,
      map_row,
      Row(Div(text='<strong>Users by Month</strong>', margin=(25, 0, 0, 0))),
      monthly_bar_plot_figure
    ))
