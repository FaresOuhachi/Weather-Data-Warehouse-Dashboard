import random

import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from dash import Dash, dcc, html, Input, Output
from pymysql.cursors import DictCursor

from Model import DataWarehouseManager

external_stylesheets = ['https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css']

# Initialize Dash app
app = Dash(__name__, external_stylesheets=external_stylesheets)


def connect_to_data_warehouse():
    warehouse = DataWarehouseManager('localhost', 'root', '', 'Weather_DataWarehouse', 'utf8mb4', DictCursor)
    warehouse.connect()
    return warehouse


# Load data (Third Layout)


def fetch_years():
    cursor = connect_to_data_warehouse().get_cursor()
    query = "SELECT year FROM DateDim"
    cursor.execute(query)
    years = cursor.fetchall()
    years_list = sorted(set(year['year'] for year in years))
    # Generate a list of years with steps of 5
    return [year for year in years_list if year % 5 == 0]


# First Layout (Heatmap)
def fetch_data_heatmap(parameter, year_range):
    cursor = connect_to_data_warehouse().get_cursor()
    min_year, max_year = year_range
    if parameter in ['PRCP', 'TAVG', 'SNWD', 'PGTM', 'SNOW', 'WDFG', 'WSFG']:
        query = f"""
                SELECT s.latitude, s.longitude,s.station_city,s.station_country,s.station_code, ROUND(AVG(w.{parameter.lower()}),2) as average_value
                FROM WeatherFact w
                JOIN StationDim s ON w.station_id = s.station_id
                JOIN DateDim d ON w.date_id = d.date_id
                WHERE d.year BETWEEN {min_year} AND {max_year}
                GROUP BY s.station_code
                """
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['latitude', 'longitude', 'station_city', 'station_country', 'station_code',
                                         'average_value'])
        return df

    elif parameter in ['TMIN']:
        query = f"""
                SELECT s.latitude, s.longitude, ROUND(MIN(w.{parameter.lower()}),2) as average_value
                FROM WeatherFact w
                JOIN StationDim s ON w.station_id = s.station_id
                JOIN DateDim d ON w.date_id = d.date_id
                WHERE d.year BETWEEN {min_year} AND {max_year}
                GROUP BY s.latitude, s.longitude
                """
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['latitude', 'longitude', 'average_value'])
        return df
    elif parameter in ['TMAX']:
        query = f"""
                SELECT s.latitude, s.longitude, ROUND(MAX(w.{parameter.lower()}),2) as average_value
                FROM WeatherFact w
                JOIN StationDim s ON w.station_id = s.station_id
                JOIN DateDim d ON w.date_id = d.date_id
                WHERE d.year BETWEEN {min_year} AND {max_year}
                GROUP BY s.latitude, s.longitude
                """
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['latitude', 'longitude', 'average_value'])
        return df
    else:
        return pd.DataFrame()


@app.callback(Output('heatmap', 'figure'), [Input('parameter-dropdown', 'value'), Input('year-range-slider', 'value')])
def update_heatmap(parameter, year_range):
    df = fetch_data_heatmap(parameter, year_range)  # Fetch data based on selected parameter and year range
    if not df.empty:
        # Define a scaling factor based on your data
        max_value = df['average_value'].max()
        min_value = df['average_value'].min()
        scaling_factor = 200 / (max_value - min_value)
        # Apply the scaling factor to the size column
        df['marker_size'] = (df['average_value'] - min_value) * scaling_factor

        fig = px.scatter_mapbox(df, lat="latitude", lon="longitude", color="average_value",
                                color_continuous_scale=px.colors.sequential.Bluered, hover_name="average_value", zoom=3,
                                mapbox_style="carto-positron", size="marker_size")
        # Set the color bar title to a more significant name
        fig.update_coloraxes(colorbar_title="Weather Parameter Value")
    else:
        fig = px.scatter_mapbox()
    return fig


# Second Layout (Bar Chart)
def fetch_data_barchart(year_range, parameter):
    cursor = connect_to_data_warehouse().get_cursor()
    min_year, max_year = year_range
    if parameter in ['PRCP', 'TAVG', 'SNWD', 'PGTM', 'SNOW', 'WDFG', 'WSFG']:
        query = f"""
                SELECT s.station_city, AVG(w.{parameter.lower()}) as average_value
                FROM WeatherFact w
                JOIN StationDim s ON w.station_id = s.station_id
                JOIN DateDim d ON w.date_id = d.date_id
                WHERE d.year BETWEEN {min_year} AND {max_year}
                GROUP BY s.station_city
                """
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['station_city', 'average_value'])
        return df
    elif parameter in ['TMIN']:
        query = f"""
                SELECT s.station_city, MIN(w.{parameter.lower()}) as average_value
                FROM WeatherFact w
                JOIN StationDim s ON w.station_id = s.station_id
                JOIN DateDim d ON w.date_id = d.date_id
                WHERE d.year BETWEEN {min_year} AND {max_year}
                GROUP BY s.station_city
                """
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['station_city', 'average_value'])
        return df
    elif parameter in ['TMAX']:
        query = f"""
                SELECT s.station_city, MAX(w.{parameter.lower()}) as average_value
                FROM WeatherFact w
                JOIN StationDim s ON w.station_id = s.station_id
                JOIN DateDim d ON w.date_id = d.date_id
                WHERE d.year BETWEEN {min_year} AND {max_year}
                GROUP BY s.station_city
                """
        cursor.execute(query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['station_city', 'average_value'])
        return df
    else:
        return pd.DataFrame()


@app.callback(Output('bar-chart', 'figure'),
              [Input('year-range-slider-bar', 'value'), Input('parameter-dropdown-bar', 'value')])
def update_bar_chart(year_range, parameter):
    df = fetch_data_barchart(year_range, parameter)  # Fetch data based on selected year range and parameter
    if not df.empty:
        # Choose a random color from the list for each bar
        specific_colors = ['#F4F1DE', '#E07A5F', '#3D405B', '#81B29A', '#F2CC8F', '#e63946', '#f1faee', '#a8dadc',
                           '#a8dadc', '#457b9d', '#fca311', '#e5989b', '#ffb4a2', '#ffcdb2', '#b5838d', '#b5e2fa',
                           '#f7a072', '#faf0ca']
        colors = [random.choice(specific_colors) for _ in range(len(df))]
        fig = go.Figure(go.Bar(x=df['station_city'], y=df['average_value'], marker_color=colors))
        fig.update_layout(title=f'Données moyennes par ville pour {parameter} en {year_range[0]}-{year_range[1]}',
                          xaxis_title='Ville', yaxis_title='Valeur moyenne',
                          yaxis=dict(range=[0, df['average_value'].max() * 1.5]))  # Ajustement de l'échelle de l'axe y
    else:
        fig = go.Figure()
    return fig


# Third Layout (Line Chart)
def load_line_data():
    cursor = connect_to_data_warehouse().get_cursor()
    # Fetch data from the database (example query)
    query = """
    SELECT YEAR, PRCP, TAVG, TMAX, TMIN, SNWD, PGTM, SNOW, WDFG, WSFG, StationDim.station_city
    FROM WeatherFact
    JOIN DateDim ON WeatherFact.date_id = DateDim.date_id
    JOIN StationDim ON WeatherFact.station_id = StationDim.station_id
    """
    cursor.execute(query)
    data = cursor.fetchall()
    df = pd.DataFrame(data)
    return df


line_chart_data = load_line_data()


@app.callback(Output('weather-graph', 'figure'), [Input('param-selector', 'value'), Input('city-selector', 'value')])
def update_graph(selected_param, selected_city):
    filtered_df = line_chart_data[
        line_chart_data['station_city'] == selected_city]  # Filter data based on selected city
    fig = px.line(filtered_df, x='YEAR', y=selected_param, title='Weather Parameters Evolution Over Years')
    fig.update_layout(xaxis_title="Year", yaxis_title="Parameter Value", legend_title="City Stations",
                      font=dict(family="Arial, sans-serif", size=12, color="RebeccaPurple"))
    return fig


# Define layout of Dash app
app.layout = html.Div([html.H1("Tableau de Bord d'Analyse Météorologique", className='header',
                               style={'textAlign': 'center', 'margin': '20px'}), dcc.Tabs(id="tabs", value='tab-1',
                                                                                          children=[
                                                                                              dcc.Tab(label='Heatmap',
                                                                                                      value='tab-1',
                                                                                                      children=[
                                                                                                          html.Div([
                                                                                                              html.Label(
                                                                                                                  "Sélectionnez le paramètre météorologique:",
                                                                                                                  className='label',
                                                                                                                  style={
                                                                                                                      'textAlign': 'center',
                                                                                                                      'margin': '20px'}),
                                                                                                              dcc.Dropdown(
                                                                                                                  id='parameter-dropdown',
                                                                                                                  options=[
                                                                                                                      {
                                                                                                                          'label': 'Analyse des précipitations (PRCP)',
                                                                                                                          'value': 'PRCP'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des températures moyennes (TAVG)',
                                                                                                                          'value': 'TAVG'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des températures maximales (TMAX)',
                                                                                                                          'value': 'TMAX'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des températures minimales (TMIN)',
                                                                                                                          'value': 'TMIN'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de l\'enneigement (SNWD)',
                                                                                                                          'value': 'SNWD'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des heures de gel (PGTM)',
                                                                                                                          'value': 'PGTM'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de la neige (SNOW)',
                                                                                                                          'value': 'SNOW'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de la direction du vent (WDFG)',
                                                                                                                          'value': 'WDFG'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de la vitesse du vent (WSFG)',
                                                                                                                          'value': 'WSFG'}],
                                                                                                                  value='PRCP',
                                                                                                                  clearable=False,
                                                                                                                  className='dropdown'),
                                                                                                              html.Label(
                                                                                                                  "Choisissez l'intervalle de temps:",
                                                                                                                  className='label',
                                                                                                                  style={
                                                                                                                      'textAlign': 'center',
                                                                                                                      'margin': '20px'}),
                                                                                                              dcc.RangeSlider(
                                                                                                                  id='year-range-slider',
                                                                                                                  min=int(
                                                                                                                      fetch_years()[
                                                                                                                          0]),
                                                                                                                  max=int(
                                                                                                                      fetch_years()[
                                                                                                                          -1]),
                                                                                                                  step=1,
                                                                                                                  value=[
                                                                                                                      int(
                                                                                                                          fetch_years()[
                                                                                                                              0]),
                                                                                                                      int(
                                                                                                                          fetch_years()[
                                                                                                                              -1])],
                                                                                                                  marks={
                                                                                                                      int(year): str(
                                                                                                                          year)
                                                                                                                      for
                                                                                                                      year
                                                                                                                      in
                                                                                                                      fetch_years()},
                                                                                                                  tooltip={
                                                                                                                      'always_visible': True,
                                                                                                                      'placement': 'bottom'},
                                                                                                                  className='slider'),
                                                                                                              dcc.Graph(
                                                                                                                  id='heatmap',
                                                                                                                  className='heatmap-graph')],
                                                                                                              className='content')]),
                                                                                              dcc.Tab(label='Bar Chart',
                                                                                                      value='tab-2',
                                                                                                      children=[
                                                                                                          html.Div([
                                                                                                              html.Label(
                                                                                                                  "Choisissez l'intervalle de temps:",
                                                                                                                  className='label',
                                                                                                                  style={
                                                                                                                      'textAlign': 'center',
                                                                                                                      'margin': '20px'}),
                                                                                                              dcc.RangeSlider(
                                                                                                                  id='year-range-slider-bar',
                                                                                                                  min=int(
                                                                                                                      fetch_years()[
                                                                                                                          0]),
                                                                                                                  max=int(
                                                                                                                      fetch_years()[
                                                                                                                          -1]),
                                                                                                                  step=1,
                                                                                                                  value=[
                                                                                                                      int(
                                                                                                                          fetch_years()[
                                                                                                                              0]),
                                                                                                                      int(
                                                                                                                          fetch_years()[
                                                                                                                              -1])],
                                                                                                                  marks={
                                                                                                                      int(year): str(
                                                                                                                          year)
                                                                                                                      for
                                                                                                                      year
                                                                                                                      in
                                                                                                                      fetch_years()},
                                                                                                                  tooltip={
                                                                                                                      'always_visible': True,
                                                                                                                      'placement': 'bottom'},
                                                                                                                  className='slider'),
                                                                                                              html.Label(
                                                                                                                  "Sélectionnez le paramètre météorologique:",
                                                                                                                  className='label',
                                                                                                                  style={
                                                                                                                      'textAlign': 'center',
                                                                                                                      'margin':'auto',
                                                                                                                      'marginBottom': '20px'}),
                                                                                                              dcc.Dropdown(
                                                                                                                  id='parameter-dropdown-bar',
                                                                                                                  options=[
                                                                                                                      {
                                                                                                                          'label': 'Analyse des précipitations (PRCP)',
                                                                                                                          'value': 'PRCP'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des températures moyennes (TAVG)',
                                                                                                                          'value': 'TAVG'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des températures maximales (TMAX)',
                                                                                                                          'value': 'TMAX'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des températures minimales (TMIN)',
                                                                                                                          'value': 'TMIN'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de l\'enneigement (SNWD)',
                                                                                                                          'value': 'SNWD'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse des heures de gel (PGTM)',
                                                                                                                          'value': 'PGTM'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de la neige (SNOW)',
                                                                                                                          'value': 'SNOW'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de la direction du vent (WDFG)',
                                                                                                                          'value': 'WDFG'},
                                                                                                                      {
                                                                                                                          'label': 'Analyse de la vitesse du vent (WSFG)',
                                                                                                                          'value': 'WSFG'}],
                                                                                                                  value='TAVG',
                                                                                                                  clearable=False,
                                                                                                                  style={
                                                                                                                      'width': '50%',
                                                                                                                      'margin': 'auto',
                                                                                                                      'marginBottom': '20px'},
                                                                                                                  className='dropdown'),
                                                                                                              dcc.Graph(
                                                                                                                  id='bar-chart',
                                                                                                                  className='bar-chart')],
                                                                                                              className='content')]),
                                                                                              dcc.Tab(
                                                                                                  label='Line Chart',
                                                                                                  value='tab-3',
                                                                                                  children=[html.Div([
                                                                                                      html.Label(
                                                                                                          "Sélectionnez le paramètre météorologique:",
                                                                                                          className='label',
                                                                                                          style={
                                                                                                              'textAlign': 'center',
                                                                                                              'margin': '20px'}),
                                                                                                      dcc.Dropdown(
                                                                                                          id='param-selector',
                                                                                                          options=[{
                                                                                                              'label': 'Precipitation (PRCP)',
                                                                                                              'value': 'PRCP'},
                                                                                                              {
                                                                                                                  'label': 'Average Temperature (TAVG)',
                                                                                                                  'value': 'TAVG'},
                                                                                                              {
                                                                                                                  'label': 'Maximum Temperature (TMAX)',
                                                                                                                  'value': 'TMAX'},
                                                                                                              {
                                                                                                                  'label': 'Minimum Temperature (TMIN)',
                                                                                                                  'value': 'TMIN'},
                                                                                                              {
                                                                                                                  'label': 'Snow Depth (SNWD)',
                                                                                                                  'value': 'SNWD'},
                                                                                                              {
                                                                                                                  'label': 'Peak Gust Time (PGTM)',
                                                                                                                  'value': 'PGTM'},
                                                                                                              {
                                                                                                                  'label': 'Snowfall (SNOW)',
                                                                                                                  'value': 'SNOW'},
                                                                                                              {
                                                                                                                  'label': 'Direction of Fastest 2-Min Wind (WDFG)',
                                                                                                                  'value': 'WDFG'},
                                                                                                              {
                                                                                                                  'label': 'Speed of Fastest 2-Min Wind (WSFG)',
                                                                                                                  'value': 'WSFG'}, ],
                                                                                                          value='PRCP',
                                                                                                          # Default value
                                                                                                          clearable=False,
                                                                                                          style={
                                                                                                              'width': '80%',
                                                                                                              'margin': 'auto'},
                                                                                                          className='dropdown'

                                                                                                      ), html.Div([
                                                                                                          html.Label(
                                                                                                              "Sélectionnez une station:",
                                                                                                              className='label',
                                                                                                              style={
                                                                                                                  'textAlign': 'center',
                                                                                                                  'margin': '20px'}),
                                                                                                          dcc.Dropdown(
                                                                                                              id='city-selector',
                                                                                                              options=[{
                                                                                                                  'label': city,
                                                                                                                  'value': city}
                                                                                                                  for
                                                                                                                  city
                                                                                                                  in
                                                                                                                  line_chart_data[
                                                                                                                      'station_city'].unique()],
                                                                                                              value=None,
                                                                                                              # Default value
                                                                                                              clearable=False,
                                                                                                              style={
                                                                                                                  'width': '80%',
                                                                                                                  'margin': 'auto',
                                                                                                                  'marginBottom': '20px'},
                                                                                                              className='dropdown'), ]),
                                                                                                      dcc.Graph(
                                                                                                          id='weather-graph',
                                                                                                          className='line-chart')],
                                                                                                      className='content')])])],
                      style={'backgroundColor': '#f1f1f1', 'padding': '20px'})

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
