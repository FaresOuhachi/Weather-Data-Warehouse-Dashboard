# Weather Data Warehouse & Dashboard

A full‑stack Python application that builds and populates a MySQL “star” data warehouse from historical weather CSV data, then exposes an interactive Dash/Plotly dashboard for exploratory geospatial and temporal analysis.

## 🚀 Features

- **Data Warehouse ETL**  
  - Automatically creates fact (`WeatherFact`) and dimension (`DateDim`, `StationDim`) tables from a flat CSV file.  
  - Batch loading of dimension and fact tables with surrogate key management via `Model.py`.  
  - Modular design separating connection logic, schema creation, and data loading.

- **Interactive Dash App**  
  - **Tab 1: Geospatial Heatmap**  
    - Scatter‑mapbox visualization of average weather parameters by station (e.g. PRCP, TAVG, TMAX, TMIN, SNWD, SNOW, WDFG, WSFG, PGTM).  
    - Slider controls for year and month ranges.  
  - **Tab 2: City‐level Bar Chart**  
    - Average parameter values by city, with optional country filter.  
    - Dynamic axis scaling and custom color palettes.  
  - **Tab 3: Parameter Time Series**  
    - Multi‑city line plots over time for any selected parameter.  
    - Dropdown filters for parameter and country.
