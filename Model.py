import pandas as pd
import pymysql


class DataWarehouseManager:
    def __init__(self, host, username, password, database, charset, cursorclass):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.charset = charset
        self.cursorclass = cursorclass
        self.date_id_map = {}  # Map to store date IDs
        self.station_id_map = {}  # Map to store station IDs

    def connect(self):
        self.conn = pymysql.connect(host=self.host, user=self.username, password=self.password, database=self.database,
                                    charset=self.charset, cursorclass=self.cursorclass)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_tables(self):
        create_tables_queries = ["""
            CREATE TABLE DateDim (
                date_id INT AUTO_INCREMENT PRIMARY KEY,
                day INT,
                month INT,
                year INT
            )
            """, """
            CREATE TABLE StationDim (
                station_id INT AUTO_INCREMENT PRIMARY KEY,
                station_code VARCHAR(255),
                station_city VARCHAR(255),
                station_country VARCHAR(255),
                latitude FLOAT,
                longitude FLOAT,
                elevation FLOAT
            )
            """, """
            CREATE TABLE WeatherFact (
                weather_id INT AUTO_INCREMENT PRIMARY KEY,
                date_id INT,
                station_id INT,
                prcp FLOAT,
                tavg FLOAT,
                tmax FLOAT,
                tmin FLOAT,
                snwd FLOAT,
                pgtm FLOAT,
                snow FLOAT,
                wdfg FLOAT,
                wsfg FLOAT,
                FOREIGN KEY (date_id) REFERENCES DateDim(date_id),
                FOREIGN KEY (station_id) REFERENCES StationDim(station_id)
            )
            """]
        for query in create_tables_queries:
            self.cursor.execute(query)

        self.conn.commit()

    def get_cursor(self):
        return self.cursor

    def insert_date_dim(self, df):
        date_dim_insert_query = "INSERT INTO DateDim (day, month, year) VALUES (%s, %s, %s)"
        date_dim_data = df[['DAY', 'MONTH', 'YEAR']].drop_duplicates().values.tolist()
        for data in date_dim_data:
            self.cursor.execute(date_dim_insert_query, data)
            self.conn.commit()
            # Get the auto-generated ID for the inserted row
            date_id = self.cursor.lastrowid
            # Map the original date values to the generated ID
            self.date_id_map[tuple(data)] = date_id

    def insert_station_dim(self, df):
        station_dim_insert_query = "INSERT INTO StationDim (station_code, station_city, station_country, latitude, longitude, elevation) VALUES (%s, %s, %s, %s, %s, %s)"
        station_dim_data = df[['STATIONCODE', 'STATIONCITY', 'STATIONCOUNTRY', 'LATITUDE', 'LONGITUDE',
                               'ELEVATION']].drop_duplicates().values.tolist()
        for data in station_dim_data:
            self.cursor.execute(station_dim_insert_query, data)
            self.conn.commit()
            # Get the auto-generated ID for the inserted row
            station_id = self.cursor.lastrowid
            # Map the original station values to the generated ID
            self.station_id_map[data[0]] = station_id

    def insert_fact_weather(self, df):
        weather_fact_insert_query = """
        INSERT INTO WeatherFact (date_id, station_id, prcp, tavg, tmax, tmin, snwd, pgtm, snow, wdfg, wsfg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        total_rows = len(df)
        batch_size = 100000  # Adjust as needed
        num_batches = (total_rows + batch_size - 1) // batch_size
        batch_count = 0

        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_df = df.iloc[batch_start:batch_end]
            batch_data = []

            for _, row in batch_df.iterrows():
                date_id = self.date_id_map[(row['DAY'], row['MONTH'], row['YEAR'])]
                station_id = self.station_id_map[row['STATIONCODE']]
                weather_fact_data = (
                    date_id, station_id, row['PRCP'], row['TAVG'], row['TMAX'], row['TMIN'], row['SNWD'], row['PGTM'],
                    row['SNOW'], row['WDFG'], row['WSFG'])
                batch_data.append(weather_fact_data)

            self.cursor.executemany(weather_fact_insert_query, batch_data)
            self.conn.commit()

            remaining_lines = total_rows - batch_end
            print(f"{remaining_lines} lines left after batch {batch_count + 1}.")

            batch_count += 1

    def load_data_warehouse(self, csv_file):
        df = pd.read_csv(csv_file, sep=',')
        self.create_tables()
        self.insert_date_dim(df)
        self.insert_station_dim(df)
        self.insert_fact_weather(df)


if __name__ == "__main__":
    warehouse_manager = DataWarehouseManager('localhost', 'root', '', 'Weather_DataWarehouse', 'utf8mb4',
                                             pymysql.cursors.DictCursor)
    print('----------------------------- Connecting to the database -----------------------------\n\n')
    warehouse_manager.connect()
    print('----------------------------- Connected !!! -----------------------------\n')
    print('----------------------------- Loading data warehouse -----------------------------\n\n')
    warehouse_manager.load_data_warehouse('Ready_Data.csv')
    print('----------------------------- Data warehouse loaded successfully !!! -----------------------------')
    warehouse_manager.disconnect()
    print('----------------------------- Disconnected !!! -----------------------------')
