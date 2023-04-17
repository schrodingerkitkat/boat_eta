import os
import folium
import requests
from flask import Flask, render_template
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Marine Traffic Data") \
    .config("spark.driver.extraClassPath", "/mnt/c/swapp/sqljdbc_10.2/enu/mssql-jdbc-10.2.0.jre8.jar") \
    .getOrCreate()

# Constants
API_KEY = os.environ['MT_ETATOPORT']
LOCATION_KEY = os.environ['MT_EXPORT']
API_URL = "https://services.marinetraffic.com/api/etatoport/"
LOCATION_URL = "https://services.marinetraffic.com/api/exportvessel/"
#find eta to LGB port, #2727
portid=2727

db_write = os.environ['eta_to_port_db_write']
user = os.environ['db_user']
write_table = os.environ['eta_to_port_db_write_table']
pasc = os.environ['SERVER_KEY']
server = os.environ['server']
db_read = os.environ['eta_to_port_db_read_']
read_table = os.environ['eta_to_port_db_read_table']

# UDF for processing API response
@udf(returnType=StringType())
def process_api_response(mmsi):
    response = requests.get(
        API_URL + API_KEY + "?v=1&portid=2727&mmsi=" + str(mmsi) + "&msgtype=simple&protocol=json"
    )
    return response.text

# Function to fetch MarineTraffic data
def fetch_marine_traffic_data(df_csv):
    # Apply UDF to fetch API data
    df_with_api_data = df_csv.withColumn("api_data", process_api_response(col("MMSI")))

    # Convert JSON data to DataFrame and join with the original DataFrame
    df_api_data = spark.read.json(df_with_api_data.select("api_data").rdd.map(lambda r: r.api_data))
    df_joined = df_csv.join(df_api_data, df_csv["MMSI"] == df_api_data["1"])

    return df_joined

# Function to preprocess and save data
def preprocess_and_save_data(df_joined):
    df_joined.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://" + server + ":1433;databaseName=" + db_write + ";encrypt=true;trustServerCertificate=true;") \
        .option("dbtable", write_table) \
        .option("user", user) \
        .option("password", pasc) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .save()

# Function to get data from SQL server
def get_data_from_sql():
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://" + server + ":1433;databaseName=" + db_read + ";encrypt=true;trustServerCertificate=true;") \
        .option("dbtable", read_table) \
        .option("user", user) \
        .option("password", pasc) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .load()
    return df.toPandas()

app = Flask(__name__)

@app.route('/map')

def map():
    # Fetch the data from the SQL server
    df = get_data_from_sql()

    # Create a map centered on a specific location (you can adjust the coordinates as needed)
    m = folium.Map(location=[40.730610, -73.935242], zoom_start=10)

    # Iterate through the data points and add them to the map
    for index, row in df.iterrows():
        folium.Marker(
            location=[row['LATITUDE'], row['LONGITUDE']],
            popup=f"MMSI: {row['MMSI']}, LAST_PORT: {row['LAST_PORT']}, LAST_PORT_TIME: {row['LAST_PORT_TIME']}, ETA_CALC: {row['ETA_CALC']}",
            icon=folium.Icon(color='blue', icon='info-sign')
        ).add_to(m)

    # Save the map to an HTML file and render it
    m.save('templates/map.html')
    return render_template('map.html')

if __name__ == '__main__':
    # Read MMSI numbers from CSV file
    df_csv = spark.read.csv("/shipments.csv", header=True)
    df_csv = df_csv.dropna(subset=["MMSI"])

    # Fetch MarineTraffic data
    marine_traffic_data = fetch_marine_traffic_data(df_csv)

    # Preprocess and save data
    preprocess_and_save_data(marine_traffic_data)

    # Start Flask app
    app.run(debug=True)
