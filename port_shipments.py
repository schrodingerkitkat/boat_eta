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



# Function to fetch MarineTraffic data
def fetch_marine_traffic_data(mmsi_list):
    df2 = pd.DataFrame([])

    for mmsi in mmsi_list:
        # Make the request to the MarineTraffic API
        response = requests.get(
            API_URL + API_KEY + "?v=1&portid="+portid+"&mmsi=" + str(mmsi) + "&msgtype=simple&protocol=json"
        )
        dfp = response.json()
        df2 = df2.append(dfp, ignore_index=True)

    return df2

# Function to preprocess and save data
def preprocess_and_save_data(df2):
    # Rename columns and create a PySpark DataFrame
    df = spark.createDataFrame(df2) \
        .withColumnRenamed("0", "SHIP_ID") \
        .withColumnRenamed("1", "MMSI") \
        .withColumnRenamed("2", "IMO") \
        .withColumnRenamed("3", "LAST_PORT_ID") \
        .withColumnRenamed("4", "LAST_PORT") \
        .withColumnRenamed("5", "LAST_PORT_UNLOCODE") \
        .withColumnRenamed("6", "LAST_PORT_TIME") \
        .withColumnRenamed("7", "NEXT_PORT_NAME") \
        .withColumnRenamed("8", "NEXT_PORT_UNLOCODE") \
        .withColumnRenamed("9", "ETA_CALC")

    # Read data from CSV file
    df_csv = spark.read.csv("/mnt/c/swapp/boat_api.csv", header=True)
    df_csv = df_csv.dropna(subset=["MMSI"])

    # Join the two DataFrames on the MMSI column
    df_joined = df.select(["MMSI", "LAST_PORT", "LAST_PORT_TIME", "ETA_CALC"]).join(df_csv, on="MMSI")

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
    db_read = ""
    user = ""
    read_table = "dbo.BilletEtaToPort"
    pasc = os.environ['SERVER_KEY']
    server = ""

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
    df_csv = spark.read.csv("shipments.csv", header=True)
    df_csv = df_csv.dropna(subset=["MMSI"])
    mmsi_rows = df_csv.select(["MMSI"]).collect()
    mmsi_list = [row["MMSI"] for row in mmsi_rows]

    # Fetch MarineTraffic data
    marine_traffic_data = fetch_marine_traffic_data(mmsi_list)

    # Preprocess and save data
    preprocess_and_save_data(marine_traffic_data)

    # Start Flask app
    app.run(debug=True)

