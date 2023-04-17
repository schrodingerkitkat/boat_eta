# Marine Traffic Data Visualization
This repository contains a Python application that fetches marine traffic data from the MarineTraffic API, processes the data using PySpark, saves the data to an SQL server, and visualizes the data on an interactive map using Flask and Folium.

# Features
Fetches marine traffic data using the MarineTraffic API
Processes data with advanced PySpark features, such as User Defined Functions (UDFs) and window functions
Saves the processed data to an SQL server
Visualizes the data on an interactive map with Flask and Folium
# Getting Started

# Prerequisites
Python 3.6 or later
PySpark
Flask
Folium
An SQL server
MarineTraffic API key

# Installation
Clone the repository:
<code>
git clone https://github.com/yourusername/marine-traffic-data-visualization.git
cd marine-traffic-data-visualization
</code>

# Install the required Python packages:

<code>
pip install -r requirements.txt
</code>

# Configuration

Set the following environment variables:

- MT_ETATOPORT: Your MarineTraffic API key for the ETA to Port service
- MT_EXPORT: Your MarineTraffic API key for the Vessel Export service
- SERVER_KEY: Your SQL server password

# Update the following constants in the app.py file with your SQL server information:

db_read: The name of the database to read data from
- db_write: The name of the database to write data to
- user: The SQL server username
- server: The SQL server address
- read_table: The name of the table to read data from
- write_table: The name of the table to write data to

# Usage
1. Run the application:

<code>
python app.py
</code>
Open your web browser and navigate to http://localhost:5000/map to view the interactive map with marine traffic data.

# Contributing
Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

# License
This project is licensed under the MIT License - see the LICENSE file for details.
