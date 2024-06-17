"""
This script extracts data from the Spanish electrical system (REE website). 
The data is recorded every 5 minutes and includes various sources of energy generation.

Authors:
Ivan Merino, Universidad Catolica del Maule
Marco Toranzo, Universidad Catolica del Maule
"""
 
# Spanish electric system
# Data from peninsule taken from 
# https://demanda.ree.es/visiona/peninsula/nacional/total
# Data every 5 minutes
 
 
#ts: Timestamp
#dem: Demand
#eol: Wind
#nuc: Nuclear
#gf: Gas/Fuel
#car: Coal
#cc: Combined Cycle
#hid: Hydroelectric
#aut: Storage
#inter: Interconnections
#icb: Co-generation
#sol: Solar
#solFot: Solar Photovoltaic
#solTer: Solar Thermal
#termRenov: Renewable Energy
#cogenResto: Co-generation Rest
 
 
import os
import requests
import json
import csv
from pathlib import Path
from datetime import datetime, timedelta
desktop_path = str(Path.home() / "Desktop")
 
# User inputs
mwh = True  # Original data is in MW. This transforms to MWh
data_cleansing = True # Remove duplicate lines, hours with A and B (averaged)
filling_gaps = True # Fill in missing information by interpolations (days with hours/minutes missing)
 
# Define the start and end date
start_date = datetime(2016, 1, 1) # The lower limit starts from 2016
end_date = datetime(2023, 12, 31)  
 
# Create the list of dates
dates = [(start_date + timedelta(days=d)).strftime('%Y-%m-%d') for d in range((end_date - start_date).days)]
 
 
# Base URL of the website to scrape
url_base = "https://demanda.ree.es/WSvisionaMovilesPeninsulaRest/resources/demandaGeneracionPeninsula?callback=angular.callbacks._3&curva=DEMANDAQH&fecha="
# Initialize a list to store all the data
all_data = []
for date in dates:
    print(date)
    # Build the URL for the current date
    url = url_base + date
    try:
        # Send an HTTP GET request to the website
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSONP response
            jsonp_data = response.text
            # Extract the JSON data
            json_data = json.loads(jsonp_data.split('angular.callbacks._3(')[1].rsplit(')', 1)[0])
            # Add the data for the current date to the list of all data
            all_data.extend(json_data['valoresHorariosGeneracion'][37:325])
        else:
            print("Error retrieving data for date:", date, ". Status code:", response.status_code)
    except requests.exceptions.RequestException as e:
        print("Error:", e)
# Save all the data to a CSV file
csv_filename = os.path.join(desktop_path, 'energy_generation_data.csv')
with open(csv_filename, 'w', newline='') as csvfile:
    # Define the CSV writer
    fieldnames = all_data[0].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    # Write the header
    writer.writeheader()
    # Write all the data
    for entry in all_data:
        writer.writerow(entry)
print("The data has been successfully saved to:", csv_filename)
 
if filling_gaps:
	df['ts'] = pd.to_datetime(df['ts'])
	# Finding missiong rows
	missing_timestamps = pd.date_range(start=df['ts'].min(), end=df['ts'].max(), freq='5T').difference(df['ts'])
 
	# Fill in the missing rows with the averages of the before and after data
	for ts in missing_timestamps:
		# Find the rows before and after that have valid data
		previous_row = df[df['ts'] < ts].iloc[-1]
		next_row = df[df['ts'] > ts].iloc[0]
		# Calculate the average of the data
		average_values = (previous_row.iloc[1:] + next_row.iloc[1:]) / 2
		# Insert a new row into the dataframe with the corresponding timestamp and average values
		new_row = {'ts': ts}
		new_row.update(average_values)
		df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
 
	# Sort the dataframe by the 'ts' column
	df.sort_values('ts', inplace=True)
 
if data_cleansing:
    # Intended to remove inconsistencies of the data
    import pandas as pd
 
    with open(csv_filename, 'r', newline='') as csvfile:
        df = pd.read_csv(csvfile)
        df['ts'] = df['ts'].str.replace('2A:', '02:').str.replace('2B:', '02:')
        df = df.groupby(df['ts']).mean().reset_index()
        # Save the DataFrame to a CSV file on the user's desktop
        csv_filename_cleaned = os.path.join(desktop_path, 'cleaned_energy_generation_data.csv')
 
        # Save the cleaned DataFrame to a CSV file
        df.to_csv(csv_filename_cleaned, index=False)
 
        print("The cleaned data has been successfully saved to:", csv_filename_cleaned)
 
 
if mwh:
    df.iloc[:, 1:] = df.iloc[:, 1:].applymap(lambda x: x * (5/60))
