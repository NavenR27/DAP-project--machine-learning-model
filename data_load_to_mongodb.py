#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pymongo import MongoClient
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO)

# making connection URI for MongoDB
connection_uri = 'mongodb://localhost:27023/'

# giving database and collection names
database_name = 'Motor_collisions'
collection_names = ['MC_Vehicles', 'MC_People']

# using chunk size for inserting data into MongoDB, as its a big dataset
chunk_size = 1000

# Connecting to MongoDB
logging.info("Connecting to MongoDB...")
with MongoClient(connection_uri) as client:
    db = client[database_name]
    logging.info("Connected!")

    for collection_name in collection_names:
        # Accessing the collection
        collection = db[collection_name]
        logging.info(f"Accessing collection '{collection_name}'...")
        
        # Reading the data from the CSV file for the collection - MC_Vehicles
        if collection_name == 'MC_Vehicles':
            # CSV file path
            csv_path = "C:/Users/Naveen/Documents/Projects/DAP/Motor_Vehicle_Collisions_-_Vehicles.csv"
            logging.info(f"Loading data from {csv_path}...")
            try:
                # Read CSV data in chunks for better loading
                for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
                    # Converting chunk to a list of dictionaries (as one dictionary per row)
                    data = chunk.to_dict(orient='records')
                    # Inserting the data into the collection
                    result = collection.insert_many(data, ordered=False, bypass_document_validation=True)
                    logging.info(f"{len(result.inserted_ids)} documents inserted into collection '{collection_name}'.")
            except Exception as e:
                logging.error(f"Error inserting documents into collection '{collection_name}': {str(e)}")
        
        # Retrieving data from the JSON endpoint for the collection - MC_People
        elif collection_name == 'MC_People':
            # JSON endpoint URL
            json_url = 'https://data.cityofnewyork.us/api/views/f55k-p6yu/columns.json'
            
            logging.info(f"Loading data from {json_url}...")
            try:
                # Sending the HTTP GET request to the JSON endpoint
                response = requests.get(json_url)
                response.raise_for_status() 
                
                # Extracting the retrieved JSON data
                json_data = response.json()
                
                # Printing the JSON data to understand its structure
                logging.info("JSON Data:")
                logging.info(json_data)
                
                # Inserting the JSON data into the collection MC_People
                result = collection.insert_one({'data': json_data})
                logging.info(f"JSON data inserted into collection '{collection_name}'.")
            except Exception as e:
                logging.error(f"Error loading data from JSON endpoint '{json_url}': {str(e)}")

logging.info("All collections accessed and data loaded.")
#final success comment

# In[ ]:




