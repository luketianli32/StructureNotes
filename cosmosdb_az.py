# -*- coding: utf-8 -*-
"""
Created on Fri Jul 18 15:58:33 2025

@author: LTian
"""

import os
import json
from azure.cosmos import CosmosClient, PartitionKey
import streamlit as st
import fitz  # PyMuPDF
from azure.storage.blob import BlobServiceClient
from dateutil import parser
from datetime import datetime
from urllib.parse import quote
import pandas as pd

# Configure your Cosmos DB settings
COSMOS_ENDPOINT = "https://luke-test.documents.azure.com:443/"
COSMOS_KEY = os.getenv("COSMOS_KEY")  
DATABASE_NAME = "SampleDB"
CONTAINER_NAME = "SampleContainer"
LOCAL_JSON_DIR = "./Output"
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") 
container_name = "pdf-files"


def format_underlying(underlying):
    if isinstance(underlying, list):
        # Extract 'Name' if present, otherwise stringify the dict
        return ", ".join(
            u.get("Name", str(u)) if isinstance(u, dict) else str(u) 
            for u in underlying
        )
    return str(underlying)

def uploadpdf():
   
   
    file_path = "example.pdf"
    
    local_directory = "boa_structured_notes/TestOthers"  # Your local path
    
    # Azure subdirectory path inside blob container
    azure_subdir = "pdfs/BNPnote"
    
    # Create blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Ensure container exists
    try:
        container_client.create_container()
    except Exception:
        pass  # Already exists
    
    # Upload all PDFs to the subdirectory
    for filename in os.listdir(local_directory):
        if filename.lower().endswith(".pdf"):
            file_path = os.path.join(local_directory, filename)
            blob_path = f"{azure_subdir}/{filename}"  # Subdirectory path in blob
    
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                print(f"Uploaded: {blob_path}")
    
    print("✅ All PDFs uploaded to subdirectory in blob storage.")



# uploadpdf()

 
 



st.title("📄 PDF Viewer from Azure Blob Storage")

# azure_subdir = "pdfs/BOAnote"  # Subdirectory in Blob Storage
# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)
base_dir = "pdfs/"  # top-level directory in blob storage
all_blobs = list(container_client.list_blobs(name_starts_with=base_dir))

subfolders = set()
for blob in all_blobs:
    # Remove base_dir prefix
    suffix = blob.name[len(base_dir):]
    # Extract first directory segment if any
    parts = suffix.split("/")
    if len(parts) > 1:
        subfolders.add(parts[0])

subfolders = sorted(list(subfolders))

if not subfolders:
    st.warning(f"No subfolders found under '{base_dir}'. Showing PDFs in base folder.")
    subfolders = [""]

# --- Select subfolder ---
selected_subfolder = st.selectbox("Select subfolder under pdfs", subfolders)

# Compose subfolder prefix
if selected_subfolder:
    azure_subdir = f"{base_dir}{selected_subfolder}/"
else:
    azure_subdir = base_dir

# --- List PDF files in selected subfolder ---
pdf_files = [
    blob.name for blob in container_client.list_blobs(name_starts_with=azure_subdir)
    if blob.name.lower().endswith(".pdf")
]

if not pdf_files:
    st.warning(f"No PDF files found in '{azure_subdir}'.")
else:
    selected_pdf = st.selectbox("Select a PDF file", pdf_files)

    if selected_pdf:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=selected_pdf)
        pdf_bytes = blob_client.download_blob().readall()

        st.download_button("Download PDF", pdf_bytes, file_name=selected_pdf.split("/")[-1])

        # Display preview of first page
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            page = doc.load_page(0)
            pix = page.get_pixmap()
            st.image(pix.tobytes(), caption="First page preview", use_column_width=True)
        except Exception as e:
            st.error(f"Failed to preview PDF: {e}")



client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
db = client.get_database_client(DATABASE_NAME)
container = db.get_container_client(CONTAINER_NAME)
# --- Query and filter documents ---


items = list(container.query_items(
    query="SELECT * FROM c",
    enable_cross_partition_query=True
))

table_data = []
for item in items:
    table_data.append({
        "ID": item.get("id", ""),
        "TradeDate": item.get("TradeDate", ""),
        "SecurityIdentifier": item.get("SecurityIdentifier", {}),
        "Underlying": item.get("Underlying", [])
    })

df = pd.DataFrame(table_data)
df["SecurityIdentifier"] = df["SecurityIdentifier"].apply(lambda x: str(x))
df["Underlying"] = df["Underlying"].apply(format_underlying)

# --- Show DataFrame ---
st.title("Cosmos DB Document Table")
st.dataframe(df, use_container_width=True)


def upload_json_to_cosmos():
    # Create Cosmos client
    client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    db = client.get_database_client(DATABASE_NAME)
    container = db.get_container_client(CONTAINER_NAME)

    # Loop through all JSON files in the directory
    for filename in os.listdir(LOCAL_JSON_DIR):
        if filename.endswith(".json"):
            file_path = os.path.join(LOCAL_JSON_DIR, filename)
            doc_id = os.path.splitext(filename)[0]  # filename without .json

            with open(file_path, "r", encoding="utf-8") as f:
                 content = f.read()
                 start = content.find('{')
                 end = content.rfind('}') + 1
                 json_str = content[start:end]
             
                 data = json.loads(json_str)

            # Ensure document has an 'id' field (Cosmos DB requires this)
            data["id"] = doc_id

            try:
                container.upsert_item(data)  # insert or update
                print(f"Uploaded: {doc_id}")
            except Exception as e:
                print(f"Error uploading {doc_id}: {e}")

# upload_json_to_cosmos()




# client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
# db = client.get_database_client(DATABASE_NAME)
# container = db.get_container_client(CONTAINER_NAME)
# items = list(container.query_items(
#     query="SELECT * FROM c",
#     enable_cross_partition_query=True
# ))
# filtered = [
#     item for item in items
#     if (
#         "TradeDate" in item and
#         item["TradeDate"] and  # not None or empty
#         parser.parse(item["TradeDate"]) > parser.parse("April 24, 2025")
#     )
# ]