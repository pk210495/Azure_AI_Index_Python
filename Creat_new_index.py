import os
import json
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SimpleField,
    SearchableField,
    SearchFieldDataType,
)
import pandas as pd

# Azure Search settings
service_name = "your service name"
index_name = "new index name"
api_key = "your_api_key"


file_path = "your path for output data"

# Create clients
endpoint = f"https://{service_name}.search.windows.net/"
credential = AzureKeyCredential(api_key)
index_client = SearchIndexClient(endpoint=endpoint, credential=credential)
search_client = SearchClient(endpoint=endpoint, index_name=index_name, credential=credential)

# Read XLSX file
df = pd.read_csv(file_path)

# Define fields based on XLSX columns
fields = []
key_field_name = None

for column in df.columns:
    if key_field_name is None:
        # Use the first column as the key field
        key_field_name = column
        fields.append(SimpleField(name=column, type=SearchFieldDataType.String, key=True))
    elif df[column].dtype == 'object':
        fields.append(SearchableField(name=column, type=SearchFieldDataType.String))
    elif df[column].dtype == 'int64':
        fields.append(SimpleField(name=column, type=SearchFieldDataType.Int64))
    elif df[column].dtype == 'float64':
        fields.append(SimpleField(name=column, type=SearchFieldDataType.Double))
    elif df[column].dtype == 'bool':
        fields.append(SimpleField(name=column, type=SearchFieldDataType.Boolean))
    else:
        fields.append(SearchableField(name=column, type=SearchFieldDataType.String))

# Create the index
index = SearchIndex(name=index_name, fields=fields)
result = index_client.create_or_update_index(index)

print(f"Index '{index_name}' created successfully.")
print(f"Key field: '{key_field_name}'")

# Function to clean and validate data
def clean_data(row):
    cleaned = {}
    for k, v in row.items():
        if pd.isna(v):
            cleaned[k] = None
        elif isinstance(v, (int, float, bool)):
            cleaned[k] = v
        else:
            cleaned[k] = str(v)
    return cleaned

# Prepare the data for upload
documents = [clean_data(row) for row in df.to_dict(orient='records')]

# Validate JSON
for doc in documents:
    try:
        json.dumps(doc)
    except TypeError as e:
        print(f"Error in document: {doc}")
        print(f"Error message: {str(e)}")
        raise

# Upload the data in batches
batch_size = 1000
for i in range(0, len(documents), batch_size):
    batch = documents[i:i+batch_size]
    try:
        result = search_client.upload_documents(documents=batch)
        print(f"Uploaded {len(result)} documents (batch {i//batch_size + 1})")
    except Exception as e:
        print(f"Error uploading batch {i//batch_size + 1}: {str(e)}")
        # You might want to implement retry logic here

print("Data upload completed.")