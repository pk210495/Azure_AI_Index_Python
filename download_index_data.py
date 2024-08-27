import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
import csv

# Azure AI Search settings
service_name = "your service name"
index_name = "index name"
api_key = "your api key"

# CSV file settings
output_file = "outputpat/.csv"

# Create a SearchClient
endpoint = f"https://{service_name}.search.windows.net/"
credential = AzureKeyCredential(api_key)
client = SearchClient(endpoint=endpoint, index_name=index_name, credential=credential)

# Function to download all documents from the index
def download_all_documents(client):
    results = []
    search_results = client.search("*", include_total_count=True)
    
    for result in search_results:
        results.append(result)
    
    return results

# Download the documents
documents = download_all_documents(client)

# Write to CSV
if documents:
    keys = documents[0].keys()
    
    with open(output_file, 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(documents)
    
    print(f"Data successfully downloaded to {output_file}")
else:
    print("No documents found in the index.")