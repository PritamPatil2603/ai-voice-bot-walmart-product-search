import pandas as pd
import os
import json
from openai import OpenAI  # Using direct OpenAI, not Azure
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Initialize OpenAI client - using standard OpenAI instead of Azure
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY")
)

# Set up the proper embedding model
EMBEDDING_MODEL = "text-embedding-3-small"  # This is an embedding-specific model

es = Elasticsearch("http://localhost:9200")

# Create Elasticsearch index for products
index_name = "walmart_products"

# Check if index exists and delete it if it does
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"Index {index_name} deleted")

# Define index mapping with vector field
mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "text"},
            "category": {"type": "keyword"},
            "brand": {"type": "keyword"},
            "price": {"type": "float"},
            "size": {"type": "text"},
            "department": {"type": "keyword"},
            "subcategory": {"type": "keyword"},
            "embedding": {
                "type": "dense_vector",
                "dims": 1536,  # Dimension for text-embedding-3-small
                "index": True,
                "similarity": "dot_product"
            }
        }
    }
}

# Create index with mapping
es.indices.create(index=index_name, body=mapping)
print(f"Index {index_name} created with vector mapping")

# Load Walmart dataset and limit to 1,000 items
df = pd.read_csv("walmart_grocery_products.csv", low_memory=False)
print(f"Dataset loaded with {len(df)} rows")

# Limit to the first 1,000 items
df = df.head(1000)
print(f"Loaded {len(df)} products for indexing")

# Function to create embeddings for each product - updated to use standard OpenAI embeddings
def generate_embedding(text):
    try:
        response = client.embeddings.create(
            input=text,
            model=EMBEDDING_MODEL
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

# Process and index products in batches
batch_size = 50  # Smaller batch size for better tracking
total_documents = len(df)
processed_documents = 0

# Use the actual column names from the dataset
name_col = 'PRODUCT_NAME'
category_col = 'CATEGORY'
brand_col = 'BRAND'
price_col = 'PRICE_CURRENT'
size_col = 'PRODUCT_SIZE'
department_col = 'DEPARTMENT'
subcategory_col = 'SUBCATEGORY'

print(f"Using columns: name={name_col}, category={category_col}, brand={brand_col}, price={price_col}, size={size_col}")

for i in range(0, total_documents, batch_size):
    batch = df.iloc[i:i+batch_size]
    actions = []
    
    for _, row in batch.iterrows():
        # Create text representation of product
        product_text = f"Product: {row[name_col] if pd.notna(row[name_col]) else 'Unknown'} "
        
        if pd.notna(row[department_col]):
            product_text += f"Department: {row[department_col]} "
        
        if pd.notna(row[category_col]):
            product_text += f"Category: {row[category_col]} "
        
        if pd.notna(row[subcategory_col]):
            product_text += f"Subcategory: {row[subcategory_col]} "
        
        if pd.notna(row[brand_col]):
            product_text += f"Brand: {row[brand_col]} "
        
        if pd.notna(row[price_col]):
            product_text += f"Price: ${row[price_col]} "
        
        if pd.notna(row[size_col]):
            product_text += f"Size: {row[size_col]} "
        
        # Generate embedding
        embedding = generate_embedding(product_text)
        
        if embedding:
            # Prepare document for indexing
            doc = {
                "_index": index_name,
                "_source": {
                    "name": row[name_col] if pd.notna(row[name_col]) else "Unknown",
                    "category": row[category_col] if pd.notna(row[category_col]) else "Unknown",
                    "brand": row[brand_col] if pd.notna(row[brand_col]) else "Unknown",
                    "price": float(row[price_col]) if pd.notna(row[price_col]) else 0.0,
                    "size": str(row[size_col]) if pd.notna(row[size_col]) else "Unknown",
                    "department": row[department_col] if pd.notna(row[department_col]) else "Unknown",
                    "subcategory": row[subcategory_col] if pd.notna(row[subcategory_col]) else "Unknown",
                    "embedding": embedding
                }
            }
            actions.append(doc)
    
    # Bulk index the batch
    if actions:
        helpers.bulk(es, actions)
    
    processed_documents += len(actions)
    print(f"Processed {processed_documents}/{total_documents} documents")
    
    # Add a small delay to avoid overwhelming the API
    time.sleep(1)

print("Indexing complete")