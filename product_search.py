import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from openai import OpenAI, AzureOpenAI
import numpy as np

# Load environment variables
load_dotenv()

# Initialize OpenAI client for embeddings
openai_client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY")
)

# Initialize Azure OpenAI client for chat completions (used in Task 1)
azure_client = AzureOpenAI(
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_version="2024-10-01-preview"
)

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200")

# Embedding model to use
EMBEDDING_MODEL = "text-embedding-3-small"
INDEX_NAME = "walmart_products"

def search_products(query_text, top_k=5):
    """
    Search for products using semantic search with OpenAI embeddings.
    
    Args:
        query_text (str): The query text from the user
        top_k (int): Number of results to return
        
    Returns:
        list: List of matching products
    """
    try:
        # Generate embedding for the query
        response = openai_client.embeddings.create(
            input=query_text,
            model=EMBEDDING_MODEL
        )
        query_embedding = response.data[0].embedding
        
        # Perform kNN search in Elasticsearch
        search_query = {
            "knn": {
                "field": "embedding",
                "query_vector": query_embedding,
                "k": top_k,
                "num_candidates": 100
            },
            "_source": ["name", "brand", "category", "price", "size", "department", "subcategory"]
        }
        
        # Execute the search
        response = es.search(
            index=INDEX_NAME,
            body=search_query
        )
        
        # Process results
        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            score = hit["_score"]
            
            result = {
                "product_name": source.get("name", ""),
                "category": source.get("category", ""),
                "brand": source.get("brand", ""),
                "price": source.get("price", 0.0),
                "size": source.get("size", ""),
                "department": source.get("department", ""),
                "subcategory": source.get("subcategory", ""),
                "score": score
            }
            results.append(result)
        
        return results
    
    except Exception as e:
        print(f"Error searching products: {e}")
        return []

# Test the search functionality if this file is run directly
if __name__ == "__main__":
    # Test with a sample query
    results = search_products("organic milk")
    for i, result in enumerate(results, 1):
        print(f"{i}. {result['product_name']}")
        print(f"   Category: {result['category']}")
        print(f"   Brand: {result['brand']}")
        print(f"   Price: ${result['price']}")
        print(f"   Score: {result['score']}")
        print()