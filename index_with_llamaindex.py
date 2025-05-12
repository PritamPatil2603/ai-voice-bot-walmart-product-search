import os
import pandas as pd
from dotenv import load_dotenv
from llama_index.core import Document, VectorStoreIndex
from llama_index.vector_stores.elasticsearch import ElasticsearchStore
from llama_index.core import StorageContext
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings
from tqdm import tqdm
from elasticsearch import Elasticsearch
import time

# Load environment variables
load_dotenv()

# Configure LlamaIndex embeddings
Settings.embed_model = OpenAIEmbedding(
    model="text-embedding-3-small",
    api_key=os.getenv("OPENAI_API_KEY")
)

def test_elasticsearch_connection():
    """Test connection to Elasticsearch before indexing."""
    try:
        es = Elasticsearch(
            cloud_id=os.getenv("ES_CLOUD_ID"),
            basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD")),
            timeout=30
        )
        
        # Test connection
        info = es.info()
        print(f"✓ Connected to Elasticsearch version: {info['version']['number']}")
        
        # Check if index exists
        index_name = "walmart_products"
        if es.indices.exists(index=index_name):
            count = es.count(index=index_name)
            print(f"✓ Index '{index_name}' already exists with {count['count']} documents")
            response = input("Do you want to delete and recreate the index? (y/n): ")
            if response.lower() == 'y':
                es.indices.delete(index=index_name)
                print(f"✓ Index '{index_name}' deleted")
            else:
                print("Exiting without changes")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Failed to connect to Elasticsearch: {e}")
        return False

def load_and_prepare_data(file_path, limit=None):
    """Load and prepare Walmart dataset."""
    print(f"Loading data from {file_path}...")
    df = pd.read_csv(file_path, low_memory=False)
    
    # Clean data
    df = df.dropna(subset=['PRODUCT_NAME', 'PRICE_CURRENT'])
    
    # Convert price to float
    df['PRICE_CURRENT'] = df['PRICE_CURRENT'].astype(float)
    
    if limit:
        df = df.head(limit)
    
    print(f"✓ Loaded {len(df)} products")
    return df

def create_documents(df):
    """Create LlamaIndex Document objects from dataframe."""
    documents = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Creating documents"):
        # Create rich text representation for better search
        doc_text = f"""
        Product: {row['PRODUCT_NAME']}
        Brand: {row.get('BRAND', 'Unknown')}
        Category: {row.get('CATEGORY', 'Unknown')}
        Department: {row.get('DEPARTMENT', 'Unknown')}
        Subcategory: {row.get('SUBCATEGORY', 'Unknown')}
        Price: ${row['PRICE_CURRENT']:.2f}
        Size: {row.get('PRODUCT_SIZE', 'Unknown')}
        Description: {row.get('BREADCRUMBS', '')}
        """
        
        # Create metadata
        metadata = {
            "name": str(row['PRODUCT_NAME']),
            "brand": str(row.get('BRAND', 'Unknown')),
            "category": str(row.get('CATEGORY', 'Unknown')),
            "department": str(row.get('DEPARTMENT', 'Unknown')),
            "subcategory": str(row.get('SUBCATEGORY', 'Unknown')),
            "price": float(row['PRICE_CURRENT']),
            "size": str(row.get('PRODUCT_SIZE', 'Unknown')),
            "sku": str(row.get('SKU', '')),
            "breadcrumbs": str(row.get('BREADCRUMBS', '')),
            "url": str(row.get('PRODUCT_URL', '')),
        }
        
        # Create Document
        doc = Document(
            text=doc_text.strip(),
            metadata=metadata,
            doc_id=str(row.get('SKU', idx))  # Use SKU as doc_id if available
        )
        documents.append(doc)
    
    return documents

def index_documents(documents, index_name="walmart_products", batch_size=50):
    """Index documents using LlamaIndex and Elasticsearch."""
    print("Setting up Elasticsearch vector store...")
    
    # Create Elasticsearch vector store with extended timeout and retry settings
    vector_store = ElasticsearchStore(
        index_name=index_name,
        es_cloud_id=os.getenv("ES_CLOUD_ID"),
        es_user=os.getenv("ES_USERNAME"),
        es_password=os.getenv("ES_PASSWORD"),
        embedding_dimension=1536,  # text-embedding-3-small dimension
        timeout=120,  # Increase timeout to 120 seconds
        retry_on_timeout=True,
        max_retries=3
    )
    
    # Create storage context
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    
    print(f"Creating index and generating embeddings in batches of {batch_size}...")
    
    index = None
    successful_docs = 0
    failed_docs = 0
    
    # Process documents in smaller batches
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(documents) + batch_size - 1) // batch_size
        
        print(f"\n--- Processing batch {batch_num}/{total_batches} ---")
        
        try:
            if i == 0:
                # Create initial index with first batch
                print("Creating initial index...")
                index = VectorStoreIndex.from_documents(
                    batch,
                    storage_context=storage_context,
                    show_progress=True
                )
                successful_docs += len(batch)
            else:
                # Add subsequent batches to existing index
                print(f"Adding documents to existing index...")
                for doc in tqdm(batch, desc=f"Batch {batch_num}"):
                    try:
                        index.insert(doc)
                        successful_docs += 1
                    except Exception as doc_error:
                        print(f"\n✗ Failed to insert document {doc.doc_id}: {doc_error}")
                        failed_docs += 1
                        continue
                    
            print(f"✓ Batch {batch_num} completed successfully")
            
            # Add a small delay between batches to avoid overwhelming the API
            if i + batch_size < len(documents):
                time.sleep(2)
                
        except Exception as e:
            print(f"\n✗ Error processing batch {batch_num}: {e}")
            print("Retrying with smaller mini-batches...")
            
            # Retry with even smaller batches
            mini_batch_size = 10
            for j in range(0, len(batch), mini_batch_size):
                mini_batch = batch[j:j + mini_batch_size]
                print(f"  Processing mini-batch {j//mini_batch_size + 1}...")
                
                try:
                    if index is None:
                        index = VectorStoreIndex.from_documents(
                            mini_batch,
                            storage_context=storage_context,
                            show_progress=False
                        )
                        successful_docs += len(mini_batch)
                    else:
                        for doc in mini_batch:
                            try:
                                index.insert(doc)
                                successful_docs += 1
                            except Exception as mini_e:
                                print(f"  ✗ Failed to insert document {doc.doc_id}: {mini_e}")
                                failed_docs += 1
                                continue
                            
                    time.sleep(1)  # Small delay between mini-batches
                    
                except Exception as mini_batch_error:
                    print(f"  ✗ Failed mini-batch: {mini_batch_error}")
                    failed_docs += len(mini_batch)
                    continue
    
    print(f"\n--- Indexing Summary ---")
    print(f"✓ Successfully indexed: {successful_docs} documents")
    print(f"✗ Failed to index: {failed_docs} documents")
    print(f"Total processed: {successful_docs + failed_docs} documents")
    
    return index

def main():
    # Configuration
    csv_path = "walmart_grocery_products.csv"
    limit = 1000  # Limit to 1000 products for testing
    batch_size = 50  # Smaller batch size to avoid timeouts
    
    # Test connection first
    print("Testing Elasticsearch connection...")
    if not test_elasticsearch_connection():
        print("\n✗ Cannot proceed without a valid Elasticsearch connection.")
        print("Please check your credentials and network connectivity.")
        return
    
    try:
        # Load and prepare data
        df = load_and_prepare_data(csv_path, limit=limit)
        
        # Create documents
        documents = create_documents(df)
        
        # Index documents with batch processing
        index = index_documents(documents, batch_size=batch_size)
        
        if index:
            print("\n✓ Indexing complete!")
            
            # Test with a simple query
            print("\nTesting with a sample query...")
            query_engine = index.as_query_engine()
            
            test_queries = ["cheese", "organic milk", "gluten free bread"]
            for query in test_queries:
                print(f"\nTesting query: '{query}'")
                try:
                    response = query_engine.query(query)
                    print(f"✓ Response: {str(response)[:200]}...")
                except Exception as query_error:
                    print(f"✗ Query failed: {query_error}")
        else:
            print("\n✗ Failed to create index")
            
    except Exception as e:
        print(f"\n✗ An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()