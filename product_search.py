import os
from dotenv import load_dotenv
from llama_index.vector_stores.elasticsearch import ElasticsearchStore
from llama_index.core import VectorStoreIndex, StorageContext
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings

# Load environment variables
load_dotenv()

# Configure LlamaIndex globally
Settings.embed_model = OpenAIEmbedding(
    model="text-embedding-3-small",
    api_key=os.getenv("OPENAI_API_KEY")
)

class ProductRetriever:
    def __init__(self):
        # Connect to existing Elasticsearch index
        self.vector_store = ElasticsearchStore(
            index_name="walmart_products",
            es_cloud_id=os.getenv("ES_CLOUD_ID"),
            es_user=os.getenv("ES_USERNAME"),
            es_password=os.getenv("ES_PASSWORD"),
            embedding_dimension=1536
        )
        
        # Create storage context
        storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
        
        # Connect to existing index
        self.index = VectorStoreIndex.from_vector_store(
            vector_store=self.vector_store,
            storage_context=storage_context
        )
        
        # Create retriever
        self.retriever = self.index.as_retriever(similarity_top_k=5)
    
    def search_products(self, query):
        """Search for products using LlamaIndex retriever."""
        try:
            # Retrieve relevant products
            results = self.retriever.retrieve(query)
            
            products = []
            for result in results:
                node = result.node
                
                # Extract product information from metadata
                product = {
                    'product_name': node.metadata.get('name', ''),
                    'score': result.score,
                    'category': node.metadata.get('category', ''),
                    'brand': node.metadata.get('brand', ''),
                    'price': node.metadata.get('price', 0.0),
                    'size': node.metadata.get('size', ''),
                    'department': node.metadata.get('department', ''),
                    'subcategory': node.metadata.get('subcategory', ''),
                    'breadcrumbs': node.metadata.get('breadcrumbs', ''),
                    'sku': node.metadata.get('sku', ''),
                    'url': node.metadata.get('url', '')
                }
                products.append(product)
            
            return products
            
        except Exception as e:
            print(f"Error searching products: {e}")
            return []

# Global retriever instance
product_retriever = ProductRetriever()

def search_products(query_text, top_k=5):
    """
    Global function to search products.
    Maintains backward compatibility with existing code.
    """
    return product_retriever.search_products(query_text)

def format_product_results(products):
    """Format product results for display."""
    if not products:
        return "No products found matching your search."
    
    formatted_results = []
    for i, product in enumerate(products, 1):
        result_text = f"{i}. {product['product_name']}"
        
        if product['brand'] and product['brand'] != "Unknown":
            result_text += f"\n   Brand: {product['brand']}"
        
        if product['category'] and product['category'] != "Unknown":
            result_text += f"\n   Category: {product['category']}"
        
        if product['department'] and product['department'] != "Unknown":
            result_text += f"\n   Department: {product['department']}"
        
        result_text += f"\n   Price: ${product['price']:.2f}"
        
        if product['size'] and product['size'] != "Unknown":
            result_text += f"\n   Size: {product['size']}"
        
        result_text += f"\n   Relevance Score: {product['score']:.3f}"
        
        formatted_results.append(result_text)
    
    return "\n\n".join(formatted_results)

# Test functionality
if __name__ == "__main__":
    test_queries = [
        "organic milk",
        "cheese",
        "gluten free bread",
        "hummus",
        "frozen pizza",
        "baby formula"
    ]
    
    print("Testing product search with proper LlamaIndex integration...\n")
    
    for query in test_queries:
        print(f"Searching for: '{query}'")
        print("-" * 50)
        
        results = search_products(query)
        
        if results:
            print(f"Found {len(results)} products:")
            print(format_product_results(results[:3]))
        else:
            print("No products found")
        
        print("\n" + "=" * 50 + "\n")