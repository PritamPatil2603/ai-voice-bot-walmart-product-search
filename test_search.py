import os
from dotenv import load_dotenv
from product_search import search_products, format_product_results
from elasticsearch import Elasticsearch

# Load environment variables
load_dotenv()

def check_index_status():
    """Check if the index exists and has documents."""
    es = Elasticsearch(
        cloud_id=os.getenv("ES_CLOUD_ID"),
        basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD"))
    )
    
    index_name = "walmart_products"
    
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"Index '{index_name}' exists with {count['count']} documents\n")
    else:
        print(f"Index '{index_name}' does not exist!\n")
        return False
    
    return True

def test_searches():
    """Test various product searches."""
    test_cases = [
        ("cheese", "Testing cheese products"),
        ("organic", "Testing organic products"),
        ("milk", "Testing milk products"),
        ("hummus", "Testing hummus products"),
        ("bread", "Testing bread products"),
        ("gluten free", "Testing gluten-free products"),
        ("baby formula", "Testing baby formula"),
        ("frozen", "Testing frozen products")
    ]
    
    for query, description in test_cases:
        print(f"\n{description}")
        print("=" * len(description))
        
        results = search_products(query)
        
        if results:
            print(f"Found {len(results)} products for '{query}':")
            print(format_product_results(results[:2]))  # Show only top 2
        else:
            print(f"No products found for '{query}'")
        
        print("\n" + "-" * 50)

def main():
    print("Product Search Testing\n")
    
    # Check index status
    if not check_index_status():
        print("Please run index_with_llamaindex.py first!")
        return
    
    # Run search tests
    test_searches()
    
    print("\nTesting complete!")

if __name__ == "__main__":
    main()