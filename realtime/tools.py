import json
import random
import chainlit as cl
from datetime import datetime, timedelta
from database import (
    get_customer_by_id, 
    get_customer_orders, 
    get_order_details, 
    update_order, 
    cancel_order,
    get_order_items,
    add_item_to_order,
    update_customer_info
)

import logging
from product_search import product_retriever

# Configure logger
logger = logging.getLogger(__name__)


# Function Definitions
identify_customer_def = {
    "name": "identify_customer",
    "description": "Identify which customer is speaking",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string"}
        },
        "required": ["customer_id"]
    }
}

product_search_def = {
    "name": "product_search",
    "description": "Search for products in the supermarket inventory",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The product query from the customer (e.g., 'Do you have cheese?', 'Show me organic milk', 'I need gluten-free bread')"
            }
        },
        "required": ["query"]
    }
}

check_order_status_def = {
    "name": "check_order_status",
    "description": "Check the status of a customer's order",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "order_id": {
                "type": "string",
                "description": "The unique identifier for the order"
            }
        },
        "required": ["customer_id", "order_id"]
    }
}

process_return_def = {
    "name": "process_return",
    "description": "Initiate a return process for a customer's order",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "order_id": {
                "type": "string",
                "description": "The unique identifier for the order to be returned"
            },
            "reason": {
                "type": "string",
                "description": "The reason for the return"
            }
        },
        "required": ["customer_id", "order_id", "reason"]
    }
}

get_product_info_def = {
    "name": "get_product_info",
    "description": "Retrieve information about a specific product",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "product_id": {
                "type": "string",
                "description": "The unique identifier for the product"
            }
        },
        "required": ["customer_id", "product_id"]
    }
}

update_account_info_def = {
    "name": "update_account_info",
    "description": "Update a customer's account information",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "field": {
                "type": "string",
                "description": "The account field to be updated (e.g., 'email', 'phone', 'address')"
            },
            "value": {
                "type": "string",
                "description": "The new value for the specified field"
            }
        },
        "required": ["customer_id", "field", "value"]
    }
}

cancel_order_def = {  
    "name": "cancel_order",  
    "description": "Cancel a customer's order before it is processed",  
    "parameters": {  
        "type": "object",  
        "properties": {  
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "order_id": {  
                "type": "string",  
                "description": "The unique identifier of the order to be cancelled"  
            },  
            "reason": {  
                "type": "string",  
                "description": "The reason for cancelling the order"  
            }  
        },  
        "required": ["customer_id", "order_id", "reason"]  
    }  
}  

schedule_callback_def = {  
    "name": "schedule_callback",  
    "description": "Schedule a callback with a customer service representative",  
    "parameters": {  
        "type": "object",  
        "properties": {  
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "callback_time": {  
                "type": "string",  
                "description": "Preferred time for the callback in ISO 8601 format"  
            }  
        },  
        "required": ["customer_id", "callback_time"]  
    }  
}  

get_customer_info_def = {  
    "name": "get_customer_info",  
    "description": "Retrieve information about a specific customer",  
    "parameters": {  
        "type": "object",  
        "properties": {  
            "customer_id": {  
                "type": "string",  
                "description": "The unique identifier for the customer"  
            }  
        },  
        "required": ["customer_id"]  
    }  
}  

get_order_item_def = {
    "name": "get_order_item",
    "description": "Get details of a specific item in an order",
    "parameters": {
        "type": "object",
        "properties": {
            "item_id": {
                "type": "string",
                "description": "The unique identifier for the item"
            },
            "order_id": {
                "type": "string",
                "description": "The unique identifier for the order (optional)"
            }
        },
        "required": ["item_id"]
    }
}

update_order_item_def = {
    "name": "update_order_item",
    "description": "Update an item in a customer's order",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {
                "type": "string",
                "description": "The unique identifier for the customer"
            },
            "order_id": {
                "type": "string",
                "description": "The unique identifier for the order"
            },
            "item_id": {
                "type": "string",
                "description": "The unique identifier for the item to update"
            },
            "new_product_name": {
                "type": "string",
                "description": "The new product name (optional)"
            },
            "new_quantity": {
                "type": "integer",
                "description": "The new quantity (optional)"
            }
        },
        "required": ["customer_id", "order_id", "item_id"]
    }
}

add_item_to_order_def = {
    "name": "add_item_to_order",
    "description": "Add a new item to an existing order",
    "parameters": {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "product_name": {"type": "string"},
            "quantity": {"type": "integer"},
            "price": {"type": "number"}
        },
        "required": ["order_id", "product_name", "quantity", "price"]
    }
}

# Add this NEW function definition
list_order_items_def = {
    "name": "list_order_items",
    "description": "List all items in a customer's order",
    "parameters": {
        "type": "object",
        "properties": {
            "customer_id": {"type": "string"},
            "order_id": {"type": "string"}
        },
        "required": ["customer_id", "order_id"]
    }
}

# Handlers
async def identify_customer_handler(customer_id):
    """Simple customer identification for demo"""
    cl.user_session.set("customer_id", customer_id)
    customer = get_customer_by_id(customer_id)
    if customer:
        return f"Hello {customer['name']}! I can now help you with your orders."
    return "Customer not found. Please check your customer ID."

async def product_search_handler(query):
    """Handler for the product_search function using LlamaIndex"""
    try:
        # Use the LlamaIndex retriever
        results = product_retriever.search_products(query)
        
        if not results:
            return "I'm sorry, I couldn't find any matching products in our inventory."
        
        # Store results in session for easy addition
        cl.user_session.set("last_search_results", results)
        
        # Format the results
        response = f"I found {len(results)} products matching '{query}':\n\n"
        
        for i, product in enumerate(results, 1):
            response += f"{i}. {product['product_name']}\n"
            
            if product['brand'] != "Unknown":
                response += f"   Brand: {product['brand']}\n"
            
            if product['department'] != "Unknown":
                response += f"   Department: {product['department']}\n"
            
            response += f"   Price: ${product['price']:.2f}\n"
            
            if product['size'] != "Unknown":
                response += f"   Size: {product['size']}\n"
            
            response += f"   Match Score: {product['score']:.2f}\n\n"
        
        response += "Would you like to add any of these to your order?"
        return response
        
    except Exception as e:
        print(f"Error in product search handler: {e}")
        return "I encountered an error while searching for products."

async def add_item_to_order_handler(order_id, product_name, quantity, price):
    """Handler to add items from search to order"""
    customer_id = cl.user_session.get("customer_id")
    
    if not customer_id:
        return "Please identify yourself first. What's your customer ID?"
    
    # Verify order belongs to customer
    orders = get_customer_orders(customer_id)
    if not any(order['order_id'] == int(order_id) for order in orders):
        return "This order doesn't belong to you."
    
    success = add_item_to_order(order_id, product_name, quantity, price)
    if success:
        return f"Added {quantity} {product_name} to order {order_id} at ${price} each."
    return "Failed to add item to order"
    
async def cancel_order_handler(customer_id, order_id, reason):
    """Handler for the cancel_order function"""
    success = cancel_order(customer_id, order_id)
    
    if not success:
        return f"Failed to cancel order {order_id}. Please check if the order exists and belongs to customer {customer_id}."
    
    # Get the current date for cancellation date
    cancellation_date = datetime.now()
    # Mock refund amount (in a real app, you would calculate this)
    refund_amount = round(random.uniform(10, 500), 2)
    
    # Read the HTML template
    with open('order_cancellation_template.html', 'r') as file:
        html_content = file.read()
    
    # Replace placeholders with actual data
    html_content = html_content.format(
        order_id=order_id,
        customer_id=customer_id,
        cancellation_date=cancellation_date.strftime("%B %d, %Y"),
        refund_amount=refund_amount,
        status="Cancelled"
    )
    
    # Return the Chainlit message with HTML content
    await cl.Message(content=f"Your order has been cancelled. Here are the details:\n{html_content}").send()
    
    return f"Order {order_id} for customer {customer_id} has been cancelled. Reason: {reason}. A confirmation email has been sent."
  
async def schedule_callback_handler(customer_id, callback_time):  
    # Read the HTML template
    with open('callback_schedule_template.html', 'r') as file:
        html_content = file.read()

    # Replace placeholders with actual data
    html_content = html_content.format(
        customer_id=customer_id,
        callback_time=callback_time
    )

    # Return the Chainlit message with HTML content
    await cl.Message(content=f"Your callback has been scheduled. Here are the details:\n{html_content}").send()
    return f"Callback scheduled for customer {customer_id} at {callback_time}. A representative will contact you then."
  
async def check_order_status_handler(customer_id, order_id):
    """Handler for the check_order_status function"""
    estimated_delivery, status, order_date = get_order_details(order_id, customer_id)
    
    # Read the HTML template
    with open('order_status_template.html', 'r') as file:
        html_content = file.read()
    
    # Replace placeholders with actual data
    html_content = html_content.format(
        order_id=order_id,
        customer_id=customer_id,
        order_date=order_date.strftime("%B %d, %Y"),
        estimated_delivery=estimated_delivery.strftime("%B %d, %Y"),
        status=status
    )
    
    # Return the Chainlit message with HTML content
    await cl.Message(content=f"Here is the detail of your order \n {html_content}").send()
    
    return f"Order {order_id} status for customer {customer_id}: {status}"
    

async def process_return_handler(customer_id, order_id, reason):
    return f"Return for order {order_id} initiated by customer {customer_id}. Reason: {reason}. Please expect a refund within 5-7 business days."

async def get_product_info_handler(customer_id, product_id):
    products = {
        "P001": {"name": "Wireless Earbuds", "price": 79.99, "stock": 50},
        "P002": {"name": "Smart Watch", "price": 199.99, "stock": 30},
        "P003": {"name": "Laptop Backpack", "price": 49.99, "stock": 100}
    }
    product_info = products.get(product_id, "Product not found")
    return f"Product information for customer {customer_id}: {json.dumps(product_info)}"

async def update_account_info_handler(customer_id, field, value):
    """Handler for the update_account_info function"""
    success = update_customer_info(customer_id, field, value)
    
    if success:
        return f"Account information updated for customer {customer_id}. {field.capitalize()} changed to: {value}"
    else:
        return f"Failed to update {field} for customer {customer_id}. Please check if the customer exists and the field is valid."

async def get_customer_info_handler(customer_id):
    """Handler for the get_customer_info function"""
    customer = get_customer_by_id(customer_id)
    
    if customer:
        return json.dumps({
            "customer_id": customer_id,
            "name": customer["name"],
            "email": customer["email"],
            "phone": customer["phone"]
        })
    else:
        return f"Customer with ID {customer_id} not found."

async def update_order_item_handler(customer_id, order_id, item_id, new_product_name=None, new_quantity=None):
    """Handler for updating an order item"""
    success = update_order(customer_id, order_id, item_id, new_product_name, new_quantity)
    
    if success:
        return f"Order {order_id}, item {item_id} has been updated successfully."
    else:
        return f"Failed to update order {order_id}, item {item_id}. Please check the IDs."

async def get_order_item_handler(item_id, order_id=None):
    """Handler for getting order item details"""
    # Convert string parameters to integers
    item_id_int = int(item_id) if isinstance(item_id, str) else item_id
    
    # Use existing get_order_items function
    if order_id:
        order_id_int = int(order_id) if isinstance(order_id, str) else order_id
        items = get_order_items(order_id_int)
        # Filter for the specific item
        item = next((i for i in items if i["item_id"] == item_id_int), None)
    else:
        # Search through all items (this is less efficient)
        all_orders = get_customer_orders("1")  # Using customer_id 1 for simplicity
        for order in all_orders:
            items = get_order_items(order["order_id"])
            item = next((i for i in items if i["item_id"] == item_id_int), None)
            if item:
                break
    
    if item:
        return json.dumps({
            "item_id": item["item_id"],
            "order_id": item["order_id"],
            "product_name": item["product_name"],
            "quantity": item["quantity"],
            "price": float(item["price"])
        })
    else:
        return f"No item found with ID {item_id}" + (f" in order {order_id}" if order_id else "")
    
# Add this NEW handler
async def list_order_items_handler(customer_id, order_id):
    """Handler for listing all items in an order"""
    # Verify order belongs to customer
    orders = get_customer_orders(customer_id)
    if not any(order['order_id'] == int(order_id) for order in orders):
        return "This order doesn't belong to you."
    
    # Get all items in the order
    items = get_order_items(order_id)
    
    if not items:
        return f"Order {order_id} is empty."
    
    response = f"Order {order_id} contains the following items:\n\n"
    total = 0
    
    for item in items:
        subtotal = float(item['price']) * item['quantity']
        response += f"• {item['product_name']}\n"
        response += f"  Quantity: {item['quantity']}\n"
        response += f"  Price per item: ${item['price']}\n"
        response += f"  Subtotal: ${subtotal:.2f}\n\n"
        total += subtotal
    
    response += f"Order Total: ${total:.2f}"
    return response


# Tools list with all handlers
tools = [
    (identify_customer_def, identify_customer_handler),
    (get_customer_info_def, get_customer_info_handler),
    (check_order_status_def, check_order_status_handler),
    (process_return_def, process_return_handler),
    (get_product_info_def, get_product_info_handler),
    (update_account_info_def, update_account_info_handler),
    (cancel_order_def, cancel_order_handler),
    (schedule_callback_def, schedule_callback_handler),
    (update_order_item_def, update_order_item_handler),
    (get_order_item_def, get_order_item_handler), 
    (product_search_def, product_search_handler),
    (add_item_to_order_def, add_item_to_order_handler)
]