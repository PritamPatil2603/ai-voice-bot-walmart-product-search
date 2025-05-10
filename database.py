import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection string
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Create a database connection"""
    try:
        # ONLY CHANGE: Add sslmode='require' for Supabase
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# ALL OTHER FUNCTIONS REMAIN EXACTLY THE SAME
# No other changes needed in this file

def get_customer_by_id(customer_id):
    """Get a customer by ID"""
    try:
        conn = get_db_connection()
        if not conn:
            return None
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM customers WHERE customer_id = %s",
                (customer_id,)
            )
            customer = cursor.fetchone()
        conn.close()
        return customer
    except Exception as e:
        print(f"Error getting customer: {e}")
        return None

def get_customer_orders(customer_id):
    """Get all orders for a customer"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM orders WHERE customer_id = %s ORDER BY order_date DESC",
                (customer_id,)
            )
            orders = cursor.fetchall()
        conn.close()
        return orders
    except Exception as e:
        print(f"Error getting orders: {e}")
        return []

def get_order_details(order_id, customer_id):
    """Get details of a specific order"""
    try:
        conn = get_db_connection()
        if not conn:
            return datetime.now() + timedelta(days=5), "Error", datetime.now()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT o.order_date, o.status, o.estimated_delivery 
                FROM orders o
                WHERE o.order_id = %s AND o.customer_id = %s
                """,
                (order_id, customer_id)
            )
            order = cursor.fetchone()
            
            if order:
                estimated_delivery = order['estimated_delivery']
                status = order['status']
                order_date = order['order_date']
                return estimated_delivery, status, order_date
            else:
                # Return default values if order not found
                return (
                    datetime.now() + timedelta(days=5),  # Default estimated delivery
                    "Not Found",                         # Default status
                    datetime.now()                       # Default order date
                )
    except Exception as e:
        print(f"Error getting order details: {e}")
        # Return default values on error
        return (
            datetime.now() + timedelta(days=5),
            "Error",
            datetime.now()
        )
    finally:
        if conn:
            conn.close()

def get_order_items(order_id):
    """Get all items in an order"""
    try:
        conn = get_db_connection()
        if not conn:
            return []
        
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM order_items WHERE order_id = %s",
                (order_id,)
            )
            items = cursor.fetchall()
        conn.close()
        return items
    except Exception as e:
        print(f"Error getting order items: {e}")
        return []

def update_order(customer_id, order_id, item_id, new_product_name=None, new_quantity=None):
    """Update an item in an order"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        with conn.cursor() as cursor:
            # Verify the order belongs to the customer
            cursor.execute(
                "SELECT 1 FROM orders WHERE order_id = %s AND customer_id = %s",
                (order_id, customer_id)
            )
            if cursor.fetchone() is None:
                conn.close()
                return False  # Order not found or doesn't belong to customer
            
            # Build the update query based on provided parameters
            update_parts = []
            params = []
            
            if new_product_name is not None:
                update_parts.append("product_name = %s")
                params.append(new_product_name)
            
            if new_quantity is not None:
                update_parts.append("quantity = %s")
                params.append(new_quantity)
            
            if not update_parts:
                conn.close()
                return False  # No updates to make
            
            # Complete the parameter list
            params.append(order_id)
            params.append(item_id)
            
            # Execute the update
            query = f"""
                UPDATE order_items
                SET {', '.join(update_parts)}
                WHERE order_id = %s AND item_id = %s
            """
            cursor.execute(query, params)
            
            success = cursor.rowcount > 0
        conn.close()
        return success
    except Exception as e:
        print(f"Error updating order: {e}")
        return False
    
def update_customer_info(customer_id, field, value):
    """Update a customer's information"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        # Convert string parameters to integers if needed
        customer_id = int(customer_id) if isinstance(customer_id, str) else customer_id
        
        with conn.cursor() as cursor:
            # Check if the field is valid
            valid_fields = ["name", "email", "phone"]
            if field.lower() not in valid_fields:
                return False
            
            # Build and execute update query
            query = f"UPDATE customers SET {field.lower()} = %s WHERE customer_id = %s"
            cursor.execute(query, (value, customer_id))
            
            success = cursor.rowcount > 0
        conn.close()
        return success
    except Exception as e:
        print(f"Error updating customer info: {e}")
        return False

def cancel_order(customer_id, order_id):
    """Cancel an order"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        with conn.cursor() as cursor:
            # Verify the order belongs to the customer
            cursor.execute(
                "SELECT 1 FROM orders WHERE order_id = %s AND customer_id = %s",
                (order_id, customer_id)
            )
            if cursor.fetchone() is None:
                conn.close()
                return False  # Order not found or doesn't belong to customer
            
            # Update the order status to 'Cancelled'
            cursor.execute(
                "UPDATE orders SET status = 'Cancelled' WHERE order_id = %s",
                (order_id,)
            )
            
            success = cursor.rowcount > 0
        conn.close()
        return success
    except Exception as e:
        print(f"Error cancelling order: {e}")
        return False