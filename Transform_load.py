# Data Pipeline - Transform and Load Script
# Created by: Gustav Christensen
# Date: December 2025
# Description: Generates fake data using Faker library, transforms it, and loads into SQL Server.
#              Useful for testing and development before production Dataverse data is available.

"""
Transform and Load (TL) Script with Fake Data Generation

This script generates fake data using Faker library, transforms it, and loads it into a SQL database.
It demonstrates the Transform and Load phases of an ETL pipeline.

Configuration via environment variables:
    - DB_CONNECTION_STRING: SQL database connection string (optional if using Module.Connection_pool)
    - DB_SCHEMA: Database schema name (default: dbo)
    - NUM_RECORDS: Number of fake records to generate (default: 1000)
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from random import choice, randint, uniform

import pandas as pd
from faker import Faker
from sqlalchemy import Column, DateTime, Float, Integer, MetaData, String, Table, inspect
from sqlalchemy.types import NVARCHAR

from Module.Connection_pool import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transform_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "dbo")
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000"))

# Initialize Faker
fake = Faker(['da_DK', 'en_US'])  # Danish and English locales


class TransformLoad:
    """Main class for generating fake data, transforming, and loading to SQL"""
    
    def __init__(self):
        self.engine = Engine
        self.script_name = os.path.basename(__file__)
        self.started = datetime.now()
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            logger.info("Script completed successfully")
        else:
            logger.error(f"Script finished with error: {exc_type.__name__}: {exc_val}")
    
    def generate_fake_customers(self, num_records):
        """Generate fake customer data using Faker"""
        logger.info(f"Generating {num_records} fake customer records...")
        
        customers = []
        for i in range(num_records):
            customer = {
                'customer_id': f'CUST{i+1:06d}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.street_address(),
                'city': fake.city(),
                'postal_code': fake.postcode(),
                'country': fake.country(),
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90),
                'registration_date': fake.date_between(start_date='-5y', end_date='today'),
                'is_active': choice([True, False]),
                'customer_segment': choice(['Premium', 'Standard', 'Basic']),
                'credit_score': randint(300, 850),
                'created_at': datetime.now()
            }
            customers.append(customer)
        
        logger.info(f"Generated {len(customers)} customer records")
        return pd.DataFrame(customers)
    
    def generate_fake_orders(self, num_records):
        """Generate fake order data using Faker"""
        logger.info(f"Generating {num_records} fake order records...")
        
        orders = []
        for i in range(num_records):
            order_date = fake.date_between(start_date='-2y', end_date='today')
            order = {
                'order_id': f'ORD{i+1:08d}',
                'customer_id': f'CUST{randint(1, min(NUM_RECORDS, 1000)):06d}',
                'order_date': order_date,
                'order_status': choice(['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']),
                'product_name': fake.word().capitalize() + ' ' + fake.word().capitalize(),
                'product_category': choice(['Electronics', 'Clothing', 'Food', 'Books', 'Home']),
                'quantity': randint(1, 10),
                'unit_price': round(uniform(10.0, 1000.0), 2),
                'discount_percent': choice([0, 5, 10, 15, 20, 25]),
                'shipping_method': choice(['Standard', 'Express', 'Overnight']),
                'payment_method': choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
                'created_at': datetime.now()
            }
            orders.append(order)
        
        logger.info(f"Generated {len(orders)} order records")
        return pd.DataFrame(orders)
    
    def generate_fake_products(self, num_records):
        """Generate fake product data using Faker"""
        logger.info(f"Generating {num_records} fake product records...")
        
        products = []
        for i in range(num_records):
            product = {
                'product_id': f'PROD{i+1:06d}',
                'product_name': fake.word().capitalize() + ' ' + fake.word().capitalize(),
                'product_description': fake.text(max_nb_chars=200),
                'category': choice(['Electronics', 'Clothing', 'Food', 'Books', 'Home']),
                'brand': fake.company(),
                'price': round(uniform(5.0, 5000.0), 2),
                'cost': round(uniform(2.0, 3000.0), 2),
                'stock_quantity': randint(0, 1000),
                'weight_kg': round(uniform(0.1, 50.0), 2),
                'dimensions': f"{randint(5, 100)}x{randint(5, 100)}x{randint(5, 100)}",
                'supplier': fake.company(),
                'created_at': datetime.now()
            }
            products.append(product)
        
        logger.info(f"Generated {len(products)} product records")
        return pd.DataFrame(products)
    
    def transform_customers(self, df):
        """Transform customer data"""
        logger.info("Transforming customer data...")
        
        df_transformed = df.copy()
        
        # Create full name
        df_transformed['full_name'] = df_transformed['first_name'] + ' ' + df_transformed['last_name']
        
        # Standardize email to lowercase
        df_transformed['email'] = df_transformed['email'].str.lower()
        
        # Clean phone numbers (remove spaces and special characters)
        df_transformed['phone'] = df_transformed['phone'].str.replace(r'[^\d+]', '', regex=True)
        
        # Calculate age from date of birth
        df_transformed['age'] = ((datetime.now() - pd.to_datetime(df_transformed['date_of_birth'])).dt.days / 365.25).astype(int)
        
        # Calculate days since registration
        df_transformed['days_since_registration'] = (datetime.now() - pd.to_datetime(df_transformed['registration_date'])).dt.days
        
        # Convert boolean to string for SQL compatibility
        df_transformed['is_active'] = df_transformed['is_active'].map({True: '1', False: '0'})
        
        # Add audit date
        df_transformed['AuditDate'] = datetime.now()
        
        logger.info(f"Transformed {len(df_transformed)} customer records")
        return df_transformed
    
    def transform_orders(self, df):
        """Transform order data"""
        logger.info("Transforming order data...")
        
        df_transformed = df.copy()
        
        # Calculate total price before discount
        df_transformed['subtotal'] = df_transformed['quantity'] * df_transformed['unit_price']
        
        # Calculate discount amount
        df_transformed['discount_amount'] = df_transformed['subtotal'] * (df_transformed['discount_percent'] / 100)
        
        # Calculate total price after discount
        df_transformed['total_price'] = df_transformed['subtotal'] - df_transformed['discount_amount']
        
        # Add shipping cost based on method
        shipping_costs = {'Standard': 5.0, 'Express': 15.0, 'Overnight': 30.0}
        df_transformed['shipping_cost'] = df_transformed['shipping_method'].map(shipping_costs)
        
        # Calculate final total including shipping
        df_transformed['final_total'] = df_transformed['total_price'] + df_transformed['shipping_cost']
        
        # Round monetary values to 2 decimal places
        money_columns = ['subtotal', 'discount_amount', 'total_price', 'shipping_cost', 'final_total']
        for col in money_columns:
            df_transformed[col] = df_transformed[col].round(2)
        
        # Calculate order age in days
        df_transformed['order_age_days'] = (datetime.now() - pd.to_datetime(df_transformed['order_date'])).dt.days
        
        # Add audit date
        df_transformed['AuditDate'] = datetime.now()
        
        logger.info(f"Transformed {len(df_transformed)} order records")
        return df_transformed
    
    def transform_products(self, df):
        """Transform product data"""
        logger.info("Transforming product data...")
        
        df_transformed = df.copy()
        
        # Calculate profit margin
        df_transformed['profit_margin'] = ((df_transformed['price'] - df_transformed['cost']) / df_transformed['price'] * 100).round(2)
        
        # Calculate markup percentage
        df_transformed['markup_percent'] = ((df_transformed['price'] - df_transformed['cost']) / df_transformed['cost'] * 100).round(2)
        
        # Categorize stock levels
        def categorize_stock(quantity):
            if quantity == 0:
                return 'Out of Stock'
            elif quantity < 20:
                return 'Low Stock'
            elif quantity < 100:
                return 'Medium Stock'
            else:
                return 'High Stock'
        
        df_transformed['stock_status'] = df_transformed['stock_quantity'].apply(categorize_stock)
        
        # Calculate inventory value
        df_transformed['inventory_value'] = (df_transformed['stock_quantity'] * df_transformed['cost']).round(2)
        
        # Standardize product names to title case
        df_transformed['product_name'] = df_transformed['product_name'].str.title()
        
        # Clean description (trim whitespace)
        df_transformed['product_description'] = df_transformed['product_description'].str.strip()
        
        # Add audit date
        df_transformed['AuditDate'] = datetime.now()
        
        logger.info(f"Transformed {len(df_transformed)} product records")
        return df_transformed
    
    def create_table_if_not_exists(self, table_name, sample_df):
        """Create table if it doesn't exist"""
        inspector = inspect(self.engine)
        
        if inspector.has_table(table_name, schema=DB_SCHEMA):
            logger.info(f"Table {DB_SCHEMA}.{table_name} already exists")
            return
        
        metadata = MetaData()
        table_columns = []
        
        for col in sample_df.columns:
            if col == 'AuditDate':
                table_columns.append(Column(col, DateTime()))
            elif sample_df[col].dtype in ['int64', 'int32']:
                table_columns.append(Column(col, Integer()))
            elif sample_df[col].dtype in ['float64', 'float32']:
                table_columns.append(Column(col, Float()))
            else:
                # Default to NVARCHAR for strings and other types
                max_length = sample_df[col].astype(str).str.len().max()
                length = min(max(100, int(max_length * 1.5)), 4000)  # Add buffer, cap at 4000
                table_columns.append(Column(col, NVARCHAR(length)))
        
        table = Table(table_name, metadata, *table_columns, schema=DB_SCHEMA)
        metadata.create_all(self.engine)
        
        logger.info(f"Created table {DB_SCHEMA}.{table_name} with {len(table_columns)} columns")
    
    def load_data(self, df, table_name, if_exists='replace'):
        """Load data into SQL database"""
        logger.info(f"Loading data to table {DB_SCHEMA}.{table_name}...")
        
        try:
            # Create table if it doesn't exist
            self.create_table_if_not_exists(table_name, df)
            
            # Load data in chunks
            chunksize = 1000
            total_loaded = 0
            
            with self.engine.begin() as connection:
                # Truncate table if replace mode
                if if_exists == 'replace':
                    inspector = inspect(self.engine)
                    if inspector.has_table(table_name, schema=DB_SCHEMA):
                        from sqlalchemy import text
                        connection.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{table_name}"))
                        logger.info(f"Truncated table {DB_SCHEMA}.{table_name}")
                
                # Insert data in chunks
                for i in range(0, len(df), chunksize):
                    chunk = df.iloc[i:i+chunksize]
                    chunk.to_sql(
                        name=table_name,
                        con=connection,
                        schema=DB_SCHEMA,
                        if_exists='append',
                        index=False
                    )
                    total_loaded += len(chunk)
                    logger.info(f"Loaded {total_loaded}/{len(df)} records to {table_name}")
            
            logger.info(f"Successfully loaded {total_loaded} records to {DB_SCHEMA}.{table_name}")
            return total_loaded
            
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise
    
    def run_etl(self):
        """Run the complete ETL process"""
        try:
            logger.info("Starting Transform and Load process...")
            logger.info(f"Number of records to generate: {NUM_RECORDS}")
            logger.info(f"Database schema: {DB_SCHEMA}")
            
            # Generate fake data
            logger.info("\n=== GENERATE PHASE ===")
            customers_df = self.generate_fake_customers(NUM_RECORDS)
            orders_df = self.generate_fake_orders(NUM_RECORDS * 2)  # More orders than customers
            products_df = self.generate_fake_products(NUM_RECORDS // 2)  # Fewer products
            
            # Transform data
            logger.info("\n=== TRANSFORM PHASE ===")
            customers_transformed = self.transform_customers(customers_df)
            orders_transformed = self.transform_orders(orders_df)
            products_transformed = self.transform_products(products_df)
            
            # Load data
            logger.info("\n=== LOAD PHASE ===")
            customer_count = self.load_data(customers_transformed, 'FakeCustomers', if_exists='replace')
            order_count = self.load_data(orders_transformed, 'FakeOrders', if_exists='replace')
            product_count = self.load_data(products_transformed, 'FakeProducts', if_exists='replace')
            
            # Summary
            logger.info("\n=== SUMMARY ===")
            logger.info(f"Total customers loaded: {customer_count}")
            logger.info(f"Total orders loaded: {order_count}")
            logger.info(f"Total products loaded: {product_count}")
            logger.info(f"Total execution time: {datetime.now() - self.started}")
            
            return 0
            
        except Exception as e:
            logger.error(f"ETL process failed: {str(e)}")
            return 1


if __name__ == "__main__":
    with TransformLoad() as tl:
        exit_code = tl.run_etl()
    
    sys.exit(exit_code)
