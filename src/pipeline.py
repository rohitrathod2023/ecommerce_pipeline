import pandas as pd
import pymysql
from datetime import datetime
import logging

class EcommerceDataPipeline:
    def __init__(self):
        """Initialize the pipeline with database configuration"""
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'rohit123',
            'database': 'ecommerce_db',
            'charset': 'utf8mb4'
        }
        self.connection = None
        
    def connect_to_database(self):
        """Establish database connection"""
        try:
            print(f"Connecting to database: {self.db_config['database']} at {self.db_config['host']} with user {self.db_config['user']}")
            self.connection = pymysql.connect(**self.db_config)
            
            # Create transactions table if it doesn't exist
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS transactions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_id VARCHAR(10) NOT NULL,
                    price DECIMAL(10, 2) NOT NULL,
                    quantity INT NOT NULL,
                    transaction_date DATE NOT NULL,
                    total_amount DECIMAL(10, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")

    def extract_from_csv(self, filepath):
        """Extract data from CSV file"""
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            raise Exception(f"Data extraction failed: {str(e)}")

    def transform_data(self, df):
        """Transform the data"""
        try:
            # Create a copy to avoid modifying the original dataframe
            transformed_df = df.copy()
            
            # Convert transaction_date to datetime
            transformed_df['transaction_date'] = pd.to_datetime(transformed_df['transaction_date'])
            
            # Calculate total amount
            transformed_df['total_amount'] = transformed_df['price'] * transformed_df['quantity']
            
            return transformed_df
            
        except Exception as e:
            raise Exception(f"Data transformation failed: {str(e)}")

    def load_to_database(self, df, table_name):
        """Load data to database"""
        try:
            with self.connection.cursor() as cursor:
                # Prepare the SQL insert statement
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Convert DataFrame to list of tuples for insertion
                values = [tuple(row) for row in df.values]
                
                # Execute insert
                cursor.executemany(insert_sql, values)
                self.connection.commit()
                
        except Exception as e:
            raise Exception(f"Data loading failed: {str(e)}")

    def generate_basic_analytics(self, table_name):
        """Generate basic analytics from the loaded data"""
        try:
            with self.connection.cursor() as cursor:
                # Get total sales
                cursor.execute(f"SELECT SUM(total_amount) as total_sales FROM {table_name}")
                total_sales = cursor.fetchone()[0]
                
                # Get sales by date
                cursor.execute(f"""
                    SELECT transaction_date, SUM(total_amount) as daily_sales
                    FROM {table_name}
                    GROUP BY transaction_date
                    ORDER BY transaction_date
                """)
                daily_sales = cursor.fetchall()
                
                # Get top products by quantity
                cursor.execute(f"""
                    SELECT product_id, SUM(quantity) as total_quantity
                    FROM {table_name}
                    GROUP BY product_id
                    ORDER BY total_quantity DESC
                    LIMIT 5
                """)
                top_products = cursor.fetchall()
                
                return {
                    'total_sales': float(total_sales) if total_sales else 0,
                    'daily_sales': [{
                        'date': str(date),
                        'sales': float(sales)
                    } for date, sales in daily_sales],
                    'top_products': [{
                        'product_id': prod_id,
                        'total_quantity': int(qty)
                    } for prod_id, qty in top_products]
                }
                
        except Exception as e:
            raise Exception(f"Analytics generation failed: {str(e)}")

    def __del__(self):
        """Cleanup database connection"""
        if self.connection and self.connection.open:
            self.connection.close()