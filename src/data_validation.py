# data_validation.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class DataValidator:
    def __init__(self):
        self.validation_results = []
        
    def validate_data(self, df):
        """Performs comprehensive data validation"""
        self.validation_results = []
        
        # Price validation
        self._validate_price(df)
        
        # Quantity validation
        self._validate_quantity(df)
        
        # Date validation
        self._validate_dates(df)
        
        # Product ID format validation
        self._validate_product_ids(df)
        
        # Check for duplicates
        self._check_duplicates(df)
        
        return {
            'is_valid': len(self.validation_results) == 0,
            'validation_results': self.validation_results,
            'clean_data': self._clean_data(df)
        }
    
    def _validate_price(self, df):
        """Validate price values"""
        if df['price'].lt(0).any():
            self.validation_results.append("Negative prices found")
        if df['price'].isnull().any():
            self.validation_results.append("Missing prices found")
            
    def _validate_quantity(self, df):
        """Validate quantity values"""
        if df['quantity'].lt(0).any():
            self.validation_results.append("Negative quantities found")
        if not df['quantity'].dtype in ['int64', 'int32']:
            self.validation_results.append("Non-integer quantities found")
            
    def _validate_dates(self, df):
        """Validate transaction dates"""
        today = datetime.now().date()
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        
        if df['transaction_date'].gt(today).any():
            self.validation_results.append("Future dates found")
        if df['transaction_date'].lt(today - timedelta(days=365)).any():
            self.validation_results.append("Dates older than 1 year found")
            
    def _validate_product_ids(self, df):
        """Validate product ID format"""
        valid_format = df['product_id'].str.match(r'^P\d{3}$')
        if not valid_format.all():
            self.validation_results.append("Invalid product ID format found")
            
    def _check_duplicates(self, df):
        """Check for duplicate transactions"""
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            self.validation_results.append(f"Found {duplicates} duplicate records")
            
    def _clean_data(self, df):
        """Clean data based on validation results"""
        clean_df = df.copy()
        
        # Remove duplicates
        clean_df = clean_df.drop_duplicates()
        
        # Handle invalid prices
        clean_df.loc[clean_df['price'] < 0, 'price'] = np.nan
        
        # Handle invalid quantities
        clean_df.loc[clean_df['quantity'] < 0, 'quantity'] = np.nan
        
        # Remove rows with any null values
        clean_df = clean_df.dropna()
        
        return clean_df