# src/test_pipeline.py

import pandas as pd
import os
from datetime import datetime
from pipeline import EcommerceDataPipeline
import logging

def create_sample_data():
    """Create a sample CSV file for testing"""
    sample_data = {
        'product_id': ['P001', 'P002', 'P003'],
        'price': [25.99, 15.50, 100.00],
        'quantity': [2, 3, 1],
        'transaction_date': ['2024-01-01', '2024-01-01', '2024-01-02']
    }
    
    df = pd.DataFrame(sample_data)
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Save to CSV
    df.to_csv('data/sample_data.csv', index=False)
    return 'data/sample_data.csv'

def test_pipeline():
    """Test all pipeline components"""
    try:
        print("Starting pipeline test...")
        
        # 1. Create sample data
        print("\n1. Creating sample data...")
        filepath = create_sample_data()
        print("✓ Sample data created successfully")
        
        # 2. Initialize pipeline
        print("\n2. Initializing pipeline...")
        pipeline = EcommerceDataPipeline()
        print("✓ Pipeline initialized successfully")
        
        # 3. Test database connection
        print("\n3. Testing database connection...")
        pipeline.connect_to_database()
        print("✓ Database connection successful")
        
        # 4. Test data extraction
        print("\n4. Testing data extraction...")
        df = pipeline.extract_from_csv(filepath)
        print(f"✓ Extracted {len(df)} records from CSV")
        
        # 5. Test data transformation
        print("\n5. Testing data transformation...")
        transformed_df = pipeline.transform_data(df)
        print("✓ Data transformation successful")
        print("\nTransformed data preview:")
        print(transformed_df.head())
        
        # 6. Test data loading
        print("\n6. Testing data loading...")
        pipeline.load_to_database(transformed_df, 'transactions')
        print("✓ Data loaded to database successfully")
        
        # 7. Test analytics
        print("\n7. Testing analytics generation...")
        analytics = pipeline.generate_basic_analytics('transactions')
        print("\nAnalytics results:")
        print(analytics)
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    test_pipeline()