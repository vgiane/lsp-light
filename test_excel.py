import polars as pl
import io
import base64

# Test if Polars can read Excel files
try:
    # Create a simple test DataFrame and save it as Excel
    df_test = pl.DataFrame({
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35],
        "City": ["New York", "London", "Tokyo"]
    })
    
    # Save to Excel
    df_test.write_excel("test_sample.xlsx")
    print("✓ Successfully created test Excel file")
    
    # Try to read it back
    df_read = pl.read_excel("test_sample.xlsx")
    print("✓ Successfully read Excel file with Polars")
    print(f"DataFrame shape: {df_read.shape}")
    print(f"Columns: {df_read.columns}")
    print(df_read)
    
except Exception as e:
    print(f"❌ Error: {e}")
    print("This might indicate an issue with Excel support in Polars")
