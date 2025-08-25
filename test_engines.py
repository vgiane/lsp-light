import polars as pl
import pandas as pd
import tempfile
import os
import time
import random
import string

def generate_large_dataset(num_rows=10000):
    """Generate a large dataset for performance testing"""
    print(f"Generating dataset with {num_rows:,} rows...")
    
    # Generate random data
    names = [f"Speaker_{i:05d}" for i in range(num_rows)]
    ages = [random.randint(18, 80) for _ in range(num_rows)]
    cities = [random.choice(["New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai"]) for _ in range(num_rows)]
    salaries = [random.randint(30000, 150000) for _ in range(num_rows)]
    departments = [random.choice(["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations"]) for _ in range(num_rows)]
    emails = [f"speaker{i}@company.com" for i in range(num_rows)]
    phone_numbers = [f"+1-555-{random.randint(1000000, 9999999)}" for _ in range(num_rows)]
    hire_dates = [f"2020-{random.randint(1,12):02d}-{random.randint(1,28):02d}" for _ in range(num_rows)]
    performance_scores = [round(random.uniform(1.0, 5.0), 2) for _ in range(num_rows)]
    active_status = [random.choice([True, False]) for _ in range(num_rows)]
    
    return pl.DataFrame({
        "Name": names,
        "Age": ages,
        "City": cities,
        "Salary": salaries,
        "Department": departments,
        "Email": emails,
        "Phone": phone_numbers,
        "Hire_Date": hire_dates,
        "Performance_Score": performance_scores,
        "Active": active_status
    })

def time_function(func, description):
    """Time a function execution"""
    start_time = time.time()
    try:
        result = func()
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"✓ {description}: {elapsed:.3f}s")
        return result, elapsed, None
    except Exception as e:
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"❌ {description}: FAILED in {elapsed:.3f}s - {e}")
        return None, elapsed, str(e)

def test_export_methods(df_polars, df_pandas, size, test_prefix="export_test"):
    """Test different export methods for both Polars and Pandas"""
    print(f"\n🔄 EXPORT PERFORMANCE TESTS:")
    
    export_results = []
    
    # Polars export tests
    print(f"\n📤 Polars Export Methods:")
    
    # Test 1: Polars write_excel (default)
    def polars_write_excel():
        filename = f"{test_prefix}_polars_default_{size}.xlsx"
        df_polars.write_excel(filename)
        return filename
    
    result1, time1, error1 = time_function(
        polars_write_excel,
        "Polars write_excel (default)"
    )
    if result1: os.remove(result1)
    export_results.append(("Polars Excel Default", time1, error1))
    
    # Test 2: Polars write_excel (no engine parameter needed)
    def polars_write_excel_optimized():
        filename = f"{test_prefix}_polars_optimized_{size}.xlsx"
        df_polars.write_excel(filename)
        return filename
    
    result2, time2, error2 = time_function(
        polars_write_excel_optimized,
        "Polars write_excel (optimized)"
    )
    if result2: os.remove(result2)
    export_results.append(("Polars Excel Optimized", time2, error2))
    
    # Test 3: Polars write_csv
    def polars_write_csv():
        filename = f"{test_prefix}_polars_{size}.csv"
        df_polars.write_csv(filename)
        return filename
    
    result3, time3, error3 = time_function(
        polars_write_csv,
        "Polars write_csv"
    )
    if result3: os.remove(result3)
    export_results.append(("Polars CSV", time3, error3))
    
    # Test 4: Polars write_parquet
    def polars_write_parquet():
        filename = f"{test_prefix}_polars_{size}.parquet"
        df_polars.write_parquet(filename)
        return filename
    
    result4, time4, error4 = time_function(
        polars_write_parquet,
        "Polars write_parquet"
    )
    if result4: os.remove(result4)
    export_results.append(("Polars Parquet", time4, error4))
    
    # Pandas export tests
    print(f"\n📤 Pandas Export Methods:")
    
    # Test 5: Pandas to_excel (default)
    def pandas_to_excel():
        filename = f"{test_prefix}_pandas_default_{size}.xlsx"
        df_pandas.to_excel(filename, index=False)
        return filename
    
    result5, time5, error5 = time_function(
        pandas_to_excel,
        "Pandas to_excel (default)"
    )
    if result5: os.remove(result5)
    export_results.append(("Pandas Excel Default", time5, error5))
    
    # Test 6: Pandas to_excel with openpyxl
    def pandas_to_excel_openpyxl():
        filename = f"{test_prefix}_pandas_openpyxl_{size}.xlsx"
        df_pandas.to_excel(filename, index=False, engine="openpyxl")
        return filename
    
    result6, time6, error6 = time_function(
        pandas_to_excel_openpyxl,
        "Pandas to_excel (openpyxl)"
    )
    if result6: os.remove(result6)
    export_results.append(("Pandas Excel OpenPyXL", time6, error6))
    
    # Test 7: Pandas to_excel with xlsxwriter
    def pandas_to_excel_xlsxwriter():
        filename = f"{test_prefix}_pandas_xlsxwriter_{size}.xlsx"
        df_pandas.to_excel(filename, index=False, engine="xlsxwriter")
        return filename
    
    result7, time7, error7 = time_function(
        pandas_to_excel_xlsxwriter,
        "Pandas to_excel (xlsxwriter)"
    )
    if result7: os.remove(result7)
    export_results.append(("Pandas Excel XlsxWriter", time7, error7))
    
    # Test 8: Pandas to_csv
    def pandas_to_csv():
        filename = f"{test_prefix}_pandas_{size}.csv"
        df_pandas.to_csv(filename, index=False)
        return filename
    
    result8, time8, error8 = time_function(
        pandas_to_csv,
        "Pandas to_csv"
    )
    if result8: os.remove(result8)
    export_results.append(("Pandas CSV", time8, error8))
    
    # Test 9: Pandas to_parquet
    def pandas_to_parquet():
        filename = f"{test_prefix}_pandas_{size}.parquet"
        df_pandas.to_parquet(filename, index=False)
        return filename
    
    result9, time9, error9 = time_function(
        pandas_to_parquet,
        "Pandas to_parquet"
    )
    if result9: os.remove(result9)
    export_results.append(("Pandas Parquet", time9, error9))
    
    # Export summary
    print(f"\n📊 EXPORT SUMMARY for {size:,} rows:")
    
    # Sort by time (successful methods only)
    successful_exports = [(name, t) for name, t, err in export_results if err is None]
    if successful_exports:
        successful_exports.sort(key=lambda x: x[1])
        print(f"🏆 Fastest export: {successful_exports[0][0]} ({successful_exports[0][1]:.3f}s)")
        print(f"💾 Rows per second: {size/successful_exports[0][1]:,.0f}")
        
        # Show top 3 fastest export methods
        print(f"\n🥇 Top 3 fastest export methods:")
        for i, (name, time_taken) in enumerate(successful_exports[:3], 1):
            medal = ["🥇", "🥈", "🥉"][i-1]
            print(f"   {medal} {name}: {time_taken:.3f}s ({size/time_taken:,.0f} rows/s)")
    
    # Compare export formats
    excel_exports = [(name, t) for name, t, err in export_results if "Excel" in name and err is None]
    csv_exports = [(name, t) for name, t, err in export_results if "CSV" in name and err is None]
    parquet_exports = [(name, t) for name, t, err in export_results if "Parquet" in name and err is None]
    
    print(f"\n📁 Format Comparison:")
    if excel_exports:
        avg_excel = sum(t for _, t in excel_exports) / len(excel_exports)
        print(f"   Excel average: {avg_excel:.3f}s")
    if csv_exports:
        avg_csv = sum(t for _, t in csv_exports) / len(csv_exports)
        print(f"   CSV average: {avg_csv:.3f}s")
    if parquet_exports:
        avg_parquet = sum(t for _, t in parquet_exports) / len(parquet_exports)
        print(f"   Parquet average: {avg_parquet:.3f}s")
    
    # Compare libraries for exports
    polars_exports = [(name, t) for name, t, err in export_results if "Polars" in name and err is None]
    pandas_exports = [(name, t) for name, t, err in export_results if "Pandas" in name and err is None]
    
    if polars_exports and pandas_exports:
        avg_polars_export = sum(t for _, t in polars_exports) / len(polars_exports)
        avg_pandas_export = sum(t for _, t in pandas_exports) / len(pandas_exports)
        
        print(f"\n⚖️  Export Library Comparison:")
        print(f"   Polars average: {avg_polars_export:.3f}s")
        print(f"   Pandas average: {avg_pandas_export:.3f}s")
        if avg_polars_export < avg_pandas_export:
            speedup = avg_pandas_export / avg_polars_export
            print(f"   🚀 Polars is {speedup:.1f}x faster for exports")
        else:
            speedup = avg_polars_export / avg_pandas_export
            print(f"   🚀 Pandas is {speedup:.1f}x faster for exports")
    
    return export_results

# Test different dataset sizes
test_sizes = [5000, 10000, 20000]

for size in test_sizes:
    print(f"\n{'='*60}")
    print(f"PERFORMANCE TEST: {size:,} rows")
    print(f"{'='*60}")
    
    # Generate test data
    df_test = generate_large_dataset(size)
    
    # Save to Excel using write_excel
    test_file = f"speaker_data_{size}.xlsx"
    
    print(f"\nCreating Excel file with {size:,} rows...")
    start_time = time.time()
    df_test.write_excel(test_file)
    write_time = time.time() - start_time
    print(f"✓ File creation: {write_time:.3f}s")
    
    file_size = os.path.getsize(test_file) / 1024 / 1024  # Size in MB
    print(f"📁 File size: {file_size:.2f} MB")
    
    print(f"\nTesting read methods:")
    
    # Test 1: Default engine
    df_read1, time1, error1 = time_function(
        lambda: pl.read_excel(test_file),
        "Method 1: Polars Default engine"
    )
    
    # Test 2: Calamine engine (fastest and most reliable)
    df_read2, time2, error2 = time_function(
        lambda: pl.read_excel(test_file, engine="calamine"),
        "Method 2: Polars Calamine engine"
    )
    
    # Test 3: BytesIO method (simulating web upload)
    def read_from_bytes():
        with open(test_file, 'rb') as f:
            file_bytes = f.read()
        import io
        file_like = io.BytesIO(file_bytes)
        return pl.read_excel(file_like)
    
    df_read3, time3, error3 = time_function(
        read_from_bytes,
        "Method 3: Polars BytesIO (web upload simulation)"
    )
    
    # Test 4: Temporary file method (our fallback)
    def read_from_temp():
        with open(test_file, 'rb') as f:
            file_bytes = f.read()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            tmp_file.write(file_bytes)
            tmp_file_path = tmp_file.name
        
        df = pl.read_excel(tmp_file_path)
        os.unlink(tmp_file_path)
        return df
    
    df_read4, time4, error4 = time_function(
        read_from_temp,
        "Method 4: Polars Temporary file fallback"
    )
    
    # Test 5: Pandas with default engine
    df_read5, time5, error5 = time_function(
        lambda: pd.read_excel(test_file),
        "Method 5: Pandas (default engine)"
    )
    
    # Test 6: Pandas with openpyxl engine
    df_read6, time6, error6 = time_function(
        lambda: pd.read_excel(test_file, engine="openpyxl"),
        "Method 6: Pandas (openpyxl engine)"
    )
    
    # Test 7: Pandas with calamine engine
    df_read7, time7, error7 = time_function(
        lambda: pd.read_excel(test_file, engine="calamine"),
        "Method 7: Pandas (calamine engine)"
    )
    
    # Test 8: Pandas from BytesIO (simulating web upload)
    def pandas_read_from_bytes():
        with open(test_file, 'rb') as f:
            file_bytes = f.read()
        import io
        file_like = io.BytesIO(file_bytes)
        return pd.read_excel(file_like)
    
    df_read8, time8, error8 = time_function(
        pandas_read_from_bytes,
        "Method 8: Pandas BytesIO (web upload simulation)"
    )
    
    # Summary for this size
    print(f"\n📊 SUMMARY for {size:,} rows:")
    methods = [
        ("Polars Default", time1, error1),
        ("Polars Calamine", time2, error2),
        ("Polars BytesIO", time3, error3),
        ("Polars Temp File", time4, error4),
        ("Pandas Default", time5, error5),
        ("Pandas OpenPyXL", time6, error6),
        ("Pandas Calamine", time7, error7),
        ("Pandas BytesIO", time8, error8)
    ]
    
    # Sort by time (successful methods only)
    successful = [(name, t) for name, t, err in methods if err is None]
    if successful:
        successful.sort(key=lambda x: x[1])
        print(f"🏆 Fastest method: {successful[0][0]} ({successful[0][1]:.3f}s)")
        print(f"💾 Rows per second: {size/successful[0][1]:,.0f}")
        
        # Show top 3 fastest methods
        print(f"\n🥇 Top 3 fastest methods:")
        for i, (name, time_taken) in enumerate(successful[:3], 1):
            medal = ["🥇", "🥈", "🥉"][i-1]
            print(f"   {medal} {name}: {time_taken:.3f}s ({size/time_taken:,.0f} rows/s)")
    
    # Show comparison between Polars vs Pandas
    polars_times = [(name, t) for name, t, err in methods if "Polars" in name and err is None]
    pandas_times = [(name, t) for name, t, err in methods if "Pandas" in name and err is None]
    
    if polars_times and pandas_times:
        avg_polars = sum(t for _, t in polars_times) / len(polars_times)
        avg_pandas = sum(t for _, t in pandas_times) / len(pandas_times)
        
        print(f"\n⚖️  Library Comparison:")
        print(f"   Polars average: {avg_polars:.3f}s")
        print(f"   Pandas average: {avg_pandas:.3f}s")
        if avg_polars < avg_pandas:
            speedup = avg_pandas / avg_polars
            print(f"   🚀 Polars is {speedup:.1f}x faster on average")
        else:
            speedup = avg_polars / avg_pandas
            print(f"   🚀 Pandas is {speedup:.1f}x faster on average")
    
    # Verify data integrity
    all_results = [
        (df_read1, time1, error1, "Polars"),
        (df_read2, time2, error2, "Polars"), 
        (df_read3, time3, error3, "Polars"),
        (df_read4, time4, error4, "Polars"),
        (df_read5, time5, error5, "Pandas"),
        (df_read6, time6, error6, "Pandas"),
        (df_read7, time7, error7, "Pandas"),
        (df_read8, time8, error8, "Pandas")
    ]
    
    successful_dfs = [(df, lib) for df, _, err, lib in all_results if err is None and df is not None]
    
    if len(successful_dfs) > 1:
        # Check if all successful reads have the same shape
        shapes = []
        for df, lib in successful_dfs:
            if lib == "Polars":
                shapes.append(df.shape)
            else:  # Pandas
                shapes.append(df.shape)
        
        if all(shape == shapes[0] for shape in shapes):
            print(f"✓ Data integrity: All methods read {shapes[0][0]:,} rows × {shapes[0][1]} columns")
        else:
            print(f"⚠️  Data integrity warning: Different shapes detected")
            for i, ((df, lib), shape) in enumerate(zip(successful_dfs, shapes)):
                print(f"     Method {i+1} ({lib}): {shape[0]:,} × {shape[1]}")
    
    # Memory usage comparison
    polars_successful = [df for df, lib in successful_dfs if lib == "Polars"]
    pandas_successful = [df for df, lib in successful_dfs if lib == "Pandas"]
    
    if polars_successful and pandas_successful:
        # Estimate memory usage (rough approximation)
        polars_memory = polars_successful[0].estimated_size() if hasattr(polars_successful[0], 'estimated_size') else "N/A"
        pandas_memory = pandas_successful[0].memory_usage(deep=True).sum() if hasattr(pandas_successful[0], 'memory_usage') else "N/A"
        
        print(f"\n💾 Memory Usage Comparison:")
        if polars_memory != "N/A":
            print(f"   Polars: {polars_memory/1024/1024:.1f} MB")
        if pandas_memory != "N/A":
            print(f"   Pandas: {pandas_memory/1024/1024:.1f} MB")
    
    # Export performance tests
    if polars_successful and pandas_successful:
        # Use the first successful DataFrame from each library for export tests
        polars_df = polars_successful[0]
        pandas_df = pandas_successful[0]
        
        export_results = test_export_methods(polars_df, pandas_df, size, f"test_{size}")
    
    # Clean up test file
    if os.path.exists(test_file):
        os.remove(test_file)
    
    print(f"\n{'='*60}")

print(f"\n🎯 PERFORMANCE TEST COMPLETE")
print(f"💡 Recommendation: Use the fastest method for your web application!")
