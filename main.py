# main.py
from fasthtml.common import *
import uvicorn
import polars as pl
import io
import base64
import os
from pathlib import Path

app, rt = fast_app()

# Global variable to store the current data and file info
current_data = None
current_file_path = None

# JavaScript for Excel file handling and sync functionality
js_code = """
    let fileHandle; // This will store the handle to the opened Excel file
    let currentFileName = '';

    // Function to create and remove a popup dynamically
    function showTemporaryMessage(message, isError = false) {
        const popup = document.createElement('div');
        popup.textContent = message;

        Object.assign(popup.style, {
            position: 'fixed',
            top: '20px',
            left: '50%',
            transform: 'translateX(-50%)',
            backgroundColor: isError ? '#f44336' : '#4CAF50',
            color: 'white',
            padding: '15px',
            borderRadius: '8px',
            zIndex: '1000',
            transition: 'opacity 0.5s ease-out',
            opacity: '1'
        });

        document.body.appendChild(popup);

        setTimeout(() => {
            popup.style.opacity = '0';
            setTimeout(() => {
                document.body.removeChild(popup);
            }, 500);
        }, 3000);
    }

    async function openExcelFile() {
        try {
            [fileHandle] = await window.showOpenFilePicker({
                types: [{
                    description: 'Excel files',
                    accept: {
                        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
                        'application/vnd.ms-excel': ['.xls']
                    }
                }]
            });
            
            if (fileHandle) {
                await loadExcelData();
            }
        } catch (err) {
            console.error('Error opening file:', err);
            if (err.name !== 'AbortError') {
                showTemporaryMessage("Could not open file. Please select an Excel file.", true);
            }
        }
    }

    async function loadExcelData() {
        if (!fileHandle) {
            showTemporaryMessage('No file selected.', true);
            return;
        }

        try {
            const file = await fileHandle.getFile();
            currentFileName = file.name;
            
            // Convert file to base64 for sending to server
            const arrayBuffer = await file.arrayBuffer();
            const base64String = btoa(String.fromCharCode(...new Uint8Array(arrayBuffer)));
            
            // Send to server for processing
            const response = await fetch('/load_excel', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    file_data: base64String,
                    file_name: currentFileName
                })
            });

            const result = await response.json();
            
            if (response.ok) {
                if (result.error) {
                    showTemporaryMessage(result.error, true);
                    return;
                }
                
                // Update UI
                document.getElementById('data-container').innerHTML = result.html || 'No data received';
                document.getElementById('sync-button').disabled = false;
                document.getElementById('export-csv').disabled = false;
                document.getElementById('export-excel').disabled = false;
                document.getElementById('export-parquet').disabled = false;
                
                const rows = result.rows !== undefined ? result.rows : 'unknown';
                const columns = result.columns !== undefined ? result.columns : 'unknown';
                
                document.getElementById('status').textContent = `Loaded: ${currentFileName} (${rows} rows, ${columns} columns)`;
                showTemporaryMessage('Excel file loaded successfully!');
            } else {
                showTemporaryMessage(result.error || 'Error loading file', true);
            }
        } catch (err) {
            console.error('Error loading Excel data:', err);
            showTemporaryMessage('Error processing Excel file.', true);
        }
    }

    async function syncData() {
        if (!fileHandle) {
            showTemporaryMessage('No file to sync with.', true);
            return;
        }

        showTemporaryMessage('Syncing data...');
        await loadExcelData();
    }

    async function exportData(format) {
        try {
            showTemporaryMessage(`Exporting as ${format.toUpperCase()}...`);
            
            const response = await fetch('/export_data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    format: format
                })
            });

            if (response.ok) {
                // Get the filename from the response headers or create a default one
                const contentDisposition = response.headers.get('content-disposition');
                let filename = `exported_data.${format}`;
                if (contentDisposition) {
                    const filenameMatch = contentDisposition.match(/filename="(.+)"/);
                    if (filenameMatch) {
                        filename = filenameMatch[1];
                    }
                }

                // Create blob and download
                const blob = await response.blob();
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.style.display = 'none';
                a.href = url;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                window.URL.revokeObjectURL(url);
                document.body.removeChild(a);
                
                showTemporaryMessage(`${format.toUpperCase()} exported successfully!`);
            } else {
                const result = await response.json();
                showTemporaryMessage(result.error || 'Export failed', true);
            }
        } catch (err) {
            console.error('Export error:', err);
            showTemporaryMessage('Export failed', true);
        }
    }
"""


def polars_to_html_table(df):
    """Convert Polars DataFrame to HTML table"""
    if df.is_empty():
        return "<p>No data to display</p>"
    
    # Start building HTML table
    html = ['<table style="border-collapse: collapse; width: 100%; max-width: 100%; overflow-x: auto;">']
    
    # Add header
    html.append('<thead>')
    html.append('<tr style="background-color: #f2f2f2;">')
    for col in df.columns:
        html.append(f'<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">{col}</th>')
    html.append('</tr>')
    html.append('</thead>')
    
    # Add body
    html.append('<tbody>')
    for row in df.iter_rows():
        html.append('<tr>')
        for value in row:
            # Handle None/null values
            display_value = "" if value is None else str(value)
            html.append(f'<td style="border: 1px solid #ddd; padding: 8px;">{display_value}</td>')
        html.append('</tr>')
    html.append('</tbody>')
    html.append('</table>')
    
    return ''.join(html)


@rt("/load_excel")
async def post(data: dict):
    """Handle Excel file upload and processing"""
    global current_data, current_file_path
    
    try:
        file_data = data.get('file_data')
        file_name = data.get('file_name')
        
        if not file_data or not file_name:
            return {"error": "Missing file data or filename"}
        
        # Decode base64 data
        file_bytes = base64.b64decode(file_data)
        
        # Create a temporary file-like object
        file_like = io.BytesIO(file_bytes)
        
        # Read Excel file with Polars
        try:
            # Try reading as xlsx first - let Polars auto-detect the engine
            if file_name.lower().endswith('.xlsx'):
                df = pl.read_excel(file_like)  # Let Polars choose the best engine
            else:
                # For .xls files, try with openpyxl
                df = pl.read_excel(file_like)
                
        except Exception as e:
            # If that fails, try saving to a temporary file and reading from there
            try:
                import tempfile
                import os
                
                # Create a temporary file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx' if file_name.lower().endswith('.xlsx') else '.xls') as tmp_file:
                    tmp_file.write(file_bytes)
                    tmp_file_path = tmp_file.name
                
                # Try reading from the temporary file
                df = pl.read_excel(tmp_file_path)
                
                # Clean up the temporary file
                os.unlink(tmp_file_path)
                
            except Exception as e2:
                return {"error": f"Error reading Excel file: {str(e)} | Fallback error: {str(e2)}"}
        
        # Store the data globally
        current_data = df
        current_file_path = file_name
        
        # Convert to HTML table
        html_table = polars_to_html_table(df)
        
        # Get DataFrame dimensions
        num_rows = df.height
        num_cols = df.width
        
        response_data = {
            "html": html_table,
            "rows": num_rows,
            "columns": num_cols,
            "success": True
        }
        
        return response_data
        
    except Exception as e:
        error_msg = f"Server error: {str(e)}"
        print(f"DEBUG: {error_msg}")
        return {"error": error_msg}


@rt("/export_data")
async def post_export(data: dict):
    """Handle data export in various formats"""
    global current_data, current_file_path
    
    try:
        if current_data is None:
            return {"error": "No data loaded. Please load an Excel file first."}
        
        export_format = data.get('format', 'csv').lower()
        
        if export_format not in ['csv', 'excel', 'xlsx', 'parquet']:
            return {"error": f"Unsupported export format: {export_format}"}
        
        # Generate base filename from original file
        if current_file_path:
            base_name = Path(current_file_path).stem
        else:
            base_name = "exported_data"
        
        # Export based on format
        if export_format == 'csv':
            # Export as CSV
            output = io.StringIO()
            current_data.write_csv(output)
            content = output.getvalue().encode('utf-8')
            filename = f"{base_name}_exported.csv"
            content_type = "text/csv"
            
        elif export_format in ['excel', 'xlsx']:
            # Export as Excel
            output = io.BytesIO()
            current_data.write_excel(output)
            content = output.getvalue()
            filename = f"{base_name}_exported.xlsx"
            content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            
        elif export_format == 'parquet':
            # Export as Parquet
            output = io.BytesIO()
            current_data.write_parquet(output)
            content = output.getvalue()
            filename = f"{base_name}_exported.parquet"
            content_type = "application/octet-stream"
        
        # Return file response
        from starlette.responses import Response
        return Response(
            content=content,
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename=\"{filename}\"",
                "Content-Length": str(len(content))
            }
        )
        
    except Exception as e:
        error_msg = f"Export error: {str(e)}"
        print(f"DEBUG: {error_msg}")
        return {"error": error_msg}


@rt("/")
def get():
    # Updated main layout for Excel file handling
    main_layout = (
        Title("Excel Data Viewer"),
        H1("Excel File Viewer with Polars"),
        P(
            "Select an Excel file (.xlsx or .xls) to view its data in a table format. "
            "Use the sync button to reload data if the file changes."
        ),
        Div(
            Button("📁 Open Excel File", onclick="openExcelFile()", 
                   style="background-color: #4CAF50; color: white; padding: 10px 20px; border: none; border-radius: 4px; margin-right: 10px;"),
            Button("🔄 Sync Data", 
                   id="sync-button", 
                   onclick="syncData()", 
                   disabled=True,
                   style="background-color: #008CBA; color: white; padding: 10px 20px; border: none; border-radius: 4px; margin-right: 10px;"),
            style="margin-bottom: 15px;",
        ),
        Div(
            P("Export Options:", style="margin: 0 0 10px 0; font-weight: bold; color: #555;"),
            Button("💾 Export CSV", 
                   id="export-csv",
                   onclick="exportData('csv')", 
                   disabled=True,
                   style="background-color: #FF9800; color: white; padding: 8px 16px; border: none; border-radius: 4px; margin-right: 8px;"),
            Button("📊 Export Excel", 
                   id="export-excel",
                   onclick="exportData('excel')", 
                   disabled=True,
                   style="background-color: #2E7D32; color: white; padding: 8px 16px; border: none; border-radius: 4px; margin-right: 8px;"),
            Button("🗂️ Export Parquet", 
                   id="export-parquet",
                   onclick="exportData('parquet')", 
                   disabled=True,
                   style="background-color: #7B1FA2; color: white; padding: 8px 16px; border: none; border-radius: 4px;"),
            style="margin-bottom: 20px; padding: 15px; background-color: #f9f9f9; border-radius: 8px;",
        ),
        Hr(),
        Div(id="status", style="font-style: italic; margin-bottom: 10px; color: #666;"),
        Div(
            id="data-container", 
            style="max-height: 600px; overflow: auto; border: 1px solid #ddd; padding: 10px; background-color: #fafafa;",
            content="Select an Excel file to view its data here..."
        ),
        Script(js_code),
    )

    compatibility_check = (
        Div(
            H2("Browser Not Supported"),
            P(
                "This feature requires a browser with the File System Access API, "
                "such as Google Chrome or Microsoft Edge."
            ),
            id="compatibility-warning",
            style=(
                "display: none; position: fixed; top: 0; left: 0; width: 100%; "
                "height: 100%; background-color: rgba(0,0,0,0.8); color: white; "
                "text-align: center; padding-top: 20%; z-index: 2000;"
            ),
        ),
        Script(
            """
            if (!window.showOpenFilePicker) {
                document.getElementById('compatibility-warning').style.display = 'block';
            }
            """
        ),
    )

    return main_layout + compatibility_check


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
