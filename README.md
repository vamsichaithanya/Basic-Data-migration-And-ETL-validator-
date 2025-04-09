# Data Validation Toolkit:-

## Overview

This Python toolkit provides a comprehensive solution for comparing and validating data between different sources,
with a focus on CSV and SQL database comparisons. The toolkit offers multiple methods to analyze differences in datasets, 
including column changes, row additions/deletions, updates, and identifying duplicate entries.

### Features:

1) Key Capabilities
- 🔍 Detailed data comparison between two datasets
- 🔢 Support for both CSV and SQL database sources
- 🔬 Multiple comparison modes:
  - Comparison without a primary key
  - Comparison with a specified primary key
  - Automatically detecting the best column to use as a primary key
- ⏳This application can able to process
  - 4L rows X 7 columns in 1 to 1.5 min with 25% of the rows are mismatched (with primary Key)
  - 4L rows X 7 columns in 2 to 2.5 min with  5% of the rows are mismatched (with out primary Key)
  - Note:
         - Non-primary validator is not stable as primary key validator
	      - reduce "threshold" to 0 and increase "num_perm" to 256 for perfect accuracy but its gonna take while to process this (also depends on input dataset)

2) Comparison Analysis
The toolkit provides comprehensive difference analysis, including:
- Added/Deleted Columns
- Added/Deleted Rows
- Updated Rows
- Duplicate Rows Detection

3) Output Formats
- Formatted Console Output
- JSON Export
- Excel Spreadsheet with Color-Coded Results

### Required Libraries:
- pandas
- joblib
- datasketch
- pyodbc
- sqlalchemy
- hashing_and_parallel_validator (local module)
- sql_def(local module)

### SQL Server Requirements:
- ODBC Driver 17 for SQL Server
- Trusted Connection to SQL Server

### Installation:

1. Clone the repository
2. Install required dependencies:

   ```
   pip install pandas, json, joblib, datasketch, pyodbc, sqlalchemy, numpy, tqdm, pathlib, os, time
   ```

4. Ensure you have the ODBC Driver 17 for SQL Server installed

### Usage Examples:

- Run the Main.py file in Works folder
- There are already some example snippet's to understand the project outputs with comments
- installation of required module is must to get desired outputs

### Modes of Comparison:

1. No Primary Key: 
   - Automatically detects row changes using MinHash

2. With Primary Key:
   - Uses specified primary key for precise comparisons
   - More accurate tracking of row changes

3. Assumed Primary Key:
   - Automatically finds the best column to use as a primary key
   - Useful when no explicit primary key exists

### Output Examples:

1) Console Output			"Recommend to use Console Output only if you're working with very small dataset or datasets with very least mismatched rows"
- Colored summary of changes
- Detailed list of added/deleted columns and rows
- Highlighted row updates

2) Excel Output
- Separate sheets for different types of changes
- Color-coded cells for easy visualization
- Automatic column width adjustment

3) Json Output
- Exports a result dictionary in a Json format

### Key Components:

- `validate` class: Core comparison logic
- `sql` class: Database connection and data retrieval\

### Limitations:

- Requires ODBC connection to SQL Server
- Performance may vary with extremely large datasets
- Assumes UTF-8 encoding for text comparisons




==================================================================================================================================================================================

# Data Migration Bridge:-

A Python utility for efficiently migrating data from various file formats to SQL Server databases with minimal configuration.

## Overview

Data Migration Bridge streamlines the Extract, Transform, Load (ETL) process for data migration tasks. 
It provides a flexible interface for extracting data from common file formats (CSV, JSON, Excel), 
transforming it into standardized pandas DataFrames, and loading it into SQL Server databases.

### Features

- Multi-format Support: Process CSV, JSON, and Excel (XLS/XLSX) files
- Flexible File Selection: Target individual files, all files in a directory, or filter by extension
- Interactive Database Options: Create new databases or connect to existing ones
- Comprehensive Error Handling: Descriptive feedback for troubleshooting
- Data Integrity Preservation: Maintains structural and content fidelity during migration
- Batch Processing Capability: Efficiently handle multiple files in a single operation

### Requirements

- Python 3.6+
- Required packages:
  - pandas
  - pyodbc
  - sqlalchemy
  - json (standard library)
  - os (standard library)

### Installation

1. Clone the repository:

2. Install required packages:
   ```
   pip install pandas, pyodbc, sqlalchemy
   ```

3. Ensure you have SQL Server and the appropriate ODBC drivers installed on your system.

### Usage

The core functionality is provided through the `liftNshift` class with three main methods:

1. Loading Files

Method 1: Process all supported files in a directory:
```
result = liftNshift.load_files(folderpath="path/to/your/data/directory")
```

Method 2: Process only files with specific extension in a directory:
```
result = liftNshift.load_files(folderpath="path/to/your/data/directory", file_Extension="csv")
```

Method 3: Process a single file:
```
result = liftNshift.load_files(file_path="path/to/your/data/file.json")
```

2. Viewing Data

Print all loaded dataframes:
```
liftNshift.printfiles(result)
```


3. Exporting to SQL Server

Export data to SQL Server (interactive mode):
```
liftNshift.exportfiles(result)
```

	||
	or
	||

- Run the Main.py file in Works folder
- There are already some example snippet's to understand the project outputs with comments
- installation of required module is must to get desired outputs

### SQL Server Configuration

The tool is configured to connect to a local SQL Server instance named `IAMGROOT\SQLEXPRESS01` using Windows authentication. To modify this:

1. Edit the `exportfiles` method in the `liftNshift.py` file
2. Update the server name and connection parameters as needed

### Notes

- Make sure SQL Server is running before attempting to export data
- The tool will sanitize dataframe names to create valid SQL table names
- When prompted during export, select whether to create a new database or use an existing one

