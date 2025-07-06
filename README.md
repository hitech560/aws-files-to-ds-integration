# AWS Files to Datasphere Loader

This project automates the ingestion of flat files (CSV/TXT) from a network-shared directory (AWS-hosted Windows share) into SAP Datasphere (SAP HANA Cloud). The pipeline performs encoding and delimiter detection, dynamically creates HANA tables, inserts data in chunks, tracks loads via a control table, and archives processed files.


## 📦 Features

- ✅ Load `.csv` files into SAP Datasphere tables
- ✅ Infer table schema from CSV header
- ✅ Chunked loading using Pandas for large files
- ✅ Automatically creates or drops target tables as needed
- ✅ Archives successfully loaded files with a timestamped filename optionally
- ✅ Writes detailed log files to the `Logs/` folder
- ✅ Supports multiple source folders via `FILE_LOCATIONS` list
- ✅ Detects CSV encoding, delimiter, quotchar and header presence
- ✅ Tracks file load metadata in control table `AWS_FILES_DS_INTEGRATION`
- ✅ Skips reloading files if they are already up-to-date based on `LAST_MODIFIED`


## 🛠 Components

- **`aws-files-to-ds.py`** – Main pipeline script
- **`uni_logger.py`** – Custom logging utility with rotating file and UTF-8-safe console loggers
- **`aws-files-to-ds.bat`** – Batch launcher for Windows

## 🔁 Pipeline Workflow

1. **Read File List**: From `File_Locations.txt` with optional path prefix.
2. **Determine Environment**: Based on `ENVIRONMENT` variable (SBX/DEV/UAT/PRD).
3. **Validate Files**: Check last modified timestamps and control table status.
4. **Infer Properties**: Detect encoding, delimiter, quote character, and header.
5. **Create Table**: Generate and optionally replace a HANA table for each file.
6. **Insert Data**: Read in chunks, add a BODS ETL timestamp, and insert.
7. **Update Control Table**: Merge metadata into `AWS_FILES_DS_INTEGRATION`.
8. **Archive File** (Optional): Move original file to `archive/` folder with timestamp.

## ⚙️ Configuration

- **`ds_config.ini`**: Must contain HANA connection and AWS file path details by environment section.
- **Environment Variables**:
  - `ENVIRONMENT`: One of `SBX`, `DEV`, `UAT`, `PRD`
  - `LOG_LEVEL`: Logging level (`DEBUG`, `INFO`, `WARNING`, etc.)

## 🚀 How to Run

```bash
# Windows (via .bat) default settings
aws-files-to-ds.bat
# or with specified parameters
aws-files-to-ds.bat dev debug force_load file_archive

# Or directly with Python with default settings
python aws-files-to-ds.py
```

## 📂 Logs

Logs are saved to `Logs/aws-files-to-ds.log` with rotating file and console output.

## 📌 Control Table

Target table: `AWS_FILES_DS_INTEGRATION`

Tracks metadata:
- `FILE_NAME`, `FILE_PATH`, `LAST_MODIFIED`
- `HAS_HEADER`, `DELIMITER`, `ENCODING`
- `TABLE_NAME`, `BODS_TIMESTAMP`, `ROW_COUNT`, `COLUMN_COUNT`
- `STATUS_FLAG`, `DS_TIMESTAMP`

