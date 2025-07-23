# Design Specification – AWS Files to Datasphere Pipeline

## Overview

This Python-based ETL pipeline automates the ingestion of flat files from AWS-hosted Windows shared drives into SAP Datasphere (SAP HANA Cloud). The system dynamically detects file structures, builds tables, loads data in chunks, and maintains load metadata in a control table.

---

## Objectives

- Automatically detect encoding, delimiter, and headers from flat files.
- Load validated content into SAP Datasphere.
- Maintain detailed metadata for traceability and status tracking.
- Archive successfully processed files.

---

## Pipeline Architecture

### High-Level Steps

1. **Environment Resolution**
   - Determine the target path prefix from `ENVIRONMENT`.
   - e.g., `DEV` → `//aws.toyota.ca/dev/BI_Reports_DEV`

2. **File List Input**
   - Read from `File_Locations.txt` to define files/folders for ingestion.

3. **Validation**
   - Check if file exists and its extension is supported (`.csv`, `.txt`).
   - Skip processing if last modified timestamp matches control table.

4. **Property Detection**
   - Encoding (e.g., `utf-8-sig`, `latin1`) and delimiter detection.
   - Use `csv.Sniffer` for reliable inference.

5. **Data Load**
   - Read file in Pandas chunks.
   - Create SAP HANA table from the first chunk.
   - Insert all chunks with consistent timestamp.

6. **Control Table Update**
   - Upsert a record to `AWS_FILES_DS_INTEGRATION` for status tracking.

7. **Archival (Optional)**
   - Move processed files to an `archive/` subfolder with timestamp.

---

## Component Summary

| Component              | Description |
|------------------------|-------------|
| `aws-files-to-ds.py`   | Main ETL logic |
| `uni_logger.py`        | Logging utility with rotation & UTF-8 support |
| `aws-files-to-ds.bat`  | Windows launcher |
| `config.ini`           | DB credentials per environment |
| `File_Locations.txt`   | Input list of files to process |

---

## Control Table: `AWS_FILES_DS_INTEGRATION`

| Column           | Purpose |
|------------------|---------|
| `FILE_NAME`      | Base filename |
| `FILE_PATH`      | Directory only |
| `LAST_MODIFIED`  | Timestamp from OS |
| `HAS_HEADER`     | Detected header flag |
| `DELIMITER`      | Detected delimiter |
| `ENCODING`       | File encoding |
| `TABLE_NAME`     | Target HANA table |
| `BODS_TIMESTAMP` | Timestamp per load batch |
| `ROW_COUNT`      | Total rows inserted |
| `COLUMN_COUNT`   | Column count |
| `STATUS_FLAG`    | Status (e.g., `BODS COMPLETED`) |
| `DS_TIMESTAMP`   | Optional downstream timestamp update |

---

## Flow Chart

<!-- ![Pipeline Flow Chart](pipeline_flowchart.png) -->
<img src='pipeline_flowchart.png' alt='Solution Flowchart' width='50%'>

---

## Future Enhancements

- Add parallel loading for large datasets
- Store raw load stats to separate audit table
- Add unit tests and validation checks pre-load

