import os
import csv
import shutil
# import chardet
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser
from uni_logger import setup_logger

import pandas as pd
from hdbcli import dbapi

# from sql_statements import upsert_stmt

# ========================== #
#         Logging Setup      #
# ========================== #

logger = setup_logger()  # Automatically names the logger based on script name


# ========================== #
#         DB Connection      #
# ========================== #

def ds_conn(config_path: Path, environment: str) -> dbapi.Connection:
    config = ConfigParser()
    config.read(config_path)
    return dbapi.connect(**dict(config.items(environment)))


def update_control_table(
    cursor: dbapi.Cursor,
    file_path: Path,
    table_name: str,
    skip_rows: int,
    skip_footer: int,
    encoding: str,
    has_header: bool,
    delimiter: str,
    quotechar: str,
    timestamp: str,
    row_count: int,
    column_count: int,
    status: str
):
    try:
        file_stats = file_path.stat()
        last_modified = datetime.fromtimestamp(
            file_stats.st_mtime
        ).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f'⚠️ Could not retrieve last modified time for <{file_path.name}> {e}')
        last_modified = None

    values = [
        file_path.name,
        str(file_path.parent).replace("\\", "/"),  # ✅ directory only
        last_modified,
        table_name,
        skip_rows,
        skip_footer,
        encoding,
        has_header,
        delimiter,
        quotechar,
        # bods_ts,
        timestamp,
        row_count,
        column_count,
        None, # DS_TIMESTAMP will be updated by DS
        status, # will be updated by DS to "DS_COMPLETED"
    ]

    # In update_control_table() — replace the upsert_stmt definition:
    upsert_stmt = load_sql(Path("control_table_upsert.sql"))

    interpolated_sql = upsert_stmt
    for val in values:
        val_repr = f"'{val}'" if isinstance(val, str) else str(val)
        interpolated_sql = interpolated_sql.replace("?", val_repr, 1)

    logger.debug(f'⚠️ Inserting control table values: \n{values}')
    logger.debug(f"🧪 Control table interpolated SQL: {interpolated_sql}")

    try:
        cursor.execute(upsert_stmt, values)
        logger.info(f'📋 Upserted control record for <{file_path.name}> with status: <{status}>')
    except dbapi.Error as e:
        logger.error(f'❌ Failed to upsert control record for <{file_path.name}>: {e}')
        raise (f'❌ Control table upsert error for <{file_path.name}>: {e}')


# ========================== #
#      Utility Functions     #
# ========================== #

def aws_env(config_path: Path, environment: str):
    match environment:
        case "DEV":
            ENV = "DEV"
        case "UAT" | "QA":
            ENV = "UAT"
        case "PROD" | "PRD":
            ENV = "PRD"
        case _:
            ENV = "SBX"    
    config = ConfigParser()
    config.read(config_path)
    return config[f'AWS_{ENV}']['env'], config[f'AWS_{ENV}']['aws_base']


# Define a helper function to load the SQL from file
def load_sql(file_path: Path) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"❌ Failed to load SQL from {file_path}: {e}")
        raise


def detect_csv_properties(file_path, encoding='utf-8'):
    """Detects delimiter, quote character, and header in a flat file."""
    # encoding = detect_file_encoding(file_path) or encoding
    try:
        with open(file_path, 'r', newline='', encoding=encoding) as csvfile:  
            # Consider specifying encoding
            # Read a sample of the file to help the sniffer
            sample = csvfile.read(4096)  # Increase sample size if needed
            
            # Use Sniffer to detect dialect and header
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=',;\t|^#') # Suggest common delimiters
            has_header = sniffer.has_header(sample)

            properties = {
                'file_path': file_path,
                'delimiter': dialect.delimiter,
                'quotechar': dialect.quotechar,
                'has_header': has_header,
                'encoding': encoding
            }
            logger.debug(
                # f'⚠️ Detected properties for file <{file_path}>: \n{properties}'
                f'⚠️ Detected properties: \n{properties}'
            )

            return properties

    except csv.Error as e:
        logger.error(f'File <{file_path}> CSV error: {e}')
        return None
    except FileNotFoundError:
        logger.error(f'❌ File not found: <{file_path}>')
        return None
    except Exception as e:
        logger.error(
            f'❌ File <{file_path}> properties detection unexpected error occurred: {e}'
        )
        return None


def sanitize_table_name(filename: str) -> str:
    return Path(filename).stem.replace('-', '_').replace(' ', '_').upper()


def infer_hana_type(series: pd.Series) -> str:
    # if pd.api.types.is_integer_dtype(series):
    #     return "INTEGER"
    # elif pd.api.types.is_float_dtype(series):
    #     return "DOUBLE"
    # elif pd.api.types.is_bool_dtype(series):
    #     return "BOOLEAN"
    # elif pd.api.types.is_datetime64_any_dtype(series):
    #     return "TIMESTAMP"
    # elif pd.api.types.is_object_dtype(series):
    #     max_len = series.dropna().astype(str).map(len).max()
    #     length = min(((max_len or 100 + 49) // 50) * 50, 1000)
    #     return f"VARCHAR({length})"
    # return "VARCHAR(500)"

    return "VARCHAR(255)"


def table_exists(cursor: dbapi.Cursor, table_name: str) -> bool:
    try:
        cursor.execute(
            f"SELECT TABLE_NAME FROM TABLES WHERE TABLE_NAME = '{table_name.upper()}'"
        )
        return cursor.fetchone() is not None
    except dbapi.Error as e:
        logger.error(f'❌ Table existence check failed for <{table_name}>: {e}')
        return False


def deduplicate_columns(columns):
    seen = {}
    result = []
    for col in columns:
        col_upper = col.upper()
        if col_upper not in seen:
            seen[col_upper] = 0
            result.append(col_upper)
        else:
            seen[col_upper] += 1
            new_col = f"{col_upper}_{seen[col_upper]}"
            while new_col in seen:
                seen[col_upper] += 1
                new_col = f"{col_upper}_{seen[col_upper]}"
            seen[new_col] = 0
            result.append(new_col)
    return result


def should_skip_file(cursor: dbapi.Cursor, file_path: Path) -> bool:
    file_name = file_path.name
    file_dir = str(file_path.parent).replace("\\", "/")
    try:
        last_modified = datetime.fromtimestamp(
            file_path.stat().st_mtime
        ).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f'⚠️ Failed to stat <{file_name}>: {e}')
        return False

    query = """
    SELECT "LAST_MODIFIED", "STATUS_FLAG"
    FROM "AWS_FILES_DS_INTEGRATION" 
    WHERE "FILE_NAME" = ? AND "FILE_PATH" = ?
    """

    try:
        cursor.execute(query, (file_name, file_dir))
        row = cursor.fetchone()
        if not row:
            logger.info(f'⏩ Loading <{file_name}>: not found in control table')
            return False
        
        loaded_last_modified = str(row[0]) if row else ''
        logger.debug(
            f'⚠️ Loaded file <{file_name}> last_modified: <{loaded_last_modified}>'
            )
        logger.debug(
            f'⚠️ Loading file <{file_name}> last_modified: <{last_modified}>'
            )
        check1 = loaded_last_modified >= last_modified
        if check1:
            logger.warning(f'⚠️ Skipping <{file_name}>: out-of-dated')
            return True

        status_flag = str(row[1]) if row else ''
        logger.debug(
            f'⚠️ File <{file_name}> Status flag: <{status_flag}>'
            )
        check2 = status_flag == "BODS COMPLETED"
        if check2:
            logger.info(f'⚠️ Skipping <{file_name}>: DS proccess pending')
            return True
    except dbapi.Error as e:
        logger.warning(f'⚠️ Skipping as file skip check failed for <{file_name}>: {e}')
        return True

    logger.info(f'⏩ Loading <{file_name}>: skip check completed and passed')
    return False


# ========================== #
#     HANA Table & Insert    #
# ========================== #

def create_table_from_df(cursor: dbapi.Cursor, df: pd.DataFrame, table_name: str) -> bool:
    # Infer HANA column definitions from DataFrame columns
    col_defs = [f'"{col.upper()}" {infer_hana_type(df[col])}' for col in df.columns]

    if not col_defs:
        logger.error(
            f'❌ No valid columns found for table <{table_name}>, skipping creation ...'
        )
        return False

    drop_stmt = f'DROP TABLE "{table_name}"'
    # Construct CREATE TABLE SQL
    create_stmt = f'CREATE COLUMN TABLE "{table_name}" (\n  {",\n  ".join(col_defs)}\n)'

    logger.debug(f'⚠️ drop_stmt = {drop_stmt}')
    logger.debug(f'⚠️ create_stmt = \n{create_stmt}')

    if table_exists(cursor, table_name):
        try:
            cursor.execute(drop_stmt)
            logger.debug(f'🧹 Dropped existing table: <{table_name}>')
        except dbapi.Error as e:
            logger.warning(f'ℹ️ Could not drop table <{table_name}>: {e}')

    try:
        cursor.execute(create_stmt)
        logger.debug(f'✅ Created table: <{table_name}>')
        return True
    except dbapi.Error as e:
        logger.error(f'❌ Could not create table <{table_name}>: {e}')
        return False


def insert_data(
        cursor: dbapi.Cursor, 
        df: pd.DataFrame, 
        table_name: str,
        chunk_num: int = 0,
    ) -> int:

    # Clean NaNs without applymap
    values = df.astype(object).where(pd.notnull(df), None).values.tolist()
    row_count = len(values)
    if row_count == 0:
        # no data to insert
        return 0

    columns = ', '.join(f'"{col.upper()}"' for col in df.columns)
    placeholders = ', '.join(['?'] * len(df.columns))
    insert_stmt = (
        f'INSERT INTO "{table_name}" (\n'
        + ',\n    '.join(f'"{col.upper()}"' for col in df.columns) +
        '\n) VALUES (\n    ' +
        ', '.join(['?'] * len(df.columns)) +
        '\n)'
    )
    logger.debug(f'⚠️ insert_stmt = \n{insert_stmt}')

    try:
        cursor.executemany(insert_stmt, values)
        logger.debug(
            f'📥 Chunk <{chunk_num}> inserted {len(values)} rows into <{table_name}>'
        )
        return row_count
    except dbapi.Error as e:
        logger.error(
            f'❌ Chunk <{chunk_num}> insert failed for table <{table_name}>: {e}'
        )
        return -1


# ========================== #
#         CSV Pipeline       #
# ========================== #

def process_csv_file_in_chunks(
    cursor: dbapi.Cursor,
    file_path: Path,
    table_name: str,
    chunksize: int = 50000,
    timestamp: str = None,
    has_header: bool = None,
    encoding: str = None,
    delimiter: str = None,
    quotechar: str = None,
    skip_rows : int = 0, # skip first N lines
    skip_footer : int = 0,  # skip last N lines
    control_callback=None,
) -> bool:
    if not file_path.exists():
        logger.error(f'❌ File not found: <{file_path.resolve()}>')
        return False    

    total_inserted = 0
    # first_chunk = True
    encodings_to_try = [encoding, 'utf-8-sig', 'latin1'] if encoding else ['utf-8-sig', 'latin1']
    if timestamp is None:
        # ✅ Compute once per file
        # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f'⚠️ BODS ETL timestamp: <{timestamp}>')

    for enc in encodings_to_try:
        logger.debug(f'⚠️ Trying encoding: <{enc}>')

        try:
            props = detect_csv_properties(file_path, encoding=enc)
            if not props:
                logger.error(f'❌ Cannot detect file properties: <{file_path.resolve()}>')
                return False
            has_header = has_header if has_header is not None else props['has_header']
            delimiter = delimiter or props['delimiter']
            quotechar = quotechar or props['quotechar']

            read_csv_kwargs = {
                'filepath_or_buffer': file_path,
                'encoding': enc,
                'chunksize': chunksize,
                'delimiter': delimiter,
                'quotechar': quotechar,
                'skiprows': skip_rows,
                # 'skipfooter': skip_bottom,
                'low_memory': False,
                'dtype': str,  # ✅ Prevent type inference that removes leading zeros
            }
            if not has_header:
                read_csv_kwargs['header'] = None

            logger.debug(f'⚠️ read_csv_kwargs: \n{read_csv_kwargs}')

            reader = pd.read_csv(**read_csv_kwargs)
            first_chunk = True
            chunk_num = 1

            try:
                chunk = next(reader)
            except StopIteration:
                return False  # no data

            while True:
                _, column_count = chunk.shape

                if has_header:
                    chunk.columns = deduplicate_columns(chunk.columns)
                else:
                    chunk.columns = [f"COL{str(i+1).zfill(3)}" for i in range(chunk.shape[1])]

                chunk = chunk.where(pd.notnull(chunk), None)
                chunk["BODS_TIMESTAMP"] = timestamp

                if first_chunk:
                    success = create_table_from_df(cursor, chunk, table_name)
                    if not success:
                        return False
                    first_chunk = False

                try:
                    next_chunk = next(reader)
                    inserted = insert_data(cursor, chunk, table_name, chunk_num)
                    if inserted < 0:
                        return False
                    total_inserted += inserted
                    chunk_num += 1
                    chunk = next_chunk
                except StopIteration:
                    # last chunk
                    if skip_footer > 0 and skip_footer < len(chunk):
                        chunk = chunk.iloc[:-skip_footer, :]
                    elif skip_footer >= len(chunk):
                        logger.warning(f'⚠️ skip_footer ({skip_footer}) >= chunk size ({len(chunk)}), skipping entire chunk.')
                        break

                    inserted = insert_data(cursor, chunk, table_name, chunk_num)
                    if inserted < 0:
                        return False
                    total_inserted += inserted
                    break

            logger.info(f'✅ Total rows inserted into <{table_name}>: {total_inserted}')

            if control_callback:
                control_callback(
                    cursor, file_path, table_name, skip_rows, skip_footer, enc, 
                    has_header, delimiter, quotechar,
                    timestamp, total_inserted, column_count,
                    "BODS COMPLETED"
                )

            return  True # success
        except UnicodeDecodeError:
            logger.warning(
                f'⚠️ Encoding <{enc}> failed for <{file_path.name}>, trying fallback ...'
            )
        except Exception as e:
            logger.error(
                f'❌ Error reading <{file_path.name}> with encoding <{enc}>: {e}'
            )

            if control_callback:
                control_callback(
                    cursor, file_path, table_name, skip_rows, skip_footer, enc, 
                    has_header, delimiter, quotechar, 
                    timestamp, 0, 0,
                    "BODS FAILED"
                )

            break

    logger.error(f'❌ All encoding attempts failed for <{file_path.name}>, skipping ...')
    return False  # ❌ failed


def archive_csv_file(
    file_path: Path, 
    archive_dir_name: str = "Archive",
    timestamp: str = None,
):
    """Move the processed CSV file to a sibling 'archive' folder with a timestamped filename."""
    if not file_path.exists():
        logger.error(f'❌ File not found: <{file_path.resolve()}>')
        return

    archive_dir = file_path.parent / archive_dir_name
    try:
        archive_dir.mkdir(exist_ok=True)
    except Exception as e:
        logger.error(f'❌ Failed to create archive directory: <{archive_dir}> - {e}')
        return

    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    else:
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d_%H%M%S')
    logger.debug(f'⚠️ CSV file archive timestamp: <{timestamp}>')

    archived_filename = f'{file_path.stem}_{timestamp}{file_path.suffix}'
    destination = archive_dir / archived_filename

    try:
        shutil.move(str(file_path), str(destination))
        logger.info(f'✅ Archived: <{file_path.name}> ➜ <{destination.name}>')
    except PermissionError as pe:
        logger.error(
            f'❌ Permission denied when archiving file: <{file_path}> ➜ <{destination}> - {pe}'
        )
    except Exception as e:
        logger.error(f'❌ Failed to archive file: <{file_path}> ➜ <{destination}> - {e}')


def read_file_list(file_path: Path, path_prefix: str = None) -> list:
    """Read file list form <path to>/File_Location.txt"""
    file_list = []
    try:
        df = pd.read_csv(file_path, quotechar='`', sep=',', dtype=str)
        if path_prefix:
            df['File Name'] = path_prefix + "/" + df['File Name']
        
        # file_list = list(zip(df['File Name'].tolist(), df['Table Name'].tolist()))
        # Fill missing columns if older format
        for col in ['Skip Rows', 'Skip Footer', 'Encoding', 
                    'Has Header', 'Delimiter', 'Quotechar']:
            if col not in df.columns:
                df[col] = None
        df = df.where(pd.notnull(df), None)
        file_list = df.to_dict('records')        

        logger.debug(f'🐍 File list with table names: \n<{file_list}>')
    except FileNotFoundError:
        logger.error(f'❌ The file <{file_path}> was not found')
    except Exception as e:
        logger.error(f'❌ An error occurred: {e}')

    return file_list

# os.getenv("FORCE_LOAD", "false").lower() == "true"
def main():
    logger.info(f'⏩ {"="*98}')

    CONFIG_PATH = Path(os.getenv("USERPROFILE")) / ".pipelines" / "ds_config.ini"
    if not CONFIG_PATH.exists():
        logger.error(
            f'❌ DS connect configuration file not found or no accessable, exiting ...'
            )
        raise FileNotFoundError(
            f'DS connect configuration file not found or no accessable: <{CONFIG_PATH}>'
            )

    # force load bypassing skip check when FORCE_LOAD is True
    FORCE_LOAD = os.getenv("FORCE_LOAD", "false").lower() == "true"
    # archive loaded file if FILE_ARCHIVE is True
    FILE_ARCHIVE = os.getenv("FILE_ARCHIVE", "false").lower() == "true" 

    ENVIRONMENT = os.getenv("ENVIRONMENT", "SBX").upper()
    ENV, AWS_BASE = aws_env(CONFIG_PATH, ENVIRONMENT)

    logger.debug(f'🐍 ENVIRONMENT: <{ENVIRONMENT}>, ENV: <{ENV}>, AWS_BASE: <{AWS_BASE}>')
    logger.debug(f'🐍 FORCE_LOAD: <{FORCE_LOAD}>, FILE_ARCHIVE: <{FILE_ARCHIVE}>')

    file_list = read_file_list(Path("File_Locations.txt"))

    total_files = 0
    sipped_files = 0
    success_files = 0

    logger.info(f'⏩ Loading file(s) to Datasphere ({ENV}) from: <{AWS_BASE}> ...')

    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.debug(f'⚠️ BODS ETL timestamp: <{timestamp}>')
    with ds_conn(CONFIG_PATH, ENV).cursor() as cursor:
        # for location, provided_table_name in file_list:
        for row in file_list:
            location = row.get('File Name')
            provided_table_name = row.get('Table Name')
            skip_rows = int(row.get('Skip Rows') or 0)
            skip_footer = int(row.get('Skip Footer') or 0)
            encoding = row.get('Encoding') or None
            has_header = str(row.get('Has Header')).strip().lower() \
                == 'true' if row.get('Has Header') else None
            delimiter = row.get('Delimiter') or None
            quotechar = row.get('Quotechar') or None

            logger.info(f'⏩ Loading from: <{location}> ...')
            path = Path(f'{AWS_BASE}/{location}')

            if path.is_file() and path.suffix.lower() in (".csv", ".txt"):
                total_files += 1
                if FORCE_LOAD:
                    logger.info(f'⏩ Force loading: <{path}> ...')                     
                else:
                    if should_skip_file(cursor, path):
                        sipped_files += 1
                        continue

                # table_name = sanitize_table_name(path.name)
                table_name = provided_table_name.strip() if provided_table_name and \
                                str(provided_table_name).strip() else sanitize_table_name(path.name)

                # ✅ Explicitly set BODS STARTED
                update_control_table(cursor, path, table_name, skip_rows, skip_footer,
                                     encoding or 'unknown', 
                                     has_header if has_header is not None else False, 
                                     delimiter or ',', quotechar or '', 
                                     timestamp, None, None, 'BODS STARTED')

                success = process_csv_file_in_chunks(
                    cursor, path, table_name, timestamp=timestamp, 
                    control_callback=update_control_table,
                    skip_rows=skip_rows, skip_footer=skip_footer, 
                    encoding=encoding, has_header=has_header, 
                    delimiter=delimiter, quotechar=quotechar
                )

                if success:
                    if FILE_ARCHIVE:
                        archive_csv_file(path, timestamp=timestamp)
                    else:
                        logger.debug(f'⚠️ Skipped archiving <{path.name}> as archive set False.')
                    success_files += 1
                else:
                    if FILE_ARCHIVE:
                        logger.warning(f'⚠️ Skipped archiving <{path.name}> due to load failure.')
            elif path.is_dir():
                csv_files = list(path.glob("*.csv")) + list(path.glob("*.txt"))
                logger.debug(f'📁 Found {len(csv_files)} CSV files in folder <{path}>')
                for file_path in csv_files:
                    total_files += 1
                    if FORCE_LOAD:
                        logger.info(f'⏩ Force loading: <{file_path}> ...')                     
                    else:
                        if should_skip_file(cursor, file_path):
                            sipped_files += 1
                            continue

                    table_name = sanitize_table_name(file_path.name)

                    # ✅ Explicitly set BODS STARTED
                    update_control_table(cursor, file_path, table_name, skip_rows, skip_footer, 
                                         encoding or 'unknown', 
                                         has_header if has_header is not None else False,
                                         delimiter or ',', quotechar or '', 
                                         timestamp, 0, 0, 'BODS STARTED')

                    success = process_csv_file_in_chunks(
                        cursor, file_path, table_name, timestamp=timestamp, 
                        control_callback=update_control_table,
                        skip_rows=skip_rows, skip_footer=skip_footer,
                        encoding=encoding, has_header=has_header, 
                        delimiter=delimiter, quotechar=quotechar
                    )

                    if success:
                        if FILE_ARCHIVE:
                            archive_csv_file(file_path, timestamp=timestamp)
                        else:
                            logger.debug(
                                f'⚠️ Skipped archiving <{file_path.name}> as archive set False.'
                            )
                        success_files += 1
                    else:
                        if FILE_ARCHIVE:
                            logger.warning(
                                f'⚠️ Skipped archiving <{file_path.name}> due to load failure.'
                            )
            else:
                logger.warning(f'⚠️ Path does not exist or is not valid CSV: <{path.resolve()}>')

    # 🔚 Summary log
    logger.info(f'🏁 Finished for Datasphere ({ENV}) from: <{AWS_BASE}>')
    logger.info(
        f'📁 Files and folders: {len(file_list)} | '
        f'📦 Total files: {total_files} | '
        f'⏭ Skipped: {sipped_files} | '
        f'✅ Loaded: {success_files} | '
        f'❌ Failed: {total_files - success_files - sipped_files}'
    )


# ========================== #
#          Main Block        #
# ========================== #

if __name__ == "__main__":
    main()
