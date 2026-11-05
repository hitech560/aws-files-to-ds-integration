import os
import csv
import shutil
import re
# import chardet
from io import StringIO
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser

import pandas as pd
from hdbcli import dbapi

from uni_logger import setup_logger


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
    month_offset: int,
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

    # try:
    #     bods_ts = datetime.strptime(timestamp, '%Y%m%d_%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
    # except Exception as e:
    #     logger.warning(f'⚠️ Invalid BODS timestamp format: <{timestamp}>, using original string')
    #     bods_ts = timestamp  # fallback to original if format fails

    control_file_name = normalize_control_file_name(file_path.name)

    values = [
        control_file_name,
        str(file_path.parent).replace("\\", "/"),  # ✅ directory only
        last_modified,
        table_name,
        skip_rows,
        skip_footer,
        encoding,
        has_header,
        delimiter,
        quotechar,
        month_offset,
        # bods_ts,
        timestamp,
        row_count,
        column_count,
        None, # DS_TIMESTAMP will be updated by DS
        status, # will be updated by DS to "DS_COMPLETED"
    ]

    # upsert_stmt = """
    # MERGE INTO "AWS_FILES_DS_INTEGRATION" AS target
    # USING (
    #     SELECT 
    #         ? AS "FILE_NAME",
    #         ? AS "FILE_PATH",
    #         ? AS "LAST_MODIFIED",
    #         ? AS "HAS_HEADER",
    #         ? AS "DELIMITER",
    #         ? AS "ENCODING",
    #         ? AS "TABLE_NAME",
    #         ? AS "BODS_TIMESTAMP",
    #         ? AS "ROW_COUNT",
    #         ? AS "COLUMN_COUNT",
    #         ? AS "DS_TIMESTAMP",
    #         ? AS "STATUS_FLAG"
    #     FROM dummy
    # ) AS src
    # ON target."FILE_NAME" = src."FILE_NAME" AND target."FILE_PATH" = src."FILE_PATH"
    # WHEN MATCHED THEN
    #     UPDATE SET
    #         "LAST_MODIFIED" = src."LAST_MODIFIED",
    #         "HAS_HEADER" = src."HAS_HEADER",
    #         "DELIMITER" = src."DELIMITER",
    #         "ENCODING" = src."ENCODING",
    #         "TABLE_NAME" = src."TABLE_NAME",
    #         "BODS_TIMESTAMP" = src."BODS_TIMESTAMP",
    #         "ROW_COUNT" = src."ROW_COUNT",
    #         "COLUMN_COUNT" = src."COLUMN_COUNT",
    #         "DS_TIMESTAMP" = src."DS_TIMESTAMP",
    #         "STATUS_FLAG" = src."STATUS_FLAG"
    # WHEN NOT MATCHED THEN
    #     INSERT (
    #         "FILE_NAME", "FILE_PATH", "LAST_MODIFIED", "HAS_HEADER",
    #         "DELIMITER", "ENCODING", "TABLE_NAME", "BODS_TIMESTAMP",
    #         "ROW_COUNT", "COLUMN_COUNT", "DS_TIMESTAMP", "STATUS_FLAG"
    #     )
    #     VALUES (
    #         src."FILE_NAME", src."FILE_PATH", src."LAST_MODIFIED", src."HAS_HEADER",
    #         src."DELIMITER", src."ENCODING", src."TABLE_NAME", src."BODS_TIMESTAMP",
    #         src."ROW_COUNT", src."COLUMN_COUNT", src."DS_TIMESTAMP", src."STATUS_FLAG"
    #     );
    # """

    # In update_control_table() — replace the upsert_stmt definition:
    upsert_stmt = load_sql(Path("control_table_upsert.sql"))

    interpolated_sql = upsert_stmt
    for val in values:
        val_repr = f"'{val}'" if isinstance(val, str) else str(val)
        interpolated_sql = interpolated_sql.replace("?", val_repr, 1)

    logger.debug(f'⚠️ Inserting control table values: \n{values}')
    logger.debug(f"🧪 Control table interpolated SQL: {interpolated_sql}")

    placeholder_count = upsert_stmt.count("?")

    if placeholder_count != len(values):
        raise RuntimeError(
            f'Control-table SQL parameter mismatch: '
            f'SQL expects {placeholder_count}, '
            f'but Python supplied {len(values)}'
        )

    try:
        cursor.execute(upsert_stmt, values)
        logger.info(
            f'📋 Upserted control record <{control_file_name}> '
            f'for physical file <{file_path.name}> with status: <{status}>'
        )
    #except dbapi.Error as e:
    #    logger.error(f'❌ Failed to upsert control record for <{file_path.name}>: {e}')
    #    raise (f'❌ Control table upsert error for <{file_path.name}>: {e}')
    except dbapi.Error as e:
        message = (
            f'❌ Control table upsert error for '
            f'<{file_path.name}>: {e}'
        )
        logger.error(message)
        raise RuntimeError(message) from e


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


def detect_csv_properties(
    file_path: Path,
    encoding: str = 'utf-8-sig',
    configured_delimiter: str = None,
    configured_quotechar: str = None,
    configured_has_header: bool = None,
):
    """Detect CSV properties while allowing metadata to override Sniffer."""
    try:
        with open(file_path, 'r', newline='', encoding=encoding) as csvfile:
            sample_lines = []
            for line in csvfile:
                if line.strip():
                    sample_lines.append(line)
                if len(sample_lines) >= 50:
                    break

            sample = ''.join(sample_lines)
            if not sample:
                logger.error(f'❌ File is empty: <{file_path}>')
                return None

            sniffer = csv.Sniffer()

            if configured_delimiter is not None:
                detected_delimiter = configured_delimiter
            else:
                dialect = sniffer.sniff(sample, delimiters=',;\t|^#')
                detected_delimiter = dialect.delimiter

            if configured_quotechar is not None:
                detected_quotechar = configured_quotechar or None
            else:
                detected_quotechar = '"'

            if configured_has_header is not None:
                detected_has_header = configured_has_header
            else:
                try:
                    detected_has_header = sniffer.has_header(sample)
                except csv.Error:
                    logger.warning(
                        f'⚠️ Could not determine header for <{file_path.name}>; '
                        'defaulting to header=True'
                    )
                    detected_has_header = True

            properties = {
                'file_path': file_path,
                'delimiter': detected_delimiter,
                'quotechar': detected_quotechar,
                'has_header': detected_has_header,
                'encoding': encoding,
            }
            logger.debug(f'⚠️ Detected properties: \n{properties}')
            return properties

    except UnicodeDecodeError as e:
        logger.warning(
            f'⚠️ Encoding <{encoding}> cannot decode <{file_path.name}>: {e}'
        )
        return None
    except csv.Error as e:
        logger.warning(
            f'⚠️ CSV detection failed for <{file_path.name}> '
            f'using <{encoding}>: {e}'
        )
        return None
    except FileNotFoundError:
        logger.error(f'❌ File not found: <{file_path}>')
        return None
    except Exception as e:
        logger.error(
            f'❌ File <{file_path}> properties detection unexpected error occurred: {e}'
        )
        return None


def normalize_control_file_name(filename: str) -> str:
    """
    Return a stable logical file name for the control table.

    Examples:
        TCI_SalesSurvey_202607.csv  -> TCI_SalesSurvey.csv
        TCI_SalesSurvey_202608.csv  -> TCI_SalesSurvey.csv
        Sales_20260731.csv          -> Sales.csv
        All_Time.txt                -> All_Time.txt

    This prevents one AWS_FILES_DS_INTEGRATION row from being created
    for every monthly physical file.
    """
    path = Path(filename)
    stem = re.sub(
        r'[_\-\s]?(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])?$',
        '',
        path.stem,
    )
    return f"{stem}{path.suffix}"


def sanitize_table_name(filename: str) -> str:
    """Create a stable table name and remove trailing YYYYMM/YYYYMMDD dates."""
    stem = Path(filename).stem
    stem = re.sub(
        r'[_\-\s]?(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])?$',
        '',
        stem,
    )
    return stem.replace('-', '_').replace(' ', '_').upper()


def shift_month(base_date: datetime, months: int = 0) -> datetime:
    """Return the first day of a month shifted from base_date."""
    month_index = base_date.year * 12 + base_date.month - 1 + months
    year, month_zero_based = divmod(month_index, 12)
    return base_date.replace(
        year=year,
        month=month_zero_based + 1,
        day=1,
    )


def resolve_filename_tokens(
    location: str,
    run_date: datetime = None,
    month_offset: int = -1,
) -> str:
    """Resolve date tokens such as {YYYYMM} in a configured file name."""
    run_date = run_date or datetime.now()
    target_month = shift_month(run_date, month_offset)

    replacements = {
        '{YYYYMM}': target_month.strftime('%Y%m'),
        '{YYYYMMDD}': run_date.strftime('%Y%m%d'),
        '{YYYY}': target_month.strftime('%Y'),
        '{MM}': target_month.strftime('%m'),
    }

    resolved = str(location)
    for token, value in replacements.items():
        resolved = resolved.replace(token, value)
    return resolved


def normalize_delimiter(value: str):
    if value is None:
        return None
    text = str(value).strip()
    mapping = {
        'comma': ',',
        'pipe': '|',
        'tab': '\t',
        'semicolon': ';',
        'caret': '^',
    }
    return mapping.get(text.lower(), text)


def normalize_quotechar(value: str):
    if value is None:
        return None
    text = str(value).strip()
    mapping = {
        'doublequote': '"',
        'singlequote': "'",
        'none': None,
        '': None,
    }
    return mapping.get(text.lower(), text)


#def infer_hana_type(series: pd.Series) -> str:
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

    #return "VARCHAR(255)"
def infer_hana_type(series: pd.Series) -> str:
    """
    Infer a safe HANA string type based on actual data length.

    Current pipeline loads flat files mostly as text, so for now we keep
    everything as character-based types, but size them properly.

    Rules:
    - NULL/empty column -> VARCHAR(255)
    - <= 5000 chars     -> VARCHAR(rounded length)
    - > 5000 chars      -> NCLOB
    """
    # Remove nulls first
    non_null = series.dropna()

    if non_null.empty:
        return "VARCHAR(255)"

    max_len = non_null.astype(str).map(len).max()

    if pd.isna(max_len) or max_len <= 0:
        return "VARCHAR(255)"

    if max_len > 5000:
        return "NCLOB"

    # tighter sizing in 100-char buckets
    length = min(((int(max_len) + 99) // 100) * 100, 5000)
    length = max(length, 255)

    return f"VARCHAR({length})"


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
    physical_file_name = file_path.name
    file_name = normalize_control_file_name(physical_file_name)
    file_dir = str(file_path.parent).replace("\\", "/")
    try:
        last_modified = datetime.fromtimestamp(
            file_path.stat().st_mtime
        ).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.warning(f'⚠️ Failed to stat <{physical_file_name}>: {e}')
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
            logger.info(
                f'⏩ Loading <{physical_file_name}>: '
                f'control key <{file_name}> not found in control table'
            )
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
            logger.warning(f'⚠️ Skipping <{physical_file_name}>: out-of-dated')
            return True

        status_flag = str(row[1]) if row else ''
        logger.debug(
            f'⚠️ File <{file_name}> Status flag: <{status_flag}>'
            )
        check2 = status_flag == "BODS COMPLETED"
        if check2:
            logger.info(f'⚠️ Skipping <{physical_file_name}>: DS proccess pending')
            return True
    except dbapi.Error as e:
        logger.warning(f'⚠️ Skipping as file skip check failed for <{physical_file_name}>: {e}')
        return True

    logger.info(f'⏩ Loading <{physical_file_name}>: skip check completed and passed')
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

    # Construct DROP/TRUNCATE/CREATE TABLE SQL
    drop_stmt = f'DROP TABLE "{table_name}"'
    truncate_stmt = f'TRUNCATE TABLE "{table_name}"'
    create_stmt = f'CREATE COLUMN TABLE "{table_name}" (\n  {",\n  ".join(col_defs)}\n)'

    logger.debug(f'⚠️ drop_stmt = {drop_stmt}')
    logger.debug(f'⚠️ truncate_stmt = {truncate_stmt}')
    logger.debug(f'⚠️ create_stmt = \n{create_stmt}')

    if table_exists(cursor, table_name):
        try:
            cursor.execute(drop_stmt)
            # cursor.execute(truncate_stmt)
            logger.debug(f'🧹 Dropped existing table: <{table_name}>')
            # logger.debug(f'🧹 Truncated existing table: <{table_name}>')
            # return True
        except dbapi.Error as e:
            logger.warning(f'ℹ️ Could not drop table <{table_name}>: {e}')
            # logger.warning(f'⚠️ Could not truncate table <{table_name}>: {e}')
            return False
    # else:
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
    month_offset: int = -1,
    control_callback=None,
) -> bool:
    if not file_path.exists():
        logger.error(f'❌ File not found: <{file_path.resolve()}>')
        return False    

    total_inserted = 0
    # first_chunk = True
    # encodings_to_try = [encoding, 'utf-8-sig', 'latin1'] if encoding else ['utf-8-sig', 'latin1']
    encodings_to_try = list(dict.fromkeys(
        [encoding, 'utf-8-sig', "cp1252", 'latin1'] if encoding else ['utf-8-sig', "cp1252", 'latin1']
    ))
    if timestamp is None:
        # ✅ Compute once per file
        # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f'⚠️ BODS ETL timestamp: <{timestamp}>')

    for enc in encodings_to_try:
        logger.debug(f'⚠️ Trying encoding: <{enc}>')

        try:
            props = detect_csv_properties(
                file_path,
                encoding=enc,
                configured_delimiter=delimiter,
                configured_quotechar=quotechar,
                configured_has_header=has_header,
            )
            # if not props:
            #     # delimiter = detect_csv_delimiter(file_path, encoding=encoding)
            #     # has_header = infer_has_header(file_path, encoding=encoding, delimiter=delimiter)
            #     # quotechar = None
            #     logger.error(f'❌ Cannot detect file properties: <{file_path.resolve()}>')
            #     return False
            # else:
            #     detected_header = props['has_header']
            #     detected_delimiter = props['delimiter']
            #     detected_quotechar = props['quotechar']
            if not props:
                logger.warning(
                    f'⚠️ Could not detect CSV properties using encoding <{enc}> '
                    f'for <{file_path.name}>, trying next encoding ...'
                )
                continue

            effective_has_header = (
                has_header if has_header is not None else props['has_header']
            )
            effective_delimiter = delimiter if delimiter is not None else props['delimiter']
            effective_quotechar = (
                quotechar if quotechar is not None else props['quotechar']
            )

            read_csv_kwargs = {
                'filepath_or_buffer': file_path,
                'encoding': enc,
                'chunksize': chunksize,
                'delimiter': effective_delimiter,
                'quotechar': effective_quotechar,
                'skiprows': skip_rows,
                # 'skipfooter': skip_bottom,
                'low_memory': False,
                'dtype': str,  # ✅ Prevent type inference that removes leading zeros
            }
            if not effective_has_header:
                read_csv_kwargs['header'] = None

            logger.debug(f'⚠️ read_csv_kwargs: \n{read_csv_kwargs}')

            # reader = pd.read_csv(**read_csv_kwargs)
            # # prev_chunk = None
            # total_inserted = 0
            # chunk_num = 0
            # first_chunk = True
            # # for chunk in pd.read_csv(**read_csv_kwargs):
            # for chunk in reader:
            #     _, column_count = chunk.shape
            #     if has_header:
            #         chunk.columns = deduplicate_columns(chunk.columns)
            #     else:
            #         # auto-name columns: COL001, COL002, ...
            #         chunk.columns = [f"COL{str(i+1).zfill(3)}" for i in range(chunk.shape[1])]

            #     chunk = chunk.where(pd.notnull(chunk), None)

            #     # ✅ Add the same ETL timestamp to every chunk
            #     chunk["BODS_TIMESTAMP"] = timestamp

            #     if first_chunk:
            #         success = create_table_from_df(cursor, chunk, table_name)
            #         if not success:
            #             return False  # ❌ Stop if table creation failed
            #         first_chunk = False

            #     chunk_num += 1
            #     inserted = insert_data(cursor, chunk, table_name, chunk_num)
            #     if inserted < 0:
            #         logger.error(
            #             f'❌ Insertion failed for <{file_path.name}> (table: <{table_name}>)'
            #         )
            #         return False # insert date failed
            #     total_inserted += inserted
            reader = pd.read_csv(**read_csv_kwargs)
            first_chunk = True
            chunk_num = 1

            try:
                chunk = next(reader)
            except StopIteration:
                return False  # no data

            while True:
                _, column_count = chunk.shape

                if effective_has_header:
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
                    effective_has_header, effective_delimiter, effective_quotechar, month_offset, 
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
                    has_header, delimiter, quotechar, month_offset, 
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
        with open(file_path, "r", encoding="utf-8-sig") as f:
            valid_lines = [
                line for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]

        if not valid_lines:
            logger.warning(f'⚠️ No valid entries found in <{file_path}>')
            return []    
        
        # df = pd.read_csv(file_path, quotechar='`', sep=',', dtype=str)
        df = pd.read_csv(
            StringIO("".join(valid_lines)),
            quotechar='`',
            sep=',',
            dtype=str
        )
        # if path_prefix:
        #     df['File Name'] = path_prefix + "/" + df['File Name']
        if path_prefix:
            df['File Name'] = path_prefix.rstrip("/") + "/" + df['File Name'].astype(str)
        
        # file_list = list(zip(df['File Name'].tolist(), df['Table Name'].tolist()))
        # Fill missing columns if folder format
        for col in [
            'Skip Rows', 'Skip Footer', 'Encoding', 
            'Has Header', 'Delimiter', 'Quotechar', 'Month Offset',
            'Table Name', 
        ]:
            if col not in df.columns:
                df[col] = None
        df = df.where(pd.notnull(df), None)
        file_list = df.to_dict('records')        

        logger.debug(f'🐍 File list with table names: \n<{file_list}>')
        return file_list
    except FileNotFoundError:
        logger.error(f'❌ The file <{file_path}> was not found')
        return None
    except Exception as e:
        logger.error(f'❌ An error occurred: {e}')
        return None

    
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

    # if file_list is None:
    #     logger.error('❌ Failed to read File_Locations.txt, exiting ...')
    #     return
    # 
    # if not file_list:
    #     logger.warning('⚠️ No valid files found in File_Locations.txt, nothing to process.')
    #     return
    if file_list is None:
        raise RuntimeError(
            "❌ Failed to read File_Locations.txt"
        )

    if not file_list:
        raise ValueError(
            "❌ File_Locations.txt is empty or contains no valid file entries."
        )
    
    listed_count = len(file_list)
    tried_count = 0
    error_count = 0
    sipped_count = 0
    loaded_count = 0
    failed_count = 0

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
            delimiter = normalize_delimiter(row.get('Delimiter'))
            quotechar = normalize_quotechar(row.get('Quotechar'))
            
            #month_offset = int(row.get('Month Offset') or 0)
            default_month_offset = int(
                os.getenv('DEFAULT_MONTH_OFFSET', '-1')
            )
            month_offset = int(
                row.get('Month Offset') or default_month_offset
            )

            # skip_rows = 0
            # if row.get('Skip Rows') not in [None, '', float('nan')]:
            #     try:
            #         skip_rows = int(row.get('Skip Rows'))
            #     except (ValueError, TypeError):
            #         skip_rows = 0

            resolved_location = resolve_filename_tokens(
                location,
                month_offset=month_offset,
            )
            logger.info(
                f'⏩ Loading from: <{resolved_location}> '
                f'(configured as <{location}>) ...'
            )
            path = Path(AWS_BASE) / resolved_location

            if path.is_file() and path.suffix.lower() in (".csv", ".txt"):
                tried_count += 1
                if FORCE_LOAD:
                    logger.info(f'⏩ Force loading: <{path}> ...')                     
                else:
                    if should_skip_file(cursor, path):
                        sipped_count += 1
                        continue

                # table_name = sanitize_table_name(path.name)
                table_name = provided_table_name.strip() if provided_table_name and \
                                str(provided_table_name).strip() else sanitize_table_name(path.name)

                # ✅ Explicitly set BODS STARTED
                # update_control_table(cursor, path, table_name, 
                #                     'unknown', False, ',', timestamp, 0, 0, 'BODS STARTED')
                update_control_table(cursor, path, table_name, skip_rows, skip_footer,
                                     encoding or 'unknown', 
                                     has_header if has_header is not None else False, 
                                     delimiter or ',', quotechar or '', month_offset, 
                                     timestamp, None, None, 'BODS STARTED')

                success = process_csv_file_in_chunks(
                    cursor, path, table_name, timestamp=timestamp, 
                    control_callback=update_control_table,
                    skip_rows=skip_rows, skip_footer=skip_footer, 
                    encoding=encoding, has_header=has_header, 
                    delimiter=delimiter, quotechar=quotechar, 
                    month_offset=month_offset
                )

                # if process_csv_file_in_chunks(
                #     cursor, path, 
                #     table_name, 
                #     timestamp=timestamp,
                #     control_callback=update_control_table,
                # ):
                if success:
                    if FILE_ARCHIVE:
                        archive_csv_file(path, timestamp=timestamp)
                    else:
                        logger.debug(f'⚠️ Skipped archiving <{path.name}> as archive set False.')
                    loaded_count += 1
                else:
                    failed_count += 1
                    # Ensure the control table reflects every processing failure,
                    # including failures that returned False without raising.
                    update_control_table(
                        cursor, path, table_name, skip_rows, skip_footer,
                        encoding or 'unknown',
                        has_header if has_header is not None else False,
                        delimiter or ',', quotechar or '', month_offset,
                        timestamp, 0, 0, 'BODS FAILED'
                    )
                    if FILE_ARCHIVE:
                        logger.warning(f'⚠️ Skipped archiving <{path.name}> due to load failure.')
            elif path.is_dir():
                csv_files = list(path.glob("*.csv")) + list(path.glob("*.txt"))
                logger.debug(f'📁 Found {len(csv_files)} CSV files in folder <{path}>')
                for file_path in csv_files:
                    tried_count += 1
                    if FORCE_LOAD:
                        logger.info(f'⏩ Force loading: <{file_path}> ...')                     
                    else:
                        if should_skip_file(cursor, file_path):
                            sipped_count += 1
                            continue

                    table_name = sanitize_table_name(file_path.name)

                    # ✅ Explicitly set BODS STARTED
                    # update_control_table(cursor, file_path, table_name, 
                    #                     'unknown', False, ',', timestamp, 0, 0, 'BODS STARTED')
                    update_control_table(cursor, file_path, table_name, skip_rows, skip_footer, 
                                         encoding or 'unknown', 
                                         has_header if has_header is not None else False,
                                         delimiter or ',', quotechar or '', month_offset, 
                                         timestamp, 0, 0, 'BODS STARTED')

                    success = process_csv_file_in_chunks(
                        cursor, file_path, table_name, timestamp=timestamp, 
                        control_callback=update_control_table,
                        skip_rows=skip_rows, skip_footer=skip_footer,
                        encoding=encoding, has_header=has_header, 
                        delimiter=delimiter, quotechar=quotechar,
                        month_offset=month_offset
                    )

                    # if process_csv_file_in_chunks(
                    #     cursor, 
                    #     file_path, 
                    #     table_name, 
                    #     timestamp=timestamp,
                    #     control_callback=update_control_table,
                    # ):
                    if success:
                        if FILE_ARCHIVE:
                            archive_csv_file(file_path, timestamp=timestamp)
                        else:
                            logger.debug(
                                f'⚠️ Skipped archiving <{file_path.name}> as archive set False.'
                            )
                        loaded_count += 1
                    else:
                        failed_count += 1
                        # Ensure the control table reflects every processing failure.
                        update_control_table(
                            cursor, file_path, table_name, skip_rows, skip_footer,
                            encoding or 'unknown',
                            has_header if has_header is not None else False,
                            delimiter or ',', quotechar or '', month_offset,
                            timestamp, 0, 0, 'BODS FAILED'
                        )
                        if FILE_ARCHIVE:
                            logger.warning(
                                f'⚠️ Skipped archiving <{file_path.name}> due to load failure.'
                            )
            else:
                error_count += 1
                logger.warning(f'⚠️ Path does not exist or is not valid CSV: <{path.resolve()}>')

    # 🔚 Summary log
    logger.info(f'🏁 Finished file(s) load for Datasphere ({ENV}) from: <{AWS_BASE}>')
    logger.info(
        f'📁 Listed: {listed_count} | '
        f'📦 Error: {error_count} | '
        f'📦 Tried: {tried_count} | '
        f'⏭ Skipped: {sipped_count} | '
        f'✅ Loaded: {loaded_count} | '
        f'❌ Failed: {failed_count}'
    )

    # Return a non-zero process exit code to the BODS wrapper only for
    # actual attempted-load failures. Missing/invalid configured paths
    # remain reported separately as Error and do not fail the wrapper.
    if failed_count > 0:
        raise RuntimeError(
            f'{failed_count} file load(s) failed. '
            'See log and AWS_FILES_DS_INTEGRATION for details.'
        )


# ========================== #
#          Main Block        #
# ========================== #

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("❌ Job terminated with failure")
        raise