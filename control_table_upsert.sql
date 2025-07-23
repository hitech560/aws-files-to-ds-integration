MERGE INTO "AWS_FILES_DS_INTEGRATION" AS target
USING (
    SELECT 
        ? AS "FILE_NAME",
        ? AS "FILE_PATH",
        ? AS "LAST_MODIFIED",
        ? AS "TABLE_NAME",
        ? AS "SKIP_ROWS",
        ? AS "SKIP_FOOTER",
        ? AS "ENCODING",
        ? AS "HAS_HEADER",
        ? AS "DELIMITER",
        ? AS "QUOTECHAR",
        ? AS "BODS_TIMESTAMP",
        ? AS "ROW_COUNT",
        ? AS "COLUMN_COUNT",
        ? AS "DS_TIMESTAMP",
        ? AS "STATUS_FLAG"
    FROM dummy
) AS src
ON target."FILE_NAME" = src."FILE_NAME" AND target."FILE_PATH" = src."FILE_PATH"
WHEN MATCHED THEN
    UPDATE SET
        "LAST_MODIFIED" = src."LAST_MODIFIED",
        "TABLE_NAME" = src."TABLE_NAME",
        "SKIP_ROWS" = src."SKIP_ROWS",
        "SKIP_FOOTER" = src."SKIP_FOOTER",
        "ENCODING" = src."ENCODING",
        "HAS_HEADER" = src."HAS_HEADER",
        "DELIMITER" = src."DELIMITER",
        "QUOTECHAR" = src."QUOTECHAR",
        "BODS_TIMESTAMP" = src."BODS_TIMESTAMP",
        "ROW_COUNT" = src."ROW_COUNT",
        "COLUMN_COUNT" = src."COLUMN_COUNT",
        "DS_TIMESTAMP" = src."DS_TIMESTAMP",
        "STATUS_FLAG" = src."STATUS_FLAG"
WHEN NOT MATCHED THEN
    INSERT (
        "FILE_NAME", "FILE_PATH", "LAST_MODIFIED", "TABLE_NAME", "SKIP_ROWS", "SKIP_FOOTER", 
        "ENCODING", "HAS_HEADER", "DELIMITER", "QUOTECHAR", 
        "BODS_TIMESTAMP", "ROW_COUNT", "COLUMN_COUNT", "DS_TIMESTAMP", "STATUS_FLAG"
    )
    VALUES (
        src."FILE_NAME", src."FILE_PATH", src."LAST_MODIFIED", src."TABLE_NAME", src."SKIP_ROWS", 
        src."SKIP_FOOTER", src."ENCODING", src."HAS_HEADER", src."DELIMITER", src."QUOTECHAR", 
        src."BODS_TIMESTAMP", src."ROW_COUNT", src."COLUMN_COUNT", 
        src."DS_TIMESTAMP", src."STATUS_FLAG"
    );
