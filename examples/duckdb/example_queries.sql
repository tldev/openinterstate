-- Example queries for a local OpenInterstate release in DuckDB.

INSTALL spatial;
LOAD spatial;

-- Update this path to your local release directory.
CREATE OR REPLACE VIEW corridors AS
SELECT * FROM read_csv_auto('release-YYYY-MM-DD/csv/corridors.csv');

CREATE OR REPLACE VIEW corridor_exits AS
SELECT * FROM read_csv_auto('release-YYYY-MM-DD/csv/corridor_exits.csv');

-- List corridors for I-95.
SELECT corridor_id, interstate_name, direction_code
FROM corridors
WHERE interstate_name = 'I-95'
ORDER BY direction_code;

-- Count exits by corridor.
SELECT corridor_id, COUNT(*) AS exit_count
FROM corridor_exits
GROUP BY corridor_id
ORDER BY exit_count DESC;
