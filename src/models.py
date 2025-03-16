import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_dp03_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "DP03Table" (
            year INTEGER,
            geoid VARCHAR(30),
            total_house INTEGER,
            inc_less_10k INTEGER,
            inc_10k_15k INTEGER,
            inc_15k_25k INTEGER,
            inc_25k_35k INTEGER,
            inc_35k_50k INTEGER,
            inc_50k_75k INTEGER,
            inc_75k_100k INTEGER,
            inc_100k_150k INTEGER,
            inc_150k_200k INTEGER,
            inc_more_200k INTEGER
            );
        """
    )


def init_dp05_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "DP05Table" (
            year INTEGER,
            geoid VARCHAR(30),
            total_pop INTEGER,
            ratio FLOAT,
            under_5_year INTEGER,
            pop_5_9_years INTEGER,
            pop_10_14_years INTEGER,
            pop_15_19_years INTEGER,
            pop_20_24_years INTEGER,
            pop_25_34_years INTEGER,
            pop_35_44_years INTEGER,
            pop_45_54_years INTEGER,
            pop_55_59_years INTEGER,
            pop_60_64_years INTEGER,
            pop_65_74_years INTEGER,
            pop_75_84_years INTEGER,
            over_85_years INTEGER
            );
        """
    )


def init_geo_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.sql("DROP SEQUENCE IF EXISTS geo_sequence;")
    conn.sql("CREATE SEQUENCE geo_sequence START 1;")
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "GeoTable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('geo_sequence'),
            geoid TEXT,
            name TEXT,
            geometry GEOMETRY,
            );
        """
    )
