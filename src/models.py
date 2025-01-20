# from sqlmodel import SQLModel, Field
# from geoalchemy2 import Geometry
# from sqlalchemy import Column
import duckdb

con = duckdb.connect("data.ddb")
con.load_extension("spatial")


# class GeoTable(SQLModel, table=True):
#     id: int = Field(primary_key=True)
#     geoid: str
#     name: str
#     geometry: Geometry = Field(sa_column=Column(Geometry("MULTIPOLYGON", srid=4269)))


# class DP03Table(SQLModel, table=True):
#     id: int = Field(primary_key=True)
#     year: int
#     geoid: str
#     total_house: int
#     inc_less_10k: int
#     inc_10k_15k: int
#     inc_15k_25k: int
#     inc_25k_35k: int
#     inc_35k_50k: int
#     inc_50k_75k: int
#     inc_75k_100k: int
#     inc_100k_150k: int
#     inc_150k_200k: int
#     inc_more_200k: int


con.sql(
    """
    CREATE TABLE IF NOT EXISTS "DP03Table" (
        id INTEGER PRIMARY KEY,
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
        )
    """
)

con.sql(
    """
    CREATE TABLE IF NOT EXISTS "GeoTable" AS
        SELECT * FROM read_parquet("data.parquet")
    """
)
