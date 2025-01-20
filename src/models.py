from sqlmodel import SQLModel, Field
from geoalchemy2 import Geometry
from sqlalchemy import Column


class GeoTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    geoid: str
    name: str
    geometry: Geometry = Field(sa_column=Column(Geometry("MULTIPOLYGON", srid=4269)))

    class Config:
        arbitrary_types_allowed = True


class DP03Table(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    geoid: str
    total_house: int
    inc_less_10k: int
    inc_10k_15k: int
    inc_15_25k: int
    inc_25k_35k: int
    inc_35k_50k: int
    inc_50k_75k: int
    inc_75k_100k: int
    inc_100k_150k: int
    inc_150k_200k: int
    inc_more_200k: int
