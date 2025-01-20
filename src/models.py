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

class PumsTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    adjinc: float
    hincip: int
    pwgtp: int