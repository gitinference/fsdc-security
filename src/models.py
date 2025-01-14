from ibis import Any
from sqlmodel import Column, Field, SQLModel
from geoalchemy2 import Geometry


class GeoTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    geoid: str
    name: str
    geometry: Any = Field(sa_column=Column(Geometry("MULTIPOLIGON")))
