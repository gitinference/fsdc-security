import logging
import os
from datetime import datetime
from pathlib import Path

import duckdb
import geopandas as gpd
import jp_tools
import polars as pl
from CensusForge import CensusAPI
from dotenv import load_dotenv

load_dotenv()


class SecurityUtils:
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = Path(saving_dir)
        self.conn = duckdb.connect()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )

    def pull_dp03(self) -> pl.DataFrame:
        for _year in range(2012, datetime.now().year - 1):
            year_dir = self.saving_dir / "raw" / "acs" / str(_year)
            file_path = year_dir / "data.parquet"

            if not file_path.exists():
                year_dir.mkdir(parents=True, exist_ok=True)

                logging.info(f"pulling {_year} data")
                r = CensusAPI(str(os.environ.get("CENSUS_TOKEN"))).query(
                    params_list=[
                        "DP03_0051E",
                        "DP03_0052E",
                        "DP03_0053E",
                        "DP03_0054E",
                        "DP03_0055E",
                        "DP03_0056E",
                        "DP03_0057E",
                        "DP03_0058E",
                        "DP03_0059E",
                        "DP03_0060E",
                        "DP03_0061E",
                        "DP05_0001E",
                        "DP05_0004E",
                        "DP05_0005E",
                        "DP05_0006E",
                        "DP05_0007E",
                        "DP05_0008E",
                        "DP05_0009E",
                        "DP05_0010E",
                        "DP05_0011E",
                        "DP05_0012E",
                        "DP05_0013E",
                        "DP05_0014E",
                        "DP05_0015E",
                        "DP05_0016E",
                        "DP05_0017E",
                    ],
                    year=_year,
                    geography="county%20subdivision",
                    extra="&in=state:72&in=county:*",
                    skip_checks=True,
                    dataset="acs-acs5-profile",
                )
                df_dp03 = pl.DataFrame(r)
                df_dp03 = df_dp03.rename(df_dp03.row(0, named=True))
                df_dp03 = df_dp03.slice(1).with_columns(
                    pl.col("*").exclude("state", "county").cast(pl.Float64)
                )
                df_dp03 = df_dp03.rename(
                    {
                        "DP03_0051E": "total_house",
                        "DP03_0052E": "inc_less_10k",
                        "DP03_0053E": "inc_10k_15k",
                        "DP03_0054E": "inc_15k_25k",
                        "DP03_0055E": "inc_25k_35k",
                        "DP03_0056E": "inc_35k_50k",
                        "DP03_0057E": "inc_50k_75k",
                        "DP03_0058E": "inc_75k_100k",
                        "DP03_0059E": "inc_100k_150k",
                        "DP03_0060E": "inc_150k_200k",
                        "DP03_0061E": "inc_more_200k",
                        "DP05_0001E": "total_pop",
                        "DP05_0004E": "ratio",
                        "DP05_0005E": "under_5_years",
                        "DP05_0006E": "pop_5_9_years",
                        "DP05_0007E": "pop_10_14_years",
                        "DP05_0008E": "pop_15_19_years",
                        "DP05_0009E": "pop_20_24_years",
                        "DP05_0010E": "pop_25_34_years",
                        "DP05_0011E": "pop_35_44_years",
                        "DP05_0012E": "pop_45_54_years",
                        "DP05_0013E": "pop_55_59_years",
                        "DP05_0014E": "pop_60_64_years",
                        "DP05_0015E": "pop_65_74_years",
                        "DP05_0016E": "pop_75_84_years",
                        "DP05_0017E": "over_85_years",
                    }
                )
                df_dp03 = df_dp03.with_columns(
                    pl.col("county subdivision")
                    .cast(pl.Int32)
                    .cast(pl.String)
                    .str.zfill(5)
                )
                df_dp03 = df_dp03.with_columns(
                    geoid=pl.col("state")
                    + pl.col("county")
                    + pl.col("county subdivision"),
                    year=_year,
                ).drop(["state", "county", "county subdivision"])
                df_dp03 = df_dp03.with_columns(pl.all().exclude("geoid").cast(pl.Int64))
                df_dp03.write_parquet(file=file_path)
                logging.info(f"succesfully inserting {_year}")
            else:
                logging.info(f"data for {_year} is in the database")

        # Consolidate and return all downloaded data
        search_path = self.saving_dir / "raw" / "acs" / "**" / "data.parquet"
        return pl.read_parquet(str(search_path))

    def pull_geo2(self):
        if not os.path.exists(f"{self.saving_dir}external/cousub.zip"):
            jp_tools.download(
                url="https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_72_cousub.zip",
                filename=f"{self.saving_dir}external/cousub.zip",
                verify=False,
            )
        if "GeoTable" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            logging.info(
                f"The GeoTable is empty inserting {self.saving_dir}external/cousub.zip"
            )
            gdf = gpd.read_file(f"{self.saving_dir}external/cousub.zip")
            gdf = gdf.rename(columns={"GEOID": "geoid", "NAME": "name"})
            gdf = gdf[~gdf["name"].str.contains("not defined")]
            df = gdf.drop(columns="geometry")
            geometry = gdf["geometry"].apply(lambda geom: geom.wkt)
            df["geometry"] = geometry
            self.conn.execute("CREATE TABLE GeoTable AS SELECT * FROM df")
            logging.info("Succefully inserting data to database")
        return self.conn.sql("SELECT * FROM GeoTable;")
