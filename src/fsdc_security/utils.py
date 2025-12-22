from json import JSONDecodeError
from datetime import datetime
from CensusForge import CensusAPI
import duckdb
from pathlib import Path
import geopandas as gpd
import polars as pl
import logging
import os


class SecurityUtils:
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
        self.conn = duckdb.connect()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
            filename=log_file,
        )
        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")

    def pull_dp03(self) -> pl.DataFrame:
        for _year in range(2012, datetime.now().year - 1):
            path_file = Path(f"{self.saving_dir}processed/pr-dp03-{_year}.parquet")
            if not path_file.exists():
                try:
                    logging.info(f"pulling {_year} data")
                    df_dp03 = CensusAPI().query(
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
                        ],
                        dataset="acs-acs5-profile",
                        year=_year,
                        extra="&for=county%20subdivision:*&in=state:72&in=county:*",
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
                        }
                    )
                    df_dp03 = df_dp03.with_columns(
                        geoid=pl.col("state")
                        + pl.col("county")
                        + pl.col("county subdivision")
                    ).drop(["state", "county", "county subdivision"])
                    df_dp03 = df_dp03.with_columns(
                        pl.all().exclude("geoid").cast(pl.Int64)
                    )
                    df_dp03.write_parquet(file=path_file)
                    logging.info(f"succesfully inserting {_year}")
                except JSONDecodeError:
                    logging.warning(f"The ACS for {_year} is not availabe")
                    continue
            else:
                logging.info(f"data for {_year} is in the database")
                continue
        return self.conn.execute(
            f"SELECT * FROM '{self.saving_dir}processed/pr-dp03-*.parquet';"
        ).pl()

    def pull_dp05(self) -> pl.DataFrame:
        for _year in range(2012, datetime.now().year - 1):
            if (
                self.conn.sql(f"SELECT * FROM 'DP05Table' WHERE year={_year}")
                .df()
                .empty
            ):
                try:
                    logging.info(f"pulling {_year} data")
                    tmp = self.pull_query(
                        params=[
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
                    )
                    tmp = tmp.rename(
                        {
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

                    tmp = tmp.with_columns(
                        geoid=pl.col("state")
                        + pl.col("county")
                        + pl.col("county subdivision")
                    ).drop(["state", "county", "county subdivision"])
                    # tmp = tmp.with_columns(pl.all().exclude("geoid").cast(pl.Int64))
                    self.conn.sql("INSERT INTO 'DP05Table' BY NAME SELECT * FROM tmp")
                    logging.info(f"succesfully inserting {_year}")
                except JSONDecodeError:
                    logging.warning(f"The ACS for {_year} is not availabe")
                    continue
            else:
                logging.info(f"data for {_year} is in the database")
                continue
        return self.conn.sql("SELECT * FROM 'DP05Table';").pl()

    def pull_geo(self):
        if not os.path.exists(f"{self.saving_dir}external/cousub.zip"):
            self.pull_file(
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
