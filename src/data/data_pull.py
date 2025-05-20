from json import JSONDecodeError
from datetime import datetime
import geopandas as gpd
from tqdm import tqdm
from ..models import get_conn, init_dp03_table, init_dp05_table
import polars as pl
import requests
import logging
import ibis
import os


class DataPull:
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        self.saving_dir = saving_dir
        self.data_file = database_file
        # self.conn = ibis.duckdb.connect(f"{self.data_file}")
        self.conn = get_conn(self.data_file)

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

    def pull_file(self, url: str, filename: str, verify: bool = True) -> None:
        """
        Pulls a file from a URL and saves it in the filename. Used by the class to pull external files.

        Parameters
        ----------
        url: str
            The URL to pull the file from.
        filename: str
            The filename to save the file to.
        verify: bool
            If True, verifies the SSL certificate. If False, does not verify the SSL certificate.

        Returns
        -------
        None
        """
        chunk_size = 10 * 1024 * 1024
        logging.info(f"started download {filename}")

        with requests.get(url, stream=True, verify=verify) as response:
            total_size = int(response.headers.get("content-length", 0))

            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Downloading",
            ) as bar:
                with open(filename, "wb") as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(
                                len(chunk)
                            )  # Update the progress bar with the size of the chunk
        logging.info(f"Succefully downloaded {filename}")

    def pull_query(self, params: list, year: int) -> pl.DataFrame:
        # prepare custom census query
        param = ",".join(params)
        base = "https://api.census.gov/data/"
        flow = "/acs/acs5/profile"
        url = f"{base}{year}{flow}?get={param}&for=county%20subdivision:*&in=state:72&in=county:*"
        df = pl.DataFrame(requests.get(url).json())

        # get names from DataFrame
        names = df.select(pl.col("column_0")).transpose()
        names = names.to_dicts().pop()
        names = dict((k, v.lower()) for k, v in names.items())

        # Pivot table
        df = df.drop("column_0").transpose()
        return df.rename(names).with_columns(year=pl.lit(year))

    def pull_dp03(self) -> pl.DataFrame:
        if "DP03Table" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            init_dp03_table(self.data_file)
        for _year in range(2012, datetime.now().year):
            if (
                self.conn.sql(f"SELECT * FROM 'DP03Table' WHERE year={_year}")
                .df()
                .empty
            ):
                try:
                    logging.info(f"pulling {_year} data")
                    tmp = self.pull_query(
                        params=[
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
                        year=_year,
                    )
                    tmp = tmp.rename(
                        {
                            "dp03_0051e": "total_house",
                            "dp03_0052e": "inc_less_10k",
                            "dp03_0053e": "inc_10k_15k",
                            "dp03_0054e": "inc_15k_25k",
                            "dp03_0055e": "inc_25k_35k",
                            "dp03_0056e": "inc_35k_50k",
                            "dp03_0057e": "inc_50k_75k",
                            "dp03_0058e": "inc_75k_100k",
                            "dp03_0059e": "inc_100k_150k",
                            "dp03_0060e": "inc_150k_200k",
                            "dp03_0061e": "inc_more_200k",
                        }
                    )
                    tmp = tmp.with_columns(
                        geoid=pl.col("state")
                        + pl.col("county")
                        + pl.col("county subdivision")
                    ).drop(["state", "county", "county subdivision"])
                    tmp = tmp.with_columns(pl.all().exclude("geoid").cast(pl.Int64))
                    self.conn.sql("INSERT INTO 'DP03Table' BY NAME SELECT * FROM tmp")
                    logging.info(f"succesfully inserting {_year}")
                except JSONDecodeError:
                    logging.warning(f"The ACS for {_year} is not availabe")
                    continue
            else:
                logging.info(f"data for {_year} is in the database")
                continue
        return self.conn.sql("SELECT * FROM 'DP03Table';").pl()

    def pull_dp05(self) -> pl.DataFrame:
        if "DP05Table" not in self.conn.sql("SHOW TABLES;").df().get("name").tolist():
            init_dp05_table(self.data_file)
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
                            "dp05_0001e": "total_pop",
                            "dp05_0004e": "ratio",
                            "dp05_0005e": "under_5_years",
                            "dp05_0006e": "pop_5_9_years",
                            "dp05_0007e": "pop_10_14_years",
                            "dp05_0008e": "pop_15_19_years",
                            "dp05_0009e": "pop_20_24_years",
                            "dp05_0010e": "pop_25_34_years",
                            "dp05_0011e": "pop_35_44_years",
                            "dp05_0012e": "pop_45_54_years",
                            "dp05_0013e": "pop_55_59_years",
                            "dp05_0014e": "pop_60_64_years",
                            "dp05_0015e": "pop_65_74_years",
                            "dp05_0016e": "pop_75_84_years",
                            "dp05_0017e": "over_85_years",
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

    def pull_geo(self) -> ibis.expr.types.relations.Table:
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
