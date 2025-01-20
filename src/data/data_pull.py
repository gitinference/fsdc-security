from sqlmodel import create_engine
from json import JSONDecodeError
from datetime import datetime
import geopandas as gpd
from tqdm import tqdm
import polars as pl
import requests
import logging
import ibis
import os

class DataPull:

    def __init__(self, database_url:str='sqlite:///db.sqlite', saving_dir:str='data/',
                        update:bool=False, debug:bool=False, dev:bool=False) -> None:

        self.debug = debug
        self.saving_dir = saving_dir
        logging.basicConfig(level=logging.INFO)

        # Check if the saving directory exists
        if not os.path.exists(self.saving_dir + "raw"):
            os.makedirs(self.saving_dir + "raw")
            logging.info(f"created the raw folder in {self.saving_dir}")
        if not os.path.exists(self.saving_dir + "processed"):
            os.makedirs(self.saving_dir + "processed")
            logging.info(f"created the processed folder in {self.saving_dir}/processed")
        if not os.path.exists(self.saving_dir + "external"):
            os.makedirs(self.saving_dir + "external")
            logging.info(f"created the external folder in {self.saving_dir}/external")
 
        self.database_url = database_url
        self.engine = create_engine(self.database_url)
        self.saving_dir = saving_dir
        self.debug = debug
        self.dev = dev
        self.update = update

        if self.database_url.startswith("sqlite"):
            self.conn = ibis.sqlite.connect(self.database_url.replace("sqlite:///", ""))
        elif self.database_url.startswith("postgres"):
            self.conn = ibis.postgres.connect(
                user=self.database_url.split("://")[1].split(":")[0],
                password=self.database_url.split("://")[1].split(":")[1].split("@")[0],
                host=self.database_url.split("://")[1].split(":")[1].split("@")[1],
                port=self.database_url.split("://")[1].split(":")[2].split("/")[0],
                database=self.database_url.split("://")[1].split(":")[2].split("/")[1])
        else:
            raise Exception("Database url is not supported")

    def pull_file(self, url:str, filename:str, verify:bool=True) -> None:
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
            total_size = int(response.headers.get('content-length', 0))

            with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024, desc='Downloading') as bar:
                with open(filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))  # Update the progress bar with the size of the chunk
        logging.info(f"Succefully downloaded {filename}")

    def pull_query(self, params:list, year:int) -> pl.DataFrame:

        # prepare custom census query
        param = ",".join(params)
        base = "https://api.census.gov/data/"
        flow = "/acs/acs1/pumspr"
        url = f'{base}{year}{flow}?get={param}'
        df = pl.DataFrame(requests.get(url).json())

        # get names from DataFrame
        names = df.select(pl.col("column_0")).transpose()
        names = names.to_dicts().pop()
        names = dict((k, v.lower()) for k,v in names.items())

        # Pivot table
        df = df.drop("column_0").transpose()
        return df.rename(names).with_columns(year=pl.lit(year))

    def pull_pums(self) -> ibis.expr.types.relations.Table:
        df = self.conn.table("pumstable")
        for _year in range(2021, datetime.now().year):
            if df.filter(df.year == _year).to_pandas().empty and _year != 2020:
                try:
                    logging.info(f"pulling {_year} data")
                    tmp = self.pull_query(params=['ADJINC', 'HINCP', 'PWGTP'], year=_year)
                    tmp = tmp.with_columns(
                        pl.col("adjinc").cast(pl.Float64).round(4),
                        pl.col("hincp").cast(pl.Int64),
                        pl.col("pwgtp").cast(pl.Int32))
                    self.conn.insert("pumstable", tmp)
                    logging.info(f"succesfully inserting {_year}")
                except JSONDecodeError:
                    logging.warning(f"The ACS for {_year} is not availabel")
                    continue
            else:
                logging.info(f"data for {_year} is in the database")
                continue
        return self.conn.table("pumstable")

    def pull_shape(self) -> ibis.expr.types.relations.Table:
        if not os.path.exists(f"{self.saving_dir}external/cousub.zip"):
            self.pull_file(url="https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_72_cousub.zip", filename=f"{self.saving_dir}external/cousub.zip")
        gdf = self.conn.table("geotable")
        if gdf.to_pandas().empty:
            logging.info(f"The GeoTable is empty inserting {self.saving_dir}external/cousub.zip")
            tmp = gpd.read_file(f"{self.saving_dir}external/cousub.zip")
            tmp = tmp[["GEOID","NAME","geometry"]].rename(columns={"GEOID":"geoid", "NAME":"name"})
            tmp.to_postgis("geotable",  self.engine, if_exists="append")
            logging.info("Succefully inserting data to database")
        return self.conn.table("geotable")

