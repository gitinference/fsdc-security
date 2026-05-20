from CensusForge.CensusForge import CensusAPI
import polars as pl
from .utils import SecurityUtils

import geopandas as gpd
from shapely import wkt


class SecurityData(SecurityUtils):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def calc_security(self) -> gpd.GeoDataFrame:
        df = self.pull_dp03()
        df = df.with_columns(
            inc_less_15k=pl.col("inc_less_10k") + pl.col("inc_10k_15k"),
            inc_more_35k=pl.col("inc_35k_50k")
            + pl.col("inc_50k_75k")
            + pl.col("inc_75k_100k")
            + pl.col("inc_100k_150k")
            + pl.col("inc_150k_200k")
            + pl.col("inc_more_200k"),
        )

        df = df.select(
            [
                "year",
                "geoid",
                "total_house",
                "inc_less_15k",
                "inc_15k_25k",
                "inc_25k_35k",
                "inc_more_35k",
            ]
        )

        df = df.with_columns(
            p_inc_less_15k=pl.col("inc_less_15k") / pl.col("total_house"),
            p_inc_15_25k=pl.col("inc_15k_25k") / pl.col("total_house"),
            p_inc_25k_35k=pl.col("inc_25k_35k") / pl.col("total_house"),
            p_inc_more_35k=pl.col("inc_more_35k") / pl.col("total_house"),
        )
        df = df.with_columns(
            insec_less_15k=pl.col("inc_less_15k") * 57 / 100,
            insec_15k_25k=pl.col("inc_15k_25k") * 29 / 100,
            insec_25k_35k=pl.col("inc_25k_35k") * 66 / 1000,
            insec_more_35k=pl.col("inc_more_35k") * 75 / 1000,
        )
        df = df.with_columns(
            total_insec=pl.col("insec_less_15k")
            + pl.col("insec_15k_25k")
            + pl.col("insec_25k_35k")
            + pl.col("insec_more_35k")
        )

        df = df.with_columns(
            insecurity_hous=pl.col("total_insec") / pl.col("total_house")
        )
        gdf = CensusAPI().pull_geos(
            url="https://www2.census.gov/geo/tiger/TIGER2024/COUSUB/tl_2024_72_cousub.zip",
            filename=f"{self.saving_dir}external/cousub.parquet",
        )
        gdf = gdf.rename(columns={"GEOID": "geoid", "NAME": "name"})
        gdf = gdf[~gdf["name"].str.contains("not defined")]

        security_df = gdf.join(
            df.to_pandas().set_index("geoid"), on="geoid", how="inner", validate="1:m"
        ).reset_index(drop=True)
        return gpd.GeoDataFrame(security_df, geometry="geometry")
