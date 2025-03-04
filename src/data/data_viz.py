from .data_process import DataClean
import altair as alt


class DataSecurity(DataClean):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def gen_graph(self):
        df = self.calc_security()
        chart = (
            alt.Chart(df)
            .mark_geoshape()
            .encode(color=alt.Color("insecurity_hous:Q").scale(scheme="viridis"))
            .transform_lookup(
                lookup="geoid", from_=alt.LookupData(df, "geoid", ["insecurity_hous"])
            )
            .project(type="mercator")
            .properties(width="container", height=300)
        )
        return chart
