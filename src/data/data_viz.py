from .data_process import DataClean
import logging
import altair as alt
alt.Scale(type='quantile')


class DataSecurity(DataClean):
    def __init__(
        self,
        saving_dir: str = "data/",
        database_file: str = "data.ddb",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, database_file, log_file)

    def gen_graph(self, var, year, type, title, domain=None, nice=False):
        # define data
        if var not in ["total_insec", "insecurity_hous"]:
            logging.error(f"Invalid variable {var}")
            raise ValueError(f"{var} is not in the available variables")
        df = self.calc_security()
        df = df[df["year"] == year]
        df = df[[var, "geoid", "geometry"]]

        # define choropleth scale
        if "threshold" in type:
            scale = alt.Scale(type=type, domain=domain, scheme="inferno")
        else:
            scale = alt.Scale(type=type, nice=nice, scheme="inferno")

        # define choropleth chart
        choropleth = (
            alt.Chart(df, title=title)
            .mark_geoshape()
            .transform_lookup(
                lookup="geoid", from_=alt.LookupData(data=df, key="geoid", fields=[var])
            )
            .encode(
                alt.Color(
                    f"{var}:Q",
                    scale=scale,
                    legend=alt.Legend(
                        direction="horizontal", orient="bottom", format=".1%"
                    ),
                )
            )
            .project(type="mercator")
            .properties(width="container", height=300)
        )
        return choropleth
