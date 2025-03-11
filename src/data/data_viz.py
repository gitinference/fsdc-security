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
        self.data = self.calc_security()

    def gen_graph_house(self, year):
        df = self.data
        df = df[df["year"] == year]
        df = df[["geoid", "insecurity_hous", "geometry"]]

        choropleth = (
            alt.Chart(df, title="something")
            .mark_geoshape()
            .transform_lookup(
                lookup="geoid",
                from_=alt.LookupData(data=df, key="geoid", fields=["insecurity_hous"]),
            )
            .encode(
                alt.Color(
                    "insecurity_hous:Q",
                    scale=alt.Scale(type="linear", scheme="viridis"),
                    legend=alt.Legend(
                        direction="horizontal", orient="bottom", format=".1%"
                    ),
                )
            )
            .project(type="mercator")
            .properties(width="container", height=300)
        )
        return choropleth

    def gen_graph_total(self, year):
        df = self.data
        df = df[df["year"] == year]
        df = df[["total_insec", "geoid", "geometry"]]
        quant = df["total_insec"]
        domain = [
            quant.min(),
            quant.quantile(1 / 7),
            quant.quantile(2 / 7),
            quant.quantile(3 / 7),
            quant.quantile(4 / 7),
            quant.quantile(5 / 7),
            quant.quantile(6 / 7),
            quant.max(),
        ]

        scale = alt.Scale(domain=domain, scheme="viridis")
        chart = (
            alt.Chart(df, title="something")
            .mark_geoshape()
            .transform_lookup(
                lookup="geoid",
                from_=alt.LookupData(data=df, key="geoid", fields=["total_insec"]),
            )
            .encode(
                alt.Color(
                    "total_insec:Q",
                    scale=scale,
                    legend=alt.Legend(direction="horizontal", orient="bottom"),
                )
            )
            .project(type="mercator")
            .properties(width="container", height=300)
        )
        return chart
