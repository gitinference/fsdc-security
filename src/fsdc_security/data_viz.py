from .fsdc_security import SecurityData
import altair as alt


class SecurityViz(SecurityData):
    def __init__(
        self,
        saving_dir: str = "data/",
        log_file: str = "data_process.log",
    ):
        super().__init__(saving_dir, log_file)
        self.data = self.calc_security()

    def gen_graph_house(self, year):
        # 1. Safe data filtering to prevent SettingWithCopyWarning
        df = self.data[self.data["year"] == year].copy()

        # Early exit if dataframe is empty for the given year
        if df.empty:
            return (
                alt.Chart()
                .mark_text()
                .properties(title=f"No data available for {year}")
            )

        # 2. Build the production-ready map
        choropleth = (
            alt.Chart(df)
            .mark_geoshape(
                stroke="white",  # Clear borders between regions
                strokeWidth=0.5,
            )
            .encode(
                color=alt.Color(
                    "insecurity_hous:Q",
                    scale=alt.Scale(
                        scheme="viridis"
                    ),  # 'linear' is default; removed redundant arg
                    legend=alt.Legend(
                        title="Food Insecurity Rate",
                        direction="horizontal",
                        orient="bottom",
                        format=".1%",
                        gradientLength=200,  # Cleaner legend sizing
                    ),
                ),
                # Crucial for production: Give the user interactive context
                tooltip=[
                    alt.Tooltip("geoid:N", title="Region ID"),
                    alt.Tooltip(
                        "insecurity_hous:Q", title="Insecurity Rate", format=".1%"
                    ),
                ],
            )
            .project(type="mercator")
            .properties(
                title=f"Total Houses with Food Insecurity ({year})",
                width="container",
                height=300,
            )
            .configure_view(
                strokeWidth=0
            )  # Removes the ugly rectangular chart box around the map
        )

        return choropleth

    def gen_graph_total(self, year):
        # 1. Safe data filtering to prevent SettingWithCopyWarning
        df = self.data[self.data["year"] == year].copy()

        # Early exit if dataframe is empty for the given year
        if df.empty:
            return (
                alt.Chart()
                .mark_text()
                .properties(title=f"No data available for {year}")
            )

        # 2. Build the production-ready map
        chart = (
            alt.Chart(df)
            .mark_geoshape(
                stroke="white",  # Clear boundaries between regions
                strokeWidth=0.5,
            )
            .encode(
                color=alt.Color(
                    "total_insec:Q",
                    scale=alt.Scale(
                        scheme="viridis",
                        type="quantile",  # Great choice for highly skewed population counts
                    ),
                    legend=alt.Legend(
                        title="Number of People",
                        direction="horizontal",
                        orient="bottom",
                        format=",d",  # Formats numbers with commas (e.g., 10,000)
                        gradientLength=200,
                    ),
                ),
                # Production UX: Interactive tooltips for specific numbers
                tooltip=[
                    alt.Tooltip("geoid:N", title="Region ID"),
                    alt.Tooltip("total_insec:Q", title="People Insecure", format=",d"),
                ],
            )
            .project(type="mercator")
            .properties(
                title=f"Amount of People with Food Insecurity ({year})",
                width="container",
                height=300,
            )
            .configure_view(
                strokeWidth=0
            )  # Removes the default bounding box chart border
        )

        return chart
