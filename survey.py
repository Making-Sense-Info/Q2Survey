import marimo

__generated_with = "0.9.16"
app = marimo.App(width="medium", layout_file="layouts/survey.slides.json")


@app.cell
def __():
    import marimo as mo
    import polars as pl
    import altair as alt
    import random
    import datetime
    return alt, datetime, mo, pl, random


@app.cell
def __(mo):
    # Various constants
    TECHNICAL_COLUMNS = ["StartDate", "EndDate", "Status", "Progress", "Duration (in seconds)", "Finished", "RecordedDate", "ResponseId", "DistributionChannel", "UserLanguage", "GDPR"]

    # Helpers
    def list_to_md(list_of_things, title=None):
        text = f"## {title} \n" if title is not None else ""
        for thing in list_of_things:
            text += f" - {thing} \n"
        return mo.md(text)
    return TECHNICAL_COLUMNS, list_to_md


@app.cell
def __(mo):
    mo.md(r"""# Analysing Q2 group 2024 survey""")
    return


@app.cell
def __(datetime, pl, random):
    survey_start = datetime.datetime(2024, 10, 15)

    df = pl.read_csv("survey-results.csv").filter(
        pl.col("Status") != '{"ImportId":"status"}').filter(
        pl.col("StartDate").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False) >= survey_start
    )
    random.seed(1234) # to get the same lines every run
    df.sample(50)
    return df, survey_start


@app.cell
def __(mo):
    mo.md(r"""To get a more advanced feeling:""")
    return


@app.cell
def __(TECHNICAL_COLUMNS, df, pl):
    df.select(pl.all().exclude(TECHNICAL_COLUMNS)).describe()
    return


@app.cell
def __(df, pl):
    answers = len(df)
    finished = len(df.filter(pl.col("Finished") == "True"))
    return answers, finished


@app.cell
def __(answers, mo):
    mo.md(f"""## Total answers: {answers}""")
    return


@app.cell
def __(alt, answers, finished, pl):
    ddd = pl.DataFrame({"Status": ["Unfinished","Finished"],"y": [answers-finished, finished]})
    base = alt.Chart(ddd, title="Finished questionnaires ratio").mark_arc().encode(theta="y", color="Status")
    pie = base.mark_arc(outerRadius=120)
    text = base.mark_text(radius=140, size=20).encode(text="y:N")
    pie + text
    return base, ddd, pie, text


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df)
        .transform_aggregate(count="count()", groupby=["b1\.q2"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b1\.q2", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b1\.q2", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Research Area", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df)
        .transform_aggregate(count="count()", groupby=["b1\.q2_8_TEXT"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b1\.q2_8_TEXT", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b1\.q2_8_TEXT", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Other research areas", width="container")
    )
    _chart
    return


@app.cell
def __(df, mo, pl):
    insts = df.select(
        pl.col("b1.q3").alias("Institution")
    ).filter(
        pl.col("Institution").is_not_null()
    )

    mo.ui.table(insts)
    return (insts,)


@app.cell
def __(df, pl):
    df.select(
        pl.col("b1.q4").str.to_lowercase().alias("Country")
    ).filter(
        pl.col("Country").is_not_null()
    ).group_by("Country").agg(
        pl.len().alias("count")
    ).sort("count", descending=True)
    return


@app.cell
def __(alt, df, pl):
    df_ddi_kl = df.select(
        pl.col("b2.q1_1").alias("Codebook"),
        pl.col("b2.q1_2").alias("Lifecycle"),
        pl.col("b2.q1_3").alias("CDI"),
    ).unpivot(["Codebook", "Lifecycle", "CDI"]).group_by(
        "variable", "value"
    ).len().sort("variable")

    df_ddi_kl

    alt.Chart(df_ddi_kl, title="Skills and knowledge for DDI products").mark_bar().encode(
        x="value:O",
        y="len:Q",
        color="value",
        column="variable"
    )
    return (df_ddi_kl,)


@app.cell
def __(df, mo, pl):
    mo.md("## Products used")
    df_pu = df.select(
        pl.col("b2.q2").alias("Products")
    ).group_by(
        "Products"
    ).len().sort("Products", descending=True)
    mo.ui.table(df_pu)
    return (df_pu,)


@app.cell
def __(df, pl):
    def mark(col_name, thing):
        return pl.when(
            pl.col(col_name).str.contains(thing)
        ).then(1).otherwise(0).alias(thing)

    def markb2q3(thing):
        return mark("b2.q3", thing)

    df.select(
        pl.col("b2.q3").alias("Lifecycle"),
        markb2q3("Concept"),
        markb2q3("Collection"),
        markb2q3("Processing"),
        markb2q3("Distribution"),
        markb2q3("Discovery"),
        markb2q3("Analysis"),
        markb2q3("Repurposing"),
        markb2q3("Archiving")
    ).unpivot(
        ["Concept", "Collection", "Processing", "Distribution", "Discovery", "Analysis", "Repurposing", "Archiving"]
    ).group_by(
        "variable"
    ).agg(
        pl.col("value").sum()
    ).sort("value", descending=True)
    return mark, markb2q3


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df, title="Datasets...")
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q4_1", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df, title="Variables...")
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q4_2", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df, title="Concepts...")
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q4_3", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df, title="Questions wording...")
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q4_4", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df, title="Responses & code lists...")
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q4_5", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df)
        .transform_aggregate(count="count()", groupby=["b2\.q5"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b2\.q5", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q5", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Questionnaire documentation with...", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b2.q6").is_not_null()))
        .transform_aggregate(count="count()", groupby=["b2\.q6"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b2\.q6", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q6", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Survey tools", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df)
        .transform_aggregate(count="count()", groupby=["b2\.q6_7_TEXT"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b2\.q6_7_TEXT", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q6_7_TEXT", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Other survey tools", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b2.q7").is_not_null()))
        .transform_aggregate(count="count()", groupby=["b2\.q7"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b2\.q7", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q7", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Documentation tools", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b2.q7_4_TEXT").is_not_null()))
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q7_4_TEXT", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(title="Internal documentation tools",width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b2.q7_5_TEXT").is_not_null()))
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q7_5_TEXT", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(title="Other documentation tools ", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df):
    _chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q10", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(title="DDI satisfaction", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b2.q11").is_not_null()))
        .transform_aggregate(count="count()", groupby=["b2\.q11"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b2\.q11", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b2\.q11", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Why?", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.select(pl.col("b3.q1").alias("b3q1"))
                  .filter(pl.col("b3q1").is_not_null()))
        .mark_bar()
        .encode(
            y=alt.Y("b3q1", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(
            title="Are you planning to document your questionnaires in DDI?", 
            width="container"
        )
    )
    _chart
    return


@app.cell
def __(df, list_to_md, pl):
    qddiyes = df.filter(
        pl.col("b3.q1") == "Yes"
    ).select(
        pl.col("b3.q2")
    ).filter(
        pl.col("b3.q2").is_not_null()
    ).to_series()

    list_to_md(qddiyes, "If _yes_, why?")
    return (qddiyes,)


@app.cell
def __(df, list_to_md, pl):
    qddino = df.filter(
        pl.col("b3.q1") == "No"
    ).select(
        pl.col("b3.q2")
    ).filter(
        pl.col("b3.q2").is_not_null()
    ).to_series()

    list_to_md(qddino, "If _no_, why?")
    return (qddino,)


@app.cell
def __(df, list_to_md, pl):
    qddiimprov = df.select(
        pl.col("b3.q3")
    ).filter(pl.col("b3.q3").is_not_null()).to_series()

    list_to_md(qddiimprov, "DDI improvements for questionnaire")
    return (qddiimprov,)


@app.cell
def __(df, mo, pl):
    training = df.select("b4.q1").filter(pl.col("b4.q1").is_not_null()).group_by("b4.q1").len()

    mo.ui.table(training)
    return (training,)


@app.cell
def __(df, list_to_md, pl):
    list_to_md(
        df.select("b4.q2").filter(pl.col("b4.q2").is_not_null()).to_series(),
        "What training?"
    )
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b4.q3").is_not_null()))
        .mark_bar()
        .encode(
            y=alt.Y("b4\.q3", type="nominal"),
            x=alt.X("count()", type="quantitative"),
        )
        .properties(width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b4.q4").is_not_null()))
        .transform_aggregate(count="count()", groupby=["b4\.q4"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b4\.q4", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b4\.q4", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Top 10 b4\.q4", width="container")
    )
    _chart
    return


@app.cell
def __(alt, df, pl):
    _chart = (
        alt.Chart(df.filter(pl.col("b4.q5").is_not_null()))
        .transform_aggregate(count="count()", groupby=["b4\.q5"])
        .transform_window(
            rank="rank()",
            sort=[
                alt.SortField("count", order="descending"),
                alt.SortField("b4\.q5", order="ascending"),
            ],
        )
        .transform_filter(alt.datum.rank <= 10)
        .mark_bar()
        .encode(
            y=alt.Y("b4\.q5", type="nominal", sort="-x"),
            x=alt.X("count", type="quantitative"),
        )
        .properties(title="Top 10 b4\.q5", width="container")
    )
    _chart
    return


if __name__ == "__main__":
    app.run()
