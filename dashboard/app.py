from hdfs import InsecureClient
import pandas as pd
import plotly.express as px
import streamlit as st


HDFS_URL = "http://namenode:9870"
HDFS_BASE = "/datalake/analytics/crime"

client = InsecureClient(HDFS_URL, user="root")


def load_hdfs_csv_dir(subdir: str) -> pd.DataFrame:
    hdfs_dir = f"{HDFS_BASE}/{subdir}"
    try:
        files = client.list(hdfs_dir)
    except Exception as e:
        st.error(f"Impossible de lister {hdfs_dir} sur HDFS : {e}")
        return pd.DataFrame()

    dfs = []
    for name in files:
        if not name.endswith(".csv"):
            continue
        path = f"{hdfs_dir}/{name}"
        with client.read(path, encoding="utf-8") as reader:
            dfs.append(pd.read_csv(reader))

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df.columns = [c.strip() for c in df.columns]
    if "AREA NAME" not in df.columns:
        for c in df.columns:
            if c.upper().replace(" ", "") == "AREANAME":
                df = df.rename(columns={c: "AREA NAME"})
                break

    return df


def tab_moyennes_par_zone():
    st.subheader("Moyenne de crimes par zone")

    period = st.sidebar.selectbox(
        "Période d'agrégation",
        ["Jour", "Semaine", "Mois"],
        key="period_select",
    )

    if period == "Jour":
        df = load_hdfs_csv_dir("avg_by_area_day")
        value_col = "avg_crimes_per_day"
        title = "Nombre moyen de crimes par jour et par zone"
    elif period == "Semaine":
        df = load_hdfs_csv_dir("avg_by_area_week")
        value_col = "avg_crimes_per_week"
        title = "Nombre moyen de crimes par semaine et par zone"
    else:
        df = load_hdfs_csv_dir("avg_by_area_month")
        value_col = "avg_crimes_per_month"
        title = "Nombre moyen de crimes par mois et par zone"

    if df.empty:
        st.warning("Aucune donnée trouvée dans HDFS pour cette agrégation.")
        return

    if value_col not in df.columns:
        st.error(f"Colonne {value_col} introuvable dans les données.")
        st.write(df.head())
        return

    zones = sorted(df["AREA NAME"].unique())
    selected_zones = st.multiselect(
        "Zones affichées",
        zones,
        default=zones[:5] if len(zones) > 5 else zones,
        key="zones_moyennes",
    )

    df_filtered = df[df["AREA NAME"].isin(selected_zones)]

    st.markdown(f"### {title}")
    st.dataframe(df_filtered.sort_values(value_col, ascending=False))

    fig = px.bar(
        df_filtered.sort_values(value_col, ascending=False),
        x="AREA NAME",
        y=value_col,
        labels={
            "AREA NAME": "Zone",
            value_col: "Nombre moyen de crimes",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def tab_top5_zones():
    st.subheader("Top 5 zones les plus à risque")

    df = load_hdfs_csv_dir("total_by_area")
    if df.empty:
        st.warning("Aucune donnée trouvée dans HDFS pour total_by_area.")
        return

    if "n_crimes_total" not in df.columns:
        st.error("Colonne n_crimes_total introuvable dans les données.")
        st.write(df.head())
        return

    df_sorted = df.sort_values("n_crimes_total", ascending=False)
    top5 = df_sorted.head(5)

    st.markdown("### Classement des zones par nombre total de crimes")
    st.dataframe(top5)

    fig = px.bar(
        top5,
        x="AREA NAME",
        y="n_crimes_total",
        labels={
            "AREA NAME": "Zone",
            "n_crimes_total": "Nombre total de crimes",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("Criminalité à Los Angeles – Dashboard")

    tab1, tab2 = st.tabs(["Moyennes par zone", "Top 5 zones les plus à risque"])

    with tab1:
        tab_moyennes_par_zone()

    with tab2:
        tab_top5_zones()


if __name__ == "__main__":
    main()
