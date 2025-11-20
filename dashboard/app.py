from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from hdfs import InsecureClient

HDFS_URL = "http://namenode:9870"
HDFS_BASE = "/datalake/analytics/crime"

client = InsecureClient(HDFS_URL, user="root")


def load_hdfs_csv_dir(subdir: str) -> pd.DataFrame:
    hdfs_dir = f"{HDFS_BASE}/{subdir}"
    try:
        files = client.list(hdfs_dir)
    except Exception as e:
        st.error(f"Cannot list HDFS directory {hdfs_dir}: {e}")
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


def main():
    st.title("Crimes à Los Angeles – Moyenne de crimes par zone")

    period = st.sidebar.selectbox(
        "Période d'agrégation",
        ["Jour", "Semaine", "Mois"],
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
        st.warning("Aucune donnée trouvée dans HDFS. Vérifie que les agrégations ont été générées.")
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
    )

    df_filtered = df[df["AREA NAME"].isin(selected_zones)]

    st.subheader(title)
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


if __name__ == "__main__":
    main()
