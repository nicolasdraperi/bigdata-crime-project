import pandas as pd
import plotly.express as px
import streamlit as st
from hdfs import InsecureClient

import folium
from streamlit_folium import st_folium
from streamlit_option_menu import option_menu


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

    period = st.selectbox(
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

def tab_evolution_annuelle():
    st.subheader("Évolution du volume de crimes par année")

    mode = st.radio(
        "Mode d'affichage",
        ["Tous crimes confondus", "Par gravité (Part 1-2)"],
        key="mode_evolution",
    )

    # Tous crimes confondus (comme avant)
    if mode == "Tous crimes confondus":
        df_global = load_hdfs_csv_dir("crimes_per_year")
        if df_global.empty:
            st.warning("Aucune donnée trouvée dans HDFS pour crimes_per_year.")
            return

        df_global.columns = [c.strip() for c in df_global.columns]
        if "year_int" not in df_global.columns or "n_crimes" not in df_global.columns:
            st.error("Colonnes year_int ou n_crimes introuvables pour crimes_per_year.")
            st.write(df_global.head())
            return

        df_global_sorted = df_global.sort_values("year_int")

        st.markdown("### Volume de crimes par année – Global (Los Angeles)")
        fig_global = px.line(
            df_global_sorted,
            x="year_int",
            y="n_crimes",
            markers=True,
            labels={
                "year_int": "Année",
                "n_crimes": "Nombre de crimes",
            },
        )
        st.plotly_chart(fig_global, use_container_width=True)

        # Par zone (sans gravité)
        st.markdown("### Volume de crimes par année – par zone")

        df_area = load_hdfs_csv_dir("crimes_per_year_area")
        if df_area.empty:
            st.warning("Aucune donnée trouvée pour crimes_per_year_area.")
            return

        df_area.columns = [c.strip() for c in df_area.columns]
        needed_cols = {"year_int", "AREA NAME", "n_crimes"}
        if not needed_cols.issubset(set(df_area.columns)):
            st.error(f"Colonnes manquantes dans crimes_per_year_area. Attendu: {needed_cols}")
            st.write(df_area.head())
            return

        zones = sorted(df_area["AREA NAME"].unique())
        selected_zones = st.multiselect(
            "Choisissez une ou plusieurs zones",
            zones,
            default=zones[:3] if len(zones) > 3 else zones,
            key="zones_evolution_simple",
        )

        df_area_filtered = df_area[df_area["AREA NAME"].isin(selected_zones)]
        df_area_filtered = df_area_filtered.sort_values(["AREA NAME", "year_int"])

        fig_area = px.line(
            df_area_filtered,
            x="year_int",
            y="n_crimes",
            color="AREA NAME",
            markers=True,
            labels={
                "year_int": "Année",
                "n_crimes": "Nombre de crimes",
                "AREA NAME": "Zone",
            },        )
        st.plotly_chart(fig_area, use_container_width=True)

    # Par gravité (Part 1-2)
    else:
        st.markdown("#### Global – par gravité")

        df_sev_global = load_hdfs_csv_dir("crimes_per_year_severity")
        if df_sev_global.empty:
            st.warning("Aucune donnée trouvée pour crimes_per_year_severity.")
            return

        df_sev_global.columns = [c.strip() for c in df_sev_global.columns]
        needed_cols_global = {"year_int", "part_1_2", "n_crimes"}
        if not needed_cols_global.issubset(set(df_sev_global.columns)):
            st.error(f"Colonnes manquantes dans crimes_per_year_severity. Attendu: {needed_cols_global}")
            st.write(df_sev_global.head())
            return

        df_sev_global = df_sev_global.sort_values(["year_int", "part_1_2"])

        fig_sev_global = px.line(
            df_sev_global,
            x="year_int",
            y="n_crimes",
            color="part_1_2",
            markers=True,
            labels={
                "year_int": "Année",
                "n_crimes": "Nombre de crimes",
                "part_1_2": "Gravité (1 = moins grave, 2 = plus grave)",
            },
        )
        st.plotly_chart(fig_sev_global, use_container_width=True)

        st.markdown("#### Par zone et par gravité")

        df_sev_area = load_hdfs_csv_dir("crimes_per_year_area_severity")
        if df_sev_area.empty:
            st.warning("Aucune donnée trouvée pour crimes_per_year_area_severity.")
            return

        df_sev_area.columns = [c.strip() for c in df_sev_area.columns]
        needed_cols_area = {"year_int", "AREA NAME", "part_1_2", "n_crimes"}
        if not needed_cols_area.issubset(set(df_sev_area.columns)):
            st.error(f"Colonnes manquantes dans crimes_per_year_area_severity. Attendu: {needed_cols_area}")
            st.write(df_sev_area.head())
            return

        zones = sorted(df_sev_area["AREA NAME"].unique())
        selected_zone = st.selectbox(
            "Zone à afficher",
            zones,
            key="zone_evolution_severity",
        )

        df_zone = df_sev_area[df_sev_area["AREA NAME"] == selected_zone]
        df_zone = df_zone.sort_values(["year_int", "part_1_2"])

        fig_zone = px.line(
            df_zone,
            x="year_int",
            y="n_crimes",
            color="part_1_2",
            markers=True,
            labels={
                "year_int": "Année",
                "n_crimes": f"Nombre de crimes ({selected_zone})",
                "part_1_2": "Gravité (1 = moins grave, 2 = plus grave)",
            },
        )
        st.plotly_chart(fig_zone, use_container_width=True)

def tab_heatmap_spatio_temporelle():
    st.subheader("Heatmap mensuelle des crimes par zone")

    df_heat = load_hdfs_csv_dir("crime_heatmap_monthly")
    if df_heat.empty:
        st.warning("Aucune donnée trouvée dans HDFS pour crime_heatmap_monthly.")
        return

    df_heat.columns = [c.strip() for c in df_heat.columns]
    needed_cols = {"year_int", "month", "AREA NAME", "LAT", "LON", "n_crimes"}
    if not needed_cols.issubset(set(df_heat.columns)):
        st.error(f"Colonnes manquantes dans crime_heatmap_monthly. Attendu: {needed_cols}")
        st.write(df_heat.head())
        return

    years = sorted(df_heat["year_int"].dropna().unique())
    year_selected = st.selectbox("Année", years, index=len(years) - 1, key="heat_year")

    df_year = df_heat[df_heat["year_int"] == year_selected]

    months = sorted(df_year["month"].dropna().unique())
    month_selected = st.selectbox("Mois", months, key="heat_month")

    df_month = df_year[df_year["month"] == month_selected]

    zones = sorted(df_month["AREA NAME"].unique())
    selected_zones = st.multiselect(
        "Zones (optionnel, vide = toutes)",
        zones,
        default=[],
        key="heat_zones",
    )

    if selected_zones:
        df_month = df_month[df_month["AREA NAME"].isin(selected_zones)]

    if df_month.empty:
        st.warning("Aucune donnée pour cette combinaison année / mois / zones.")
        return

    heat_data = df_month[["LAT", "LON", "n_crimes"]].dropna().values.tolist()

    st.write(
        f"Nombre de points sur la carte : {len(heat_data)} "
        f"(année {year_selected}, mois {month_selected})"
    )

    m = folium.Map(location=[34.05, -118.24], zoom_start=10)

    from folium.plugins import HeatMap

    HeatMap(
        heat_data,
        radius=10,
        blur=15,
        max_zoom=13,
    ).add_to(m)

    st_folium(m, width=900, height=600)

def tab_high_risk_contexts():
    st.subheader("Scénarios de contexte à haut risque")

    df = load_hdfs_csv_dir("high_risk_contexts")
    if df.empty:
        st.warning("Aucune donnée trouvée dans HDFS pour high_risk_contexts.")
        return

    df.columns = [c.strip() for c in df.columns]

    expected_cols = {
        "AREA NAME",
        "time_slot",
        "place_type",
        "age_range",
        "sex_norm",
        "n_crimes",
        "z_score",
        "risk_level",
    }
    if not expected_cols.issubset(set(df.columns)):
        st.error(f"Colonnes manquantes dans high_risk_contexts. Attendu au minimum : {expected_cols}")
        st.write(df.head())
        return

    st.markdown("Filtrer les scénarios :")

    col1, col2, col3 = st.columns(3)

    with col1:
        zones = sorted(df["AREA NAME"].unique())
        selected_zones = st.multiselect(
            "Zones",
            zones,
            default=[],
            key="hr_zones",
        )

    with col2:
        time_slots = sorted(df["time_slot"].unique())
        selected_time_slots = st.multiselect(
            "Tranches horaires",
            time_slots,
            default=[],
            key="hr_times",
        )

    with col3:
        risk_levels = sorted(df["risk_level"].unique())
        selected_risks = st.multiselect(
            "Niveau de risque",
            risk_levels,
            default=["Très élevé", "Élevé"] if "Très élevé" in risk_levels else risk_levels,
            key="hr_risks",
        )
    col4, col5 = st.columns(2)
    with col4:
        age_ranges = sorted(df["age_range"].unique())
        selected_ages = st.multiselect(
            "Tranches d'âge",
            age_ranges,
            default=[],
            key="hr_ages",
        )

    with col5:
        sexes = sorted(df["sex_norm"].unique())
        selected_sexes = st.multiselect(
            "Sexe de la victime",
            sexes,
            default=[],
            key="hr_sexes",
        )

    df_filtered = df

    if selected_zones:
        df_filtered = df_filtered[df_filtered["AREA NAME"].isin(selected_zones)]
    if selected_time_slots:
        df_filtered = df_filtered[df_filtered["time_slot"].isin(selected_time_slots)]
    if selected_risks:
        df_filtered = df_filtered[df_filtered["risk_level"].isin(selected_risks)]
    if selected_ages:
        df_filtered = df_filtered[df_filtered["age_range"].isin(selected_ages)]
    if selected_sexes:
        df_filtered = df_filtered[df_filtered["sex_norm"].isin(selected_sexes)]

    if df_filtered.empty:
        st.warning("Aucun scénario ne correspond aux filtres sélectionnés.")
        return
    st.markdown("### Résumé")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Nombre de scénarios", len(df_filtered))
    with c2:
        st.metric("Crimes max sur un scénario", int(df_filtered["n_crimes"].max()))
    with c3:
        st.metric("Z-score max", round(float(df_filtered["z_score"].max()), 2))
    st.markdown("### Scénarios à haut risque (triés par score)")
    df_display = df_filtered.sort_values("z_score", ascending=False)
    st.dataframe(df_display)
    st.markdown("### Top scénarios (bar chart)")

    top_n = min(20, len(df_display))
    df_top = df_display.head(top_n).copy()
    df_top["context"] = (
        df_top["AREA NAME"]
        + " | "
        + df_top["time_slot"]
        + " | "
        + df_top["place_type"].str.slice(0, 25)
        + "..."
    )

    fig = px.bar(
        df_top,
        x="context",
        y="z_score",
        hover_data=["n_crimes", "age_range", "sex_norm", "risk_level"],
        labels={
            "context": "Contexte",
            "z_score": "Score de risque (z-score)",
        },
    )
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        """
        **Note méthodo :**  
        Ces scénarios sont identifiés comme à risque élevé car le nombre de crimes
        observés pour cette combinaison (zone × tranche horaire × type de lieu × tranche d’âge × sexe)
        est significativement supérieur à la moyenne (z-score ≥ 2, avec un volume minimal de cas).
        """
    )

def main():
    st.title("Criminalité à Los Angeles – Dashboard")

    with st.sidebar:
        st.markdown("## Navigation")

        page = option_menu(
            None,
            [
                "Moyennes par zone",
                "Top 5 zones les plus à risque",
                "Évolution annuelle",
                "Heatmap spatio-temporelle",
                "Scénarios à haut risque",
            ],
            menu_icon="list",
            default_index=0,
        )

    if page == "Moyennes par zone":
        tab_moyennes_par_zone()
    elif page == "Top 5 zones les plus à risque":
        tab_top5_zones()
    elif page == "Évolution annuelle":
        tab_evolution_annuelle()
    elif page == "Heatmap spatio-temporelle":
        tab_heatmap_spatio_temporelle()
    elif page == "Scénarios à haut risque":
        tab_high_risk_contexts()


if __name__ == "__main__":
    main()
