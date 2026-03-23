
import re
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import MarkerCluster

st.set_page_config(
    page_title="BI Comunicação Plano Fixo",
    page_icon="📊",
    layout="wide",
)

DATA_DIR = Path(__file__).parent

PLAN_ID_MAP = {
    1: "Trimestral com informe Mensal",
    2: "Trimestral com informe Trimestral",
    3: "Semestral com informe Mensal",
    4: "Semestral com informe Mensal com Bandeira",
    6: "Semestral com informe Trimestral",
    7: "Semestral com informe Semestral",
    8: "Semestral com informe Semestral Rural",
    9: "Anual com informe Mensal",
    10: "Anual com informe Trimestral",
    11: "Anual com informe Semestral",
    12: "Anual com informe Semestral Rural",
}

PLAN_ATTRIBUTES = {
    1: {"prazo": "Trimestral", "informe": "Mensal", "com_bandeira": "Não", "rural": "Não"},
    2: {"prazo": "Trimestral", "informe": "Trimestral", "com_bandeira": "Não", "rural": "Não"},
    3: {"prazo": "Semestral", "informe": "Mensal", "com_bandeira": "Não", "rural": "Não"},
    4: {"prazo": "Semestral", "informe": "Mensal", "com_bandeira": "Sim", "rural": "Não"},
    6: {"prazo": "Semestral", "informe": "Trimestral", "com_bandeira": "Não", "rural": "Não"},
    7: {"prazo": "Semestral", "informe": "Semestral", "com_bandeira": "Não", "rural": "Não"},
    8: {"prazo": "Semestral", "informe": "Semestral", "com_bandeira": "Não", "rural": "Sim"},
    9: {"prazo": "Anual", "informe": "Mensal", "com_bandeira": "Não", "rural": "Não"},
    10: {"prazo": "Anual", "informe": "Trimestral", "com_bandeira": "Não", "rural": "Não"},
    11: {"prazo": "Anual", "informe": "Semestral", "com_bandeira": "Não", "rural": "Não"},
    12: {"prazo": "Anual", "informe": "Semestral", "com_bandeira": "Não", "rural": "Sim"},
}


def parse_coord(value: str):
    if pd.isna(value):
        return np.nan, np.nan
    text = str(value).strip()
    nums = re.findall(r"-?\d+(?:[.,]\d+)?", text)
    if len(nums) < 2:
        return np.nan, np.nan
    lat = float(nums[0].replace(",", "."))
    lon = float(nums[1].replace(",", "."))
    return lat, lon


@st.cache_data(show_spinner=False)
def load_data():
    interessados = pd.read_csv(DATA_DIR / "df_interessados.csv")
    email = pd.read_csv(DATA_DIR / "df_COM_EMAIL.csv")
    im = pd.read_csv(DATA_DIR / "df_COM_IM.csv")

    interessados["DTH_INTERESSE"] = pd.to_datetime(interessados["DTH_INTERESSE"], errors="coerce")
    interessados["DT_PRIM_CTT_IM"] = pd.to_datetime(interessados["DT_PRIM_CTT_IM"], errors="coerce")
    interessados["DT_PRIM_CTT_EMAIL"] = pd.to_datetime(interessados["DT_PRIM_CTT_EMAIL"], errors="coerce")
    interessados["ULT_CTT_DT_IM"] = pd.to_datetime(interessados["ULT_CTT_DT_IM"], errors="coerce")
    interessados["ULT_CTT_DT_EMAIL"] = pd.to_datetime(interessados["ULT_CTT_DT_EMAIL"], errors="coerce")

    coords = interessados["COORDENADA GEOGRAFICA"].apply(parse_coord)
    interessados["lat"] = [c[0] for c in coords]
    interessados["lon"] = [c[1] for c in coords]

    interessados["ID_PLANO"] = pd.to_numeric(interessados["ID_PLANO"], errors="coerce").astype("Int64")
    interessados["plano_nome"] = interessados["ID_PLANO"].map(PLAN_ID_MAP)

    interessados["prazo_plano"] = interessados["ID_PLANO"].map(
        lambda x: PLAN_ATTRIBUTES.get(int(x), {}).get("prazo") if pd.notna(x) else np.nan
    )
    interessados["informe"] = interessados["ID_PLANO"].map(
        lambda x: PLAN_ATTRIBUTES.get(int(x), {}).get("informe") if pd.notna(x) else np.nan
    )
    interessados["com_bandeira"] = interessados["ID_PLANO"].map(
        lambda x: PLAN_ATTRIBUTES.get(int(x), {}).get("com_bandeira") if pd.notna(x) else np.nan
    )
    interessados["rural"] = interessados["ID_PLANO"].map(
        lambda x: PLAN_ATTRIBUTES.get(int(x), {}).get("rural") if pd.notna(x) else np.nan
    )

    email["DataEnvio"] = pd.to_datetime(email["DataEnvio"], errors="coerce")
    email["UC"] = pd.to_numeric(email["UC"], errors="coerce").astype("Int64")
    email["canal"] = "E-mail"
    email["comunicacao"] = email["Ação"].fillna("Sem ação")
    email["data_evento"] = email["DataEnvio"]
    email["uc_join"] = email["UC"]

    im["DATA_ENVIO"] = pd.to_datetime(im["DATA_ENVIO"], errors="coerce")
    im["NUMCDC"] = pd.to_numeric(im["NUMCDC"], errors="coerce").astype("Int64")
    im["canal"] = "IM"
    im["comunicacao"] = im["TEMPLATE"].fillna("Sem template")
    im["data_evento"] = im["DATA_ENVIO"]
    im["uc_join"] = im["NUMCDC"]

    uc_dims = interessados[
        ["NUM_UC", "MUNICIPIO", "ID_PLANO", "plano_nome", "prazo_plano", "informe", "com_bandeira", "rural", "lat", "lon"]
    ].drop_duplicates("NUM_UC").rename(columns={"NUM_UC": "uc_join"})

    comm_cols = ["canal", "comunicacao", "data_evento", "uc_join"]
    comunicacoes = pd.concat(
        [
            email[comm_cols],
            im[comm_cols],
        ],
        ignore_index=True,
    )
    comunicacoes = comunicacoes.merge(uc_dims, on="uc_join", how="left")
    comunicacoes["tem_dimensao_uc"] = comunicacoes["MUNICIPIO"].notna()

    return interessados, email, im, comunicacoes


def filter_interessados(
    df,
    municipios,
    prazos,
    informes,
    bandeiras,
    rural_sel,
    planos,
    status_sel,
    data_inicio,
    data_fim,
):
    out = df.copy()

    if municipios:
        out = out[out["MUNICIPIO"].isin(municipios)]
    if prazos:
        out = out[out["prazo_plano"].isin(prazos)]
    if informes:
        out = out[out["informe"].isin(informes)]
    if bandeiras:
        out = out[out["com_bandeira"].isin(bandeiras)]
    if rural_sel:
        out = out[out["rural"].isin(rural_sel)]
    if planos:
        out = out[out["plano_nome"].isin(planos)]
    if status_sel:
        out = out[out["IND_SITUACAO"].isin(status_sel)]
    if data_inicio is not None:
        out = out[out["DTH_INTERESSE"] >= pd.Timestamp(data_inicio)]
    if data_fim is not None:
        out = out[out["DTH_INTERESSE"] <= pd.Timestamp(data_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    return out


def filter_comunicacoes(
    df,
    municipios,
    prazos,
    informes,
    bandeiras,
    rural_sel,
    planos,
    canais,
    comunicacoes_sel,
    data_inicio,
    data_fim,
):
    out = df.copy()

    if municipios:
        out = out[out["MUNICIPIO"].isin(municipios)]
    if prazos:
        out = out[out["prazo_plano"].isin(prazos)]
    if informes:
        out = out[out["informe"].isin(informes)]
    if bandeiras:
        out = out[out["com_bandeira"].isin(bandeiras)]
    if rural_sel:
        out = out[out["rural"].isin(rural_sel)]
    if planos:
        out = out[out["plano_nome"].isin(planos)]
    if canais:
        out = out[out["canal"].isin(canais)]
    if comunicacoes_sel:
        out = out[out["comunicacao"].isin(comunicacoes_sel)]
    if data_inicio is not None:
        out = out[out["data_evento"] >= pd.Timestamp(data_inicio)]
    if data_fim is not None:
        out = out[out["data_evento"] <= pd.Timestamp(data_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    return out


def cumulative_interest(df):
    base = df.dropna(subset=["DTH_INTERESSE"]).copy()
    if base.empty:
        return pd.DataFrame(columns=["Data", "Interesses do dia", "Acumulado"])
    daily = (
        base.groupby(base["DTH_INTERESSE"].dt.date)
        .size()
        .rename("Interesses do dia")
        .reset_index()
        .rename(columns={"DTH_INTERESSE": "Data"})
    )
    daily["Data"] = pd.to_datetime(daily["Data"])
    all_days = pd.DataFrame({"Data": pd.date_range(daily["Data"].min(), daily["Data"].max(), freq="D")})
    daily = all_days.merge(daily, on="Data", how="left").fillna({"Interesses do dia": 0})
    daily["Interesses do dia"] = daily["Interesses do dia"].astype(int)
    daily["Acumulado"] = daily["Interesses do dia"].cumsum()
    return daily


def cumulative_comms(df):
    base = df.dropna(subset=["data_evento"]).copy()
    if base.empty:
        return pd.DataFrame(columns=["Data", "Comunicações do dia", "Acumulado"])
    daily = (
        base.groupby(base["data_evento"].dt.date)
        .size()
        .rename("Comunicações do dia")
        .reset_index()
        .rename(columns={"data_evento": "Data"})
    )
    daily["Data"] = pd.to_datetime(daily["Data"])
    all_days = pd.DataFrame({"Data": pd.date_range(daily["Data"].min(), daily["Data"].max(), freq="D")})
    daily = all_days.merge(daily, on="Data", how="left").fillna({"Comunicações do dia": 0})
    daily["Comunicações do dia"] = daily["Comunicações do dia"].astype(int)
    daily["Acumulado"] = daily["Comunicações do dia"].cumsum()
    return daily


def make_municipio_map(df_municipio):
    valid = df_municipio.dropna(subset=["lat", "lon"]).copy()
    if valid.empty:
        return None

    center_lat = valid["lat"].mean()
    center_lon = valid["lon"].mean()

    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=6, tiles="cartodbpositron")
    cluster = MarkerCluster().add_to(fmap)

    for _, row in valid.iterrows():
        popup = folium.Popup(
            f"""
            <b>{row['MUNICIPIO']}</b><br>
            Interesses: {int(row['interesses'])}<br>
            UCs únicas: {int(row['ucs'])}<br>
            Plano líder: {row['plano_top']}
            """,
            max_width=280,
        )
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=max(6, min(18, np.sqrt(row["interesses"]) + 4)),
            popup=popup,
            tooltip=row["MUNICIPIO"],
            fill=True,
            fill_opacity=0.75,
            weight=1,
        ).add_to(cluster)
    return fmap


interessados, email, im, comunicacoes = load_data()

st.title("BI de Comunicação — Plano Fixo")
st.caption("Filtros por município, plano, prazo, informe, bandeira, rural, template/ação e gráficos acumulados.")

# Initialize municipality in session state
if "map_selected_municipio" not in st.session_state:
    st.session_state["map_selected_municipio"] = None

with st.sidebar:
    st.header("Filtros")

    all_municipios = sorted([m for m in interessados["MUNICIPIO"].dropna().unique().tolist()])
    municipio_options = ["Todos"] + all_municipios
    map_choice = st.selectbox(
        "Município selecionado no mapa",
        municipio_options,
        index=0 if not st.session_state["map_selected_municipio"] else municipio_options.index(st.session_state["map_selected_municipio"]) if st.session_state["map_selected_municipio"] in municipio_options else 0,
        help="Clique em um marcador do mapa e depois escolha aqui para aplicar o filtro.",
    )

    if map_choice == "Todos":
        default_municipios = []
    else:
        default_municipios = [map_choice]

    municipios = st.multiselect(
        "Município",
        all_municipios,
        default=default_municipios,
        help="Você também pode usar o mapa para decidir o município."
    )

    prazos = st.multiselect("Prazo do plano", ["Anual", "Semestral", "Trimestral"])
    informes = st.multiselect("Informe", ["Mensal", "Trimestral", "Semestral"])
    bandeiras = st.multiselect("Com bandeira", ["Sim", "Não"])
    rural_sel = st.multiselect("Rural", ["Sim", "Não"])
    planos = st.multiselect("Plano detalhado", sorted([v for v in PLAN_ID_MAP.values()]))

    status_sel = st.multiselect(
        "Status em df_interessados",
        sorted([s for s in interessados["IND_SITUACAO"].dropna().unique().tolist()])
    )

    canais = st.multiselect("Canal de comunicação", ["E-mail", "IM"])

    comm_options = sorted(comunicacoes["comunicacao"].dropna().unique().tolist())
    comunicacoes_sel = st.multiselect("Template / Ação", comm_options)

    min_interest = interessados["DTH_INTERESSE"].min()
    max_interest = interessados["DTH_INTERESSE"].max()
    start_default = min_interest.date() if pd.notna(min_interest) else None
    end_default = max_interest.date() if pd.notna(max_interest) else None

    date_range = st.date_input(
        "Período de interesse",
        value=(start_default, end_default) if start_default and end_default else (),
    )

    data_inicio, data_fim = None, None
    if isinstance(date_range, tuple) and len(date_range) == 2:
        data_inicio, data_fim = date_range

    st.markdown("---")
    st.caption("Observação: município/plano/informe nos gráficos de comunicações dependem do vínculo por UC com `df_interessados`.")

filtered_interessados = filter_interessados(
    interessados, municipios, prazos, informes, bandeiras, rural_sel, planos, status_sel, data_inicio, data_fim
)
filtered_comms = filter_comunicacoes(
    comunicacoes, municipios, prazos, informes, bandeiras, rural_sel, planos, canais, comunicacoes_sel, data_inicio, data_fim
)

# Municipality summary for map
map_base = filter_interessados(
    interessados, [], prazos, informes, bandeiras, rural_sel, planos, status_sel, data_inicio, data_fim
)
municipio_summary = (
    map_base.dropna(subset=["MUNICIPIO"])
    .groupby("MUNICIPIO", as_index=False)
    .agg(
        interesses=("NUM_UC", "size"),
        ucs=("NUM_UC", "nunique"),
        lat=("lat", "median"),
        lon=("lon", "median"),
    )
)
top_plans = (
    map_base.dropna(subset=["MUNICIPIO", "plano_nome"])
    .groupby(["MUNICIPIO", "plano_nome"])
    .size()
    .reset_index(name="n")
    .sort_values(["MUNICIPIO", "n"], ascending=[True, False])
    .drop_duplicates("MUNICIPIO")
    .rename(columns={"plano_nome": "plano_top"})
)
municipio_summary = municipio_summary.merge(top_plans[["MUNICIPIO", "plano_top"]], on="MUNICIPIO", how="left")
municipio_summary["plano_top"] = municipio_summary["plano_top"].fillna("N/A")

col_map, col_kpi = st.columns([1.35, 1])

with col_map:
    st.subheader("Mapa por município")
    fmap = make_municipio_map(municipio_summary)
    if fmap is not None:
        map_data = st_folium(fmap, height=500, width=None)
        last_obj = map_data.get("last_object_clicked_tooltip") if isinstance(map_data, dict) else None
        if last_obj:
            st.session_state["map_selected_municipio"] = last_obj
            st.info(f"Município clicado no mapa: {last_obj}. Selecione-o no filtro lateral para aplicar.")
    else:
        st.warning("Não há coordenadas suficientes para desenhar o mapa com os filtros atuais.")

with col_kpi:
    st.subheader("Resumo")
    total_interesses = len(filtered_interessados)
    ucs_interesses = filtered_interessados["NUM_UC"].nunique()
    total_comms = len(filtered_comms)
    ucs_comms = filtered_comms["uc_join"].nunique()
    covered_rate = (filtered_comms["tem_dimensao_uc"].mean() * 100) if len(filtered_comms) else 0

    k1, k2 = st.columns(2)
    k1.metric("Interesses", f"{total_interesses:,}".replace(",", "."))
    k2.metric("UCs interessadas", f"{ucs_interesses:,}".replace(",", "."))

    k3, k4 = st.columns(2)
    k3.metric("Comunicações filtradas", f"{total_comms:,}".replace(",", "."))
    k4.metric("UCs com comunicação", f"{ucs_comms:,}".replace(",", "."))

    st.metric("% comunicações ligadas a UC com dimensão", f"{covered_rate:.1f}%")

    if not filtered_interessados.empty:
        st.write("**Mix de prazo**")
        prazo_mix = filtered_interessados["prazo_plano"].value_counts(dropna=False)
        st.dataframe(
            prazo_mix.rename_axis("Prazo").reset_index(name="Qtd"),
            use_container_width=True,
            hide_index=True,
        )

row1_col1, row1_col2 = st.columns(2)

with row1_col1:
    st.subheader("Acumulado de interesses")
    cum_int = cumulative_interest(filtered_interessados)
    if not cum_int.empty:
        fig = px.line(
            cum_int,
            x="Data",
            y="Acumulado",
            markers=True,
            hover_data=["Interesses do dia"],
        )
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), yaxis_title="Acumulado")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados de interesse para os filtros atuais.")

with row1_col2:
    st.subheader("Acumulado de comunicações")
    cum_comm = cumulative_comms(filtered_comms)
    if not cum_comm.empty:
        fig = px.line(
            cum_comm,
            x="Data",
            y="Acumulado",
            color="Comunicações do dia" if False else None,
            markers=True,
            hover_data=["Comunicações do dia"],
        )
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), yaxis_title="Acumulado")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem comunicações para os filtros atuais.")

row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    st.subheader("Interesses por plano")
    plano_counts = (
        filtered_interessados["plano_nome"]
        .fillna("Sem classificação")
        .value_counts()
        .rename_axis("Plano")
        .reset_index(name="Qtd")
    )
    if not plano_counts.empty:
        fig = px.bar(plano_counts, x="Plano", y="Qtd")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), xaxis_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para os filtros atuais.")

with row2_col2:
    st.subheader("Comunicações por template / ação")
    comm_counts = (
        filtered_comms["comunicacao"]
        .fillna("Sem nome")
        .value_counts()
        .head(20)
        .rename_axis("Comunicação")
        .reset_index(name="Qtd")
    )
    if not comm_counts.empty:
        fig = px.bar(comm_counts, x="Comunicação", y="Qtd")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), xaxis_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para os filtros atuais.")

row3_col1, row3_col2 = st.columns(2)

with row3_col1:
    st.subheader("Interesses por município")
    muni_counts = (
        filtered_interessados["MUNICIPIO"]
        .fillna("Sem município")
        .value_counts()
        .head(20)
        .rename_axis("Município")
        .reset_index(name="Qtd")
    )
    if not muni_counts.empty:
        fig = px.bar(muni_counts, x="Município", y="Qtd")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), xaxis_title=None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para os filtros atuais.")

with row3_col2:
    st.subheader("Canal de comunicação")
    canal_counts = (
        filtered_comms["canal"]
        .fillna("Sem canal")
        .value_counts()
        .rename_axis("Canal")
        .reset_index(name="Qtd")
    )
    if not canal_counts.empty:
        fig = px.pie(canal_counts, names="Canal", values="Qtd", hole=0.4)
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para os filtros atuais.")

st.subheader("Tabelas detalhadas")

tab1, tab2 = st.tabs(["Interesses", "Comunicações"])

with tab1:
    cols = [
        "NUM_UC", "IND_SITUACAO", "DTH_INTERESSE", "MUNICIPIO",
        "plano_nome", "prazo_plano", "informe", "com_bandeira", "rural",
        "CTTs_ANTES_ACEITE_TOTAL", "DIAS_ACEITE_MAX", "ULT_CTT_ANTES_ACEITE"
    ]
    export_int = filtered_interessados[cols].copy()
    st.dataframe(export_int, use_container_width=True, hide_index=True)
    st.download_button(
        "Baixar interesses filtrados (CSV)",
        export_int.to_csv(index=False).encode("utf-8-sig"),
        file_name="interesses_filtrados.csv",
        mime="text/csv",
    )

with tab2:
    cols = [
        "canal", "comunicacao", "data_evento", "uc_join", "MUNICIPIO",
        "plano_nome", "prazo_plano", "informe", "com_bandeira", "rural"
    ]
    export_comm = filtered_comms[cols].copy()
    st.dataframe(export_comm, use_container_width=True, hide_index=True)
    st.download_button(
        "Baixar comunicações filtradas (CSV)",
        export_comm.to_csv(index=False).encode("utf-8-sig"),
        file_name="comunicacoes_filtradas.csv",
        mime="text/csv",
    )
