import re
from io import BytesIO
from pathlib import Path

import folium
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from folium.plugins import MarkerCluster
from streamlit_folium import st_folium

st.set_page_config(
    page_title="BI Comunicação Plano Fixo",
    page_icon="📊",
    layout="wide",
)

APP_DIR = Path(__file__).parent.resolve()
DATA_SEARCH_DIRS = [
    APP_DIR,
    APP_DIR / "data",
    APP_DIR.parent,
    APP_DIR.parent / "data",
]

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

REQUIRED_FILES = {
    "interessados": "df_interessados.parquet",
    "email": "df_COM_EMAIL.parquet",
    "im": "df_COM_IM.parquet",
}



def parse_coord(value):
    if pd.isna(value):
        return np.nan, np.nan
    text = str(value).strip()
    nums = re.findall(r"-?\d+(?:[.,]\d+)?", text)
    if len(nums) < 2:
        return np.nan, np.nan
    lat = float(nums[0].replace(",", "."))
    lon = float(nums[1].replace(",", "."))
    return lat, lon



def find_parquet_file(filename: str):
    for base in DATA_SEARCH_DIRS:
        candidate = base / filename
        if candidate.exists():
            return candidate
    return None



def read_parquet_source(uploaded_file, expected_filename: str):
    if uploaded_file is not None:
        data = uploaded_file.getvalue()
        return pd.read_parquet(BytesIO(data))

    file_path = find_parquet_file(expected_filename)
    if file_path is None:
        searched = "\n".join(str(p / expected_filename) for p in DATA_SEARCH_DIRS)
        raise FileNotFoundError(
            f"Arquivo não encontrado: {expected_filename}\n\nProcurei em:\n{searched}"
        )
    return pd.read_parquet(file_path)



def build_sidebar_uploaders():
    with st.sidebar:
        st.markdown("---")
        st.subheader("Arquivos Parquet")
        st.caption("O app usa apenas .parquet. Você pode manter os arquivos no repositório ou enviar aqui.")
        uploaded_interessados = st.file_uploader(
            "df_interessados.parquet",
            type=["parquet"],
            key="upload_interessados_parquet",
        )
        uploaded_email = st.file_uploader(
            "df_COM_EMAIL.parquet",
            type=["parquet"],
            key="upload_email_parquet",
        )
        uploaded_im = st.file_uploader(
            "df_COM_IM.parquet",
            type=["parquet"],
            key="upload_im_parquet",
        )
    return uploaded_interessados, uploaded_email, uploaded_im



def enrich_interessados(interessados: pd.DataFrame) -> pd.DataFrame:
    interessados = interessados.copy()

    date_cols = [
        "DTH_INTERESSE",
        "DT_PRIM_CTT_IM",
        "DT_PRIM_CTT_EMAIL",
        "ULT_CTT_DT_IM",
        "ULT_CTT_DT_EMAIL",
    ]
    for col in date_cols:
        if col in interessados.columns:
            interessados[col] = pd.to_datetime(interessados[col], errors="coerce")

    if "COORDENADA GEOGRAFICA" in interessados.columns:
        coords = interessados["COORDENADA GEOGRAFICA"].apply(parse_coord)
        interessados["lat"] = [c[0] for c in coords]
        interessados["lon"] = [c[1] for c in coords]
    else:
        interessados["lat"] = np.nan
        interessados["lon"] = np.nan

    if "ID_PLANO" in interessados.columns:
        interessados["ID_PLANO"] = pd.to_numeric(interessados["ID_PLANO"], errors="coerce").astype("Int64")
    else:
        interessados["ID_PLANO"] = pd.Series(pd.array([pd.NA] * len(interessados), dtype="Int64"))

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
    return interessados



def enrich_email(email: pd.DataFrame) -> pd.DataFrame:
    email = email.copy()
    if "DataEnvio" in email.columns:
        email["DataEnvio"] = pd.to_datetime(email["DataEnvio"], errors="coerce")
    if "UC" in email.columns:
        email["UC"] = pd.to_numeric(email["UC"], errors="coerce").astype("Int64")
    email["canal"] = "E-mail"
    email["comunicacao"] = email.get("Ação", pd.Series(index=email.index)).fillna("Sem ação")
    email["data_evento"] = email.get("DataEnvio", pd.NaT)
    email["uc_join"] = email.get("UC", pd.Series(index=email.index, dtype="Int64"))
    return email



def enrich_im(im: pd.DataFrame) -> pd.DataFrame:
    im = im.copy()
    if "DATA_ENVIO" in im.columns:
        im["DATA_ENVIO"] = pd.to_datetime(im["DATA_ENVIO"], errors="coerce")
    if "NUMCDC" in im.columns:
        im["NUMCDC"] = pd.to_numeric(im["NUMCDC"], errors="coerce").astype("Int64")
    im["canal"] = "IM"
    im["comunicacao"] = im.get("TEMPLATE", pd.Series(index=im.index)).fillna("Sem template")
    im["data_evento"] = im.get("DATA_ENVIO", pd.NaT)
    im["uc_join"] = im.get("NUMCDC", pd.Series(index=im.index, dtype="Int64"))
    return im


@st.cache_data(show_spinner=False)
def load_data_cached(interessados_bytes, email_bytes, im_bytes):
    up_i = BytesIO(interessados_bytes) if interessados_bytes is not None else None
    up_e = BytesIO(email_bytes) if email_bytes is not None else None
    up_m = BytesIO(im_bytes) if im_bytes is not None else None

    interessados = pd.read_parquet(up_i) if up_i is not None else read_parquet_source(None, REQUIRED_FILES["interessados"])
    email = pd.read_parquet(up_e) if up_e is not None else read_parquet_source(None, REQUIRED_FILES["email"])
    im = pd.read_parquet(up_m) if up_m is not None else read_parquet_source(None, REQUIRED_FILES["im"])

    interessados = enrich_interessados(interessados)
    email = enrich_email(email)
    im = enrich_im(im)

    uc_dims_cols = [
        "NUM_UC", "MUNICIPIO", "ID_PLANO", "plano_nome", "prazo_plano", "informe",
        "com_bandeira", "rural", "lat", "lon"
    ]
    uc_dims_cols = [c for c in uc_dims_cols if c in interessados.columns]
    uc_dims = interessados[uc_dims_cols].drop_duplicates("NUM_UC").rename(columns={"NUM_UC": "uc_join"})

    comm_cols = ["canal", "comunicacao", "data_evento", "uc_join"]
    comunicacoes = pd.concat([email[comm_cols], im[comm_cols]], ignore_index=True)
    comunicacoes = comunicacoes.merge(uc_dims, on="uc_join", how="left")
    comunicacoes["tem_dimensao_uc"] = comunicacoes["MUNICIPIO"].notna() if "MUNICIPIO" in comunicacoes.columns else False

    return interessados, email, im, comunicacoes



def load_data(uploaded_interessados=None, uploaded_email=None, uploaded_im=None):
    b_i = uploaded_interessados.getvalue() if uploaded_interessados is not None else None
    b_e = uploaded_email.getvalue() if uploaded_email is not None else None
    b_m = uploaded_im.getvalue() if uploaded_im is not None else None
    return load_data_cached(b_i, b_e, b_m)



def filter_interessados(df, municipios, prazos, informes, bandeiras, rural_sel, planos, status_sel, data_inicio, data_fim):
    out = df.copy()
    if municipios and "MUNICIPIO" in out.columns:
        out = out[out["MUNICIPIO"].isin(municipios)]
    if prazos and "prazo_plano" in out.columns:
        out = out[out["prazo_plano"].isin(prazos)]
    if informes and "informe" in out.columns:
        out = out[out["informe"].isin(informes)]
    if bandeiras and "com_bandeira" in out.columns:
        out = out[out["com_bandeira"].isin(bandeiras)]
    if rural_sel and "rural" in out.columns:
        out = out[out["rural"].isin(rural_sel)]
    if planos and "plano_nome" in out.columns:
        out = out[out["plano_nome"].isin(planos)]
    if status_sel and "IND_SITUACAO" in out.columns:
        out = out[out["IND_SITUACAO"].isin(status_sel)]
    if data_inicio is not None and "DTH_INTERESSE" in out.columns:
        out = out[out["DTH_INTERESSE"] >= pd.Timestamp(data_inicio)]
    if data_fim is not None and "DTH_INTERESSE" in out.columns:
        out = out[out["DTH_INTERESSE"] <= pd.Timestamp(data_fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    return out



def filter_comunicacoes(df, municipios, prazos, informes, bandeiras, rural_sel, planos, canais, comunicacoes_sel, data_inicio, data_fim):
    out = df.copy()
    if municipios and "MUNICIPIO" in out.columns:
        out = out[out["MUNICIPIO"].isin(municipios)]
    if prazos and "prazo_plano" in out.columns:
        out = out[out["prazo_plano"].isin(prazos)]
    if informes and "informe" in out.columns:
        out = out[out["informe"].isin(informes)]
    if bandeiras and "com_bandeira" in out.columns:
        out = out[out["com_bandeira"].isin(bandeiras)]
    if rural_sel and "rural" in out.columns:
        out = out[out["rural"].isin(rural_sel)]
    if planos and "plano_nome" in out.columns:
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
    base = df.dropna(subset=["DTH_INTERESSE"]).copy() if "DTH_INTERESSE" in df.columns else pd.DataFrame()
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


if "map_selected_municipio" not in st.session_state:
    st.session_state["map_selected_municipio"] = None

st.title("BI de Comunicação — Plano Fixo")
st.caption("Filtros por município, plano, prazo, informe, bandeira, rural, template/ação e gráficos acumulados.")

uploaded_interessados, uploaded_email, uploaded_im = build_sidebar_uploaders()

try:
    interessados, email, im, comunicacoes = load_data(uploaded_interessados, uploaded_email, uploaded_im)
except FileNotFoundError as e:
    st.error(str(e))
    st.info(
        "Coloque os arquivos .parquet no repositório (raiz ou pasta data/) ou envie os três arquivos pela sidebar."
    )
    st.stop()
except Exception as e:
    st.error(f"Erro ao carregar os arquivos parquet: {e}")
    st.stop()

with st.sidebar:
    st.header("Filtros")
    all_municipios = sorted([m for m in interessados.get("MUNICIPIO", pd.Series(dtype=str)).dropna().unique().tolist()])
    municipio_options = ["Todos"] + all_municipios
    current_map_sel = st.session_state.get("map_selected_municipio")
    map_choice = st.selectbox(
        "Município selecionado no mapa",
        municipio_options,
        index=0 if not current_map_sel or current_map_sel not in municipio_options else municipio_options.index(current_map_sel),
        help="Clique em um marcador do mapa e depois escolha aqui para aplicar o filtro.",
    )
    default_municipios = [] if map_choice == "Todos" else [map_choice]
    municipios = st.multiselect("Município", all_municipios, default=default_municipios)
    prazos = st.multiselect("Prazo do plano", ["Anual", "Semestral", "Trimestral"])
    informes = st.multiselect("Informe", ["Mensal", "Trimestral", "Semestral"])
    bandeiras = st.multiselect("Com bandeira", ["Sim", "Não"])
    rural_sel = st.multiselect("Rural", ["Sim", "Não"])
    planos = st.multiselect("Plano detalhado", sorted([v for v in PLAN_ID_MAP.values()]))
    status_values = sorted([s for s in interessados.get("IND_SITUACAO", pd.Series(dtype=str)).dropna().unique().tolist()])
    status_sel = st.multiselect("Status em df_interessados", status_values)
    canais = st.multiselect("Canal de comunicação", ["E-mail", "IM"])
    comm_options = sorted(comunicacoes["comunicacao"].dropna().unique().tolist())
    comunicacoes_sel = st.multiselect("Template / Ação", comm_options)

    min_interest = interessados.get("DTH_INTERESSE", pd.Series(dtype="datetime64[ns]")).min()
    max_interest = interessados.get("DTH_INTERESSE", pd.Series(dtype="datetime64[ns]")).max()
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
    st.caption("Município/plano/informe nas comunicações dependem do vínculo por UC com df_interessados.")

filtered_interessados = filter_interessados(
    interessados, municipios, prazos, informes, bandeiras, rural_sel, planos, status_sel, data_inicio, data_fim
)
filtered_comms = filter_comunicacoes(
    comunicacoes, municipios, prazos, informes, bandeiras, rural_sel, planos, canais, comunicacoes_sel, data_inicio, data_fim
)

map_base = filter_interessados(
    interessados, [], prazos, informes, bandeiras, rural_sel, planos, status_sel, data_inicio, data_fim
)
municipio_summary = (
    map_base.dropna(subset=["MUNICIPIO"])
    .groupby("MUNICIPIO", as_index=False)
    .agg(interesses=("NUM_UC", "size"), ucs=("NUM_UC", "nunique"), lat=("lat", "median"), lon=("lon", "median"))
)
if not municipio_summary.empty:
    top_plans = (
        map_base.dropna(subset=["MUNICIPIO", "plano_nome"])
        .groupby(["MUNICIPIO", "plano_nome"]).size().reset_index(name="n")
        .sort_values(["MUNICIPIO", "n"], ascending=[True, False])
        .drop_duplicates("MUNICIPIO")
        .rename(columns={"plano_nome": "plano_top"})
    )
    municipio_summary = municipio_summary.merge(top_plans[["MUNICIPIO", "plano_top"]], on="MUNICIPIO", how="left")
    municipio_summary["plano_top"] = municipio_summary["plano_top"].fillna("N/A")
else:
    municipio_summary = pd.DataFrame(columns=["MUNICIPIO", "interesses", "ucs", "lat", "lon", "plano_top"])

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
    ucs_interesses = filtered_interessados["NUM_UC"].nunique() if "NUM_UC" in filtered_interessados.columns else 0
    total_comms = len(filtered_comms)
    ucs_comms = filtered_comms["uc_join"].nunique() if "uc_join" in filtered_comms.columns else 0
    covered_rate = (filtered_comms["tem_dimensao_uc"].mean() * 100) if len(filtered_comms) and "tem_dimensao_uc" in filtered_comms.columns else 0
    k1, k2 = st.columns(2)
    k1.metric("Interesses", f"{total_interesses:,}".replace(",", "."))
    k2.metric("UCs interessadas", f"{ucs_interesses:,}".replace(",", "."))
    k3, k4 = st.columns(2)
    k3.metric("Comunicações filtradas", f"{total_comms:,}".replace(",", "."))
    k4.metric("UCs com comunicação", f"{ucs_comms:,}".replace(",", "."))
    st.metric("% comunicações ligadas a UC com dimensão", f"{covered_rate:.1f}%")
    if not filtered_interessados.empty and "prazo_plano" in filtered_interessados.columns:
        st.write("**Mix de prazo**")
        prazo_mix = filtered_interessados["prazo_plano"].value_counts(dropna=False)
        st.dataframe(prazo_mix.rename_axis("Prazo").reset_index(name="Qtd"), width="stretch", hide_index=True)

row1_col1, row1_col2 = st.columns(2)
with row1_col1:
    st.subheader("Acumulado de interesses")
    cum_int = cumulative_interest(filtered_interessados)
    if not cum_int.empty:
        fig = px.line(cum_int, x="Data", y="Acumulado", markers=True, hover_data=["Interesses do dia"])
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), yaxis_title="Acumulado")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sem dados de interesse para os filtros atuais.")
with row1_col2:
    st.subheader("Acumulado de comunicações")
    cum_comm = cumulative_comms(filtered_comms)
    if not cum_comm.empty:
        fig = px.line(cum_comm, x="Data", y="Acumulado", markers=True, hover_data=["Comunicações do dia"])
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), yaxis_title="Acumulado")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sem comunicações para os filtros atuais.")

row2_col1, row2_col2 = st.columns(2)
with row2_col1:
    st.subheader("Interesses por plano")
    plano_counts = filtered_interessados.get("plano_nome", pd.Series(dtype=str)).fillna("Sem classificação").value_counts().rename_axis("Plano").reset_index(name="Qtd")
    if not plano_counts.empty:
        fig = px.bar(plano_counts, x="Plano", y="Qtd")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), xaxis_title=None)
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sem dados para os filtros atuais.")
with row2_col2:
    st.subheader("Comunicações por template / ação")
    comm_counts = filtered_comms["comunicacao"].fillna("Sem nome").value_counts().head(20).rename_axis("Comunicação").reset_index(name="Qtd")
    if not comm_counts.empty:
        fig = px.bar(comm_counts, x="Comunicação", y="Qtd")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), xaxis_title=None)
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sem dados para os filtros atuais.")

row3_col1, row3_col2 = st.columns(2)
with row3_col1:
    st.subheader("Interesses por município")
    muni_counts = filtered_interessados.get("MUNICIPIO", pd.Series(dtype=str)).fillna("Sem município").value_counts().head(20).rename_axis("Município").reset_index(name="Qtd")
    if not muni_counts.empty:
        fig = px.bar(muni_counts, x="Município", y="Qtd")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), xaxis_title=None)
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sem dados para os filtros atuais.")
with row3_col2:
    st.subheader("Canal de comunicação")
    canal_counts = filtered_comms["canal"].fillna("Sem canal").value_counts().rename_axis("Canal").reset_index(name="Qtd")
    if not canal_counts.empty:
        fig = px.pie(canal_counts, names="Canal", values="Qtd", hole=0.4)
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sem dados para os filtros atuais.")

st.subheader("Tabelas detalhadas")
tab1, tab2 = st.tabs(["Interesses", "Comunicações"])
with tab1:
    cols = [
        "NUM_UC", "IND_SITUACAO", "DTH_INTERESSE", "MUNICIPIO", "plano_nome", "prazo_plano",
        "informe", "com_bandeira", "rural", "CTTs_ANTES_ACEITE_TOTAL", "DIAS_ACEITE_MAX", "ULT_CTT_ANTES_ACEITE"
    ]
    cols = [c for c in cols if c in filtered_interessados.columns]
    export_int = filtered_interessados[cols].copy()
    st.dataframe(export_int, width="stretch", hide_index=True)
    st.download_button(
        "Baixar interesses filtrados (CSV)",
        export_int.to_csv(index=False).encode("utf-8-sig"),
        file_name="interesses_filtrados.csv",
        mime="text/csv",
    )
with tab2:
    cols = [
        "canal", "comunicacao", "data_evento", "uc_join", "MUNICIPIO", "plano_nome",
        "prazo_plano", "informe", "com_bandeira", "rural"
    ]
    cols = [c for c in cols if c in filtered_comms.columns]
    export_comm = filtered_comms[cols].copy()
    st.dataframe(export_comm, width="stretch", hide_index=True)
    st.download_button(
        "Baixar comunicações filtradas (CSV)",
        export_comm.to_csv(index=False).encode("utf-8-sig"),
        file_name="comunicacoes_filtradas.csv",
        mime="text/csv",
    )
