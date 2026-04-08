
import re
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="BI Comunicação Plano Fixo", page_icon="📊", layout="wide")

APP_DIR = Path(__file__).parent.resolve()
DATA_SEARCH_DIRS = [
    APP_DIR,
    APP_DIR / "data",
    APP_DIR.parent,
    APP_DIR.parent / "data",
]

INTEREST_STATUSES = {"A", "I", "R", "D", "X"}

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
    1: dict(prazo="Trimestral", informe="Mensal", bandeira="Sem bandeira", rural="Não"),
    2: dict(prazo="Trimestral", informe="Trimestral", bandeira="Sem bandeira", rural="Não"),
    3: dict(prazo="Semestral", informe="Mensal", bandeira="Sem bandeira", rural="Não"),
    4: dict(prazo="Semestral", informe="Mensal", bandeira="Com bandeira", rural="Não"),
    6: dict(prazo="Semestral", informe="Trimestral", bandeira="Sem bandeira", rural="Não"),
    7: dict(prazo="Semestral", informe="Semestral", bandeira="Sem bandeira", rural="Não"),
    8: dict(prazo="Semestral", informe="Semestral", bandeira="Sem bandeira", rural="Sim"),
    9: dict(prazo="Anual", informe="Mensal", bandeira="Sem bandeira", rural="Não"),
    10: dict(prazo="Anual", informe="Trimestral", bandeira="Sem bandeira", rural="Não"),
    11: dict(prazo="Anual", informe="Semestral", bandeira="Sem bandeira", rural="Não"),
    12: dict(prazo="Anual", informe="Semestral", bandeira="Sem bandeira", rural="Sim"),
}

WHATSAPP_TEMPLATES = {
    "gisaconnect_fatura_fixa_v1",
    "tpl_wa_sandbox_fatura_fixa_reforco",
    "tpl_wa_sandbox_fatura_fixa_ultimos_dias",
}
SMS_TEMPLATES = {
    "tpl_sms_sandbox_oferta_inicial",
    "tpl_sms_sandbox_fatura_fixa_reforco",
    "tpl_sms_sandbox_fatura_fixa_conversao",
}
PUSH_TEMPLATES = {
    "tpl_push_sandbox_oferta_inicial",
    "tpl_push_sandbox_fatura_fixa_reforco",
    "tpl_push_sandbox_fatura_fixa_conversao",
    "tpl_push_sandbox_fatura_fixa_ultimos_dias",
}
EMAIL_ACTIONS = {
    "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251017",
    "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251021",
    "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251028",
    "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251104",
    "242_1_72830_SANDBOX_PLANO_FIXO_LANCAMENTO",
    "242_1_72830_SANDBOX_PLANO_FIXO_LANCAMENTO_20251209",
    "242_2_72831_SANDBOX_PLANO_FIXO_REFORCO",
    "242_2_72831_SANDBOX_PLANO_FIXO_REFORCO_20251209",
}

EMAIL_GROUPS = {
    "EMAIL Outubro": {
        "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251017",
        "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251021",
        "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251028",
    },
    "EMAIL Novembro": {
        "242_1_72830_SANDBOX_PLANO_FIXO_D0 20251104",
    },
    "EMAIL Dezembro": {
        "242_1_72830_SANDBOX_PLANO_FIXO_LANCAMENTO_20251209",
        "242_2_72831_SANDBOX_PLANO_FIXO_REFORCO_20251209",
    },
    "EMAIL Lançamento": {
        "242_1_72830_SANDBOX_PLANO_FIXO_LANCAMENTO",
    },
    "EMAIL Reforço": {
        "242_2_72831_SANDBOX_PLANO_FIXO_REFORCO",
    },
}

CHANNEL_TEMPLATE_GROUPS = {
    "Gisaconect": {"gisaconnect_fatura_fixa_v1"},
    "WhatsApp": {
        "tpl_wa_sandbox_fatura_fixa_reforco",
        "tpl_wa_sandbox_fatura_fixa_ultimos_dias",
    },
    "SMS": {
        "tpl_sms_sandbox_oferta_inicial",
        "tpl_sms_sandbox_fatura_fixa_reforco",
        "tpl_sms_sandbox_fatura_fixa_conversao",
    },
    "Push": {
        "tpl_push_sandbox_oferta_inicial",
        "tpl_push_sandbox_fatura_fixa_reforco",
        "tpl_push_sandbox_fatura_fixa_conversao",
        "tpl_push_sandbox_fatura_fixa_ultimos_dias",
    },
}

EMAIL_ACTION_TO_GROUP = {}
for group_name, action_set in EMAIL_GROUPS.items():
    for action in action_set:
        EMAIL_ACTION_TO_GROUP[action] = group_name

IM_TEMPLATE_TO_GROUP = {}
for group_name, template_set in CHANNEL_TEMPLATE_GROUPS.items():
    for template in template_set:
        IM_TEMPLATE_TO_GROUP[template] = group_name


def find_parquet(filename: str) -> Path:
    for base in DATA_SEARCH_DIRS:
        candidate = base / filename
        if candidate.exists():
            return candidate
    searched = "\n".join(str(p / filename) for p in DATA_SEARCH_DIRS)
    raise FileNotFoundError(f"Arquivo não encontrado: {filename}\nLocais buscados:\n{searched}")


def parse_coord(text):
    if pd.isna(text):
        return np.nan, np.nan
    s = str(text).strip()
    nums = re.findall(r"-?\d+[.,]?\d*", s)
    if len(nums) < 2:
        return np.nan, np.nan
    lat = float(nums[0].replace(",", "."))
    lon = float(nums[1].replace(",", "."))
    return lat, lon


def add_plan_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ID_PLANO"] = pd.to_numeric(df.get("ID_PLANO"), errors="coerce").astype("Int64")
    df["PLANO_DETALHADO"] = df["ID_PLANO"].map(PLAN_ID_MAP).fillna("Sem plano identificado")
    attrs = df["ID_PLANO"].map(PLAN_ATTRIBUTES)
    df["PRAZO_PLANO"] = attrs.map(lambda x: x.get("prazo") if isinstance(x, dict) else "Não identificado")
    df["INFORME"] = attrs.map(lambda x: x.get("informe") if isinstance(x, dict) else "Não identificado")
    df["BANDEIRA"] = attrs.map(lambda x: x.get("bandeira") if isinstance(x, dict) else "Não identificado")
    df["RURAL"] = attrs.map(lambda x: x.get("rural") if isinstance(x, dict) else "Não identificado")
    return df


def prepare_interessados(interessados: pd.DataFrame) -> pd.DataFrame:
    df = interessados.copy()
    df["NUM_UC"] = pd.to_numeric(df["NUM_UC"], errors="coerce").astype("Int64")
    df["IND_SITUACAO"] = df["IND_SITUACAO"].astype(str).str.strip()
    df["DTH_INTERESSE"] = pd.to_datetime(df["DTH_INTERESSE"], errors="coerce")
    df["MUNICIPIO"] = df["MUNICIPIO"].astype(str).str.strip().replace({"nan": np.nan})
    lat_lon = df["COORDENADA GEOGRAFICA"].apply(parse_coord)
    df["LAT"] = lat_lon.map(lambda x: x[0])
    df["LON"] = lat_lon.map(lambda x: x[1])
    df = add_plan_dimensions(df)
    df["TEM_COMUNICACAO"] = pd.to_numeric(df["CTTs_ANTES_ACEITE_TOTAL"], errors="coerce").fillna(0).gt(0)
    return df


def classify_im_channel(template: str) -> str:
    if template in WHATSAPP_TEMPLATES:
        return "WhatsApp"
    if template in SMS_TEMPLATES:
        return "SMS"
    if template in PUSH_TEMPLATES:
        return "Push"
    return "Outro IM"


def normalize_email_group(action: str) -> str:
    return EMAIL_ACTION_TO_GROUP.get(action, action)


def normalize_im_group(template: str) -> str:
    return IM_TEMPLATE_TO_GROUP.get(template, template)


def prepare_comunicacoes(email: pd.DataFrame, im: pd.DataFrame, dim_lookup: pd.DataFrame) -> pd.DataFrame:
    email_df = email.copy()
    email_df["UC"] = pd.to_numeric(email_df["UC"], errors="coerce").astype("Int64")
    email_df["Data"] = pd.to_datetime(email_df["DataEnvio"], errors="coerce")
    email_df["Template_Acao"] = email_df["Ação"].astype(str).str.strip()
    email_df["Template_Acao_Grupo"] = email_df["Template_Acao"].map(normalize_email_group)
    email_df["Canal"] = np.where(email_df["Template_Acao"].isin(EMAIL_ACTIONS), "Email", "Outro Email")
    email_df["Mensagens"] = pd.to_numeric(email_df["Qtde"], errors="coerce").fillna(1)
    email_df = email_df.rename(columns={"UC": "NUM_UC"})
    email_df = email_df[["NUM_UC", "Data", "Canal", "Template_Acao", "Template_Acao_Grupo", "Mensagens"]]

    im_df = im.copy()
    im_df["NUM_UC"] = pd.to_numeric(im_df["NUMCDC"], errors="coerce").astype("Int64")
    im_df["Data"] = pd.to_datetime(im_df["DATA_ENVIO"], errors="coerce")
    im_df["Template_Acao"] = im_df["TEMPLATE"].astype(str).str.strip()
    im_df["Template_Acao_Grupo"] = im_df["Template_Acao"].map(normalize_im_group)
    im_df["Canal"] = im_df["Template_Acao"].map(classify_im_channel)
    im_df["Mensagens"] = 1
    im_df = im_df[["NUM_UC", "Data", "Canal", "Template_Acao", "Template_Acao_Grupo", "Mensagens"]]

    comunicacoes = pd.concat([email_df, im_df], ignore_index=True)
    comunicacoes = comunicacoes.merge(dim_lookup, on="NUM_UC", how="left")
    return comunicacoes


@st.cache_data(show_spinner=False)
def load_data():
    interessados = pd.read_parquet(find_parquet("df_interessados.parquet"))
    email = pd.read_parquet(find_parquet("df_COM_EMAIL.parquet"))
    im = pd.read_parquet(find_parquet("df_COM_IM.parquet"))

    interessados = prepare_interessados(interessados)

    dim_lookup = (
        interessados[
            [
                "NUM_UC",
                "MUNICIPIO",
                "PLANO_DETALHADO",
                "PRAZO_PLANO",
                "INFORME",
                "BANDEIRA",
                "RURAL",
                "LAT",
                "LON",
            ]
        ]
        .drop_duplicates(subset=["NUM_UC"])
        .copy()
    )

    comunicacoes = prepare_comunicacoes(email, im, dim_lookup)
    return interessados, comunicacoes


def apply_filters(interessados: pd.DataFrame, comunicacoes: pd.DataFrame):
    st.sidebar.header("Filtros")

    min_date = min(
        x for x in [interessados["DTH_INTERESSE"].min(), comunicacoes["Data"].min()] if pd.notna(x)
    )
    max_date = max(
        x for x in [interessados["DTH_INTERESSE"].max(), comunicacoes["Data"].max()] if pd.notna(x)
    )
    date_range = st.sidebar.date_input(
        "Período",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )
    
    municipios = sorted(x for x in interessados["MUNICIPIO"].dropna().unique().tolist() if x)
    municipio = st.sidebar.selectbox("Município", ["Todos"] + municipios, index=0)

    prazos = sorted(x for x in interessados["PRAZO_PLANO"].dropna().unique().tolist() if x)
    prazo_sel = st.sidebar.multiselect("Prazo do plano", prazos, default=prazos)

    informes = sorted(x for x in interessados["INFORME"].dropna().unique().tolist() if x)
    informe_sel = st.sidebar.multiselect("Informe", informes, default=informes)

    bandeiras = sorted(x for x in interessados["BANDEIRA"].dropna().unique().tolist() if x)
    bandeira_sel = st.sidebar.multiselect("Bandeira", bandeiras, default=bandeiras)

    rural_vals = sorted(x for x in interessados["RURAL"].dropna().unique().tolist() if x)
    rural_sel = st.sidebar.multiselect("Rural", rural_vals, default=rural_vals)

    planos = sorted(x for x in interessados["PLANO_DETALHADO"].dropna().unique().tolist() if x)
    plano_sel = st.sidebar.multiselect("Plano detalhado", planos, default=planos)

    canais = ["WhatsApp", "SMS", "Push", "Email"]
    canal_sel = st.sidebar.multiselect("Canal de comunicação", canais, default=canais)

    email_group_order = [
        "EMAIL Outubro",
        "EMAIL Novembro",
        "EMAIL Dezembro",
        "EMAIL Lançamento",
        "EMAIL Reforço",
    ]
    channel_group_order = ["Gisaconect", "WhatsApp", "SMS", "Push"]

    template_options = [x for x in email_group_order if x in comunicacoes["Template_Acao_Grupo"].unique()]
    template_options += [x for x in channel_group_order if x in comunicacoes["Template_Acao_Grupo"].unique()]

    leftovers = sorted(
        x for x in comunicacoes["Template_Acao_Grupo"].dropna().unique().tolist()
        if x not in template_options and x
    )
    template_options.extend(leftovers)

    template_sel = st.sidebar.multiselect("Template / Ação", template_options, default=template_options)
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        dt_ini = pd.Timestamp(date_range[0])
        dt_fim = pd.Timestamp(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    else:
        dt_ini, dt_fim = min_date, max_date

    f_int = interessados.copy()
    f_com = comunicacoes.copy()

    if municipio != "Todos":
        f_int = f_int[f_int["MUNICIPIO"] == municipio]
        f_com = f_com[f_com["MUNICIPIO"] == municipio]

    f_int = f_int[
        f_int["PRAZO_PLANO"].isin(prazo_sel)
        & f_int["INFORME"].isin(informe_sel)
        & f_int["BANDEIRA"].isin(bandeira_sel)
        & f_int["RURAL"].isin(rural_sel)
        & f_int["PLANO_DETALHADO"].isin(plano_sel)
    ]
    f_int = f_int[(f_int["DTH_INTERESSE"] >= dt_ini) & (f_int["DTH_INTERESSE"] <= dt_fim)]

    f_com = f_com[
        f_com["Canal"].isin(canal_sel)
        & f_com["PRAZO_PLANO"].isin(prazo_sel)
        & f_com["INFORME"].isin(informe_sel)
        & f_com["BANDEIRA"].isin(bandeira_sel)
        & f_com["RURAL"].isin(rural_sel)
        & f_com["PLANO_DETALHADO"].isin(plano_sel)
        & f_com["Template_Acao_Grupo"].isin(template_sel)
    ]
    f_com = f_com[(f_com["Data"] >= dt_ini) & (f_com["Data"] <= dt_fim)]

    return f_int, f_com


def render_top_banner(image_name: str = "Header.png"):
    banner_path = APP_DIR / image_name
    if banner_path.exists():
        st.image(str(banner_path), use_container_width=True)
    else:
        st.warning(f"Banner não encontrado: {banner_path}")


def metric_card(label: str, value: str, help_text: str = ""):
    st.metric(label=label, value=value, help=help_text)


def format_int(value) -> str:
    return f"{int(value):,}".replace(",", ".")


def format_pct(value: float) -> str:
    return f"{value:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def render_metric_block(title: str, metrics: list[tuple[str, str, str]], n_cols: int = 7):
    if title:
        st.markdown(f"### {title}")
    for start in range(0, len(metrics), n_cols):
        row = metrics[start:start + n_cols]
        cols = st.columns(len(row))
        for col, (label, value, help_text) in zip(cols, row):
            with col:
                metric_card(label, value, help_text)


def build_map(df: pd.DataFrame):
    map_df = (
        df.dropna(subset=["LAT", "LON"])
        .groupby("MUNICIPIO", as_index=False)
        .agg(
            LAT=("LAT", "median"),
            LON=("LON", "median"),
            UCs=("NUM_UC", pd.Series.nunique),
        )
    )
    if map_df.empty:
        st.info("Sem coordenadas disponíveis para exibir o mapa.")
        return

    fig = px.scatter_mapbox(
        map_df,
        lat="LAT",
        lon="LON",
        size="UCs",
        hover_name="MUNICIPIO",
        hover_data={"UCs": True, "LAT": False, "LON": False},
        zoom=6.2,
        center={"lat": -20.5, "lon": -54.6},
        height=480,
        text="MUNICIPIO",
    )
    fig.update_traces(
        textposition="top center",
        textfont=dict(color="black", size=11),
        marker=dict(opacity=0.8)
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(color="black"),
    )
    st.plotly_chart(fig, width="stretch")


def plot_cumulative_interested_by_plan(df: pd.DataFrame):
    base = df[df["IND_SITUACAO"].isin(INTEREST_STATUSES)].copy()
    base = base.dropna(subset=["DTH_INTERESSE", "PLANO_DETALHADO", "PRAZO_PLANO"])

    if base.empty:
        st.info("Sem dados de UCs interessadas no período.")
        return

    base["Data"] = base["DTH_INTERESSE"].dt.floor("D")

    # Daily unique UCs by plan
    daily = (
        base.groupby(["Data", "PLANO_DETALHADO", "PRAZO_PLANO"], as_index=False)["NUM_UC"]
        .nunique()
        .rename(columns={"NUM_UC": "UCs interessadas"})
    )

    # Complete date range from first to last date in filtered data
    all_dates = pd.date_range(
        start=base["Data"].min(),
        end=base["Data"].max(),
        freq="D"
    )

    # One row per plan
    plans = daily[["PLANO_DETALHADO", "PRAZO_PLANO"]].drop_duplicates()

    # Cartesian product: every date x every plan
    full_index = (
        plans.assign(_key=1)
        .merge(pd.DataFrame({"Data": all_dates, "_key": 1}), on="_key")
        .drop(columns="_key")
    )

    # Merge actual counts; missing days become zero
    full_daily = (
        full_index.merge(
            daily,
            on=["Data", "PLANO_DETALHADO", "PRAZO_PLANO"],
            how="left"
        )
        .fillna({"UCs interessadas": 0})
        .sort_values(["PLANO_DETALHADO", "Data"])
    )

    # Continuous cumulative line for every plan
    full_daily["Acumulado"] = (
        full_daily.groupby("PLANO_DETALHADO")["UCs interessadas"].cumsum()
    )

    symbol_map = {
        "Trimestral": "triangle-up",
        "Semestral": "square",
        "Anual": "circle",
    }

    fig = px.line(
        full_daily,
        x="Data",
        y="Acumulado",
        color="PLANO_DETALHADO",
        symbol="PRAZO_PLANO",
        symbol_map=symbol_map,
        markers=True,
    )

    fig.update_traces(marker=dict(size=8))
    fig.update_layout(
        margin=dict(l=0, r=0, t=20, b=0),
        yaxis_title="UCs interessadas acumuladas",
        xaxis_title="",
        legend_title="Plano",
    )

    st.plotly_chart(fig, width="stretch")


def plot_cumulative_messages(df: pd.DataFrame):
    daily = (
        df.dropna(subset=["Data"])
        .assign(Data=lambda x: x["Data"].dt.floor("D"))
        .groupby(["Data", "Canal"], as_index=False)["Mensagens"]
        .sum()
        .sort_values("Data")
    )
    if daily.empty:
        st.info("Sem comunicações no período.")
        return
    daily["Acumulado"] = daily.groupby("Canal")["Mensagens"].cumsum()
    fig = px.line(daily, x="Data", y="Acumulado", color="Canal")
    fig.update_layout(margin=dict(l=0, r=0, t=20, b=0), yaxis_title="Mensagens acumuladas", xaxis_title="")
    st.plotly_chart(fig, width="stretch")


def plot_bar(df: pd.DataFrame, group_col: str, title: str, metric_name: str = "UCs interessadas"):
    if df.empty:
        st.info("Sem dados para exibir.")
        return
    base = (
        df.groupby(group_col, as_index=False)["NUM_UC"]
        .nunique()
        .rename(columns={"NUM_UC": metric_name})
        .sort_values(metric_name, ascending=False)
    )
    fig = px.bar(base, x=group_col, y=metric_name)
    fig.update_layout(title=title, margin=dict(l=0, r=0, t=40, b=0), xaxis_title="")
    st.plotly_chart(fig, width="stretch")


def main():
    interessados, comunicacoes = load_data()

    total_ucs_interessadas = interessados["NUM_UC"].dropna().nunique()

    # ===== TOTAL (SEM FILTRO) =====
    total_contacted_ucs = comunicacoes["NUM_UC"].dropna().nunique()
    total_messages = comunicacoes["Mensagens"].sum()
    total_messages_by_channel = (
        comunicacoes.groupby("Canal", as_index=False)["Mensagens"]
        .sum()
        .set_index("Canal")["Mensagens"]
        .to_dict()
    )

    total_interested_with_contact = (
        interessados[
            interessados["TEM_COMUNICACAO"] & interessados["IND_SITUACAO"].isin(INTEREST_STATUSES)
        ]["NUM_UC"]
        .dropna()
        .nunique()
    )

    total_pct_contacted_that_interested = (
        100 * total_interested_with_contact / total_contacted_ucs if total_contacted_ucs else 0
    )

    render_top_banner("Header.png")
    st.title("BI Comunicação Plano Fixo")

    f_int, f_com = apply_filters(interessados, comunicacoes)

    interested_filtered = (
        f_int[f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)]["NUM_UC"].dropna().nunique()
    )
    interested_with_contact_filtered = (
        f_int[f_int["TEM_COMUNICACAO"] & f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)]["NUM_UC"]
        .dropna()
        .nunique()
    )
    pct_of_total_interested = (
        100 * interested_with_contact_filtered / total_ucs_interessadas if total_ucs_interessadas else 0
    )

    contacted_ucs_filtered = f_com["NUM_UC"].dropna().nunique()
    pct_contacted_that_interested = (
        100 * interested_with_contact_filtered / contacted_ucs_filtered if contacted_ucs_filtered else 0
    )

    filtered_messages = f_com["Mensagens"].sum()
    filtered_messages_by_channel = (
        f_com.groupby("Canal", as_index=False)["Mensagens"]
        .sum()
        .set_index("Canal")["Mensagens"]
        .to_dict()
    )

    render_metric_block(
        "Total",
        [
            ("UCs contactadas", format_int(total_contacted_ucs), "UCs únicas presentes nos arquivos IM + Email, sem aplicar filtros."),
            ("Total de UCs interessadas", format_int(total_ucs_interessadas), "Total de UCs únicas no df_interessados."),
            ("UCs interessadas com contato", format_int(total_interested_with_contact), "UCs interessadas com pelo menos uma comunicação anterior, sem aplicar filtros."),
            ("% das UCs contactadas que demonstraram interesse", format_pct(total_pct_contacted_that_interested), "UCs interessadas com contato dividido pelas UCs contactadas dos arquivos IM + Email, sem aplicar filtros."),
            ("Mensagens por Email", format_int(total_messages_by_channel.get("Email", 0)), "Total de mensagens de Email, sem aplicar filtros."),
            ("Mensagens por WhatsApp", format_int(total_messages_by_channel.get("WhatsApp", 0)), "Total de mensagens de WhatsApp, sem aplicar filtros."),
            ("Mensagens por SMS", format_int(total_messages_by_channel.get("SMS", 0)), "Total de mensagens de SMS, sem aplicar filtros."),
            ("Mensagens por Push", format_int(total_messages_by_channel.get("Push", 0)), "Total de mensagens de Push, sem aplicar filtros."),
        ],
        n_cols=4,
    )

    render_metric_block(
        "Filtrado",
        [
            ("UCs interessadas no filtro", format_int(interested_filtered), "UCs únicas do df_interessados dentro dos filtros atuais."),
            ("UCs interessadas com contato", format_int(interested_with_contact_filtered), "UCs interessadas com pelo menos uma comunicação anterior dentro dos filtros."),
            ("% do total de UCs interessadas", format_pct(pct_of_total_interested), "Percentual das UCs interessadas com contato sobre o total de UCs únicas do df_interessados."),
            ("UCs contactadas", format_int(contacted_ucs_filtered), "UCs únicas presentes nos arquivos IM + Email após os filtros."),
            ("% das UCs contactadas que demonstraram interesse", format_pct(pct_contacted_that_interested), "UCs interessadas com contato dividido pelas UCs contactadas dos arquivos IM + Email no filtro."),
            ("Mensagens filtradas", format_int(filtered_messages), "Total de mensagens após os filtros."),
            ("Mensagens por Email", format_int(filtered_messages_by_channel.get("Email", 0)), "Mensagens de Email após os filtros."),
            ("Mensagens por WhatsApp", format_int(filtered_messages_by_channel.get("WhatsApp", 0)), "Mensagens de WhatsApp após os filtros."),
            ("Mensagens por SMS", format_int(filtered_messages_by_channel.get("SMS", 0)), "Mensagens de SMS após os filtros."),
            ("Mensagens por Push", format_int(filtered_messages_by_channel.get("Push", 0)), "Mensagens de Push após os filtros."),
        ],
        n_cols=5,
    )

    build_map(f_int)
    st.subheader("Linha do tempo de Interesse")
    plot_cumulative_interested_by_plan(f_int)
    
    st.subheader("Mensagens acumuladas")
    plot_cumulative_messages(f_com[f_com["Canal"].isin(["Email", "WhatsApp", "SMS", "Push"])])
    
    col3, col4 = st.columns(2)
    with col3:
        plot_bar(f_int[f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)], "PLANO_DETALHADO", "UCs interessadas por plano")
    with col4:
        plot_bar(f_int[f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)], "MUNICIPIO", "UCs interessadas por município")

    comm_col1, comm_col2 = st.columns(2)
    with comm_col1:
        if not f_com.empty:
            by_canal = (
                f_com.groupby("Canal", as_index=False)["Mensagens"].sum().sort_values("Mensagens", ascending=False)
            )
            fig = px.bar(by_canal, x="Canal", y="Mensagens")
            fig.update_layout(title="Mensagens por canal", margin=dict(l=0, r=0, t=40, b=0), xaxis_title="")
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem comunicações para exibir.")
    with comm_col2:
        if not f_com.empty:
            by_template = (
                f_com.groupby("Template_Acao_Grupo", as_index=False)["Mensagens"]
                .sum()
                .sort_values("Mensagens", ascending=False)
                .head(20)
            )
            fig = px.bar(by_template, x="Template_Acao_Grupo", y="Mensagens")
            fig.update_layout(title="Mensagens por template / ação", margin=dict(l=0, r=0, t=40, b=0), xaxis_title="")
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("Sem comunicações para exibir.")

    with st.expander("Tabela de UCs interessadas filtradas"):
        show_cols = [
            "NUM_UC", "IND_SITUACAO", "DTH_INTERESSE", "MUNICIPIO", "PLANO_DETALHADO",
            "PRAZO_PLANO", "INFORME", "BANDEIRA", "RURAL", "TEM_COMUNICACAO"
        ]
        st.dataframe(
            f_int[show_cols].sort_values(["MUNICIPIO", "NUM_UC"], ascending=[True, True]),
            width="stretch",
            height=350
        )

    with st.expander("Tabela de comunicações filtradas"):
        show_cols = [
            "NUM_UC", "Data", "Canal", "Template_Acao_Grupo", "Mensagens",
            "MUNICIPIO", "PLANO_DETALHADO", "PRAZO_PLANO", "INFORME", "BANDEIRA", "RURAL"
        ]
        st.dataframe(
            f_com[show_cols].sort_values(["Data", "NUM_UC"], ascending=[False, True]),
            width="stretch",
            height=350
        )


if __name__ == "__main__":
    main()
