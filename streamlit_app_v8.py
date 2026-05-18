import re
from pathlib import Path
import base64
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

try:
    from streamlit_dynamic_filters import DynamicFilters
except ImportError:  # pragma: no cover - handled at runtime in Streamlit
    DynamicFilters = None

st.set_page_config(page_title="BI Comunicação Fatura Fixa", page_icon="📊", layout="wide")

APP_DIR = Path(__file__).parent.resolve()
DATA_SEARCH_DIRS = [
    APP_DIR,
    APP_DIR / "data",
    APP_DIR.parent,
    APP_DIR.parent / "data",
]

INTEREST_STATUSES = {"A", "I", "R", "D", "X"}
NO_COMM_LABEL = "Sem comunicação no período"
FILTERS_NAME = "bi_comunicacao_filters"

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


FILTER_DISPLAY_COLUMNS = {
    "MUNICIPIO": "Município",
    "PLANO_DETALHADO": "Planos",
    "PRAZO_PLANO": "Acerto",
    "INFORME": "Informe",
    "BANDEIRA": "Bandeira",
    "RURAL": "Rural",
    "Canal": "Canal de comunicação",
    "Template_Acao_Grupo": "Template / Ação",
}
FILTER_COLUMNS = list(FILTER_DISPLAY_COLUMNS.values())


# Custos gerais carregados de CSV.
# Coloque o arquivo custos_comunicacao.csv na mesma pasta do app ou na pasta data/.
COSTS_FILENAME = "custos_comunicacao.csv"
COST_TYPE_MESSAGING = "Mensageria"
COST_TYPE_MEDIA = "Mídia"

def apply_header_css():
    st.markdown(
        """
        <style>
            .stAppViewContainer .main .block-container {
                padding-top: 0.8rem;
            }
            .custom-header-wrap {
                width: 100%;
                margin: 0 0 0.35rem 0;
                padding: 0;
            }
            .custom-header-img {
                width: 100%;
                display: block;
                border-radius: 0;
            }
            h1 {
                margin-top: 0.15rem !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_header_image(image_name: str = "Header.png"):
    image_path = APP_DIR / image_name
    if image_path.exists():
        mime = "image/png" if image_path.suffix.lower() == ".png" else "image/jpeg"
        encoded = base64.b64encode(image_path.read_bytes()).decode("utf-8")
        st.markdown(
            f"""
            <div class="custom-header-wrap">
                <img class="custom-header-img" src="data:{mime};base64,{encoded}" alt="Header" />
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.warning(f"Imagem de header não encontrada em: {image_path}")



def find_parquet(filename: str) -> Path:
    for base in DATA_SEARCH_DIRS:
        candidate = base / filename
        if candidate.exists():
            return candidate
    searched = "\n".join(str(p / filename) for p in DATA_SEARCH_DIRS)
    raise FileNotFoundError(f"Arquivo não encontrado: {filename}\nLocais buscados:\n{searched}")


def find_data_file(filename: str) -> Path:
    for base in DATA_SEARCH_DIRS:
        candidate = base / filename
        if candidate.exists():
            return candidate
    searched = "\n".join(str(p / filename) for p in DATA_SEARCH_DIRS)
    raise FileNotFoundError(f"Arquivo não encontrado: {filename}\nLocais buscados:\n{searched}")


@st.cache_data(show_spinner=False)
def load_costs_csv() -> pd.DataFrame:
    costs = pd.read_csv(find_data_file(COSTS_FILENAME), sep=";", decimal=",", encoding="utf-8-sig")

    required_cols = {
        "tipo",
        "item",
        "canal",
        "envios_reportados",
        "cliques",
        "investimento",
        "custo_unitario_reportado",
        "base_calculo",
    }
    missing = required_cols - set(costs.columns)
    if missing:
        raise ValueError(f"Colunas ausentes em {COSTS_FILENAME}: {sorted(missing)}")

    for col in ["tipo", "item", "canal", "base_calculo"]:
        costs[col] = costs[col].fillna("").astype(str).str.strip()

    for col in ["envios_reportados", "cliques", "investimento", "custo_unitario_reportado"]:
        costs[col] = pd.to_numeric(costs[col], errors="coerce")

    return costs


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


def clear_dynamic_filters(filters_name: str = FILTERS_NAME):
    for key in list(st.session_state.keys()):
        if filters_name in key:
            st.session_state.pop(key)


def build_shared_filter_base(interessados_date: pd.DataFrame, comunicacoes_date: pd.DataFrame) -> pd.DataFrame:
    dim_cols = [
        "NUM_UC",
        "MUNICIPIO",
        "PLANO_DETALHADO",
        "PRAZO_PLANO",
        "INFORME",
        "BANDEIRA",
        "RURAL",
    ]

    interested_dims = interessados_date[dim_cols].dropna(subset=["NUM_UC"]).drop_duplicates(subset=["NUM_UC"])
    comm_dims = comunicacoes_date[dim_cols].dropna(subset=["NUM_UC"]).drop_duplicates(subset=["NUM_UC"])

    uc_dims = (
        pd.concat([interested_dims.assign(_priority=0), comm_dims.assign(_priority=1)], ignore_index=True)
        .sort_values(["NUM_UC", "_priority"])
        .drop_duplicates(subset=["NUM_UC"], keep="first")
        .drop(columns="_priority")
    )

    com_base = comunicacoes_date[
        ["_COMM_ROW_ID", "NUM_UC", "Canal", "Template_Acao_Grupo"]
    ].copy()
    com_base = com_base.merge(uc_dims, on="NUM_UC", how="left")
    com_base["_ROW_KIND"] = "comunicacao"

    no_comm_ucs = interested_dims.loc[
        ~interested_dims["NUM_UC"].isin(comunicacoes_date["NUM_UC"].dropna().unique())
    ].copy()
    no_comm_ucs["_COMM_ROW_ID"] = pd.NA
    no_comm_ucs["Canal"] = NO_COMM_LABEL
    no_comm_ucs["Template_Acao_Grupo"] = NO_COMM_LABEL
    no_comm_ucs["_ROW_KIND"] = "sem_comunicacao"

    shared = pd.concat([com_base, no_comm_ucs], ignore_index=True, sort=False)

    for source_col, display_col in FILTER_DISPLAY_COLUMNS.items():
        fill_value = NO_COMM_LABEL if source_col in {"Canal", "Template_Acao_Grupo"} else "Não informado"
        shared[display_col] = shared[source_col].fillna(fill_value).astype(str).str.strip()

    return shared


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

    if isinstance(date_range, tuple) and len(date_range) == 2:
        dt_ini = pd.Timestamp(date_range[0])
        dt_fim = pd.Timestamp(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    else:
        dt_ini, dt_fim = min_date, max_date

    interessados_date = interessados[
        (interessados["DTH_INTERESSE"] >= dt_ini) & (interessados["DTH_INTERESSE"] <= dt_fim)
    ].copy()

    comunicacoes_date = comunicacoes[
        (comunicacoes["Data"] >= dt_ini) & (comunicacoes["Data"] <= dt_fim)
    ].copy()
    comunicacoes_date["_COMM_ROW_ID"] = np.arange(len(comunicacoes_date), dtype=int)

    if DynamicFilters is None:
        st.sidebar.error(
            "Pacote streamlit-dynamic-filters não encontrado. Instale com: pip install streamlit-dynamic-filters"
        )
        return interessados_date, comunicacoes_date

    shared_filter_base = build_shared_filter_base(interessados_date, comunicacoes_date)

    with st.sidebar:
        st.button("Limpar filtros", on_click=clear_dynamic_filters)

    if shared_filter_base.empty:
        return interessados_date.iloc[0:0].copy(), comunicacoes_date.iloc[0:0].copy()

    dynamic_filters = DynamicFilters(
        shared_filter_base,
        filters=FILTER_COLUMNS,
        filters_name=FILTERS_NAME,
    )
    dynamic_filters.display_filters(location="sidebar")
    filtered_base = dynamic_filters.filter_df().copy()

    selected_ucs = filtered_base["NUM_UC"].dropna().unique()
    f_int = interessados_date[interessados_date["NUM_UC"].isin(selected_ucs)].copy()

    selected_comm_ids = (
        filtered_base.loc[filtered_base["_ROW_KIND"] == "comunicacao", "_COMM_ROW_ID"]
        .dropna()
        .astype(int)
        .unique()
    )
    f_com = comunicacoes_date[comunicacoes_date["_COMM_ROW_ID"].isin(selected_comm_ids)].copy()
    f_com = f_com.drop(columns=["_COMM_ROW_ID"], errors="ignore")

    return f_int, f_com


def metric_card(label: str, value: str, help_text: str = ""):
    st.metric(label=label, value=value, help=help_text)


def format_int(value) -> str:
    return f"{int(value):,}".replace(",", ".")


def format_pct(value: float) -> str:
    return f"{value:,.2f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def render_metric_block(title: str, metrics: list[tuple[str, str, str]], n_cols: int = 7):
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
        marker=dict(opacity=0.8),
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

    daily = (
        base.groupby(["Data", "PLANO_DETALHADO", "PRAZO_PLANO"], as_index=False)["NUM_UC"]
        .nunique()
        .rename(columns={"NUM_UC": "UCs interessadas"})
    )

    all_dates = pd.date_range(start=base["Data"].min(), end=base["Data"].max(), freq="D")
    plans = daily[["PLANO_DETALHADO", "PRAZO_PLANO"]].drop_duplicates()

    full_index = (
        plans.assign(_key=1)
        .merge(pd.DataFrame({"Data": all_dates, "_key": 1}), on="_key")
        .drop(columns="_key")
    )

    full_daily = (
        full_index.merge(daily, on=["Data", "PLANO_DETALHADO", "PRAZO_PLANO"], how="left")
        .fillna({"UCs interessadas": 0})
        .sort_values(["PLANO_DETALHADO", "Data"])
    )

    full_daily["Acumulado"] = full_daily.groupby("PLANO_DETALHADO")["UCs interessadas"].cumsum()

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
    base = df.dropna(subset=["Data", "Canal"]).copy()

    if base.empty:
        st.info("Sem comunicações no período.")
        return

    base["Data"] = base["Data"].dt.floor("D")

    # Daily messages by channel
    daily = (
        base.groupby(["Data", "Canal"], as_index=False)["Mensagens"]
        .sum()
    )

    # Full date range from first to last date in the filtered data
    all_dates = pd.date_range(
        start=base["Data"].min(),
        end=base["Data"].max(),
        freq="D"
    )

    # One row per channel
    canais = daily[["Canal"]].drop_duplicates()

    # Cartesian product: every date x every channel
    full_index = (
        canais.assign(_key=1)
        .merge(pd.DataFrame({"Data": all_dates, "_key": 1}), on="_key")
        .drop(columns="_key")
    )

    # Fill missing days with zero messages
    full_daily = (
        full_index.merge(daily, on=["Data", "Canal"], how="left")
        .fillna({"Mensagens": 0})
        .sort_values(["Canal", "Data"])
    )

    # Continuous cumulative line for every channel
    full_daily["Acumulado"] = (
        full_daily.groupby("Canal")["Mensagens"].cumsum()
    )

    fig = px.line(full_daily, x="Data", y="Acumulado", color="Canal")
    fig.update_layout(
        margin=dict(l=0, r=0, t=20, b=0),
        yaxis_title="Mensagens acumuladas",
        xaxis_title=""
    )
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



def format_currency(value) -> str:
    value = 0 if pd.isna(value) else float(value)
    return "R$ " + f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def build_cost_context(
    comunicacoes: pd.DataFrame,
    costs_df: pd.DataFrame,
    total_ucs_interessadas: int,
) -> dict:
    """Build cost totals and rates from custos_comunicacao.csv."""
    messaging_costs = costs_df[
        (costs_df["tipo"] == COST_TYPE_MESSAGING)
        & costs_df["canal"].ne("")
    ].copy()
    media_costs = costs_df[costs_df["tipo"] == COST_TYPE_MEDIA].copy()

    costed_message_channels = messaging_costs["canal"].dropna().astype(str).unique().tolist()

    known = comunicacoes[
        comunicacoes["Canal"].isin(costed_message_channels)
        & comunicacoes["NUM_UC"].notna()
    ].copy()

    known_messages_by_channel = (
        known.groupby("Canal", as_index=False)["Mensagens"]
        .sum()
        .set_index("Canal")["Mensagens"]
        .to_dict()
    )

    rates_known = {}
    messages_without_user_report = 0.0
    for _, row in messaging_costs.iterrows():
        channel = row["canal"]
        known_messages = float(known_messages_by_channel.get(channel, 0))
        reported_messages = float(row["envios_reportados"]) if pd.notna(row["envios_reportados"]) else 0.0
        total_cost = float(row["investimento"]) if pd.notna(row["investimento"]) else 0.0
        rates_known[channel] = total_cost / known_messages if known_messages > 0 else 0
        messages_without_user_report += max(reported_messages - known_messages, 0)

    known_costed_messages = sum(float(known_messages_by_channel.get(ch, 0)) for ch in costed_message_channels)
    total_messaging_reported_sends = messaging_costs["envios_reportados"].fillna(0).sum()
    total_messaging_cost = messaging_costs["investimento"].fillna(0).sum()
    total_media_cost = media_costs["investimento"].fillna(0).sum()
    total_general_cost = total_messaging_cost + total_media_cost
    avrg_cost_per_UC = total_general_cost/total_ucs_interessadas if total_ucs_interessadas > 0 else 0
    avrg_cost_per_UC_msg = total_messaging_cost/total_ucs_interessadas if total_ucs_interessadas > 0 else 0

    return {
        "costs_df": costs_df,
        "messaging_costs": messaging_costs,
        "media_costs": media_costs,
        "costed_message_channels": costed_message_channels,
        "known_messages_by_channel": known_messages_by_channel,
        "rates_known": rates_known,
        "known_costed_messages": known_costed_messages,
        "messages_without_user_report": messages_without_user_report,
        "total_messaging_reported_sends": total_messaging_reported_sends,
        "total_messaging_cost": total_messaging_cost,
        "total_media_cost": total_media_cost,
        "total_general_cost": total_general_cost,
        "cost_per_UC": avrg_cost_per_UC,
        "cost_per_UC_msg": avrg_cost_per_UC_msg,
    }


def estimate_messaging_cost(df: pd.DataFrame, cost_context: dict) -> float:
    if df.empty:
        return 0.0
    costed_channels = cost_context["costed_message_channels"]
    by_channel = (
        df[df["Canal"].isin(costed_channels)]
        .groupby("Canal", as_index=False)["Mensagens"]
        .sum()
    )
    total = 0.0
    for _, row in by_channel.iterrows():
        total += float(row["Mensagens"]) * float(cost_context["rates_known"].get(row["Canal"], 0))
    return total


def add_cost_columns(df: pd.DataFrame, cost_context: dict) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        df["Custo estimado"] = []
        return df
    df["Custo unitário com UC conhecida"] = df["Canal"].map(cost_context["rates_known"]).fillna(0)
    df["Custo estimado"] = df["Mensagens"].astype(float) * df["Custo unitário com UC conhecida"].astype(float)
    return df


def render_cost_report(cost_context: dict):
    st.markdown("### Custos gerais")
    render_metric_block(
        "",
        [
            ("Custo Total Geral", format_currency(cost_context["total_general_cost"]), "Mensagens + Mídia"),
            ("Custo de Mensagens", format_currency(cost_context["total_messaging_cost"]), "Push + SMS + WhatsApp"),
            ("Investimento em Mídia", format_currency(cost_context["total_media_cost"]), "Custo total com Meta + Google + OneStation + Rádio e TV"),
            ("Mensagens Total", format_int(cost_context["total_messaging_reported_sends"]), "Total oficial de envios contratados."),
            ("Mensagens com UC", format_int(cost_context["known_costed_messages"]), "Mensagens presentes no relatório"),
            ("Mensagens sem UC", format_int(cost_context["messages_without_user_report"]), "Diferença entre os ralatórios."),
            ("Custo Total por UC", format_currency(cost_context["cost_per_UC"]), "Custo por mensagem total"),
            ("Custo Mensagem por UC", format_currency(cost_context["cost_per_UC_msg"]), "Custo por mensagem total"),
        ],
        n_cols=4,
    )

    cost_rows = []
    for _, row in cost_context["messaging_costs"].iterrows():
        channel = row["canal"]
        reported = float(row["envios_reportados"]) if pd.notna(row["envios_reportados"]) else 0.0
        known_msgs = float(cost_context["known_messages_by_channel"].get(channel, 0))
        cost_rows.append(
            {
                "Canal": channel,
                "Envios": reported,
                "Mensagens com UC conhecida": known_msgs,
                "Mensagens sem UC no relatório": max(reported - known_msgs, 0),
                "Custo total": row["investimento"],
                "Custo/envio": row["custo_unitario_reportado"],                
            }
        )

    with st.expander("Detalhamento de custos de mensageria"):
        st.dataframe(
            pd.DataFrame(cost_rows),
            width="stretch",
            height=180,
            column_config={
                "Envios reportados": st.column_config.NumberColumn(
                    "Envios reportados",
                    format="%d",
                ),
                "Mensagens com UC conhecida": st.column_config.NumberColumn(
                    "Mensagens com UC conhecida",
                    format="%d",
                ),
                "Mensagens sem UC no report": st.column_config.NumberColumn(
                    "Mensagens sem UC no report",
                    format="%d",
                ),
                "Custo total": st.column_config.NumberColumn(
                    "Custo total",
                    format="R$ %.2f",
                ),
                "Custo/envio": st.column_config.NumberColumn(
                    "Custo/envio",
                    format="R$ %.2f",
                ),                
            },
        )

    with st.expander("Detalhamento de investimento em mídia"):
        show_cols = ["item", "cliques", "investimento", "custo_unitario_reportado", "base_calculo"]
        media_detail = cost_context["media_costs"][show_cols].rename(
            columns={
                "item": "Mídia",
                "cliques": "Cliques",
                "investimento": "Investimento",
                "custo_unitario_reportado": "Custo por clique",
                "base_calculo": "Base de cálculo",
            }
        )
        
        st.dataframe(
            media_detail,
            width="stretch",
            height=240,
            column_config={
                "Cliques": st.column_config.NumberColumn(
                    "Cliques",
                    format="%d",
                ),
                "Investimento": st.column_config.NumberColumn(
                    "Investimento",
                    format="R$ %.2f",
                ),
                "Custo por clique": st.column_config.NumberColumn(
                    "Custo por clique",
                    format="R$ %.4f",
                ),
            },
        )


def filter_by_date_without_dynamic_filters(interessados: pd.DataFrame, comunicacoes: pd.DataFrame, key_prefix: str):
    min_date = min(
        x for x in [interessados["DTH_INTERESSE"].min(), comunicacoes["Data"].min()] if pd.notna(x)
    )
    max_date = max(
        x for x in [interessados["DTH_INTERESSE"].max(), comunicacoes["Data"].max()] if pd.notna(x)
    )
    date_range = st.date_input(
        "Período",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
        key=f"{key_prefix}_date_range",
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        dt_ini = pd.Timestamp(date_range[0])
        dt_fim = pd.Timestamp(date_range[1]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    else:
        dt_ini, dt_fim = min_date, max_date

    f_int = interessados[
        (interessados["DTH_INTERESSE"] >= dt_ini) & (interessados["DTH_INTERESSE"] <= dt_fim)
    ].copy()
    f_com = comunicacoes[
        (comunicacoes["Data"] >= dt_ini) & (comunicacoes["Data"] <= dt_fim)
    ].copy()
    return f_int, f_com


def get_comparison_options(interessados: pd.DataFrame, comunicacoes: pd.DataFrame, group_col: str) -> list[str]:
    values = []
    if group_col in interessados.columns:
        values.extend(interessados[group_col].dropna().astype(str).unique().tolist())
    if group_col in comunicacoes.columns:
        values.extend(comunicacoes[group_col].dropna().astype(str).unique().tolist())
    return sorted(set(values))


def slice_for_comparison_value(
    interessados: pd.DataFrame,
    comunicacoes: pd.DataFrame,
    group_col: str,
    value: str,
):
    if group_col in comunicacoes.columns:
        c_slice = comunicacoes[comunicacoes[group_col].astype(str).eq(str(value))].copy()
    else:
        c_slice = comunicacoes.iloc[0:0].copy()

    if group_col in interessados.columns:
        i_slice = interessados[interessados[group_col].astype(str).eq(str(value))].copy()
    else:
        selected_ucs = c_slice["NUM_UC"].dropna().unique()
        i_slice = interessados[interessados["NUM_UC"].isin(selected_ucs)].copy()

    return i_slice, c_slice


def build_comparison_summary(
    interessados: pd.DataFrame,
    comunicacoes: pd.DataFrame,
    group_col: str,
    selected_values: list[str],
    cost_context: dict,
) -> pd.DataFrame:
    rows = []
    for value in selected_values:
        i_slice, c_slice = slice_for_comparison_value(interessados, comunicacoes, group_col, value)
        interested_base = i_slice[i_slice["IND_SITUACAO"].isin(INTEREST_STATUSES)]
        with_contact = interested_base[interested_base["TEM_COMUNICACAO"].fillna(False)]
        without_contact = interested_base[~interested_base["TEM_COMUNICACAO"].fillna(False)]
        messages_by_channel = (
            c_slice.groupby("Canal", as_index=False)["Mensagens"]
            .sum()
            .set_index("Canal")["Mensagens"]
            .to_dict()
        )
        estimated_cost = estimate_messaging_cost(c_slice, cost_context)
        total_messages = float(c_slice["Mensagens"].sum()) if not c_slice.empty else 0.0

        rows.append(
            {
                "Grupo": value,
                "UCs interessadas": interested_base["NUM_UC"].dropna().nunique(),
                "UCs interessadas com contato": with_contact["NUM_UC"].dropna().nunique(),
                "UCs interessadas sem contato": without_contact["NUM_UC"].dropna().nunique(),
                "Mensagens totais": total_messages,
                "Mensagens Push": float(messages_by_channel.get("Push", 0)),
                "Mensagens SMS": float(messages_by_channel.get("SMS", 0)),
                "Mensagens WhatsApp": float(messages_by_channel.get("WhatsApp", 0)),
                "Mensagens Email": float(messages_by_channel.get("Email", 0)),
                "Custo mensageria estimado": estimated_cost,
                "Custo por mensagem conhecida": estimated_cost / total_messages if total_messages > 0 else 0,
            }
        )
    return pd.DataFrame(rows)


def plot_comparison_bars(summary: pd.DataFrame):
    if summary.empty:
        st.info("Selecione ao menos uma opção para comparar.")
        return
    metrics = [
        "UCs interessadas",
        "UCs interessadas com contato",
        "UCs interessadas sem contato",
        "Mensagens totais",
        "Custo mensageria estimado",
    ]
    long_df = summary.melt(id_vars="Grupo", value_vars=metrics, var_name="Métrica", value_name="Valor")
    fig = px.bar(long_df, x="Grupo", y="Valor", color="Métrica", barmode="group")
    fig.update_layout(title="Comparativo de métricas", margin=dict(l=0, r=0, t=40, b=0), xaxis_title="")
    st.plotly_chart(fig, width="stretch")


def plot_comparison_cumulative_interested(interessados: pd.DataFrame, group_col: str, selected_values: list[str]):
    if group_col not in interessados.columns:
        st.info("Para Canal e Template/Ação, o gráfico de UCs interessadas usa as UCs vinculadas às comunicações; veja a tabela de resumo.")
        return
    base = interessados[
        interessados[group_col].astype(str).isin([str(v) for v in selected_values])
        & interessados["IND_SITUACAO"].isin(INTEREST_STATUSES)
    ].dropna(subset=["DTH_INTERESSE"]).copy()
    if base.empty:
        st.info("Sem UCs interessadas para o comparativo selecionado.")
        return
    base["Data"] = base["DTH_INTERESSE"].dt.floor("D")
    base["Grupo"] = base[group_col].astype(str)
    daily = base.groupby(["Data", "Grupo"], as_index=False)["NUM_UC"].nunique().rename(columns={"NUM_UC": "UCs"})
    all_dates = pd.date_range(base["Data"].min(), base["Data"].max(), freq="D")
    groups = pd.DataFrame({"Grupo": selected_values}).astype(str)
    full_index = groups.assign(_key=1).merge(pd.DataFrame({"Data": all_dates, "_key": 1}), on="_key").drop(columns="_key")
    full_daily = full_index.merge(daily, on=["Data", "Grupo"], how="left").fillna({"UCs": 0}).sort_values(["Grupo", "Data"])
    full_daily["Acumulado"] = full_daily.groupby("Grupo")["UCs"].cumsum()
    fig = px.line(full_daily, x="Data", y="Acumulado", color="Grupo", markers=True)
    fig.update_layout(title="UCs interessadas acumuladas", margin=dict(l=0, r=0, t=40, b=0), xaxis_title="", yaxis_title="UCs")
    st.plotly_chart(fig, width="stretch")


def plot_comparison_cumulative_messages(comunicacoes: pd.DataFrame, group_col: str, selected_values: list[str]):
    if group_col not in comunicacoes.columns:
        st.info("Sem coluna de comparação nas comunicações para este gráfico.")
        return
    base = comunicacoes[
        comunicacoes[group_col].astype(str).isin([str(v) for v in selected_values])
    ].dropna(subset=["Data"]).copy()
    if base.empty:
        st.info("Sem mensagens para o comparativo selecionado.")
        return
    base["Data"] = base["Data"].dt.floor("D")
    base["Grupo"] = base[group_col].astype(str)
    daily = base.groupby(["Data", "Grupo"], as_index=False)["Mensagens"].sum()
    all_dates = pd.date_range(base["Data"].min(), base["Data"].max(), freq="D")
    groups = pd.DataFrame({"Grupo": selected_values}).astype(str)
    full_index = groups.assign(_key=1).merge(pd.DataFrame({"Data": all_dates, "_key": 1}), on="_key").drop(columns="_key")
    full_daily = full_index.merge(daily, on=["Data", "Grupo"], how="left").fillna({"Mensagens": 0}).sort_values(["Grupo", "Data"])
    full_daily["Acumulado"] = full_daily.groupby("Grupo")["Mensagens"].cumsum()
    fig = px.line(full_daily, x="Data", y="Acumulado", color="Grupo")
    fig.update_layout(title="Mensagens acumuladas", margin=dict(l=0, r=0, t=40, b=0), xaxis_title="", yaxis_title="Mensagens")
    st.plotly_chart(fig, width="stretch")


def render_overview_page(interessados: pd.DataFrame, comunicacoes: pd.DataFrame, costs_df: pd.DataFrame):
    total_ucs_interessadas = interessados["NUM_UC"].dropna().nunique()
    cost_context = build_cost_context(comunicacoes, costs_df, total_ucs_interessadas)

    f_int, f_com = apply_filters(interessados, comunicacoes)
    total_contacted_ucs = comunicacoes["NUM_UC"].dropna().nunique()
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
    total_interested_without_contact = (
        interessados[
            (~interessados["TEM_COMUNICACAO"].fillna(False))
            & interessados["IND_SITUACAO"].isin(INTEREST_STATUSES)
        ]["NUM_UC"]
        .dropna()
        .nunique()
    )

    interested_filtered = f_int[f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)]["NUM_UC"].dropna().nunique()
    interested_with_contact_filtered = (
        f_int[f_int["TEM_COMUNICACAO"] & f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)]["NUM_UC"]
        .dropna()
        .nunique()
    )
    interested_without_contact_filtered = (
        f_int[(~f_int["TEM_COMUNICACAO"].fillna(False)) & f_int["IND_SITUACAO"].isin(INTEREST_STATUSES)]["NUM_UC"]
        .dropna()
        .nunique()
    )
    pct_of_total_interested = (
        100 * interested_with_contact_filtered / total_ucs_interessadas if total_ucs_interessadas else 0
    )
    pct_of_total_interested_without = (
        100 * interested_without_contact_filtered / total_ucs_interessadas if total_ucs_interessadas else 0
    )

    filtered_messages = f_com["Mensagens"].sum()
    filtered_messages_by_channel = (
        f_com.groupby("Canal", as_index=False)["Mensagens"]
        .sum()
        .set_index("Canal")["Mensagens"]
        .to_dict()
    )
    total_messages = comunicacoes.loc[
        comunicacoes["Canal"].isin(["Email", "WhatsApp", "SMS", "Push"]),
        "Mensagens"
    ].sum()

    render_metric_block(
        "Total",
        [
            ("UCs contactadas", format_int(total_contacted_ucs), "Total de UCs que receberam algum contato, sem aplicar filtros."),
            ("Total de UCs interessadas", format_int(total_ucs_interessadas), "Total de UCs que demonstraram interesse."),
            ("UCs interessadas com contato", format_int(total_interested_with_contact), "UCs com pelo menos uma comunicação anterior ao interesse, sem aplicar filtros."),
            ("UCs interessadas sem contato", format_int(total_interested_without_contact), "UCs sem comunicação anterior ao interesse, sem aplicar filtros."),
        ],
        n_cols=4,
    )

    render_metric_block(
        "",
        [
            ("Mensagens totais", format_int(total_messages), "Total de mensagens enviadas, sem aplicar filtros."),
            ("Mensagens por Email", format_int(total_messages_by_channel.get("Email", 0)), "Total de mensagens de Email, sem aplicar filtros."),
            ("Mensagens por WhatsApp", format_int(total_messages_by_channel.get("WhatsApp", 0)), "Total de mensagens de WhatsApp, sem aplicar filtros."),
            ("Mensagens por SMS", format_int(total_messages_by_channel.get("SMS", 0)), "Total de mensagens de SMS, sem aplicar filtros."),
            ("Mensagens por Push", format_int(total_messages_by_channel.get("Push", 0)), "Total de mensagens de Push, sem aplicar filtros."),
        ],
        n_cols=5,
    )

    render_cost_report(cost_context)

    render_metric_block(
        "Filtrado",
        [
            ("UCs interessadas no filtro", format_int(interested_filtered), "UCs interessadas dentro dos filtros atuais."),
            ("UCs interessadas com contato", format_int(interested_with_contact_filtered), "UCs interessadas com pelo menos uma comunicação anterior à data de interesse dentro dos filtros."),
            ("% do total de UCs interessadas", format_pct(pct_of_total_interested), "Percentual das UCs interessadas com contato sobre o total."),
            ("UCs interessadas sem contato", format_int(interested_without_contact_filtered), "UCs interessadas sem nenhuma comunicação anterior à data de interesse dentro dos filtros."),
            ("% do total de UCs interessadas sem contato", format_pct(pct_of_total_interested_without), "Percentual das UCs interessadas sem contato sobre o total."),
            ("Total Mensagens filtradas", format_int(filtered_messages), "Total de mensagens após os filtros."),
            ("Custo mensageria filtrado", format_currency(estimate_messaging_cost(f_com, cost_context)), "Custo estimado com base no custo por mensagem com UC conhecida."),
            ("Mensagens por Email", format_int(filtered_messages_by_channel.get("Email", 0)), "Mensagens de Email após os filtros."),
            ("Mensagens por WhatsApp", format_int(filtered_messages_by_channel.get("WhatsApp", 0)), "Mensagens de WhatsApp após os filtros."),
            ("Mensagens por SMS", format_int(filtered_messages_by_channel.get("SMS", 0)), "Mensagens de SMS após os filtros."),
            ("Mensagens por Push", format_int(filtered_messages_by_channel.get("Push", 0)), "Mensagens de Push após os filtros."),
        ],
        n_cols=4,
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
            by_canal = f_com.groupby("Canal", as_index=False)["Mensagens"].sum().sort_values("Mensagens", ascending=False)
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
            height=350,
        )

    with st.expander("Tabela de comunicações filtradas"):
        show_cols = [
            "NUM_UC", "Data", "Canal", "Template_Acao_Grupo", "Mensagens",
            "MUNICIPIO", "PLANO_DETALHADO", "PRAZO_PLANO", "INFORME", "BANDEIRA", "RURAL"
        ]
        comm_report = add_cost_columns(f_com[show_cols].copy(), cost_context)
        st.dataframe(
            comm_report.sort_values(["Data", "NUM_UC"], ascending=[False, True]),
            width="stretch",
            height=350,
        )


def render_comparison_page(interessados: pd.DataFrame, comunicacoes: pd.DataFrame, costs_df: pd.DataFrame):
    total_ucs_interessadas = interessados["NUM_UC"].dropna().nunique()
    cost_context = build_cost_context(comunicacoes, costs_df, total_ucs_interessadas)
    st.subheader("Comparativo entre grupos")
    st.caption("Selecione uma categoria e compare até três grupos em métricas, custos e evolução acumulada.")

    f_int, f_com = filter_by_date_without_dynamic_filters(interessados, comunicacoes, "comparison")

    comparison_categories = {
        "Município": "MUNICIPIO",
        "Plano": "PLANO_DETALHADO",
        "Acerto": "PRAZO_PLANO",
        "Informe": "INFORME",
        "Bandeira": "BANDEIRA",
        "Rural": "RURAL",
        "Canal": "Canal",
        "Template / Ação": "Template_Acao_Grupo",
    }

    col_a, col_b = st.columns([1, 2])
    with col_a:
        category_label = st.selectbox("Categoria", list(comparison_categories.keys()))
    group_col = comparison_categories[category_label]
    options = get_comparison_options(f_int, f_com, group_col)
    with col_b:
        selected_values = st.multiselect(
            "Selecione até 3 opções para comparar",
            options=options,
            max_selections=3,
        )

    if not selected_values:
        st.info("Selecione ao menos uma opção para gerar o comparativo.")
        return

    summary = build_comparison_summary(f_int, f_com, group_col, selected_values, cost_context)

    display_summary = summary.copy()
    for col in ["Mensagens totais", "Mensagens Push", "Mensagens SMS", "Mensagens WhatsApp", "Mensagens Email"]:
        display_summary[col] = display_summary[col].map(lambda x: format_int(x))
    for col in ["Custo mensageria estimado", "Custo por mensagem conhecida"]:
        display_summary[col] = display_summary[col].map(format_currency)

    st.markdown("### Resumo comparativo")
    st.dataframe(display_summary, width="stretch", height=180)

    plot_comparison_bars(summary)

    graph_col1, graph_col2 = st.columns(2)
    with graph_col1:
        plot_comparison_cumulative_interested(f_int, group_col, selected_values)
    with graph_col2:
        plot_comparison_cumulative_messages(f_com, group_col, selected_values)

    with st.expander("Detalhamento de mensagens e custos por grupo"):
        detail_rows = []
        for value in selected_values:
            _, c_slice = slice_for_comparison_value(f_int, f_com, group_col, value)
            c_costed = add_cost_columns(c_slice, cost_context)
            if c_costed.empty:
                continue
            by_channel = (
                c_costed.groupby("Canal", as_index=False)
                .agg(
                    Mensagens=("Mensagens", "sum"),
                    Custo_estimado=("Custo estimado", "sum"),
                    UCs=("NUM_UC", pd.Series.nunique),
                )
            )
            by_channel.insert(0, "Grupo", value)
            detail_rows.append(by_channel)
        if detail_rows:
            detail_df = pd.concat(detail_rows, ignore_index=True)
            st.dataframe(
                detail_df,
                width="stretch",
                height=300,
                column_config={
                    "Mensagens": st.column_config.NumberColumn(
                        "Mensagens",
                        format="%d",
                    ),
                    "Custo_estimado": st.column_config.NumberColumn(
                        "Custo estimado",
                        format="R$ %.2f",
                    ),
                    "UCs": st.column_config.NumberColumn(
                        "UCs",
                        format="%d",
                    ),
                },
            )
        else:
            st.info("Sem mensagens para detalhar.")

    with st.expander("Exportar relatório do comparativo"):
        st.download_button(
            label="Baixar resumo comparativo em CSV",
            data=summary.to_csv(index=False, sep=";", decimal=",", encoding="utf-8-sig"),
            file_name="comparativo_custos_comunicacao.csv",
            mime="text/csv",
        )


def main():
    apply_header_css()
    interessados, comunicacoes = load_data()
    costs_df = load_costs_csv()

    render_header_image("Header.png")
    st.title("BI Comunicação Fatura Fixa")

    page = st.sidebar.radio(
        "Página",
        ["Visão Geral", "Comparativo"],
        key="page_selector",
    )

    if page == "Visão Geral":
        render_overview_page(interessados, comunicacoes, costs_df)
    else:
        render_comparison_page(interessados, comunicacoes, costs_df)


if __name__ == "__main__":
    main()
