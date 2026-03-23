# BI Comunicação Plano Fixo

App em Streamlit para explorar:
- mapa por município
- filtros por município, prazo do plano, informe, bandeira, rural e plano detalhado
- filtro por template / ação
- gráficos acumulados de interesses e comunicações
- exportação CSV dos dados filtrados

## Arquivos esperados
Coloque estes arquivos na mesma pasta do app:
- `app.py`
- `df_interessados.csv`
- `df_COM_EMAIL.csv`
- `df_COM_IM.csv`

## Rodar localmente
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Publicar no GitHub / Streamlit Community Cloud
1. Suba `app.py`, `requirements.txt`, `README.md` e os CSVs para o repositório.
2. No Streamlit Community Cloud, conecte o repositório.
3. Defina `app.py` como arquivo principal.
4. Deploy.

## Observação
Os filtros de município/plano/informe nas comunicações dependem da ligação por UC com `df_interessados`, porque os CSVs brutos de comunicação não trazem município e plano nativamente.
