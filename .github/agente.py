import asyncio
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from playwright.async_api import async_playwright
from datetime import datetime

# ─── CONFIGURAÇÕES ────────────────────────────────────────────────────────────
EMAIL_REMETENTE = os.environ["EMAIL_REMETENTE"]
EMAIL_SENHA     = os.environ["EMAIL_SENHA"]
EMAIL_DESTINO   = "pedro.cazelli@essenzsolucoes.com"
URL             = "https://imers-o-dados-jan-2026-cazelli.streamlit.app/"

# ─── ENVIAR E-MAIL DE SUCESSO ─────────────────────────────────────────────────
def enviar_email_sucesso(horario: str):
    msg = MIMEMultipart()
    msg["From"]    = EMAIL_REMETENTE
    msg["To"]      = EMAIL_DESTINO
    msg["Subject"] = "⚠️ App Streamlit estava dormindo — acordado com sucesso"

    corpo = f"""
    <html><body>
    <p>Olá, Pedro!</p>
    <p>O monitoramento identificou que o app Streamlit estava <b>inativo (sleeping)</b>.</p>
    <p><b>Horário da detecção:</b> {horario}</p>
    <p><b>URL:</b> <a href="{URL}">{URL}</a></p>
    <p>O botão <i>"Yes, get this app back up!"</i> foi clicado automaticamente.</p>
    <br>
    <p>— Agente de Monitoramento</p>
    </body></html>
    """
    msg.attach(MIMEText(corpo, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
        servidor.login(EMAIL_REMETENTE, EMAIL_SENHA)
        servidor.sendmail(EMAIL_REMETENTE, EMAIL_DESTINO, msg.as_string())

    print(f"[{horario}] ✅ E-mail de sucesso enviado.")

# ─── ENVIAR E-MAIL DE ERRO ────────────────────────────────────────────────────
def enviar_email_erro(horario: str, erro: str):
    msg = MIMEMultipart()
    msg["From"]    = EMAIL_REMETENTE
    msg["To"]      = EMAIL_DESTINO
    msg["Subject"] = "❌ Erro no Agente de Monitoramento Streamlit"

    corpo = f"""
    <html><body>
    <p>Olá, Pedro!</p>
    <p>O agente de monitoramento encontrou um <b>erro inesperado</b> durante a execução.</p>
    <p><b>Horário do erro:</b> {horario}</p>
    <p><b>URL monitorada:</b> <a href="{URL}">{URL}</a></p>
    <p><b>Descrição do erro:</b></p>
    <pre style="background:#f4f4f4;padding:10px;border-radius:5px;">{erro}</pre>
    <p>O site pode estar fora do ar ou inacessível. Verifique manualmente.</p>
    <br>
    <p>— Agente de Monitoramento</p>
    </body></html>
    """
    msg.attach(MIMEText(corpo, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
        servidor.login(EMAIL_REMETENTE, EMAIL_SENHA)
        servidor.sendmail(EMAIL_REMETENTE, EMAIL_DESTINO, msg.as_string())

    print(f"[{horario}] ❌ E-mail de erro enviado.")

# ─── VERIFICAR E ACORDAR O SITE ───────────────────────────────────────────────
async def verificar_site():
    horario = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"[{horario}] Verificando site...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto(URL, timeout=60000)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(5)

            botao = page.get_by_role("button", name="Yes, get this app back up!")

            if await botao.is_visible():
                print(f"[{horario}] 😴 App dormindo! Clicando no botão...")
                await botao.click()
                await asyncio.sleep(5)
                enviar_email_sucesso(horario)
            else:
                print(f"[{horario}] ✅ App está ativo. Nenhuma ação necessária.")

        except Exception as e:
            erro_msg = str(e)
            print(f"[{horario}] ❌ Erro: {erro_msg}")
            enviar_email_erro(horario, erro_msg)

        finally:
            await browser.close()

# ─── ENTRY POINT ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(verificar_site())
