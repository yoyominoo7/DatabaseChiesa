import os
from flask import Flask, request
from datetime import time
import requests
from app import build_application, weekly_report
import asyncio

# --- Flask web server ---
flask_app = Flask(__name__)

# Costruisci l'applicazione Telegram
application = build_application()

# Pianifica i job settimanali
application.job_queue.run_daily(
    weekly_report,
    time=time(hour=9, minute=0),
    days=(0,),  # 0 = lunedì
    name="weekly_report_job"
)

@flask_app.route("/")
def home():
    return "Bot is running!"

# Endpoint webhook per Telegram
@flask_app.route(f"/{os.environ['TELEGRAM_BOT_TOKEN']}", methods=["POST"])
def webhook():
    update = request.get_json(force=True)
    # Processa direttamente l'update in modo asincrono
    asyncio.get_event_loop().create_task(application.process_update(update))
    return "OK", 200

def set_webhook():
    """Registra il webhook su Telegram all'avvio."""
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    external_url = os.environ.get("RENDER_EXTERNAL_URL")

    if not external_url:
        raise RuntimeError("RENDER_EXTERNAL_URL non impostato nelle variabili d'ambiente")

    webhook_url = f"{external_url}/{token}"
    print("Imposto webhook su:", webhook_url)

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/setWebhook",
            data={"url": webhook_url},
            timeout=10
        )
        print("Risposta Telegram:", resp.text)
    except Exception as e:
        print("Errore durante setWebhook:", e)

if __name__ == "__main__":
    # Registra il webhook su Telegram
    set_webhook()

    # Inizializza e avvia l'application (dispatcher + job queue)
    application.initialize()
    application.start()

    # Avvia Flask
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port)
