import os
from flask import Flask
from datetime import time
from app import build_application   # importa la funzione dal tuo app.py
from app import weekly_report       # importa la funzione weekly_report

# --- Flask web server ---
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

def schedule_jobs(application):
    # Pianifica il job settimanale: ogni lunedì alle 9:00
    application.job_queue.run_daily(
        weekly_report,              # funzione da eseguire
        time=time(hour=9, minute=0),# orario
        days=(0,),                  # 0 = lunedì
        name="weekly_report_job"
    )

if __name__ == "__main__":
    # Costruisci l'applicazione Telegram
    app = build_application()

    # Pianifica i job settimanali
    schedule_jobs(app)

    # Avvia il bot in modalità webhook
    port = int(os.environ.get("PORT", 5000))
    token = os.environ["TELEGRAM_BOT_TOKEN"]

    # ⚠️ Qui devi mettere l'URL pubblico del tuo servizio Render
    # Esempio: https://nome-servizio.onrender.com
    external_url = os.environ.get("RENDER_EXTERNAL_URL")

    webhook_url = f"{external_url}/{token}"
    print("Webhook URL che sto passando a Telegram:", webhook_url)

    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path=token,
        webhook_url=webhook_url,
        drop_pending_updates=True
    )

    # Avvia anche Flask (serve per la route "/")
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)
