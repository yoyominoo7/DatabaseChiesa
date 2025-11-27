import os
from flask import Flask, request
from datetime import time
from telegram import Update
from app import build_application, weekly_report

flask_app = Flask(__name__)
app = build_application()

@flask_app.route("/")
def home():
    return "Bot is running!"

@flask_app.route(f"/{os.environ['TELEGRAM_BOT_TOKEN']}", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, app.bot)   # ✅ conversione corretta
    app.update_queue.put_nowait(update)
    return "OK", 200

def schedule_jobs(application):
    application.job_queue.run_daily(
        weekly_report,
        time=time(hour=9, minute=0),
        days=(0,),  # lunedì
        name="weekly_report_job"
    )

if __name__ == "__main__":
    schedule_jobs(app)

    token = os.environ["TELEGRAM_BOT_TOKEN"]
    external_url = os.environ.get("RENDER_EXTERNAL_URL")  # es: https://databasechiesa.onrender.com
    webhook_url = f"{external_url}/{token}"

    # Imposta webhook su Telegram
    app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)

    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port)
