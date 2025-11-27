import os
from flask import Flask, request
from datetime import time
from app import build_application, weekly_report

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
    application.update_queue.put(update)
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port)
