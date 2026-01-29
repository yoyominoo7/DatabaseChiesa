import os
import threading
from flask import Flask
from datetime import time
from app import build_application, weekly_report
import pytz
# --- Flask web server ---
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

ROME_TZ = pytz.timezone("Europe/Rome")

def schedule_jobs(application):
    application.job_queue.run_daily(
        weekly_report,
        time=time(hour=10, minute=0, tzinfo=ROME_TZ),  # 10:00 ora italiana
        days=(0,),  # 0 = lunedì
        name="weekly_report_job"
    )

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    # Costruisci l'applicazione Telegram
    app = build_application()

    # Pianifica i job settimanali
    schedule_jobs(app)

    # Avvia Flask in un thread separato (per UptimeRobot)
    threading.Thread(target=run_flask).start()

    # Avvia il bot in modalità polling
    app.run_polling(drop_pending_updates=True)
