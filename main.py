import os
import threading
from flask import Flask
from app import build_application

# --- Flask web server ---
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port)

# --- Telegram bot ---
def run_bot():
    app = build_application()
    app.run_polling()

if __name__ == "__main__":
    # Avvia Flask in un thread separato
    threading.Thread(target=run_flask).start()
    # Avvia il bot
    run_bot()
