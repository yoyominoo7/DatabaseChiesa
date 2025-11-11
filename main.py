import os
import threading
from flask import Flask
from app import build_application   # importa la funzione dal tuo app.py

# --- Flask web server ---
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    # IMPORTANTE: disattiva il reloader per evitare doppie istanze del bot
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)

# --- Avvio bot ---
def run_bot():
    app = build_application()
    # Avvia il polling, eliminando eventuali update pendenti
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    # Avvia Flask in un thread separato
    threading.Thread(target=run_flask, daemon=True).start()
    # Avvia il bot
    run_bot()
