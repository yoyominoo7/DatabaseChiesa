import os
import threading
from flask import Flask
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram import Update

BOT_TOKEN = os.getenv("BOT_TOKEN")

# --- Flask web server ---
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    # IMPORTANTE: disattiva il reloader per evitare doppie istanze
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)

# --- Telegram bot ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Ciao! Il bot è attivo 🚀")

def run_bot():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))

    # Avvia il polling, eliminando eventuali update pendenti
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    # Avvia Flask in un thread separato
    threading.Thread(target=run_flask, daemon=True).start()
    # Avvia il bot
    run_bot()
