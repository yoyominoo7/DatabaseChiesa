from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from flask import Flask, request
import os
import asyncio

TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("RENDER_EXTERNAL_URL") + "/webhook"  # Render genera questo URL automaticamente

app = Flask(__name__)
bot_app = Application.builder().token(TOKEN).build()

# --- Comandi del bot ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Ciao 👋 sono vivo su Render!")

bot_app.add_handler(CommandHandler("start", start))

# --- Endpoint Webhook ---
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    asyncio.run(bot_app.process_update(Update.de_json(data, bot_app.bot)))
    return "OK", 200

# --- Setup Webhook quando parte il server ---
@app.before_first_request
def set_webhook():
    asyncio.run(bot_app.bot.set_webhook(WEBHOOK_URL))

# --- Avvio server Flask ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
