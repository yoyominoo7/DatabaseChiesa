import os
from telegram import Update
from telegram.ext import ApplicationBuilder

# -------------------------
# 1️⃣ Variabili d'ambiente
# -------------------------
TOKEN = os.getenv("TELEGRAM_TOKEN")
app = ApplicationBuilder().token(TOKEN).build()
PORT = int(os.getenv("PORT", 10000))
WEBHOOK_URL = os.getenv("RENDER_EXTERNAL_URL") + "/webhook"

if not TOKEN:
    raise ValueError("Devi impostare la variabile d'ambiente TELEGRAM_TOKEN!")

# -------------------------
# 2️⃣ Setup bot
# -------------------------
app_bot = Application.builder().token(TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start del bot"""
    await update.message.reply_text("Ciao! Sono vivo su Render 🚀")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /help del bot"""
    await update.message.reply_text("/start - avvia il bot\n/help - mostra comandi disponibili")

# Aggiungi i comandi
app_bot.add_handler(CommandHandler("start", start))
app_bot.add_handler(CommandHandler("help", help_command))

# -------------------------
# 3️⃣ Avvio webhook
# -------------------------
if __name__ == "__main__":
    print(f"Bot in ascolto su {WEBHOOK_URL}")
    app_bot.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="/webhook",
        webhook_url=WEBHOOK_URL
    )
