import os
import logging
from datetime import datetime, timedelta, timezone, time
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, BigInteger, func
from telegram.helpers import escape_markdown
import html
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    Message,
    BotCommand,
    BotCommandScopeChatMember,
    CallbackQuery,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean, ForeignKey
)
from sqlalchemy.orm import sessionmaker, declarative_base
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
booking_msg_map = {}

# ---- ENV ----
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PRIESTS_GROUP_ID = int(os.getenv("PRIESTS_GROUP_ID", "0"))
DIRECTORS_GROUP_ID = int(os.getenv("DIRECTORS_GROUP_ID", "0"))
SECRETARIES_IDS = {int(x) for x in os.getenv("SECRETARIES_IDS", "").split(",") if x}
PRIESTS_IDS = {int(x) for x in os.getenv("PRIESTS_IDS", "").split(",") if x}
DIRECTORS_IDS = {int(x) for x in os.getenv("DIRECTORS_IDS", "").split(",") if x}
DIRECTORS_TOPIC_ID = int(os.getenv("DIRECTORS_TOPIC_ID"))
# ---- DB ----
Base = declarative_base()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

SACRAMENTS = [
    "battesimo",
    "cammino_dell_abisso",
    "rivelazione_divina",
    "confessione",
    "unzione",
    "matrimonio",
    "divorzio",
]

STATUS = ["pending", "assigned", "in_progress", "completed", "canceled"]

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    role = Column(String, nullable=False)
    rp_name = Column(String)
    nickname_mc = Column(String)

class Booking(Base):
    __tablename__ = "bookings"

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)
    client_telegram_id = Column(BigInteger)
    rp_name = Column(String)
    nickname_mc = Column(String)
    sacrament = Column(String, nullable=False)
    notes = Column(String)
    status = Column(String, nullable=False, default="pending")
    secretary_username = Column(String, nullable=True)   # 👈 solo colonna
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Assignment(Base):
    __tablename__ = "assignments"
    id = Column(Integer, primary_key=True)
    booking_id = Column(Integer, ForeignKey("bookings.id"))
    priest_telegram_id = Column(BigInteger)
    priest_username = Column(String, nullable=True)   # 👈 nuovo campo
    assigned_by = Column(BigInteger)
    assigned_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    taken_at = Column(DateTime)
    due_alert_sent = Column(Boolean, default=False)

class EventLog(Base):
    __tablename__ = "events_log"
    id = Column(Integer, primary_key=True)
    booking_id = Column(Integer)
    actor_id = Column(BigInteger)
    action = Column(String)
    ts = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    details = Column(String)
class Priest(Base):
    __tablename__ = "priests"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False)
    username = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    Base.metadata.create_all(engine)

# ---- UTILS ----
def is_secretary(user_id: int) -> bool:
    return user_id in SECRETARIES_IDS

def is_priest(user_id: int) -> bool:
    return user_id in PRIESTS_IDS

def is_director(user_id: int) -> bool:
    return user_id in DIRECTORS_IDS

def role_required(check_func, msg="**𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄** ⚓️\n\n❌ Hey, sembra che tu non abbia il permesso per effettuare questo comando.\n\nSe pensi sia un errore contatta 👉 @LavatiScimmiaInfuocata"):
    def decorator(func):
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            user_id = update.effective_user.id
            if not check_func(user_id):
                await update.effective_message.reply_text(msg, parse_mode="Markdown")
                return
            return await func(update, context)
        return wrapper
    return decorator

# ---- CONVERSATION STATES ----
IG_RP_NAME, IG_NICK, IG_SACRAMENT, IG_NOTES, IG_CONFIRM = range(5)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return ConversationHandler.END

    user = update.effective_user
    user_id = user.id

    roles = []
    if is_priest(user_id):
        session = SessionLocal()
        try:
            priest = session.query(Priest).filter_by(telegram_id=user_id).first()
            if priest:
                # Aggiorna username se è cambiato
                if priest.username != user.username:
                    priest.username = user.username
                    session.add(priest)
            else:
                # Crea nuovo sacerdote
                priest = Priest(
                    telegram_id=user_id,
                    username=user.username,
                    created_at=datetime.now()
                )
                session.add(priest)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        roles.append("sacerdote")

    if is_secretary(user_id):
        roles.append("segretario")
    if is_director(user_id):
        roles.append("direzione")
    # Nessun ruolo
    if not roles:
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Non risulti avere un ruolo valido per usare questo bot.",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    # Un solo ruolo → messaggio diretto
    if len(roles) == 1:
        role = roles[0]
        await _send_role_welcome(update.message, role)
        return ConversationHandler.END

    # Più ruoli → scelta con bottoni (senza opzione "fedele")
    buttons = [[InlineKeyboardButton(r.capitalize(), callback_data=f"role_{r}")] for r in roles]
    await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🌟 Poiché sei un <b>VIP della chiesa</b>, possiedi più ruoli!\n\n👉 Puoi usarne solo uno alla volta: scegli quale messaggio di start ti serve tra quelli indicati qui sotto:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )
    return ConversationHandler.END


async def choose_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    role = query.data.replace("role_", "")
    # Mostra il messaggio corrispondente al ruolo selezionato
    await _send_role_welcome(query.message, role)


async def _send_role_welcome(target_message: Message, role: str):
    if role == "sacerdote":
        await target_message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Benvenuto! Questo bot ti aiuterà nelle tue mansioni da <b>sacerdote</b>.\n\n📜 Comandi principali:\n- <code>/mie_assegnazioni</code> → controlla i sacramenti che ti vengono assegnati (riceverai notifiche automatiche).\n\n⚠️ Ricorda: è tuo dovere verificare quotidianamente le assegnazioni.\n\nSe hai difficoltà o riscontri problemi contatta 👉 <b>Consiglio degli Anziani</b>.",
            parse_mode="HTML"
        )
    elif role == "segretario":
        await target_message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📖 Benvenuto! Questo bot ti aiuterà nelle tue mansioni da <b>segretario</b>.\n\n📜 Comandi principali:\n- <code>/prenota_ingame</code> → registra ogni sacramento pagato, così potrà essere assegnato a un sacerdote.\n\n⚠️ Non creare prenotazioni false o di prova: rischi di rompere il bot!\n\nSe hai difficoltà o riscontri problemi contatta 👉 <b>Consiglio degli Anziani</b>.",
            parse_mode="HTML"
        )
    elif role == "direzione":
        await target_message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n👑 Benvenuto! Questo bot ti aiuterà nelle tue mansioni da <b>Patriarca</b>.\n\n📜 Comandi principali:\n- <code>/assegna &lt;id prenotazione&gt; &lt;@sacerdote&gt;</code> → assegna una prenotazione a un sacerdote.\n- <code>/riassegna &lt;id prenotazione&gt; &lt;@sacerdote&gt;</code> → riassegna una prenotazione già assegnata.\n- <code>/lista_prenotazioni</code> → consulta le prenotazioni filtrate:\n   • ⏳ <b>pending</b> → prenotazioni in attesa\n   • 📌 <b>assigned</b> → prenotazioni assegnate\n   • ✅ <b>completed</b> → prenotazioni completate\n   • 👤 <b>@sacerdote</b> → prenotazioni di un sacerdote\n   • 🎮 <b>nick fedele</b> → prenotazioni di un fedele\n\nSe hai difficoltà o riscontri problemi contatta 👉 <b>Falco</b> o <b>yomino</b>.",
            parse_mode="HTML"
        )
    else:
        await target_message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Ruolo non riconosciuto.",
            parse_mode="HTML"
        )

def sacrament_keyboard():
    buttons = [[InlineKeyboardButton(s.title().replace("_", " "), callback_data=f"sac_{s}")] for s in SACRAMENTS]
    cancel = [InlineKeyboardButton("❌ Annulla", callback_data="cancel")]
    return InlineKeyboardMarkup(buttons + [cancel])

def confirm_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Conferma", callback_data="confirm")],
        [InlineKeyboardButton("❌ Annulla", callback_data="cancel")],
    ])

# ---- INGAME FLOW (SECRETARIES) ----
@role_required(
    is_secretary,
    "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Non risulti essere un <b>segretario</b>, perciò non puoi eseguire il comando.\n\nSe pensi sia un errore contatta 👉 @LavatiScimmiaInfuocata."
)
async def prenota_ingame(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if context.user_data.get("ingame_active"):
        await update.message.delete()

    context.user_data["ingame_active"] = True

    if update.effective_chat.type != "private":
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Questo comando può essere usato <b>solo in privato</b> con il bot.",
            parse_mode="HTML"
        )
        return ConversationHandler.END
    msg = await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📝 Per iniziare la procedura di registrazione, inserisci la <b>@ del fedele</b> che ha prenotato:\n\n"
        "Prima di proseguire, assicurati che il contatto inserito sia corretto. Se si tratta di un divorzio inserisci un puntino.\n",
        parse_mode="HTML"
    )

    context.user_data["last_prompt_id"] = msg.message_id
    return IG_RP_NAME

async def ig_rp_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["rp_name"] = update.message.text.strip()

    await update.message.delete()
    if "last_prompt_id" in context.user_data:
        try:
            await context.bot.delete_message(update.effective_chat.id, context.user_data["last_prompt_id"])
        except Exception:
            pass

    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🎮 Bene! Adesso ti chiedo di inserire il <b>nickname di Minecraft</b> del fedele.\n\n➡️ Se si tratta di un matrimonio o di un divorzio inserisci il nome dei due coniugi.",
        parse_mode="HTML"
    )
    context.user_data["last_prompt_id"] = msg.message_id
    return IG_NICK


async def ig_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["nickname_mc"] = update.message.text.strip()

    await update.message.delete()
    if "last_prompt_id" in context.user_data:
        try:
            await context.bot.delete_message(update.effective_chat.id, context.user_data["last_prompt_id"])
        except Exception:
            pass

    kb = ReplyKeyboardMarkup([[KeyboardButton(s.replace("_"," "))] for s in SACRAMENTS],
                             one_time_keyboard=False, resize_keyboard=True)
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✝️ Seleziona uno o più <b>sacramenti</b>.\n\n➡️ Scrivi <b>'fine'</b> quando hai terminato:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    context.user_data["last_prompt_id"] = msg.message_id
    context.user_data["sacraments"] = []
    return IG_SACRAMENT


async def ig_sacrament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = update.message.text.lower().replace(" ", "_")

    await update.message.delete()
    if "last_prompt_id" in context.user_data:
        try:
            await context.bot.delete_message(update.effective_chat.id, context.user_data["last_prompt_id"])
        except Exception:
            pass

    # --- FINE ---
    if s == "fine":
        if not context.user_data["sacraments"]:
            remaining = [s for s in SACRAMENTS if s not in context.user_data["sacraments"]]
            kb = ReplyKeyboardMarkup([[KeyboardButton(x.replace("_"," "))] for x in remaining],
                                     one_time_keyboard=False, resize_keyboard=True)

            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ Attenzione, non hai selezionato nessun <b>sacramento</b>.\n\n➡️ Riprova:",
                reply_markup=kb,
                parse_mode="HTML"
            )
            context.user_data["last_prompt_id"] = msg.message_id
            return IG_SACRAMENT

        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Siamo arrivati quasi alla fine.\n\n➡️ Inserisci delle <b>note aggiuntive</b> (se non ci sono scrivi 'no')\nSe si tratta di un divorzio scrivi il motivo:",
            parse_mode="HTML"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_NOTES

    # --- SACRAMENTO NON VALIDO ---
    if s not in SACRAMENTS:
        remaining = [x for x in SACRAMENTS if x not in context.user_data["sacraments"]]
        kb = ReplyKeyboardMarkup([[KeyboardButton(x.replace("_"," "))] for x in remaining],
                                 one_time_keyboard=False, resize_keyboard=True)

        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Il sacramento inserito non è <b>valido</b>.\n\n➡️ Riprova:",
            reply_markup=kb,
            parse_mode="HTML"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_SACRAMENT

    # 🔒 BLOCCO SACRAMENTI DUPLICATI (anche se scritti a mano)
    if s in context.user_data["sacraments"]:
        remaining = [
            x for x in SACRAMENTS
            if x not in context.user_data["sacraments"]
            and x not in ("divorzio", "matrimonio")
        ]
        kb = ReplyKeyboardMarkup([[KeyboardButton(x.replace("_"," "))] for x in remaining],
                                 one_time_keyboard=False, resize_keyboard=True)

        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="<b>⚠️ Questo sacramento è già stato selezionato.</b>\n\n"
                 "➡️ Scegline un altro oppure scrivi <b>'fine'</b>:",
            reply_markup=kb,
            parse_mode="HTML"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_SACRAMENT

    # --- LOGICA MATRIMONIO / DIVORZIO ---
    if s in ("divorzio", "matrimonio"):

        if not context.user_data["sacraments"]:
            context.user_data["sacraments"] = [s]

            nome = "Divorzio" if s == "divorzio" else "Matrimonio"

            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📌 Hai selezionato <b>{nome}</b>.\n"
                    f"➡️ Il {nome.lower()} non può essere combinato con altri sacramenti.\n"
                    "Procediamo direttamente alle <b>note</b>:"
                ),
                parse_mode="HTML"
            )
            context.user_data["last_prompt_id"] = msg.message_id
            return IG_NOTES

        else:
            remaining = [
                x for x in SACRAMENTS
                if x not in context.user_data["sacraments"]
                and x not in ("divorzio", "matrimonio")
            ]
            kb = ReplyKeyboardMarkup([[KeyboardButton(x.replace("_"," "))] for x in remaining],
                                     one_time_keyboard=False, resize_keyboard=True)

            nome = "divorzio" if s == "divorzio" else "matrimonio"
            msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"<b>⚠️ Il {nome} può essere registrato solo come sacramento unico.</b>\n"
                    "➡️ Non è stato aggiunto.\n\n"
                    "Seleziona un altro sacramento oppure scrivi <b>'fine'</b>:"
                ),
                reply_markup=kb,
                parse_mode="HTML"
            )
            context.user_data["last_prompt_id"] = msg.message_id
            return IG_SACRAMENT

    # --- AGGIUNTA NORMALE ---
    context.user_data["sacraments"].append(s)

    remaining = [
        x for x in SACRAMENTS
        if x not in context.user_data["sacraments"]
        and x not in ("divorzio", "matrimonio")
    ]

    kb = ReplyKeyboardMarkup([[KeyboardButton(x.replace("_"," "))] for x in remaining],
                             one_time_keyboard=False, resize_keyboard=True)

    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ Il sacramento è stato <b>aggiunto con successo</b>!\n\n"
             "➡️ Selezionane un altro oppure scrivi <b>'fine'</b>:",
        reply_markup=kb,
        parse_mode="HTML"
    )
    context.user_data["last_prompt_id"] = msg.message_id
    return IG_SACRAMENT



async def ig_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    notes = update.message.text.strip()

    # elimina messaggi
    await update.message.delete()
    if "last_prompt_id" in context.user_data:
        try:
            await context.bot.delete_message(update.effective_chat.id, context.user_data["last_prompt_id"])
        except Exception:
            pass

    if notes.lower() == "no":
        notes = ""
    context.user_data["notes"] = notes

    # 🔹 Escape dei campi variabili per HTML
    rp_name = html.escape(context.user_data['rp_name'])
    nickname_mc = html.escape(context.user_data['nickname_mc'])
    sacrament_display = html.escape(", ".join(context.user_data["sacraments"]).replace("_"," "))
    safe_notes = html.escape(notes) if notes else "nessuna nota presente."

    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
            "📋 Sei arrivato alla fine della registrazione.\n\n"
            "Qui sotto è presente il <i>resoconto</i> delle informazioni scritte da te. "
            "Controlla che siano giuste e conferma la tua registrazione:\n\n"
            f"• 👤 Contatto Telegram: <b>{rp_name}</b>\n"
            f"• 🎮 Nick: <b>{nickname_mc}</b>\n"
            f"• ✝️ Sacramenti: <b>{sacrament_display}</b>\n"
            f"• 📝 Note: <b>{safe_notes}</b>"
        ),
        reply_markup=confirm_keyboard(),
        parse_mode="HTML"
    )
    context.user_data["last_prompt_id"] = msg.message_id
    return IG_CONFIRM

async def ig_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # 🔹 CANCELLAZIONE PROCEDURA
    if query.data == "cancel":
        await query.edit_message_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ La prenotazione è stata <i>annullata con successo</i>!\n\n"
            "➡️ Se vuoi effettuarla di nuovo digita <code>/prenota_ingame</code>",
            parse_mode="HTML"
        )
        context.user_data.pop("ingame_active", None)   # 🔥 sblocca procedura
        return ConversationHandler.END

    if query.data != "confirm":
        return

    user = update.effective_user
    user_id = user.id

    # 🔹 PERMESSI
    if not is_secretary(user_id):
        await query.edit_message_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Non hai il <i>permesso</i> per eseguire questa azione.",
            parse_mode="HTML"
        )
        context.user_data.pop("ingame_active", None)   # 🔥 sblocca procedura
        return ConversationHandler.END

    session = SessionLocal()
    try:
        sacrament_display_raw = ", ".join(context.user_data.get("sacraments", []))
        is_divorce = sacrament_display_raw == "divorzio"

        # 🔹 Lo status cambia SOLO per le prenotazioni normali
        booking_status = "registered" if is_divorce else "pending"

        booking = Booking(
            source="ingame",
            rp_name=context.user_data["rp_name"],
            nickname_mc=context.user_data["nickname_mc"],
            sacrament=sacrament_display_raw,
            notes=context.user_data["notes"],
            status=booking_status,
            secretary_username=user.username or f"ID:{user.id}"
        )
        session.add(booking)
        session.commit()

        session.add(EventLog(
            booking_id=booking.id,
            actor_id=user_id,
            action="create",
            details="ingame"
        ))
        session.commit()

        rp_name = html.escape(booking.rp_name)
        nickname_mc = html.escape(booking.nickname_mc)
        sacrament_display = html.escape(sacrament_display_raw.replace("_"," "))
        safe_notes = html.escape(booking.notes) if booking.notes else "nessuna nota presente."
        secretary_tag = f"@{user.username}" if user.username else f"ID:{user.id}"
        secretary_tag_safe = html.escape(secretary_tag)

        # 🔹 MESSAGGIO DI CONFERMA PER IL SEGRETARIO
        if is_divorce:
            await query.edit_message_text(
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
                f"📑 Il <b>divorzio</b> è stato <i>registrato correttamente</i>! (ID #{booking.id})\n\n"
                "📋 Resoconto delle informazioni inserite:\n\n"
                f"• 🎮 Nick: <b>{nickname_mc}</b>\n"
                f"• 💔 Divorzio registrato\n"
                f"• 📝 Motivo: <b>{safe_notes}</b>",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text(
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
                f"✅ La tua prenotazione è stata <i>registrata con successo</i>! (ID #{booking.id})\n\n"
                "📋 Resoconto delle informazioni inserite:\n\n"
                f"• 👤 Contatto Telegram: <b>{rp_name}</b>\n"
                f"• 🎮 Nick: <b>{nickname_mc}</b>\n"
                f"• ✝️ Sacramenti: <b>{sacrament_display}</b>\n"
                f"• 📝 Note: <b>{safe_notes}</b>",
                parse_mode="HTML"
            )

        # 🔹 MESSAGGIO ALLA DIREZIONE
        if is_divorce:
            # 🔥 DIVORZIO → nessun tasto assegna, topic diverso
            timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")

            await context.bot.send_message(
                DIRECTORS_GROUP_ID,
                f"<b>📑 NUOVA REGISTRAZIONE DI DIVORZIO</b> (ID #{booking.id})\n\n"
                f"• 🎮 Nick: <b>{nickname_mc}</b>\n"
                f"• 💔 Divorzio registrato\n"
                f"• 📝 Motivo: <b>{safe_notes}</b>\n"
                f"• 🕒 Registrato il: <b>{timestamp}</b>\n\n"
                f"📌 Registrato dal segretario: <b>{secretary_tag_safe}</b>",
                parse_mode="HTML",
                message_thread_id=12973
            )

        else:
            # 🔥 PRENOTAZIONE NORMALE → tasto assegna + topic normale
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Assegna", callback_data=f"assign_{booking.id}")]
            ])

            msg = await context.bot.send_message(
                DIRECTORS_GROUP_ID,
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📢 È presente una nuova <b>prenotazione</b>! (ID #{booking.id})\n\n"
                f"• 👤 Contatto Telegram: <b>{rp_name}</b>\n"
                f"• 🎮 Nick: <b>{nickname_mc}</b>\n"
                f"• ✝️ Sacramenti: <b>{sacrament_display}</b>\n"
                f"• 📝 Note: <b>{safe_notes}</b>\n\n"
                f"📌 Prenotazione registrata dal segretario: <b>{secretary_tag_safe}</b>\n\n"
                "⚠️ Ricorda di verificare i campi inseriti e di assegnarla il prima possibile a un sacerdote.",
                reply_markup=kb,
                parse_mode="HTML",
                message_thread_id=DIRECTORS_TOPIC_ID
            )

            booking_msg_map[booking.id] = msg.message_id

        # 🔥 Sblocca la procedura /prenota_ingame
        context.user_data.pop("ingame_active", None)

        return ConversationHandler.END

    finally:
        session.close()


# ---- DIREZIONE: CALLBACK "Assegna" ----
async def assign_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # 🔹 Annulla assegnazione: elimina il messaggio e pulisce lo stato
    if data == "cancel_assign":
        msg_id = context.user_data.get("assign_msg_id")
        if msg_id:
            try:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=msg_id
                )
            except Exception:
                pass
        context.user_data.pop("assign_msg_id", None)
        context.user_data.pop("assign_booking_id", None)
        await query.answer("❌ Assegnazione annullata.", show_alert=False)
        return

    # Da qui in giù gestiamo solo i callback "assign_<id>"
    if not data.startswith("assign_"):
        return

    if not is_director(update.effective_user.id):
        await query.answer("❌ Non hai il permesso.", show_alert=True)
        return

    booking_id = int(data.replace("assign_", ""))

    # 🔒 Se questa prenotazione è già in fase di assegnazione, blocca l'azione
    in_progress_booking_id = context.user_data.get("assign_booking_id")
    if in_progress_booking_id == booking_id:
        return
    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if not booking or booking.status != "pending":
            await query.answer("⚠️ Prenotazione non valida o già assegnata.", show_alert=True)
            return
        # 🔹 Calcolo settimana corrente (lunedì ➝ domenica)
        now = datetime.now(timezone.utc)
        start_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end_week = start_week + timedelta(days=6, hours=23, minutes=59, seconds=59)

        assigns_week = (
            session.query(Assignment.priest_telegram_id, func.count(Assignment.id))
            .join(Booking, Booking.id == Assignment.booking_id)
            .filter(Booking.updated_at >= start_week, Booking.updated_at <= end_week)
            .group_by(Assignment.priest_telegram_id)
            .all()
        )
        counts = {pid: cnt for pid, cnt in assigns_week}

        all_priests = session.query(Priest).all()

        real_priests = [
            p for p in all_priests
            if not is_director(p.telegram_id) and not is_secretary(p.telegram_id)
        ]

        secretaries = [
            p for p in all_priests
            if is_secretary(p.telegram_id) and not is_director(p.telegram_id)
        ]

        # 🔹 Ordina sacerdoti per assegnazioni settimanali
        sorted_priests = sorted(real_priests, key=lambda p: counts.get(p.telegram_id, 0))
        top3 = sorted_priests[:3]

        # 🔹 Costruisci bottoni SOLO per sacerdoti e segretari
        selectable = real_priests + secretaries
        buttons = [
            [InlineKeyboardButton(f"@{p.username}", callback_data=f"do_assign_{booking_id}_{p.telegram_id}")]
            for p in selectable
        ]
        buttons.append([InlineKeyboardButton("❌ Annulla", callback_data="cancel_assign")])

        # 🔹 Testo suggerimento sacerdoti
        priest_lines = [
            f"- @{p.username}: {counts.get(p.telegram_id, 0)} assegnazioni"
            for p in top3
        ]
        priest_text = "\n".join(priest_lines) if priest_lines else "ℹ️ Nessun sacerdote disponibile."

        # 🔹 Testo riepilogo segretari (esclusi direttori)
        secretary_lines = [
            f"- @{s.username}: {counts.get(s.telegram_id, 0)} assegnazioni"
            for s in secretaries
        ]
        secretary_text = "\n".join(secretary_lines) if secretary_lines else "ℹ️ Nessun segretario registrato."

        # 🔹 Messaggio finale
        msg = await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
            f"🙏 Seleziona il sacerdote per la prenotazione #{booking.id}:\n\n"
            f"📊 <b>I 3 sacerdoti con meno assegnazioni questa settimana:</b>\n{priest_text}\n\n"
            f"🗂 <b>Riepilogo segretari (devono ricevere meno incarichi):</b>\n{secretary_text}",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML",
            message_thread_id=DIRECTORS_TOPIC_ID
        )
        context.user_data["assign_msg_id"] = msg.message_id
        context.user_data["assign_booking_id"] = booking.id
    finally:
        session.close()


# ---- DIREZIONE: CALLBACK scelta sacerdote ----
async def do_assign_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Rimuovi il prefisso "do_assign_"
    data = query.data.replace("do_assign_", "")
    booking_id, priest_id = data.split("_")
    booking_id = int(booking_id)
    priest_id = int(priest_id)
    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        priest = session.query(Priest).filter(Priest.telegram_id == priest_id).first()

        if not booking or not priest:
            await query.answer("❌ Errore: prenotazione o sacerdote non trovati.", show_alert=True)
            return

        # 🔹 Aggiorna stato prenotazione
        booking.status = "assigned"
        booking.updated_at = datetime.now(timezone.utc)
        session.add(booking)

        assign = Assignment(
            booking_id=booking.id,
            priest_telegram_id=priest.telegram_id,
            priest_username=priest.username,
            assigned_by=update.effective_user.id,
        )
        session.add(assign)
        session.add(EventLog(
            booking_id=booking.id,
            actor_id=update.effective_user.id,
            action="assign",
            details=f"to @{priest.username}"
        ))
        session.commit()

        # 🔹 Elimina messaggio con lista sacerdoti
        assign_msg_id = context.user_data.get("assign_msg_id")
        if assign_msg_id:
            await context.bot.delete_message(DIRECTORS_GROUP_ID, assign_msg_id)
        # 🔹 Rimuovi pulsante "Assegna" dal messaggio originale (usando mappa globale)
        booking_msg_id = booking_msg_map.get(booking.id)
        if booking_msg_id:
            await context.bot.edit_message_reply_markup(
                chat_id=DIRECTORS_GROUP_ID,
                message_id=booking_msg_id,
                reply_markup=None   # 🔹 niente message_thread_id qui
            )
        # 🔹 Notifica al gruppo Direzione
        await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ Prenotazione #{booking.id} <b>assegnata</b> a @{priest.username}.",
            parse_mode="HTML",
            message_thread_id=DIRECTORS_TOPIC_ID
        )

        # 🔹 Notifica al sacerdote (qui NON serve il topic, va in chat privata)
        await context.bot.send_message(
            priest.telegram_id,
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Hey sacerdote! Ti è stata <b>assegnata una nuova prenotazione</b> (#{booking.id}).\n➡️ Utilizza <code>/mie_assegnazioni</code> per i dettagli.",
            parse_mode="HTML"
        )

        # 🔹 Pianifica job di notifica 48h
        context.job_queue.run_once(
            notify_uncompleted,
            when=48*3600,
            data={"booking_id": booking.id, "priest_id": priest.telegram_id, "username": priest.username},
            name=f"notify_{booking.id}"
        )
    finally:
        session.close()

@role_required(is_director, "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Non hai il permesso per eseguire questo comando.")
async def riassegna(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != DIRECTORS_GROUP_ID:
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Questo comando può essere usato <b>solo nel gruppo Direzione</b>.",
            parse_mode="HTML"
        )
        return

    session = SessionLocal()
    try:
        priests = session.query(Priest).all()
    finally:
        session.close()

    buttons = [
        [InlineKeyboardButton(f"@{p.username}", callback_data=f"reassign_choose_priest_{p.telegram_id}")]
        for p in priests
    ]
    buttons.append([InlineKeyboardButton("❌ Annulla", callback_data="reassign_cancel")])

    await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Scegli il sacerdote a cui vuoi riassegnare una prenotazione:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )

async def reassign_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # ❌ Annulla
    if data == "reassign_cancel":
        await query.message.delete()
        return

    # 🔙 Torna alla lista sacerdoti
    if data == "reassign_back_to_priests":
        session = SessionLocal()
        try:
            priests = session.query(Priest).all()
        finally:
            session.close()

        buttons = [
            [InlineKeyboardButton(f"@{p.username}", callback_data=f"reassign_choose_priest_{p.telegram_id}")]
            for p in priests
        ]
        buttons.append([InlineKeyboardButton("❌ Annulla", callback_data="reassign_cancel")])

        await query.edit_message_text(
            "<b>🙏 Scegli il sacerdote a cui vuoi riassegnare una prenotazione:</b>",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )
        return

    # 🔙 Torna alla lista prenotazioni (paginata)
    if data == "reassign_back_to_bookings":
        context.user_data["reassign_page"] = 1
        await show_reassign_bookings_page(query, context)
        return

    # 🔄 Paginazione
    if data == "reassign_page_prev":
        context.user_data["reassign_page"] -= 1
        await show_reassign_bookings_page(query, context)
        return

    if data == "reassign_page_next":
        context.user_data["reassign_page"] += 1
        await show_reassign_bookings_page(query, context)
        return

    # 1️⃣ Scelta sacerdote destinatario
    if data.startswith("reassign_choose_priest_"):
        priest_id = int(data.replace("reassign_choose_priest_", ""))
        context.user_data["reassign_priest"] = priest_id

        # 🔎 Recupera TUTTE le prenotazioni assegnate e non completate
        session = SessionLocal()
        try:
            bookings = session.query(Booking).filter(
                Booking.status == "assigned"
            ).all()
        finally:
            session.close()

        if not bookings:
            await query.edit_message_text(
                "<b>❌ Non ci sono prenotazioni assegnate da riassegnare.</b>",
                parse_mode="HTML"
            )
            return

        # Salva lista prenotazioni e pagina corrente
        context.user_data["reassign_bookings"] = [b.id for b in bookings]
        context.user_data["reassign_page"] = 1

        await show_reassign_bookings_page(query, context)
        return

    # 2️⃣ Scelta prenotazione → esegui riassegnamento
    if data.startswith("reassign_choose_booking_"):
        booking_id = int(data.replace("reassign_choose_booking_", ""))
        priest_id = context.user_data.get("reassign_priest")

        session = SessionLocal()
        try:
            priest = session.query(Priest).filter(Priest.telegram_id == priest_id).first()
            username = priest.username if priest else None
        finally:
            session.close()

        await complete_reassign(update, context, booking_id, priest_id, username)

        await query.edit_message_text(
            f"🔄 Prenotazione #{booking_id} riassegnata a @{username}.",
            parse_mode="HTML"
        )

async def show_reassign_bookings_page(query, context):
    bookings = context.user_data.get("reassign_bookings", [])
    page = context.user_data.get("reassign_page", 1)

    per_page = 5
    total_pages = (len(bookings) + per_page - 1) // per_page

    start = (page - 1) * per_page
    end = start + per_page
    page_items = bookings[start:end]

    buttons = [
        [InlineKeyboardButton(f"Prenotazione #{bid}", callback_data=f"reassign_choose_booking_{bid}")]
        for bid in page_items
    ]

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data="reassign_page_prev"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data="reassign_page_next"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("⬅️ Indietro", callback_data="reassign_back_to_priests")])
    buttons.append([InlineKeyboardButton("❌ Annulla", callback_data="reassign_cancel")])

    await query.edit_message_text(
        "<b>📋 Seleziona la prenotazione da riassegnare:</b>",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )


async def complete_reassign(update, context, booking_id, priest_id, username):
    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if not booking:
            await update.effective_message.reply_text(
                "❌ Prenotazione inesistente.",
                parse_mode="HTML"
            )
            return

        if booking.status in ("completed", "cancelled"):
            await update.effective_message.reply_text(
                f"❌ La prenotazione #{booking.id} è {booking.status.upper()} e non può essere riassegnata.",
                parse_mode="HTML"
            )
            return

        existing_assign = session.query(Assignment).filter_by(booking_id=booking.id).first()
        if not existing_assign:
            await update.effective_message.reply_text(
                f"⚠️ La prenotazione #{booking.id} non è ancora stata assegnata.",
                parse_mode="HTML"
            )
            return

        # 🔄 RIASSEGNAZIONE (tua logica originale)
        existing_assign.priest_telegram_id = priest_id
        existing_assign.priest_username = username
        existing_assign.assigned_by = update.effective_user.id
        booking.updated_at = datetime.now(timezone.utc)

        session.add(existing_assign)
        session.add(booking)

        session.add(EventLog(
            booking_id=booking.id,
            actor_id=update.effective_user.id,
            action="reassign",
            details=f"to @{username}"
        ))
        session.commit()

        # Notifica sacerdote
        try:
            await context.bot.send_message(
                priest_id,
                f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄 ⚓️\n\n🙏 Hey sacerdote! Ti è appena stata riassegnata una prenotazione #{booking.id}.\n➡️ Utilizza /mie_assegnazioni per i dettagli.",
                parse_mode="HTML"
            )
        except:
            pass

        # Cancella job precedente
        for job in context.job_queue.get_jobs_by_name(f"notify_{booking.id}"):
            job.schedule_removal()

        # Nuovo job 48h
        context.job_queue.run_once(
            notify_uncompleted,
            when=48*3600,
            data={"booking_id": booking.id, "priest_id": priest_id, "username": username},
            name=f"notify_{booking.id}"
        )

    finally:
        session.close()

async def notify_uncompleted(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    booking_id = job_data["booking_id"]

    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if booking and booking.status == "assigned":
            await context.bot.send_message(
                DIRECTORS_GROUP_ID,
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ La prenotazione #{booking.id} assegnata al sacerdote <b>{job_data['username']}</b> non è stata completata entro <b>48 ore</b>.",
                parse_mode="HTML",
                message_thread_id=12872 
            )
    finally:
        session.close()

# ---- SACERDOTE: LISTA E COMPLETAMENTO ----
@role_required(is_priest, "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Non hai il permesso per eseguire il comando.")
async def mie_assegnazioni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Questo comando può essere usato <b>solo in privato</b> con il bot.",
            parse_mode="HTML"
        )
        return

    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        assigns_raw = (
            session.query(Assignment)
            .filter(Assignment.priest_telegram_id == priest_id)
            .all()
        )

        if not assigns_raw:
            await update.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Al momento non ti è stata <b>assegnata alcuna prenotazione</b>, ma questo durerà ancora per poco!",
                parse_mode="HTML"
            )
            return

        # 🔹 Ordina: prima assigned, poi in_progress, poi completed
        assigns = sorted(
            assigns_raw,
            key=lambda a: (
                {"assigned": 0, "in_progress": 1, "completed": 2}.get(
                    session.query(Booking).get(a.booking_id).status,
                    2
                ),
                -a.id
            )
        )

        per_page = 5
        page = int(context.args[0]) if context.args else 1
        total_pages = (len(assigns) + per_page - 1) // per_page

        start = (page - 1) * per_page
        end = start + per_page
        assigns_page = assigns[start:end]

        msgs = []
        for a in assigns_page:
            b = session.query(Booking).get(a.booking_id)
            if not b:
                continue

            if b.status == "assigned":
                msgs.append(
                    f"⚠️ <b>#{b.id} [DA COMPLETARE]</b> - {b.sacrament.replace('_',' ')}\n"
                    f"👤 Contatto TG: {b.rp_name or 'Nessun contatto presente.'}\n"
                    f"🎮 Nick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"📝 Note: {b.notes or 'Nessuna nota.'}"
                )
            else:
                msgs.append(
                    f"✅ #{b.id} [{b.status.upper()}] - {b.sacrament.replace('_',' ')}\n"
                    f"👤 Contatto TG: {b.rp_name or 'Nessun contatto presente.'}\n"
                    f"🎮 Nick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"📝 Note: {b.notes or 'Nessuna nota.'}"
                )

        text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n" + "\n\n".join(msgs)
        text += f"\n\n📄 Pagina {page}/{total_pages}"

        # Bottoni di navigazione
        buttons_nav = []
        if page > 1:
            buttons_nav.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"assign_page_{page-1}"))
        if page < total_pages:
            buttons_nav.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"assign_page_{page+1}"))

        # Bottone completamento
        button_complete = [InlineKeyboardButton("✝️ Completa una prenotazione", callback_data="completa_menu")]

        if buttons_nav:
            kb = InlineKeyboardMarkup([buttons_nav, button_complete])
        else:
            kb = InlineKeyboardMarkup([button_complete])

        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
    finally:
        session.close()
async def mie_assegnazioni_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])

    priest_id = query.from_user.id
    session = SessionLocal()
    try:
        assigns_raw = (
            session.query(Assignment)
            .filter(Assignment.priest_telegram_id == priest_id)
            .all()
        )

        if not assigns_raw:
            await query.edit_message_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Al momento non ti è stata <b>assegnata alcuna prenotazione</b>.",
                parse_mode="HTML"
            )
            return

        # 🔹 Ordina: prima assigned, poi in_progress, poi completed
        assigns = sorted(
            assigns_raw,
            key=lambda a: (
                {"assigned": 0, "in_progress": 1, "completed": 2}.get(
                    session.query(Booking).get(a.booking_id).status,
                    2
                ),
                -a.id
            )
        )

        per_page = 5
        total_pages = (len(assigns) + per_page - 1) // per_page
        start = (page - 1) * per_page
        end = start + per_page
        assigns_page = assigns[start:end]

        msgs = []
        for a in assigns_page:
            b = session.query(Booking).get(a.booking_id)
            if not b:
                continue

            if b.status == "assigned":
                msgs.append(
                    f"⚠️ <b>#{b.id} [DA COMPLETARE]</b> - {b.sacrament.replace('_',' ')}\n"
                    f"👤 Contatto TG: {b.rp_name or 'Nessun contatto presente.'}\n"
                    f"🎮 Nick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"📝 Note: {b.notes or 'Nessuna nota.'}"
                )
            else:
                msgs.append(
                    f"✅ #{b.id} [{b.status.upper()}] - {b.sacrament.replace('_',' ')}\n"
                    f"👤 Contatto TG: {b.rp_name or 'Nessun contatto presente.'}\n"
                    f"🎮 Nick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"📝 Note: {b.notes or 'Nessuna nota.'}"
                )

        text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n" + "\n\n".join(msgs)
        text += f"\n\n📄 Pagina {page}/{total_pages}"

        # Bottoni di navigazione
        buttons_nav = []
        if page > 1:
            buttons_nav.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"assign_page_{page-1}"))
        if page < total_pages:
            buttons_nav.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"assign_page_{page+1}"))

        # Bottone completamento
        button_complete = [InlineKeyboardButton("✝️ Completa una prenotazione", callback_data="completa_menu")]

        if buttons_nav:
            kb = InlineKeyboardMarkup([buttons_nav, button_complete])
        else:
            kb = InlineKeyboardMarkup([button_complete])

        await query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
    finally:
        session.close()


# ---- Callback: mostra menu completamento ----
async def completa_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    priest_id = query.from_user.id
    session = SessionLocal()
    try:
        assigns = (
            session.query(Assignment)
            .filter(Assignment.priest_telegram_id == priest_id)
            .all()
        )
        if not assigns:
            await query.edit_message_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Al momento non ti è stata <b>assegnata alcuna prenotazione</b>.",
                parse_mode="HTML"
            )
            return

        keyboard = []
        for a in assigns:
            b = session.query(Booking).get(a.booking_id)
            if b and b.status == "assigned":
                keyboard.append([InlineKeyboardButton(f"#{b.id}", callback_data=f"completa_{b.id}")])

        keyboard.append([InlineKeyboardButton("⬅️ Torna indietro", callback_data="back_menu")])

        await query.edit_message_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✝️ Seleziona l'<b>ID della prenotazione</b> che vuoi contrassegnare come <b>completata</b>:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    finally:
        session.close()
        

# ---- Callback: completa prenotazione ----
async def completa_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    priest_id = query.from_user.id
    booking_id = int(query.data.split("_")[1])

    session = SessionLocal()
    try:
        b = session.query(Booking).get(booking_id)
        if not b:
            await query.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ L'<b>ID della prenotazione</b> selezionata risulta inesistente.",
                parse_mode="HTML"
            )
            return

        if b.status == "completed":
            await query.message.reply_text(
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ La prenotazione #{b.id} risulta già <b>completata</b>.",
                parse_mode="HTML"
            )
            return

        a = session.query(Assignment).filter(
            Assignment.booking_id == booking_id,
            Assignment.priest_telegram_id == priest_id
        ).first()
        if not a:
            await query.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ L'<b>ID della prenotazione</b> selezionata non ti è assegnata.",
                parse_mode="HTML"
            )
            return

        # Aggiorna stato
        b.status = "completed"
        b.updated_at = datetime.now(timezone.utc)
        session.add(b)
        session.add(EventLog(booking_id=b.id, actor_id=priest_id, action="complete", details=""))
        session.commit()

        # Cancella eventuale job di notifica 48h
        for job in context.job_queue.get_jobs_by_name(f"notify_{b.id}"):
            job.schedule_removal()

        await query.message.reply_text(
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ Grande! Prenotazione #{b.id} contrassegnata come <b>completata</b>.",
            parse_mode="HTML"
        )
        await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✝️ Sacramento <b>completato</b> #{b.id} da @{query.from_user.username or priest_id}.",
            parse_mode="HTML",
            message_thread_id=DIRECTORS_TOPIC_ID   # 🔹 aggiunto parametro per inviare nel topic
        )

        # 🔹 Rimuovi bottone corrispondente
        keyboard = query.message.reply_markup.inline_keyboard
        new_keyboard = [row for row in keyboard if not any(btn.callback_data == f"completa_{booking_id}" for btn in row)]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
    finally:
        session.close()
async def back_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    priest_id = query.from_user.id
    session = SessionLocal()
    try:
        assigns = (
            session.query(Assignment)
            .filter(Assignment.priest_telegram_id == priest_id)
            .order_by(Assignment.id.desc())
            .all()
        )
        if not assigns:
            await query.edit_message_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Al momento non ti è stata <b>assegnata alcuna prenotazione</b>, ma questo durerà ancora per poco!",
                parse_mode="HTML"
            )
            return

        per_page = 5
        page = 1   # 🔹 quando torni indietro riparti dalla prima pagina
        total_pages = (len(assigns) + per_page - 1) // per_page

        start = (page - 1) * per_page
        end = start + per_page
        assigns_page = assigns[start:end]

        msgs = []
        for a in assigns_page:
            b = session.query(Booking).get(a.booking_id)
            if not b:
                continue
            if b.status == "assigned":
                msgs.append(
                    f"⚠️ <b>#{b.id} [DA COMPLETARE]</b> - {b.sacrament.replace('_',' ')}\n"
                    f"👤 Contatto TG: {b.rp_name or 'Nessun contatto presente.'}\n"
                    f"🎮 Nick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"📝 Note: {b.notes or 'Nessuna nota.'}"
                )
            else:
                msgs.append(
                    f"✅ #{b.id} [{b.status.upper()}] - {b.sacrament.replace('_',' ')}\n"
                    f"👤 Contatto TG: {b.rp_name or 'Nessun contatto presente.'}\n"
                    f"🎮 Nick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"📝 Note: {b.notes or 'Nessuna nota.'}"
                )

        text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n" + "\n\n".join(msgs)
        text += f"\n\n📄 Pagina {page}/{total_pages}"

        # Bottoni di navigazione
        buttons_nav = []
        if page > 1:
            buttons_nav.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"assign_page_{page-1}"))
        if page < total_pages:
            buttons_nav.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"assign_page_{page+1}"))

        # Bottone completamento su riga separata
        button_complete = [InlineKeyboardButton("✝️ Completa una prenotazione", callback_data="completa_menu")]

        if buttons_nav:
            kb = InlineKeyboardMarkup([buttons_nav, button_complete])
        else:
            kb = InlineKeyboardMarkup([button_complete])

        await query.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
    finally:
        session.close()


# ---- CANCEL ----
async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Processo <b>annullato</b>.",
        parse_mode="HTML"
    )
    return ConversationHandler.END


@role_required(is_director, "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Solo la <b>Direzione</b> può usare questo comando.")
async def lista_prenotazioni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != DIRECTORS_GROUP_ID:
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Questo comando può essere usato <b>solo nel gruppo Direzione</b>.",
            parse_mode="HTML"
        )
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏳ In attesa", callback_data="filter_pending")],
        [InlineKeyboardButton("📌 Assegnate", callback_data="filter_assigned")],
        [InlineKeyboardButton("✅ Completate", callback_data="filter_completed")],
        [InlineKeyboardButton("🙏 Per sacerdote", callback_data="filter_priests")],
        [InlineKeyboardButton("🎮 Cerca fedele", callback_data="search_fedele")],
        [InlineKeyboardButton("🔎 Cerca per ID", callback_data="search_id")],
        [InlineKeyboardButton("❌ Chiudi Pannello", callback_data="close_panel")],
    ])

    await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Scegli il tipo di prenotazioni da visualizzare:",
        reply_markup=kb,
        parse_mode="HTML",
        message_thread_id=DIRECTORS_TOPIC_ID   # 🔹 aggiunto parametro per inviare nel topic
    )

async def lista_prenotazioni_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    session = SessionLocal()
    try:
        if data.startswith("filter_"):
            filtro = data.replace("filter_", "")
            # 🔹 Filtra per stato (pending / assigned / completed)
            if filtro in STATUS:
                context.user_data["last_list"] = {
                    "kind": "status",
                    "status": filtro,
                    "title": f"📋 Prenotazioni {filtro.upper()}"
                }
                bookings = session.query(Booking).filter(
                    Booking.status == filtro
                ).order_by(Booking.id.desc()).all()

                await _send_paginated_bookings(
                    query, bookings, f"📋 Prenotazioni {filtro.upper()}",
                    filtro, page=1
                )

            # 🔹 Filtra per sacerdote → mostra elenco sacerdoti
            elif filtro == "priests":
                priests = session.query(Priest).all()
                buttons = [
                    [InlineKeyboardButton(f"@{p.username or p.telegram_id}", callback_data=f"priest_{p.telegram_id}")]
                    for p in priests
                ]
                buttons.append([InlineKeyboardButton("⬅️ Torna indietro", callback_data="back_main")])

                new_text = (
                    "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
                    "🙏 Scegli un sacerdote:"
                )
                new_markup = InlineKeyboardMarkup(buttons)

                if query.message.text != new_text or query.message.reply_markup != new_markup:
                    await query.edit_message_text(new_text, reply_markup=new_markup, parse_mode="HTML")
        elif data.startswith("priest_"):
            priest_id = int(data.replace("priest_", ""))
            priest = session.query(Priest).filter(
                Priest.telegram_id == priest_id
            ).first()
            priest_tag = f"@{priest.username}" if priest and priest.username else str(priest_id)
            # 🔹 Recupera TUTTE le prenotazioni del sacerdote
            assigns = session.query(Assignment).filter(
                Assignment.priest_telegram_id == priest_id
            ).all()
            bookings = [
                session.query(Booking).get(a.booking_id)
                for a in assigns
                if session.query(Booking).get(a.booking_id)
            ]
            bookings = sorted(
                bookings,
                key=lambda b: (
                    0 if b.status == "assigned" else 1,
                    -b.id
                )
            )

            context.user_data["last_list"] = {
                "kind": "priest_all",
                "priest_id": priest_id,
                "title": f"📋 Prenotazioni sacerdote {priest_tag}"
            }

            await _send_paginated_bookings(
                query,
                bookings,
                f"📋 Prenotazioni sacerdote {priest_tag}",
                f"{priest_id}",
                page=1
            )

        elif data.startswith("bookings_page_"):
            payload = data[len("bookings_page_"):]
            try:
                page_part, filtro = payload.split("_", 1)
                page = int(page_part)
            except:
                # Torna al pannello principale
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏳ In attesa", callback_data="filter_pending")],
                    [InlineKeyboardButton("📌 Assegnate", callback_data="filter_assigned")],
                    [InlineKeyboardButton("✅ Completate", callback_data="filter_completed")],
                    [InlineKeyboardButton("🙏 Per sacerdote", callback_data="filter_priests")],
                    [InlineKeyboardButton("🎮 Cerca fedele", callback_data="search_fedele")],
                    [InlineKeyboardButton("🔎 Cerca per ID", callback_data="search_id")],
                    [InlineKeyboardButton("❌ Chiudi Pannello", callback_data="close_panel")],
                ])
                new_text = (
                    "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
                    "📋 Scegli il tipo di prenotazioni da visualizzare:"
                )
                await query.edit_message_text(new_text, reply_markup=kb, parse_mode="HTML")
                return

            last = context.user_data.get("last_list") or {}
            kind = last.get("kind")

            # 🔹 Paginazione per stato
            if kind == "status":
                status = last.get("status")
                bookings = session.query(Booking).filter(
                    Booking.status == status
                ).order_by(Booking.id.desc()).all()

                title = last.get("title") or f"📋 Prenotazioni {status.upper()}"
                await _send_paginated_bookings(query, bookings, title, status, page=page)

            # 🔹 Paginazione per sacerdote (TUTTE le prenotazioni)
            elif kind == "priest_all":
                priest_id = last.get("priest_id")
                priest = session.query(Priest).filter(
                    Priest.telegram_id == int(priest_id)
                ).first()

                priest_tag = f"@{priest.username}" if priest and priest.username else str(priest_id)

                assigns = session.query(Assignment).filter(
                    Assignment.priest_telegram_id == int(priest_id)
                ).all()

                bookings = [
                    session.query(Booking).get(a.booking_id)
                    for a in assigns
                    if session.query(Booking).get(a.booking_id)
                ]

                # 🔹 Ordine richiesto
                bookings = sorted(
                    bookings,
                    key=lambda b: (
                        0 if b.status == "assigned" else 1,
                        -b.id
                    )
                )

                title = last.get("title") or f"📋 Prenotazioni sacerdote {priest_tag}"
                await _send_paginated_bookings(query, bookings, title, f"{priest_id}", page=page)

            # 🔹 Ricerca nickname
            elif kind == "search_nick":
                term = last.get("term") or ""
                bookings = session.query(Booking).filter(
                    Booking.nickname_mc.ilike(f"%{term}%")
                ).order_by(Booking.id.desc()).all()

                title = last.get("title") or f"📋 Prenotazioni del fedele '{term}'"
                await _send_paginated_bookings(query, bookings, title, term, page=page)

            # 🔹 Ricerca per ID
            elif kind == "search_id":
                bid = last.get("booking_id")
                booking = session.query(Booking).get(bid) if bid else None
                bookings = [booking] if booking else []

                title = last.get("title") or f"📋 Prenotazione #{bid}"
                await _send_paginated_bookings(query, bookings, title, str(bid or ""), page=page)

        elif data == "back_main":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏳ In attesa", callback_data="filter_pending")],
                [InlineKeyboardButton("📌 Assegnate", callback_data="filter_assigned")],
                [InlineKeyboardButton("✅ Completate", callback_data="filter_completed")],
                [InlineKeyboardButton("🙏 Per sacerdote", callback_data="filter_priests")],
                [InlineKeyboardButton("🎮 Cerca fedele", callback_data="search_fedele")],
                [InlineKeyboardButton("🔎 Cerca per ID", callback_data="search_id")],
                [InlineKeyboardButton("❌ Chiudi Pannello", callback_data="close_panel")],
            ])
            new_text = (
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
                "📋 Scegli il tipo di prenotazioni da visualizzare:"
            )
            await query.edit_message_text(new_text, reply_markup=kb, parse_mode="HTML")

        elif data == "search_fedele":
            msg = await query.message.reply_text(
                "✍️ Inserisci il nickname del fedele con un messaggio in chat:",
                parse_mode="HTML",
                message_thread_id=DIRECTORS_TOPIC_ID
            )
            context.user_data["search_mode"] = "fedele"
            context.user_data["last_prompt_message_id"] = msg.message_id

        elif data == "search_id":
            msg = await query.message.reply_text(
                "✍️ Inserisci l'ID della prenotazione con un messaggio in chat:",
                parse_mode="HTML",
                message_thread_id=DIRECTORS_TOPIC_ID
            )
            context.user_data["search_mode"] = "id"
            context.user_data["last_prompt_message_id"] = msg.message_id

        elif data == "close_panel":
            try:
                await query.message.delete()
            except:
                pass
    finally:
        session.close()


async def lista_prenotazioni_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.user_data.get("search_mode")
    if not mode:
        return

    prompt_id = context.user_data.get("last_prompt_message_id")
    if prompt_id:
        try:
            await update.message.bot.delete_message(
                chat_id=update.effective_chat.id,
                message_id=prompt_id
            )
        except Exception as e:
            # Non bloccare il flusso se non riesce a cancellare
            print("Errore cancellando il prompt:", e)
        # resetta l'ID del prompt
        context.user_data["last_prompt_message_id"] = None

    session = SessionLocal()
    try:
        if mode == "fedele":
            filtro = update.message.text.strip()
            bookings = session.query(Booking).filter(
                Booking.nickname_mc.ilike(f"%{filtro}%")
            ).order_by(Booking.id.desc()).all()

            if bookings:
                context.user_data["last_list"] = {
                    "kind": "search_nick",
                    "term": filtro,
                    "title": f"📋 Prenotazioni del fedele '{filtro}'"
                }

                await _send_paginated_bookings(
                    update.message,
                    bookings,
                    f"📋 Prenotazioni del fedele '{filtro}'",
                    filtro,
                    page=1
                )
            else:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")]
                ])
                await update.message.reply_text(
                    f"❌ Nessuna prenotazione trovata per il fedele <b>{filtro}</b>.",
                    reply_markup=kb,
                    parse_mode="HTML",
                    message_thread_id=DIRECTORS_TOPIC_ID   # 🔹 invio nel topic
                )
        elif mode == "id":
            try:
                booking_id = int(update.message.text.strip())
            except ValueError:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")]
                ])
                await update.message.reply_text(
                    "❌ Devi inserire un ID numerico valido.",
                    reply_markup=kb,
                    parse_mode="HTML",
                    message_thread_id=DIRECTORS_TOPIC_ID   # 🔹 invio nel topic
                )
                return

            booking = session.query(Booking).get(booking_id)
            if booking:
                context.user_data["last_list"] = {
                    "kind": "search_id",
                    "booking_id": booking_id,
                    "title": f"📋 Prenotazione #{booking_id}"
                }

                await _send_paginated_bookings(
                    update.message,
                    [booking],
                    f"📋 Prenotazione #{booking_id}",
                    str(booking_id),
                    page=1
                )
            else:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")]
                ])
                await update.message.reply_text(
                    f"❌ Nessuna prenotazione trovata con ID <b>{booking_id}</b>.",
                    reply_markup=kb,
                    parse_mode="HTML",
                    message_thread_id=DIRECTORS_TOPIC_ID   # 🔹 invio nel topic
                )
    finally:
        session.close()

    # 🔹 Reset modalità ricerca
    context.user_data["search_mode"] = None


async def _send_paginated_bookings(target, bookings, titolo, filtro, page=1):
    if not bookings:
        msg = f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Nessuna prenotazione trovata per <b>{titolo}</b>."
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")]
        ])

        if isinstance(target, Message):
            await target.reply_text(msg, reply_markup=kb, parse_mode="HTML", message_thread_id=DIRECTORS_TOPIC_ID)
        elif isinstance(target, CallbackQuery):
            if target.message.text != msg or target.message.reply_markup != kb:
                await target.edit_message_text(msg, reply_markup=kb, parse_mode="HTML")
        return

    per_page = 5
    total_pages = (len(bookings) + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    bookings_page = bookings[start:end]

    lines = [f"--- 📋 {titolo} --- (Totale: {len(bookings)})"]

    session = SessionLocal()
    try:
        for b in bookings_page:
            assignment = session.query(Assignment).filter_by(booking_id=b.id).first()

            priest_tag = "Nessuno."
            if assignment and getattr(assignment, "priest_telegram_id", None):
                priest = session.query(Priest).filter(Priest.telegram_id == assignment.priest_telegram_id).first()
                if priest and priest.username:
                    priest_tag = f"@{priest.username}"
                else:
                    priest_tag = str(assignment.priest_telegram_id)

            secretary_tag = f"@{b.secretary_username}" if getattr(b, "secretary_username", None) else "Nessun contatto presente."
            timestamp = b.created_at.strftime("%d/%m/%Y %H:%M") if getattr(b, "created_at", None) else "-"

            lines.append(
                f"📌 Prenotazione #{b.id} [{b.status.upper()}]\n"
                f"• ✝️ Sacramento/i: {b.sacrament.replace('_',' ')}\n"
                f"• 🎮 Nick Minecraft: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                f"• 👤 Contatto TG fedele: {b.rp_name or 'Nessun contatto inserito.'}\n"
                f"• 📝 Note: {b.notes or 'Nessuna nota.'}\n"
                f"• 📖 Registrata dal segretario: {secretary_tag}\n"
                f"• ⏰ Orario: {timestamp}\n"
                f"• 🙏 Assegnata a: {priest_tag}\n"
                "-----------------------------"
            )
    finally:
        session.close()

    text = "\n".join(lines) + f"\n\n📄 Pagina {page}/{total_pages}"

    keyboard = []
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"bookings_page_{page-1}_{filtro or 'all'}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"bookings_page_{page+1}_{filtro or 'all'}"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")])

    ids_page = ",".join(str(b.id) for b in bookings_page)
    keyboard.append([InlineKeyboardButton("🗑 Rimuovi queste prenotazioni", callback_data=f"confirm_remove_{ids_page}")])

    kb = InlineKeyboardMarkup(keyboard)

    if isinstance(target, Message):
        await target.reply_text(text, reply_markup=kb, parse_mode="HTML", message_thread_id=DIRECTORS_TOPIC_ID)
    elif isinstance(target, CallbackQuery):
        if target.message.text != text or target.message.reply_markup != kb:
            await target.edit_message_text(text, reply_markup=kb, parse_mode="HTML")

# 🔎 Callback per conferma/annulla rimozione prenotazioni
async def handle_remove_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    session = SessionLocal()
    try:
        if data.startswith("confirm_remove_"):
            ids_str = data.replace("confirm_remove_", "")
            booking_ids = [int(x) for x in ids_str.split(",")]

            removed, not_found = [], []
            for booking_id in booking_ids:
                booking = session.query(Booking).get(booking_id)
                if not booking:
                    not_found.append(booking_id)
                    continue

                # Elimina assignment ed event log collegati
                session.query(Assignment).filter_by(booking_id=booking.id).delete()
                session.query(EventLog).filter_by(booking_id=booking.id).delete()
                session.delete(booking)
                removed.append(booking_id)

            session.commit()

            # Messaggio finale
            msg_parts = []
            if removed:
                msg_parts.append(
                    f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ Prenotazioni <b>rimosse</b>: {', '.join(map(str, removed))}"
                )
            if not_found:
                msg_parts.append(
                    f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Prenotazioni <b>non trovate</b>: {', '.join(map(str, not_found))}"
                )

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")]
            ])

            await query.edit_message_text(
                "\n".join(msg_parts) if msg_parts else
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Nessuna prenotazione rimossa.",
                parse_mode="HTML",
                reply_markup=kb
            )

        elif data == "cancel_remove":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Torna al pannello principale", callback_data="back_main")]
            ])

            await query.edit_message_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Rimozione <b>annullata</b>.",
                parse_mode="HTML",
                reply_markup=kb
            )

    finally:
        session.close()

async def weekly_report(app):
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)

        # Inizio settimana (lunedì)
        start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Fine settimana (domenica inclusa)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)

        # Prenotazioni completate nella settimana
        completed = session.query(Booking).filter(
            Booking.status == "completed",
            Booking.updated_at >= start,
            Booking.updated_at <= end
        ).all()

        total = len(completed)

        per_priest = {}
        priest_sacraments = {}

        for b in completed:
            a = session.query(Assignment).filter(
                Assignment.booking_id == b.id
            ).first()

            pid = a.priest_telegram_id if a else None
            if not pid:
                continue

            per_priest[pid] = per_priest.get(pid, 0) + 1

            if b.sacrament:
                sac_list = [s.strip() for s in b.sacrament.split(",")]
                notes = (b.notes or "").lower()

                if pid not in priest_sacraments:
                    priest_sacraments[pid] = {}

                for sac in sac_list:
                    sac_key = sac

                    # 🔹 MATRIMONIO BASE / PREMIUM
                    if sac.lower() == "matrimonio":
                        if "premium" in notes:
                            sac_key = "matrimonio premium"
                        elif "base" in notes or "default" in notes:
                            sac_key = "matrimonio base"

                    priest_sacraments[pid][sac_key] = priest_sacraments[pid].get(sac_key, 0) + 1

        per_sacrament = {}

        for b in completed:
            if not b.sacrament:
                continue

            sac_list = [s.strip() for s in b.sacrament.split(",")]
            notes = (b.notes or "").lower()

            for sac in sac_list:
                sac_key = sac

                # 🔹 MATRIMONIO BASE / PREMIUM
                if sac.lower() == "matrimonio":
                    if "premium" in notes:
                        sac_key = "matrimonio premium"
                    elif "base" in notes or "default" in notes:
                        sac_key = "matrimonio base"

                per_sacrament[sac_key] = per_sacrament.get(sac_key, 0) + 1

        open_items = session.query(Booking).filter(
            Booking.status.in_(["pending", "assigned", "in_progress"])
        ).count()

        lines = [
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️",
            "",
            "📊 <b>Report settimanale</b>",
            f"🗓 Periodo: <b>{start.date()} ➝ {end.date()}</b>",
            f"✝️ Totale sacramenti completati: <b>{total}</b>",
            "",
            "🏆 <b>Classifica sacerdoti:</b>"
        ]

        if per_priest:
            for pid, num in sorted(per_priest.items(), key=lambda x: x[1], reverse=True):
                priest = session.query(Priest).filter(
                    Priest.telegram_id == pid
                ).first()

                priest_tag = f"@{priest.username}" if priest and priest.username else str(pid)

                detail = []
                if pid in priest_sacraments:
                    for sac, count in priest_sacraments[pid].items():
                        sac_name = sac.replace("_", " ")
                        if count > 1:
                            detail.append(f"{sac_name} ({count} volte)")
                        else:
                            detail.append(sac_name)

                detail_str = ", ".join(detail) if detail else "Nessun sacramento registrato"

                lines.append(f"- 🙏 Sacerdote <b>{priest_tag}</b>: {num} ➝ {detail_str}")
        else:
            lines.append("ℹ️ Nessun sacramento completato dai sacerdoti questa settimana.")

        lines.append("")
        lines.append("✝️ <b>Dettaglio per sacramento (totale):</b>")

        if per_sacrament:
            for sac, num in per_sacrament.items():
                lines.append(f"- {sac.replace('_',' ')}: {num}")
        else:
            lines.append("ℹ️ Nessun sacramento completato questa settimana.")

        lines.append("")
        lines.append(f"📌 Prenotazioni ancora <b>aperte</b>: {open_items}")

        # Invio al gruppo direzione nel topic configurato
        await app.bot.send_message(
            DIRECTORS_GROUP_ID,
            "\n".join(lines),
            parse_mode="HTML",
            message_thread_id=12874
        )

    finally:
        session.close()


async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Si è verificato un <b>errore</b>.\n\n➡️ Sei pregato di segnalarlo a @LavatiScimmiaInfuocata.",
            parse_mode="HTML"
        )
# ---- DEBUG: Recupera ID del topic ----
async def get_topic_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.is_topic_message:
        thread_id = update.message.message_thread_id
        await update.message.reply_text(
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🆔 L'ID di questo topic è: <code>{thread_id}</code>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ Devi usare questo comando <b>all'interno di un topic</b> del gruppo Direzione.",
            parse_mode="HTML"
        )


# ---- BUILD APPLICATION ----
def build_application():
    # Inizializza DB
    init_db()
    # Costruisci l'applicazione Telegram
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)
    # --- START & Ruoli ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(choose_role, pattern=r"^role_(sacerdote|segretario|direzione)$"))
    # --- Prenotazioni ingame (Segretari) ---
    conv_ingame = ConversationHandler(
        entry_points=[CommandHandler("prenota_ingame", prenota_ingame)],
        states={
            IG_RP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ig_rp_name)],
            IG_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, ig_nick)],
            IG_SACRAMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, ig_sacrament)],
            IG_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, ig_notes)],
            IG_CONFIRM: [CallbackQueryHandler(ig_confirm)],
        },
        fallbacks=[CommandHandler("cancel", cancel_handler)],
        allow_reentry=True,
    )
    app.add_handler(conv_ingame)
    # --- Direzione ---
    app.add_handler(CommandHandler("riassegna", riassegna))  
    app.add_handler(CallbackQueryHandler(reassign_callback, pattern=r"^reassign_"))

    app.add_handler(CommandHandler("lista_prenotazioni", lista_prenotazioni))
    app.add_handler(CallbackQueryHandler(handle_remove_callback, pattern=r"^(confirm_remove_|cancel_remove)"))
    app.add_handler(CommandHandler("get_topic_id", get_topic_id))

    # 🔹 Assegnazioni tramite pulsanti
    app.add_handler(CallbackQueryHandler(assign_callback, pattern=r"^assign_\d+$"))
    app.add_handler(CallbackQueryHandler(do_assign_callback, pattern=r"^do_assign_\d+_\d+$"))

    # 🔹 Pannello avanzato prenotazioni
    app.add_handler(CallbackQueryHandler(
        lista_prenotazioni_callback,
        pattern=r"^(filter_|priest_|bookings_page_|back_main|search_fedele|search_id|close_panel)"
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lista_prenotazioni_search))

    # --- Sacerdoti ---
    app.add_handler(CommandHandler("mie_assegnazioni", mie_assegnazioni))
    app.add_handler(CallbackQueryHandler(mie_assegnazioni_page, pattern=r"^assign_page_\d+$"))
    app.add_handler(CallbackQueryHandler(completa_menu, pattern=r"^completa_menu$"))
    app.add_handler(CallbackQueryHandler(completa_booking, pattern=r"^completa_\d+$"))
    app.add_handler(CallbackQueryHandler(back_menu, pattern=r"^back_menu$"))

    return app
