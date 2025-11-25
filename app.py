import os
import logging
from datetime import datetime, timedelta, timezone, time
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, BigInteger
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

# ---- ENV ----
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
PRIESTS_GROUP_ID = int(os.getenv("PRIESTS_GROUP_ID", "0"))
DIRECTORS_GROUP_ID = int(os.getenv("DIRECTORS_GROUP_ID", "0"))
SECRETARIES_IDS = {int(x) for x in os.getenv("SECRETARIES_IDS", "").split(",") if x}
PRIESTS_IDS = {int(x) for x in os.getenv("PRIESTS_IDS", "").split(",") if x}
DIRECTORS_IDS = {int(x) for x in os.getenv("DIRECTORS_IDS", "").split(",") if x}

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
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Benvenuto! Questo bot ti aiuterà nelle tue mansioni da <b>sacerdote</b>.\n\n📜 Comandi principali:\n- <code>/mie_assegnazioni</code> → controlla i sacramenti che ti vengono assegnati (riceverai notifiche automatiche).\n- <code>/completa &lt;id prenotazione&gt;</code> → contrassegna una prenotazione come completata.\n\n⚠️ Ricorda: è tuo dovere verificare quotidianamente le assegnazioni.\n\nSe hai difficoltà o riscontri problemi contatta 👉 <b>Consiglio degli Anziani</b>.",
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
    if update.effective_chat.type != "private":
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Questo comando può essere usato <b>solo in privato</b> con il bot.",
            parse_mode="HTML"
        )
        return ConversationHandler.END
    msg = await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📝 Per iniziare la procedura di registrazione, inserisci la <b>@ del fedele</b> che ha prenotato:",
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
        text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🎮 Bene! Adesso ti chiedo di inserire il <b>nickname di Minecraft</b> del fedele.\n\n➡️ Se si tratta di un matrimonio inserisci il nome dei due coniugi.",
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
    kb = ReplyKeyboardMarkup([[KeyboardButton(s.replace("_"," "))] for s in SACRAMENTS],
                             one_time_keyboard=False, resize_keyboard=True)
    # elimina messaggi
    await update.message.delete()
    if "last_prompt_id" in context.user_data:
        try:
            await context.bot.delete_message(update.effective_chat.id, context.user_data["last_prompt_id"])
        except Exception:
            pass

    if s == "fine":
        if not context.user_data["sacraments"]:
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
            text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Siamo arrivati quasi alla fine.\n\n➡️ Inserisci delle <b>note aggiuntive</b> (se non ci sono scrivi 'no'):",
            parse_mode="HTML"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_NOTES

    if s not in SACRAMENTS:
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Il sacramento inserito non è <b>valido</b>.\n\n➡️ Riprova:",
            reply_markup=kb,
            parse_mode="HTML"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_SACRAMENT

    context.user_data["sacraments"].append(s)
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ Il sacramento è stato <b>aggiunto con successo</b>!\n\n➡️ Selezionane un altro oppure scrivi <b>'fine'</b>:",
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
    if query.data == "cancel":
        await query.edit_message_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ La prenotazione è stata <i>annullata con successo</i>!\n\n"
            "➡️ Se vuoi effettuarla di nuovo digita <code>/prenota_ingame</code>",
            parse_mode="HTML"
        )
        return ConversationHandler.END
    if query.data != "confirm":
        return

    user = update.effective_user
    user_id = user.id
    if not is_secretary(user_id):
        await query.edit_message_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Non hai il <i>permesso</i> per eseguire questa azione.",
            parse_mode="HTML"
        )
        return ConversationHandler.END

    session = SessionLocal()
    try:
        sacrament_display_raw = ", ".join(context.user_data.get("sacraments", []))

        booking = Booking(
            source="ingame",
            rp_name=context.user_data["rp_name"],
            nickname_mc=context.user_data["nickname_mc"],
            sacrament=sacrament_display_raw,
            notes=context.user_data["notes"],
            status="pending",
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

        await query.edit_message_text(
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ La tua prenotazione è stata <i>registrata con successo</i>! (ID #{booking.id})\n\n"
            "📋 Resoconto delle informazioni inserite:\n\n"
            f"• 👤 Contatto Telegram: <b>{rp_name}</b>\n"
            f"• 🎮 Nick: <b>{nickname_mc}</b>\n"
            f"• ✝️ Sacramenti: <b>{sacrament_display}</b>\n"
            f"• 📝 Note: <b>{safe_notes}</b>",
            parse_mode="HTML"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Assegna", callback_data=f"assign_{booking.id}")]
        ])

        # 🔹 Salviamo l'ID del messaggio originale della prenotazione
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
            parse_mode="HTML"
        )

        # Salvo l'ID del messaggio originale per poterlo modificare dopo
        context.user_data[f"booking_msg_{booking.id}"] = msg.message_id

        return ConversationHandler.END
    finally:
        session.close()

# ---- DIREZIONE: CALLBACK "Assegna" ----
async def assign_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_director(update.effective_user.id):
        await query.answer("❌ Non hai il permesso.", show_alert=True)
        return

    booking_id = int(query.data.replace("assign_", ""))

    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if not booking or booking.status != "pending":
            await query.answer("⚠️ Prenotazione non valida o già assegnata.", show_alert=True)
            return

        # Costruisci lista sacerdoti
        priests = session.query(Priest).all()
        buttons = [
            [InlineKeyboardButton(f"@{p.username}", callback_data=f"do_assign_{booking_id}_{p.telegram_id}")]
            for p in priests
        ]
        buttons.append([InlineKeyboardButton("❌ Annulla", callback_data="cancel_assign")])

        msg = await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Seleziona il sacerdote per la prenotazione #{booking.id}:",
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode="HTML"
        )

        # Salva l'ID del messaggio per poterlo cancellare dopo
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
        # 🔹 Rimuovi pulsante "Assegna" dal messaggio originale
        booking_msg_id = context.user_data.get(f"booking_msg_{booking.id}")
        if booking_msg_id:
            await context.bot.edit_message_reply_markup(
                chat_id=DIRECTORS_GROUP_ID,
                message_id=booking_msg_id,
                reply_markup=None
            )
        # 🔹 Notifica al gruppo Direzione
        await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n✅ Prenotazione #{booking.id} <b>assegnata</b> a @{priest.username}.",
            parse_mode="HTML"
        )
        # 🔹 Notifica al sacerdote
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

    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ Sintassi errata!\n\n➡️ Utilizzo corretto: <code>/riassegna &lt;id richiesta&gt; &lt;@username&gt;</code>",
            parse_mode="HTML"
        )
        return

    booking_id = int(args[1])
    target = args[2]

    if not target.startswith("@"):
        await update.message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ Devi specificare l'<b>@username</b> del sacerdote (es. @nomeutente).",
            parse_mode="HTML"
        )
        return

    username = target.lstrip("@")

    session = SessionLocal()
    try:
        priest = session.query(Priest).filter_by(username=username).first()
        if not priest:
            await update.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Username non valido o sacerdote non registrato.",
                parse_mode="HTML"
            )
            return

        priest_id = priest.telegram_id

        if not is_priest(priest_id):
            await update.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ L'utente indicato non è registrato come <b>sacerdote</b>.",
                parse_mode="HTML"
            )
            return

        booking = session.query(Booking).get(booking_id)
        if not booking:
            await update.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ La <b>prenotazione</b> inserita risulta inesistente.",
                parse_mode="HTML"
            )
            return

        # 🔎 Blocco se la prenotazione è completata o annullata
        if booking.status in ("completed", "cancelled"):
            await update.message.reply_text(
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ La prenotazione #{booking.id} è <b>{booking.status.upper()}</b> e non può essere riassegnata.",
                parse_mode="HTML"
            )
            return

        existing_assign = session.query(Assignment).filter_by(booking_id=booking.id).first()
        if not existing_assign:
            await update.message.reply_text(
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ La prenotazione #{booking.id} non è ancora stata assegnata.\n➡️ Usa <code>/assegna</code>.",
                parse_mode="HTML"
            )
            return

        # Aggiorna l'assegnazione
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

        await update.message.reply_text(
            f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🔄 Prenotazione #{booking.id} <b>riassegnata</b> a @{username}.",
            parse_mode="HTML"
        )
        try:
            await context.bot.send_message(
                priest_id,
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Hey sacerdote! Ti è appena stata <b>riassegnata una prenotazione</b> (#{booking.id}).\n➡️ Utilizza <code>/mie_assegnazioni</code> per i dettagli.",
                parse_mode="HTML"
            )
        except telegram.error.Forbidden:
            await update.message.reply_text(
                f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n⚠️ Impossibile notificare @{username} in privato.\n➡️ Deve avviare il bot.",
                parse_mode="HTML"
            )

        # 🔎 Cancella eventuale job precedente
        for job in context.job_queue.get_jobs_by_name(f"notify_{booking.id}"):
            job.schedule_removal()

        # Pianifica nuovo job di 48 ore
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
                parse_mode="HTML"
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
        assigns = (
            session.query(Assignment)
            .filter(Assignment.priest_telegram_id == priest_id)
            .order_by(Assignment.id.desc())
            .all()
        )
        if not assigns:
            await update.message.reply_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Al momento non ti è stata <b>assegnata alcuna prenotazione</b>, ma questo durerà ancora per poco!",
                parse_mode="HTML"
            )
            return

        msgs = []
        for a in assigns:
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

        # Bottone per completare
        complete_button = [InlineKeyboardButton("✝️ Completa una prenotazione", callback_data="completa_menu")]
        kb = InlineKeyboardMarkup([complete_button])

        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
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
            parse_mode="HTML"
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
    complete_button = [InlineKeyboardButton("✝️ Completa una prenotazione", callback_data="completa_menu")]
    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([complete_button])

# ---- CANCEL ----
async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Processo <b>annullato</b>.",
        parse_mode="HTML"
    )
    return ConversationHandler.END


async def check_sla(app):
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(hours=48)
        overdue = session.query(Assignment).all()
        for a in overdue:
            b = session.query(Booking).get(a.booking_id)
            if not b or b.status == "completed":
                continue

            ref_time = a.taken_at or a.assigned_at
            if ref_time:
                # Normalizza ref_time in UTC
                if ref_time.tzinfo is None:
                    ref_time = ref_time.replace(tzinfo=timezone.utc)
                else:
                    ref_time = ref_time.astimezone(timezone.utc)

                if ref_time < threshold and not a.due_alert_sent:
                    a.due_alert_sent = True
                    session.add(a)
                    session.add(EventLog(
                        booking_id=b.id,
                        actor_id=0,
                        action="alert",
                        details="48h SLA"
                    ))
                    session.commit()
                    await app.bot.send_message(
                        DIRECTORS_GROUP_ID,
                        f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n"
                        f"⚠️ ALERT: Prenotazione #{b.id} assegnata al sacerdote "
                        f"<b>{a.priest_telegram_id}</b> da oltre <b>48h</b>.",
                        parse_mode="HTML"
                    )
    finally:
        session.close()
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
    ])

    await update.message.reply_text(
        "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Scegli il tipo di prenotazioni da visualizzare:",
        reply_markup=kb,
        parse_mode="HTML"
    )


async def lista_prenotazioni_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    session = SessionLocal()
    try:
        if data.startswith("filter_"):
            filtro = data.replace("filter_", "")
            if filtro in STATUS:
                context.user_data["last_list"] = {"kind": "status", "status": filtro, "title": f"📋 Prenotazioni {filtro.upper()}"}
                bookings = session.query(Booking).filter(Booking.status == filtro).order_by(Booking.id.desc()).all()
                await _send_paginated_bookings(query, bookings, f"📋 Prenotazioni {filtro.upper()}", filtro, page=1)

            elif filtro == "priests":
                priests = session.query(Priest).all()
                buttons = [[InlineKeyboardButton(f"@{p.username or p.telegram_id}", callback_data=f"priest_{p.telegram_id}")]
                           for p in priests]
                buttons.append([InlineKeyboardButton("⬅️ Torna indietro", callback_data="back_main")])
                new_text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Scegli un sacerdote:"
                new_markup = InlineKeyboardMarkup(buttons)
                if query.message.text != new_text or query.message.reply_markup != new_markup:
                    await query.edit_message_text(new_text, reply_markup=new_markup, parse_mode="HTML")

        elif data.startswith("priest_"):
            priest_id = int(data.replace("priest_", ""))
            priest = session.query(Priest).filter(Priest.telegram_id == priest_id).first()
            priest_tag = f"@{priest.username}" if priest and priest.username else str(priest_id)

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📌 Assegnate", callback_data=f"priestfilter_{priest_id}_assigned")],
                [InlineKeyboardButton("✅ Completate", callback_data=f"priestfilter_{priest_id}_completed")],
                [InlineKeyboardButton("⬅️ Torna indietro", callback_data="filter_priests")],
            ])
            new_text = f"<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n🙏 Filtra le prenotazioni del sacerdote selezionato ({priest_tag}):"
            if query.message.text != new_text or query.message.reply_markup != kb:
                await query.edit_message_text(new_text, reply_markup=kb, parse_mode="HTML")

        elif data.startswith("priestfilter_"):
            _, priest_id, status = data.split("_")
            priest_id = int(priest_id)
            priest = session.query(Priest).filter(Priest.telegram_id == priest_id).first()
            priest_tag = f"@{priest.username}" if priest and priest.username else str(priest_id)

            context.user_data["last_list"] = {
                "kind": "priest",
                "priest_id": priest_id,
                "status": status,
                "title": f"📋 Prenotazioni sacerdote {priest_tag} [{status}]"
            }

            assigns = session.query(Assignment).filter(Assignment.priest_telegram_id == priest_id).all()
            bookings = [session.query(Booking).get(a.booking_id) for a in assigns if session.query(Booking).get(a.booking_id)]
            bookings = [b for b in bookings if b and b.status == status]

            await _send_paginated_bookings(query, bookings, f"📋 Prenotazioni sacerdote {priest_tag} [{status}]", f"{priest_id}", page=1)

        elif data.startswith("bookings_page_"):
            payload = data[len("bookings_page_"):]
            try:
                page_part, filtro = payload.split("_", 1)
                page = int(page_part)
            except (ValueError, IndexError):
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏳ In attesa", callback_data="filter_pending")],
                    [InlineKeyboardButton("📌 Assegnate", callback_data="filter_assigned")],
                    [InlineKeyboardButton("✅ Completate", callback_data="filter_completed")],
                    [InlineKeyboardButton("🙏 Per sacerdote", callback_data="filter_priests")],
                    [InlineKeyboardButton("🎮 Cerca fedele", callback_data="search_fedele")],
                    [InlineKeyboardButton("🔎 Cerca per ID", callback_data="search_id")],
                ])
                new_text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Scegli il tipo di prenotazioni da visualizzare:"
                if query.message.text != new_text or query.message.reply_markup != kb:
                    await query.edit_message_text(new_text, reply_markup=kb, parse_mode="HTML")
                return

            last = context.user_data.get("last_list") or {}
            kind = last.get("kind")

            if kind == "status":
                status = last.get("status")
                bookings = session.query(Booking).filter(Booking.status == status).order_by(Booking.id.desc()).all()
                title = last.get("title") or f"📋 Prenotazioni {status.upper()}"
                await _send_paginated_bookings(query, bookings, title, status, page=page)

            elif kind == "priest":
                priest_id = last.get("priest_id")
                status = last.get("status")
                priest = session.query(Priest).filter(Priest.telegram_id == int(priest_id)).first()
                priest_tag = f"@{priest.username}" if priest and priest.username else str(priest_id)
                assigns = session.query(Assignment).filter(Assignment.priest_telegram_id == int(priest_id)).all()
                bookings = [session.query(Booking).get(a.booking_id) for a in assigns if session.query(Booking).get(a.booking_id)]
                bookings = [b for b in bookings if b and b.status == status]
                title = last.get("title") or f"📋 Prenotazioni sacerdote {priest_tag} [{status}]"
                await _send_paginated_bookings(query, bookings, title, f"{priest_id}", page=page)

            elif kind == "search_nick":
                term = last.get("term") or ""
                bookings = session.query(Booking).filter(Booking.nickname_mc.ilike(f"%{term}%")).order_by(Booking.id.desc()).all()
                title = last.get("title") or f"📋 Prenotazioni del fedele '{term}'"
                await _send_paginated_bookings(query, bookings, title, term, page=page)

            elif kind == "search_id":
                bid = last.get("booking_id")
                booking = session.query(Booking).get(bid) if bid else None
                bookings = [booking] if booking else []
                title = last.get("title") or f"📋 Prenotazione #{bid}"
                await _send_paginated_bookings(query, bookings, title, str(bid or ""), page=page)

            else:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("⏳ In attesa", callback_data="filter_pending")],
                    [InlineKeyboardButton("📌 Assegnate", callback_data="filter_assigned")],
                    [InlineKeyboardButton("✅ Completate", callback_data="filter_completed")],
                    [InlineKeyboardButton("🙏 Per sacerdote", callback_data="filter_priests")],
                    [InlineKeyboardButton("🎮 Cerca fedele", callback_data="search_fedele")],
                    [InlineKeyboardButton("🔎 Cerca per ID", callback_data="search_id")],
                ])
                new_text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Scegli il tipo di prenotazioni da visualizzare:"
                if query.message.text != new_text or query.message.reply_markup != kb:
                    await query.edit_message_text(new_text, reply_markup=kb, parse_mode="HTML")
        elif data == "back_main":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏳ In attesa", callback_data="filter_pending")],
                [InlineKeyboardButton("📌 Assegnate", callback_data="filter_assigned")],
                [InlineKeyboardButton("✅ Completate", callback_data="filter_completed")],
                [InlineKeyboardButton("🙏 Per sacerdote", callback_data="filter_priests")],
                [InlineKeyboardButton("🎮 Cerca fedele", callback_data="search_fedele")],
                [InlineKeyboardButton("🔎 Cerca per ID", callback_data="search_id")],
            ])
            new_text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n📋 Scegli il tipo di prenotazioni da visualizzare:"
            if query.message.text != new_text or query.message.reply_markup != kb:
                await query.edit_message_text(
                    new_text,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
        elif data == "search_fedele":
            msg = await query.message.reply_text(
                "✍️ Inserisci il nickname del fedele con un messaggio in chat:",
                parse_mode="HTML"
            )
            context.user_data["search_mode"] = "fedele"
            context.user_data["last_prompt_message_id"] = msg.message_id
        elif data == "search_id":
            msg = await query.message.reply_text(
                "✍️ Inserisci l'ID della prenotazione con un messaggio in chat:",
                parse_mode="HTML"
            )
            context.user_data["search_mode"] = "id"
            context.user_data["last_prompt_message_id"] = msg.message_id
        elif data == "close_panel":
            new_text = "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Pannello prenotazioni chiuso."
            if query.message.text != new_text:
                await query.edit_message_text(
                    new_text,
                    parse_mode="HTML"
                )

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
                    parse_mode="HTML"
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
                    parse_mode="HTML"
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
                    parse_mode="HTML"
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
            await target.reply_text(msg, reply_markup=kb, parse_mode="HTML")
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
        await target.reply_text(text, reply_markup=kb, parse_mode="HTML")
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

            await query.edit_message_text(
                "\n".join(msg_parts) if msg_parts else
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\nℹ️ Nessuna prenotazione rimossa.",
                parse_mode="HTML"
            )

        elif data == "cancel_remove":
            await query.edit_message_text(
                "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Rimozione <b>annullata</b>.",
                parse_mode="HTML"
            )

    finally:
        session.close()


async def weekly_report(app):
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        # Inizio settimana (lunedì)
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        # Fine settimana (domenica)
        end = start + timedelta(days=7)

        # Prenotazioni completate nella settimana
        completed = session.query(Booking).filter(
            Booking.status == "completed",
            Booking.updated_at >= start,
            Booking.updated_at < end
        ).all()

        total = len(completed)

        # Classifica per sacerdote
        per_priest = {}
        for b in completed:
            a = session.query(Assignment).filter(Assignment.booking_id == b.id).first()
            pid = a.priest_telegram_id if a else None
            if pid:
                per_priest[pid] = per_priest.get(pid, 0) + 1

        # Conteggio per sacramento
        per_sacrament = {}
        for b in completed:
            if b.sacrament:
                sac_list = b.sacrament.split(",")
                for sac in sac_list:
                    sac = sac.strip()
                    per_sacrament[sac] = per_sacrament.get(sac, 0) + 1

        # Prenotazioni ancora aperte
        open_items = session.query(Booking).filter(
            Booking.status.in_(["pending", "assigned", "in_progress"])
        ).count()

        # Costruzione messaggio
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
                priest = session.query(Priest).filter(Priest.telegram_id == pid).first()
                priest_tag = f"@{priest.username}" if priest and priest.username else str(pid)
                lines.append(f"- 🙏 Sacerdote <b>{priest_tag}</b>: {num}")
        else:
            lines.append("ℹ️ Nessun sacramento completato dai sacerdoti questa settimana.")

        lines.append("")
        lines.append("✝️ <b>Dettaglio per sacramento:</b>")
        if per_sacrament:
            for sac, num in per_sacrament.items():
                lines.append(f"- {sac.replace('_',' ')}: {num}")
        else:
            lines.append("ℹ️ Nessun sacramento completato questa settimana.")

        lines.append("")
        lines.append(f"📌 Prenotazioni ancora <b>aperte</b>: {open_items}")

        # Invio al gruppo direzione
        await app.bot.send_message(DIRECTORS_GROUP_ID, "\n".join(lines), parse_mode="HTML")

    finally:
        session.close()


async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "<b>𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄</b> ⚓️\n\n❌ Si è verificato un <b>errore</b>.\n\n➡️ Sei pregato di segnalarlo a @LavatiScimmiaInfuocata.",
            parse_mode="HTML"
        )


# ---- BUILD APPLICATION ----
def build_application():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)
    # START (solo benvenuto)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(choose_role, pattern=r"^role_(sacerdote|segretario|direzione)$"))
    # Ingame booking conversation (secretaries)
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
    # Direzione
    # 🔹 Rimosso il comando /assegna (ora gestito da pulsanti inline)
    app.add_handler(CommandHandler("riassegna", riassegna))
    app.add_handler(CommandHandler("lista_prenotazioni", lista_prenotazioni))
    app.add_handler(CallbackQueryHandler(handle_remove_callback, pattern="^(confirm_remove_|cancel_remove)"))
    # 🔹 Nuovi handler per assegnazione tramite pulsanti
    app.add_handler(CallbackQueryHandler(assign_callback, pattern=r"^assign_\d+$"))
    app.add_handler(CallbackQueryHandler(do_assign_callback, pattern=r"^do_assign_\d+_\d+$"))
    # 🔹 Nuovi handler per pannello avanzato prenotazioni
    app.add_handler(CallbackQueryHandler(
        lista_prenotazioni_callback,
        pattern="^(filter_|priest_|priestfilter_|bookings_page_|back_main|search_fedele|search_id|close_panel)"
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lista_prenotazioni_search))
    # Sacerdoti
    app.add_handler(CommandHandler("mie_assegnazioni", mie_assegnazioni))
    app.add_handler(CommandHandler("completa", completa))
    # Paginazioni assegnazioni
    app.add_handler(CallbackQueryHandler(mie_assegnazioni_page, pattern=r"^assign_page_\d+$"))
    # 🔹 Nuovi callback per completamento prenotazioni
    app.add_handler(CallbackQueryHandler(completa_menu, pattern=r"^completa_menu$"))
    app.add_handler(CallbackQueryHandler(completa_booking, pattern=r"^completa_\d+$"))
    app.add_handler(CallbackQueryHandler(back_menu, pattern=r"^back_menu$"))

    # Scheduler
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(check_sla, "interval", hours=1, args=[app])
    scheduler.add_job(weekly_report, "cron", day_of_week="sun", hour=23, minute=55, args=[app])
    scheduler.start()

    return app
