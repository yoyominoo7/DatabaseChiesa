import os
import logging
from datetime import datetime, timedelta, timezone, time
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, BigInteger

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    Message,
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

def role_required(check_func, msg="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nHey, sembra che tu non abbia il permesso per effettuare questo comando. Se pensi sia un errore contatta @LavatiScimmiaInfuocata"):
    def decorator(func):
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            user_id = update.effective_user.id
            if not check_func(user_id):
                await update.effective_message.reply_text(msg)
                return
            return await func(update, context)
        return wrapper
    return decorator
# ---- CONVERSATION STATES ----
CHOOSE_MODE, CHOOSE_ROLE, START_SACRAMENT, ENTER_NICK, ENTER_NOTES, CONFIRM_BOOKING = range(6)
IG_RP_NAME, IG_NICK, IG_SACRAMENT, IG_NOTES, IG_CONFIRM = range(5)


def sacrament_keyboard():
    buttons = [[InlineKeyboardButton(s.title().replace("_", " "), callback_data=f"sac_{s}")] for s in SACRAMENTS]
    cancel = [InlineKeyboardButton("❌Annulla", callback_data="cancel")]
    return InlineKeyboardMarkup(buttons + [cancel])

def confirm_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅Conferma", callback_data="confirm")],
        [InlineKeyboardButton("❌Annulla", callback_data="cancel")],
    ])

# ---- CLIENT FLOW ----
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return ConversationHandler.END
    user = update.effective_user
    user_id = user.id
    roles = []

    # --- Registrazione automatica sacerdote ---
    if is_priest(user_id):
        session = SessionLocal()
        try:
            priest = session.query(Priest).filter_by(telegram_id=user_id).first()
            if priest:
                # aggiorna username se cambiato
                priest.username = user.username
            else:
                priest = Priest(telegram_id=user_id, username=user.username)
                session.add(priest)
            session.commit()
        finally:
            session.close()
        roles.append("sacerdote")
    # ------------------------------------------

    if is_secretary(user_id):
        roles.append("segretario")
    if is_director(user_id):
        roles.append("direzione")

    if not roles:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Singolo sacramento", callback_data="mode_single")],
            [InlineKeyboardButton("Più sacramenti", callback_data="mode_multi")],
        ])
        await update.message.reply_text(
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Attraverso questo bot potrai richiedere di prenotare lo svolgimento di un sacramento direttamente da telegram.\n\nPer iniziare, scegli se vuoi prenotare un singolo sacramento oppure più sacramenti.\n\nRicorda, l'uso improprio del bot comporterà il ban permanente da esso. Se hai difficoltà o riscontri problemi contatta @LavatiScimmiaInfuocata.",
            reply_markup=kb
        )
        return CHOOSE_MODE

    # Caso: un solo ruolo → messaggio automatico
    if len(roles) == 1:
        role = roles[0]
        if role == "sacerdote":
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Questo bot ti aiuterà nelle tue mansioni da sacerdote. \n\n- /mie_assegnazioni\nConsidera questo bot come il tuo nuovo migliore amico, dovrai contattarlo quotidianamente per controllare i sacramenti che ti vengono assegnati. Quando avrai una nuova assegnazione ti verrà inviata una notifica.\n\n- /completa <id prenotazione>\nUna volta completata la prenotazione assegnata dovrai contrassegnarlo come completa attraverso questo comando.\n\n Se hai difficoltà o riscontri problemi contatta il Consiglio degli Anziani.")
        elif role == "segretario":
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Questo bot ti aiuterà nelle tue mansioni da segretario.\n\n- /prenota_ingame\nConsidera questo bot come il tuo nuovo migliore amico, ogni volta che farai pagare un sacramento dovrai registrarlo attraverso questo comando che permetterà, alla fine, di assegnare lo svolgimento del sacramento a un sacerdote.\nMi raccomando non creare prenotazioni false o di prova, rischi di rompere il bot. \n\n Se hai difficoltà o riscontri problemi contatta il Consiglio degli Anziani.")
        elif role == "direzione":
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Questo bot ti aiuterà nelle tue mansioni da Patriarca.\n\n- /assegna <id prenotazione> <@ del sacerdote>\nConsidera questo bot come il tuo nuovo migliore amico, ogni volta che arriverà una nuova prenotazione dovrai assegnarla a un sacerdote affinché esso la completi. Cerca di assegnare meno prenotazioni ai segretari e di più ai sacerdoti.\n\n- /riassegna <id prenotazione> <@ del sacerdote>\nHai sbagliato ad assegnare una prenotazione? Nessun problema, puoi riassegnarla tranquillamente.\n\n- /lista_prenotazioni <pending / assigned / completed / @ del sacerdote / nick del fedele>\nPending: potrai vedere tutte le prenotazioni in attesa di assegnazione.\nassigned: potrai vedere tutte le prenotazioni assegnate ai sacerdoti.\ncompleted: potrai vedere tutte le prenotazioni completate dai sacerdoti.\n@ del sacerdote: potrai vedere tutte le prenotazioni assegnate e completate di un sacerdote.\nnick del fedele: potrai vedere tutti i sacramenti prenotati dal fedele.\n\n Se hai difficoltà o riscontri problemi contatta Falco o yomino.")
        return ConversationHandler.END

    # Caso: più ruoli → scelta con bottoni (aggiungiamo anche 'fedele')
    buttons = [[InlineKeyboardButton(r.capitalize(), callback_data=f"role_{r}")] for r in roles]
    buttons.append([InlineKeyboardButton("Fedele", callback_data="role_fedele")])

    await update.message.reply_text(
        "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n"
        "\n"
        "Poiché sei un vip della chiesa, possiedi più ruoli! Però puoi usarne solo uno alla volta, scegli quale messaggio di start hai bisogno tra quelli indicati qui sotto:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return CHOOSE_ROLE

async def choose_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    mode = query.data
    if mode == "mode_single":
        context.user_data["multi"] = False
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nPerfetto, hai scelto di prenotare un singolo sacramento, il prossimo passo è scegliere quale.")
        await context.bot.send_message(query.message.chat_id, "Utilizza i bottoni qui sotto per procedere:", reply_markup=sacrament_keyboard())
        return START_SACRAMENT
    elif mode == "mode_multi":
        context.user_data["multi"] = True
        context.user_data["sacraments"] = []
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nPerfetto, hai scelto di prenotare più sacramenti, il prossimo passo è scegliere quali.")
        await context.bot.send_message(query.message.chat_id, "Utilizza i bottoni qui sotto per procedere:", reply_markup=sacrament_keyboard())
        return START_SACRAMENT

async def choose_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    role = query.data.replace("role_", "")

    if role == "sacerdote":
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Questo bot ti aiuterà nelle tue mansioni da sacerdote. \n\n- /mie_assegnazioni\nConsidera questo bot come il tuo nuovo migliore amico, dovrai contattarlo quotidianamente per controllare i sacramenti che ti vengono assegnati. Quando avrai una nuova assegnazione ti verrà inviata una notifica.\n\n- /completa <id prenotazione>\nUna volta completata la prenotazione assegnata dovrai contrassegnarlo come completa attraverso questo comando.\n\n Se hai difficoltà o riscontri problemi contatta il Consiglio degli Anziani.")
        return ConversationHandler.END
    elif role == "segretario":
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Questo bot ti aiuterà nelle tue mansioni da segretario.\n\n- /prenota_ingame\nConsidera questo bot come il tuo nuovo migliore amico, ogni volta che farai pagare un sacramento dovrai registrarlo attraverso questo comando che permetterà, alla fine, di assegnare lo svolgimento del sacramento a un sacerdote.\nMi raccomando non creare prenotazioni false o di prova, rischi di rompere il bot. \n\n Se hai difficoltà o riscontri problemi contatta il Consiglio degli Anziani.")
        return ConversationHandler.END
    elif role == "direzione":
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Questo bot ti aiuterà nelle tue mansioni da Patriarca.\n\n- /assegna <id prenotazione> <@ del sacerdote>\nConsidera questo bot come il tuo nuovo migliore amico, ogni volta che arriverà una nuova prenotazione dovrai assegnarla a un sacerdote affinché esso la completi. Cerca di assegnare meno prenotazioni ai segretari e di più ai sacerdoti.\n\n- /riassegna <id prenotazione> <@ del sacerdote>\nHai sbagliato ad assegnare una prenotazione? Nessun problema, puoi riassegnarla tranquillamente.\n\n- /lista_prenotazioni <pending / assigned / completed / @ del sacerdote / nick del fedele>\nPending: potrai vedere tutte le prenotazioni in attesa di assegnazione.\nassigned: potrai vedere tutte le prenotazioni assegnate ai sacerdoti.\ncompleted: potrai vedere tutte le prenotazioni completate dai sacerdoti.\n@ del sacerdote: potrai vedere tutte le prenotazioni assegnate e completate di un sacerdote.\nnick del fedele: potrai vedere tutti i sacramenti prenotati dal fedele.\n\n Se hai difficoltà o riscontri problemi contatta Falco o yomino.")
        return ConversationHandler.END
    elif role == "fedele":
        # Flusso standard da fedele
        await query.edit_message_text(
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBenvenuto nel bot ufficiale del Culto di Poseidone! Attraverso questo bot potrai richiedere di prenotare lo svolgimento di un sacramento direttamente da telegram.\n\nPer iniziare, scegli quale sacramento vuoi prenotare.\n\nRicorda, l'uso improprio del bot comporterà il ban permanente da esso. Se hai difficoltà o riscontri problemi contatta @LavatiScimmiaInfuocata.",
            reply_markup=sacrament_keyboard()
        )
        # Qui ritorni lo stato iniziale della conversazione
        return START_SACRAMENT


async def choose_sacrament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nLa prenotazione è stata annullata con successo! Se vuoi effettuarla di nuovo digita /start")
        return ConversationHandler.END
    if not query.data.startswith("sac_"):
        return
    sacr = query.data[4:]

    if context.user_data.get("multi"):
        context.user_data["sacraments"].append(sacr)
        # Chiedi se vuole aggiungere altri sacramenti o passare al nick
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Aggiungi un altro sacramento", callback_data="add_more")],
            [InlineKeyboardButton("Prosegui con il prossimo passo", callback_data="go_nick")],
        ])
        await query.edit_message_text(
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nHai scelto il sacramento {sacr.replace('_',' ')}.\nVuoi aggiungere un altro sacramento o procedere con il prossimo passo?",
            reply_markup=kb
        )
        return START_SACRAMENT
    else:
        context.user_data["sacrament"] = sacr
        await query.delete_message()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBene! Adesso ti chiedo di rispondere a questo messaggio con il tuo nickname di minecraft:"
        )
        return ENTER_NICK

async def multi_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "add_more":
        await query.edit_message_text("Bene! Scegli il prossimo sacramento:", reply_markup=sacrament_keyboard())
        return START_SACRAMENT
    elif query.data == "go_nick":
        if context.user_data.get("multi"):
            context.user_data["sacrament"] = ",".join(context.user_data.get("sacraments", []))
        await query.edit_message_text(
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBene! Adesso ti chiedo di rispondere a questo messaggio con il tuo nickname di minecraft:"
        )
        return ENTER_NICK

async def enter_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    nick = update.message.text.strip()
    context.user_data["nickname_mc"] = nick
    await update.message.delete()
    await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nVuoi aggiungere una particolare richiesta? inviala qui sotto. Se non vuoi aggiungere nulla, rispondi a questo messaggio con 'no':")
    return ENTER_NOTES


async def enter_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    notes = update.message.text.strip()
    if notes.lower() == "no":
        notes = ""
    context.user_data["notes"] = notes

    await update.message.delete()

    if update.message.reply_to_message:
        try:
            await update.message.reply_to_message.delete()
        except Exception:
            pass
    if context.user_data.get("multi"):
        sacramenti = ", ".join([s.replace("_", " ") for s in context.user_data.get("sacraments", [])])
    else:
        sacramenti = context.user_data.get("sacrament", "N/D").replace("_", " ")
    nickname = context.user_data.get("nickname_mc", "N/D")
    await update.message.reply_text(
        f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nSei arrivato alla fine della prenotazione. Qui sotto è presente il resoconto delle informazioni scritte da te. Controlla che siano giuste e conferma la tua prenotazione.\n\n"
        f"•Nickname minecraft: {nickname}\n"
        f"•Sacramento richiesto: {sacramenti}\n"
        f"•Note Aggiuntive: {notes or 'nessuna nota.'}",
        reply_markup=confirm_keyboard()
    )
    return CONFIRM_BOOKING

async def confirm_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nLa prenotazione è stata annullata con successo! Se vuoi effettuarla di nuovo digita /start")
        return ConversationHandler.END
    if query.data != "confirm":
        return

    user = update.effective_user
    session = SessionLocal()
    try:
        # Gestione singolo vs multiplo
        if context.user_data.get("multi"):
            sacrament_value = ",".join(context.user_data.get("sacraments", []))
        else:
            sacrament_value = context.user_data.get("sacrament")

        booking = Booking(
            source="telegram",
            client_telegram_id=user.id,
            rp_name=None,
            nickname_mc=context.user_data.get("nickname_mc"),   # nuovo campo
            sacrament=sacrament_value,
            notes=context.user_data.get("notes", ""),
            status="pending",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(booking)
        session.commit()

        session.add(EventLog(
            booking_id=booking.id,
            actor_id=user.id,
            action="create",
            details="telegram"
        ))
        session.commit()

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Prendi in carico", callback_data=f"take_{booking.id}")],
        ])
        text = (
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n"
            f"🛎 Driiinnn! È arrivata una nuova richiesta di prenotazione per effettuare un sacramento!\n\n"
            f"•Richiesta effettuata da: '@{user.username or user.id}' (ID:#{booking.id})\n"
            f"•Sacramento richiesto: {booking.sacrament.replace('_',' ')}\n"
            f"•Nickname Minecraft: {booking.nickname_mc or 'non presente.'}\n"
            f"•Note Aggiuntive: {booking.notes or 'non presente.'}\n\n"
            f"✅ Verifica l’interesse del richiedente e la correttezza dei campi.\nSe è una richiesta meme ignoralo.Altrimenti, prendi in carico la prenotazione e contattalo in privato per completare la procedura."
        )
        await context.bot.send_message(PRIESTS_GROUP_ID, text, reply_markup=kb)

        await query.edit_message_text(
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nLa tua prenotazione (ID #{booking.id}) è andata a buon fine! A breve un sacerdote ti contatterà in privato per effettuare il sacramento."
        )

        return ConversationHandler.END
    finally:
        session.close()

# ---- SACERDOTI: presa in carico ----
async def priests_take(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith("take_"):
        return
    priest_id = update.effective_user.id
    if not is_priest(priest_id):
        await query.edit_message_reply_markup(None)
        await query.message.reply_text("Solo i sacerdoti possono prendere in carico.")
        return
    booking_id = int(data.split("_")[1])
    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if not booking or booking.status not in ["pending", "assigned"]:
            await query.message.reply_text("Richiesta non disponibile.")
            return
        booking.status = "in_progress"
        booking.updated_at = datetime.now(timezone.utc)
        session.add(booking)

        assign = Assignment(
            booking_id=booking.id,
            priest_telegram_id=priest_id,
            assigned_by=None,
            assigned_at=datetime.now(timezone.utc),
            taken_at=datetime.now(timezone.utc),
        )
        session.add(assign)
        session.add(EventLog(booking_id=booking.id, actor_id=priest_id, action="take", details="priests_group"))
        session.commit()

        await query.edit_message_text(query.message.text + "\n‼️La prenotazione è stata presa in carico da @" + (update.effective_user.username or str(priest_id)))
    finally:
        session.close()

# ---- INGAME FLOW (SECRETARIES) ----
@role_required(is_secretary, "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nNon risulti essere un segretario, perciò non puoi eseguire il comando. Se pensi sia un errore contatta @LavatiScimmiaInfuocata.")
async def prenota_ingame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n❌ Questo comando può essere usato solo in privato con il bot.")
        return ConversationHandler.END  # termina la conversazione
    msg = await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nPer iniziare la procedura di registrazione, inserisci la @ del fedele che ha prenotato:")
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
        text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nBene! Adesso ti chiedo di inserire il nickname di minecraft del fedele (se si tratta di un matrimonio inserisci il nome dei due coniugi):"
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
        text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nSeleziona uno o più sacramenti (scrivi 'fine' quando hai terminato):",
        reply_markup=kb
    )
    context.user_data["last_prompt_id"] = msg.message_id
    context.user_data["sacraments"] = []
    return IG_SACRAMENT


async def ig_sacrament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = update.message.text.lower().replace(" ", "_")

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
                text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nAttenzione,non hai selezionato nessun sacramento. Riprova:"
            )
            context.user_data["last_prompt_id"] = msg.message_id
            return IG_SACRAMENT
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Siamo arrivati quasi alla fine. Inserisci delle note aggiuntive (se non ci sono scrivi 'no'):"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_NOTES

    if s not in SACRAMENTS:
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Il sacramento inserito non è valido. Riprova:"
        )
        context.user_data["last_prompt_id"] = msg.message_id
        return IG_SACRAMENT

    context.user_data["sacraments"].append(s)
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Il Sacramento è stato aggiunto con successo! Selezionane un altro oppure scrivi 'fine':"
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

    sacrament_display = ", ".join(context.user_data["sacraments"])
    msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nSei arrivato alla fine della registrazione. Qui sotto è presente il resoconto delle informazioni scritte da te. Controlla che siano giuste e conferma la tua registrazione.\n\n •Contatto Telegram: {context.user_data['rp_name']}\n"
            f"•Nick: {context.user_data['nickname_mc']}\n"
            f"•Sacramenti: {sacrament_display.replace('_',' ')}\n"
            f"•Note: {notes or 'nessuna nota presente.'}"
        ),
        reply_markup=confirm_keyboard()
    )
    context.user_data["last_prompt_id"] = msg.message_id
    return IG_CONFIRM

async def ig_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text(
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nLa prenotazione è stata annullata con successo! Se vuoi effettuarla di nuovo digita /prenota_ingame"
        )
        return ConversationHandler.END
    if query.data != "confirm":
        return

    user = update.effective_user
    user_id = user.id
    if not is_secretary(user_id):
        await query.edit_message_text(
            "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nNon hai il permesso per eseguire questa azione."
        )
        return ConversationHandler.END

    session = SessionLocal()
    try:
        # Prepariamo la stringa dei sacramenti (singolo o multiplo)
        sacrament_display = ", ".join(context.user_data.get("sacraments", []))

        booking = Booking(
            source="ingame",
            rp_name=context.user_data["rp_name"],
            nickname_mc=context.user_data["nickname_mc"],
            sacrament=sacrament_display,
            notes=context.user_data["notes"],
            status="pending",
        )
        session.add(booking)
        session.commit()

        # Log dell'evento
        session.add(EventLog(
            booking_id=booking.id,
            actor_id=user_id,
            action="create",
            details="ingame"
        ))
        session.commit()

        # Messaggio di conferma al segretario
        await query.edit_message_text(
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n✅La tua prenotazione è stata registrata con successo! (ID #{booking.id})\n\nQui sotto ti uscirà un resoconto delle informazioni inserite:\n\n"
            f"Contatto telegram: {booking.rp_name}\n"
            f"Nick: {booking.nickname_mc}\n"
            f"Sacramenti: {sacrament_display.replace('_',' ')}\n"
            f"Note: {booking.notes or 'nessuna nota presente.'}"
        )

        # 🔎 Recupera il tag del segretario
        secretary_tag = f"@{user.username}" if user.username else f"ID:{user.id}"

        # Notifica alla Direzione con riepilogo completo
        await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\nE' presente una nuova prenotazione! #{booking.id}\n\n"
            f"Contatto Telegram: {booking.rp_name}\n"
            f"Nick: {booking.nickname_mc}\n"
            f"Sacramenti: {sacrament_display.replace('_',' ')}\n"
            f"Note: {booking.notes or 'Nessuna nota'}\n\n"
            f"📌 Prenotazione registrata dal segretario: {secretary_tag}\n\n"
            f"Ricorda di verificare i campi inseriti e di assegnarla il prima possibile a un sacerdote."
        )

        return ConversationHandler.END
    finally:
        session.close()

# ---- DIREZIONE: ASSEGNAZIONE ----
@role_required(is_director, "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Non hai il permesso per eseguire questo comando.")
async def assegna(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != DIRECTORS_GROUP_ID:
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Questo comando può essere usato solo nel gruppo Direzione.")
        return

    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n La sintassi risulta errata, utilizza: /assegna <id richiesta> <@username>")
        return

    booking_id = int(args[1])
    target = args[2]

    if not target.startswith("@"):
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Attento, devi specificare l'@username del sacerdote (es. @nomeutente).")
        return

    username = target.lstrip("@")

    session = SessionLocal()
    try:
        priest = session.query(Priest).filter_by(username=username).first()
        if not priest:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n l'Username inserito non è valido o il sacerdote non è registrato.")
            return

        priest_id = priest.telegram_id

        if not is_priest(priest_id):
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n L'utente indicato non è registrato come sacerdote.")
            return

        booking = session.query(Booking).get(booking_id)
        if not booking:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n La Prenotazione inserita risulta inesistente.")
            return

        existing_assign = session.query(Assignment).filter_by(booking_id=booking.id).first()
        if booking.status == "assigned" or existing_assign:
            await update.message.reply_text(f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n La prenotazione #{booking.id} è già stata assegnata a un sacerdote, se vuoi riassegnarla digita /riassegna.")
            return

        # Aggiorna stato prenotazione
        booking.status = "assigned"
        booking.updated_at = datetime.now(timezone.utc)
        session.add(booking)

        # Salva assegnazione includendo l'username del sacerdote
        assign = Assignment(
            booking_id=booking.id,
            priest_telegram_id=priest_id,
            priest_username=username,  # <— assicurati che esista questa colonna
            assigned_by=update.effective_user.id,
        )
        session.add(assign)

        session.add(EventLog(
            booking_id=booking.id,
            actor_id=update.effective_user.id,
            action="assign",
            details=f"to @{username}"
        ))
        session.commit()

        await update.message.reply_text(f"Prenotazione #{booking.id} assegnata a @{username}.")
        await context.bot.send_message(
            priest_id,
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Hey sacerdote! Ti è stata assegnata una nuova prenotazione. (#{booking.id}). Utilizza /mie_assegnazioni per i dettagli."
        )

        # Notifica dopo 48 ore se non completata
        context.job_queue.run_once(
            notify_uncompleted,
            when=48*3600,
            data={"booking_id": booking.id, "priest_id": priest_id, "username": username},
            name=f"notify_{booking.id}"
        )
    finally:
        session.close()

        
@role_required(is_director, "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Non hai il permesso per eseguire questo comando.")
async def riassegna(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != DIRECTORS_GROUP_ID:
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Questo comando può essere usato solo nel gruppo Direzione.")
        return

    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Sintassi errata! Utilizzo corretto: /riassegna <id richiesta> <@username>")
        return

    booking_id = int(args[1])
    target = args[2]

    if not target.startswith("@"):
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Attento! Devi specificare l'@username del sacerdote (es. @nomeutente).")
        return

    username = target.lstrip("@")

    session = SessionLocal()
    try:
        priest = session.query(Priest).filter_by(username=username).first()
        if not priest:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Username non valido o sacerdote non registrato.")
            return

        priest_id = priest.telegram_id

        if not is_priest(priest_id):
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n L'utente indicato non è registrato come sacerdote.")
            return

        booking = session.query(Booking).get(booking_id)
        if not booking:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n La Prenotazione inserita risulta come inesistente.")
            return

        # 🔎 Blocco se la prenotazione è completata (o annullata)
        if booking.status in ("completed", "cancelled"):
            await update.message.reply_text(
                f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ La prenotazione #{booking.id} è {booking.status.upper()} e non può essere riassegnata."
            )
            return

        existing_assign = session.query(Assignment).filter_by(booking_id=booking.id).first()
        if not existing_assign:
            await update.message.reply_text(f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n La prenotazione #{booking.id} non è ancora stata assegnata. Usa /assegna.")
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

        await update.message.reply_text(f"Prenotazione #{booking.id} riassegnata a @{username}.")
        try:
            await context.bot.send_message(
                priest_id,
                f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Hey sacerdote! Ti è appena stata assegnata una nuova prenotazione. (#{booking.id}) Utilizza /mie_assegnazioni per i dettagli."
            )
        except telegram.error.Forbidden:
            await update.message.reply_text(
                f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ⚠️ Impossibile notificare @{username} in privato. Deve avviare il bot."
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
            priest_id = job_data["priest_id"]
            await context.bot.send_message(
                DIRECTORS_GROUP_ID,
                f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ⚠️ La prenotazione #{booking.id} assegnata al sacerdote {job_data['username']} "
                f"non è stata completata entro 48 ore."
            )
    finally:
        session.close()


# ---- SACERDOTE: LISTA E COMPLETAMENTO ----
@role_required(is_priest, "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Non hai il permesso per eseguire il comando.")
async def mie_assegnazioni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Questo comando può essere usato solo in privato con il bot.")
        return
    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        # Recupera tutte le assegnazioni ordinate dalla più recente
        assigns = (
            session.query(Assignment)
            .filter(Assignment.priest_telegram_id == priest_id)
            .order_by(Assignment.id.desc())
            .all()
        )
        if not assigns:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Al momento non ti è stata assegnata alcuna prenotazione, ma questo durerà ancora per poco!")
            return

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
            # Evidenzia quelle ancora da completare
            if b.status == "assigned":
                msgs.append(
                    f"⚠️ **#{b.id} [DA COMPLETARE]** - {b.sacrament.replace('_',' ')}\n"
                    f"Contatto TG: {b.rp_name or 'Nessun contatto presente.'} \nNick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"Note: {b.notes or 'Nessuna nota.'}"
                )
            else:
                msgs.append(
                    f"#{b.id} [{b.status}] - {b.sacrament.replace('_',' ')}\n"
                    f"Contatto TG: {b.rp_name or 'Nessun contatto presente.'} \nNick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"Note: {b.notes or 'Nessuna nota.'}"
                )

        text = "\n\n".join(msgs)
        text += f"\n\nPagina {page}/{total_pages}"

        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"assign_page_{page-1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"assign_page_{page+1}"))

        kb = InlineKeyboardMarkup([buttons]) if buttons else None

        await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")
    finally:
        session.close()


async def mie_assegnazioni_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[-1])

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
            await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Al momento non ti è stata assegnata alcuna prenotazione, ma questo durerà ancora per poco!")
            return

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
                    f"⚠️ **#{b.id} [DA COMPLETARE]** - {b.sacrament.replace('_',' ')}\n"
                    f"Contatto TG: {b.rp_name or 'Nessun contatto presente.'} \nNick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"Note: {b.notes or 'Nessuna nota.'}"
                )
            else:
                msgs.append(
                    f"#{b.id} [{b.status}] - {b.sacrament.replace('_',' ')}\n"
                    f"Contatto TG: {b.rp_name or 'Nessun contatto presente.'} \nNick: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                    f"Note: {b.notes or 'Nessuna nota'}"
                )

        text = "\n\n".join(msgs)
        text += f"\n\nPagina {page}/{total_pages}"

        buttons = []
        if page > 1:
            buttons.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"assign_page_{page-1}"))
        if page < total_pages:
            buttons.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"assign_page_{page+1}"))

        kb = InlineKeyboardMarkup([buttons]) if buttons else None

        await query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
    finally:
        session.close()


@role_required(is_priest, "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Non hai il permesso per eseguire questo comando.")
async def completa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Questo comando può essere usato solo in privato con il bot.")
        return
    args = update.message.text.split()
    if len(args) != 2:
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Sintassi errata! Uso corretto: /completa <id richiesta>")
        return

    booking_id = int(args[1])
    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        b = session.query(Booking).get(booking_id)
        if not b:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n L'id della prenotazione inserita risulta inesistente.")
            return

        a = session.query(Assignment).filter(
            Assignment.booking_id == booking_id,
            Assignment.priest_telegram_id == priest_id
        ).first()
        if not a:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n L'id della prenotazione inserita non ti è assegnata.")
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

        await update.message.reply_text(f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Grande! Prenotazione #{b.id} contrassegnata come completata.")
        await context.bot.send_message(
            DIRECTORS_GROUP_ID,
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Sacramento completato #{b.id} da @{update.effective_user.username or priest_id}."
        )
    finally:
        session.close()

# ---- CANCEL ----
async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Processo annullato.")
    return ConversationHandler.END

# ---- SCHEDULER ----
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
            if ref_time and ref_time < threshold and not a.due_alert_sent:
                a.due_alert_sent = True
                session.add(a)
                session.add(EventLog(booking_id=b.id, actor_id=0, action="alert", details="48h SLA"))
                session.commit()
                await app.bot.send_message(
                    DIRECTORS_GROUP_ID,
                    f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ALERT: Prenotazione #{b.id} in lista di {a.priest_telegram_id} da oltre 48h."
                )
    finally:
        session.close()
@role_required(is_director, "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Solo la Direzione può usare questo comando.")
async def lista_prenotazioni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != DIRECTORS_GROUP_ID:
        await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Questo comando può essere usato solo nel gruppo Direzione.")
        return

    args = update.message.text.split()

    # 🔎 Caso rimozione con conferma
    if len(args) >= 3 and args[1].lower() == "rimuovi":
        try:
            booking_ids = [int(x) for x in args[2:]]
        except ValueError:
            await update.message.reply_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Devi specificare solo ID numerici validi.")
            return

        # Messaggio di conferma
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Conferma", callback_data=f"confirm_remove_{','.join(map(str, booking_ids))}"),
                InlineKeyboardButton("❌ Annulla", callback_data="cancel_remove")
            ]
        ])
        await update.message.reply_text(
            f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ⚠️ Vuoi davvero rimuovere le prenotazioni: {', '.join(map(str, booking_ids))}?",
            reply_markup=kb
        )
        return

    # 🔎 Caso normale: visualizzazione lista
    filtro = args[1].lower() if len(args) == 2 else None

    session = SessionLocal()
    try:
        bookings = []
        titolo = "Riepilogo prenotazioni"

        if filtro:
            if filtro in STATUS:
                bookings = session.query(Booking).filter(Booking.status == filtro).order_by(Booking.id.desc()).all()
                titolo = f"Prenotazioni {filtro.upper()}"
            else:
                try:
                    priest_id = int(filtro)
                    assigns = session.query(Assignment).filter(Assignment.priest_telegram_id == priest_id).all()
                    bookings = [session.query(Booking).get(a.booking_id) for a in assigns if session.query(Booking).get(a.booking_id)]
                    titolo = f"Prenotazioni sacerdote {priest_id}"
                except ValueError:
                    bookings = session.query(Booking).filter(Booking.nickname_mc.ilike(f"%{filtro}%")).order_by(Booking.id.desc()).all()
                    titolo = f"Prenotazioni del fedele '{filtro}'"
        else:
            bookings = session.query(Booking).order_by(Booking.id.desc()).all()

        await _send_paginated_bookings(update.message, bookings, titolo, filtro, page=1)
    finally:
        session.close()


# 🔎 Callback per conferma/annulla
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

                session.query(Assignment).filter_by(booking_id=booking.id).delete()
                session.query(EventLog).filter_by(booking_id=booking.id).delete()
                session.delete(booking)
                removed.append(booking_id)

            session.commit()

            msg_parts = []
            if removed:
                msg_parts.append(f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ✅ Prenotazioni rimosse: {', '.join(map(str, removed))}")
            if not_found:
                msg_parts.append(f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Non trovate: {', '.join(map(str, not_found))}")

            await query.edit_message_text("\n".join(msg_parts) if msg_parts else "𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Nessuna prenotazione rimossa.")

        elif data == "cancel_remove":
            await query.edit_message_text("𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n ❌ Rimozione annullata.")
    finally:
        session.close()

async def _send_paginated_bookings(target, bookings, titolo, filtro, page=1):
    if not bookings:
        msg = f"𝐂𝐔𝐋𝐓𝐎 𝐃𝐈 𝐏𝐎𝐒𝐄𝐈𝐃𝐎𝐍𝐄\n\n Nessuna prenotazione trovata per {titolo}."
        if isinstance(target, Message):
            await target.reply_text(msg)
        elif isinstance(target, CallbackQuery):
            await target.edit_message_text(msg)
        return

    per_page = 10
    total_pages = (len(bookings) + per_page - 1) // per_page
    start = (page - 1) * per_page
    end = start + per_page
    bookings_page = bookings[start:end]

    lines = [f"--- {titolo} --- (Totale: {len(bookings)})"]

    session = SessionLocal()
    try:
        for b in bookings_page:
            assignment = session.query(Assignment).filter_by(booking_id=b.id).first()
            priest_tag = f"@{assignment.priest_username}" if assignment and getattr(assignment, "priest_username", None) else "-"

            # Tag del segretario che ha registrato la prenotazione
            secretary_tag = f"@{b.secretary_username}" if getattr(b, "secretary_username", None) else "-"

            # Orario (preferisci created_at, fallback updated_at)
            if getattr(b, "created_at", None):
                timestamp = b.created_at.strftime("%d/%m/%Y %H:%M")
            elif getattr(b, "updated_at", None):
                timestamp = b.updated_at.strftime("%d/%m/%Y %H:%M")
            else:
                timestamp = "-"

            lines.append(
                f"📌 Prenotazione #{b.id} [{b.status.upper()}]\n"
                f"• Sacramento/i: {b.sacrament.replace('_',' ')}\n"
                f"• Nick Minecraft: {b.nickname_mc or 'Nessun nickname inserito.'}\n"
                f"• Contatto TG fedele: {b.rp_name or 'Nessun contatto inserito.'}\n"
                f"• Note: {b.notes or 'Nessuna nota.'}\n"
                f"• Registrata dal segretario: {secretary_tag or 'Nessun segretario registrato.'}\n"
                f"• Orario: {timestamp}\n"
                f"• Assegnata a: {priest_tag or 'Nessuno.'}\n"
                "-----------------------------"
            )
    finally:
        session.close()

    text = "\n".join(lines) + f"\n\nPagina {page}/{total_pages}"

    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton("⬅️ Indietro", callback_data=f"bookings_page_{page-1}_{filtro or 'all'}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton("Avanti ➡️", callback_data=f"bookings_page_{page+1}_{filtro or 'all'}"))

    kb = InlineKeyboardMarkup([buttons]) if buttons else None

    if isinstance(target, Message):
        await target.reply_text(text, reply_markup=kb)
    elif isinstance(target, CallbackQuery):
        await target.edit_message_text(text, reply_markup=kb)


async def lista_prenotazioni_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split("_")
    page = int(data[2])
    filtro = data[3] if len(data) > 3 and data[3] != "all" else None

    session = SessionLocal()
    try:
        bookings = []
        titolo = "Riepilogo prenotazioni"

        if filtro:
            if filtro in STATUS:
                bookings = session.query(Booking).filter(Booking.status == filtro).order_by(Booking.id.desc()).all()
                titolo = f"Prenotazioni {filtro.upper()}"
            else:
                try:
                    priest_id = int(filtro)
                    assigns = session.query(Assignment).filter(Assignment.priest_telegram_id == priest_id).all()
                    bookings = [session.query(Booking).get(a.booking_id) for a in assigns if session.query(Booking).get(a.booking_id)]
                    titolo = f"Prenotazioni sacerdote {priest_id}"
                except ValueError:
                    bookings = session.query(Booking).filter(Booking.nickname_mc.ilike(f"%{filtro}%")).order_by(Booking.id.desc()).all()
                    titolo = f"Prenotazioni del fedele '{filtro}'"
        else:
            bookings = session.query(Booking).order_by(Booking.id.desc()).all()

        # 🔎 Qui passo direttamente query (non query.message!)
        await _send_paginated_bookings(query, bookings, titolo, filtro, page)

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
            pid = a.priest_telegram_id if a else "N/A"
            per_priest[pid] = per_priest.get(pid, 0) + 1

        # Conteggio per sacramento
        per_sacrament = {}
        for b in completed:
            if b.sacrament:
                # Se multipli, separa con virgola
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
            f"📊 Report settimanale",
            f"Periodo: {start.date()} ➝ {end.date()}",
            f"Totale sacramenti completati: {total}",
            "",
            "🏆 Classifica sacerdoti:"
        ]
        if per_priest:
            for pid, num in sorted(per_priest.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- Sacerdote {pid}: {num}")
        else:
            lines.append("Nessun sacramento completato dai sacerdoti questa settimana.")

        lines.append("")
        lines.append("✝️ Dettaglio per sacramento:")
        if per_sacrament:
            for sac, num in per_sacrament.items():
                lines.append(f"- {sac.replace('_',' ')}: {num}")
        else:
            lines.append("Nessun sacramento completato questa settimana.")

        lines.append("")
        lines.append(f"📌 Prenotazioni ancora aperte: {open_items}")

        # Invio al gruppo direzione
        await app.bot.send_message(DIRECTORS_GROUP_ID, "\n".join(lines))

    finally:
        session.close()

async def on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)
    if update and update.effective_message:
        await update.effective_message.reply_text("Si è verificato un errore. Riprova tra poco.")

# ---- BUILD APPLICATION ----
def build_application():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_error_handler(on_error)

    # Client booking conversation
    conv_client = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            # scelta singolo/multiplo
            CHOOSE_MODE: [CallbackQueryHandler(choose_mode, pattern=r"^mode_")],
            # scelta ruolo (quando l’utente ha più ruoli)
            CHOOSE_ROLE: [CallbackQueryHandler(choose_role, pattern=r"^role_")],
            # scelta sacramento
            START_SACRAMENT: [
                CallbackQueryHandler(choose_sacrament, pattern=r"^sac_.*|cancel"),
                CallbackQueryHandler(multi_flow, pattern=r"^(add_more|go_nick)$"),
            ],
            # inserimento nick Minecraft
            ENTER_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_nick)],
            # inserimento note
            ENTER_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_notes)],
            # conferma finale
            CONFIRM_BOOKING: [CallbackQueryHandler(confirm_booking, pattern=r"^confirm|cancel")],
        },
        fallbacks=[CommandHandler("cancel", cancel_handler)],
        allow_reentry=True,
    )
    app.add_handler(conv_client)

    # Take in priests group
    app.add_handler(CallbackQueryHandler(priests_take, pattern=r"^take_\d+$"))

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
    app.add_handler(CallbackQueryHandler(choose_role, pattern=r"^role_"))

    # Direzione
    app.add_handler(CommandHandler("assegna", assegna))
    app.add_handler(CommandHandler("riassegna", riassegna))   # <--- aggiunto
    app.add_handler(CommandHandler("lista_prenotazioni", lista_prenotazioni))
    app.add_handler(CallbackQueryHandler(handle_remove_callback, pattern="^(confirm_remove_|cancel_remove)"))

    # Sacerdoti
    app.add_handler(CommandHandler("mie_assegnazioni", mie_assegnazioni))
    app.add_handler(CommandHandler("completa", completa))

    # Callback per la paginazione delle assegnazioni
    app.add_handler(CallbackQueryHandler(mie_assegnazioni_page, pattern=r"^assign_page_\d+$"))
    app.add_handler(CallbackQueryHandler(lista_prenotazioni_page, pattern=r"^bookings_page_\d+_.+$"))
    # Scheduler jobs
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(check_sla, "interval", hours=1, args=[app])
    scheduler.add_job(weekly_report, "cron", day_of_week="sun", hour=23, minute=55, args=[app])
    scheduler.start()

    return app
