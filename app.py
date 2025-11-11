import os
import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, BigInteger
from datetime import datetime, timezone

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
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

def init_db():
    Base.metadata.create_all(engine)

# ---- UTILS ----
def is_secretary(user_id: int) -> bool:
    return user_id in SECRETARIES_IDS

def is_priest(user_id: int) -> bool:
    return user_id in PRIESTS_IDS

def is_director(user_id: int) -> bool:
    return user_id in DIRECTORS_IDS

def role_required(check_func, msg="Permesso negato."):
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
START_SACRAMENT, ENTER_NOTES, CONFIRM_BOOKING = range(3)
IG_RP_NAME, IG_NICK, IG_SACRAMENT, IG_NOTES, IG_CONFIRM = range(5)

def sacrament_keyboard():
    buttons = [[InlineKeyboardButton(s.title().replace("_", " "), callback_data=f"sac_{s}")] for s in SACRAMENTS]
    cancel = [InlineKeyboardButton("Annulla", callback_data="cancel")]
    return InlineKeyboardMarkup(buttons + [cancel])

def confirm_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Conferma", callback_data="confirm")],
        [InlineKeyboardButton("Annulla", callback_data="cancel")],
    ])

# ---- CLIENT FLOW ----
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Benvenuto! Scegli il sacramento che desideri prenotare. Puoi aggiungere note in seguito."
    )
    await update.message.reply_text("Seleziona il sacramento:", reply_markup=sacrament_keyboard())
    return START_SACRAMENT

async def choose_sacrament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text("Prenotazione annullata.")
        return ConversationHandler.END
    if not query.data.startswith("sac_"):
        return
    sacr = query.data[4:]
    context.user_data["sacrament"] = sacr
    await query.edit_message_text(
        f"Hai scelto: {sacr.replace('_',' ')}.\nAggiungi eventuali note o richieste speciali, oppure scrivi 'no'."
    )
    return ENTER_NOTES

async def enter_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    notes = update.message.text.strip()
    if notes.lower() == "no":
        notes = ""
    context.user_data["notes"] = notes
    await update.message.reply_text(
        f"Confermi la prenotazione per: {context.user_data['sacrament'].replace('_',' ')}?\nNote: {notes or '-'}",
        reply_markup=confirm_keyboard()
    )
    return CONFIRM_BOOKING

async def confirm_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text("Prenotazione annullata.")
        return ConversationHandler.END
    if query.data != "confirm":
        return
    user = update.effective_user
    session = SessionLocal()
    try:
        booking = Booking(
            source="telegram",
            client_telegram_id=user.id,
            rp_name=None,
            nickname_mc=None,
            sacrament=context.user_data["sacrament"],
            notes=context.user_data.get("notes",""),
            status="pending",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(booking)
        session.commit()

        # Log
        session.add(EventLog(booking_id=booking.id, actor_id=user.id, action="create", details="telegram"))
        session.commit()

        # Inoltro nel gruppo sacerdoti
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Prendi in carico", callback_data=f"take_{booking.id}")],
        ])
        text = (
            f"Nuova richiesta #{booking.id}\n"
            f"Sacramento: {booking.sacrament.replace('_',' ')}\n"
            f"Note: {booking.notes or '-'}\n"
            f"Cliente: @{user.username or user.id}"
        )
        await context.bot.send_message(PRIESTS_GROUP_ID, text, reply_markup=kb)

        await query.edit_message_text("Prenotazione registrata! Un sacerdote ti contatterà in privato.")
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

        await query.edit_message_text(query.message.text + "\nPreso in carico da @" + (update.effective_user.username or str(priest_id)))
        # Contatta cliente in privato (se presente)
        if booking.client_telegram_id:
            await context.bot.send_message(
                booking.client_telegram_id,
                "Ciao! Sono il sacerdote che ha preso in carico la tua richiesta. Scrivimi quando sei disponibile per il sacramento."
            )
    finally:
        session.close()

# ---- INGAME FLOW (SECRETARIES) ----
@role_required(is_secretary, "Solo i segretari possono usare questo comando.")
async def prenota_ingame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Inserisci Nome e Cognome roleplay del cliente:")
    return IG_RP_NAME

async def ig_rp_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["rp_name"] = update.message.text.strip()
    await update.message.reply_text("Inserisci nick Minecraft:")
    return IG_NICK

async def ig_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["nickname_mc"] = update.message.text.strip()
    kb = ReplyKeyboardMarkup([[KeyboardButton(s.replace("_"," "))] for s in SACRAMENTS], one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Seleziona il sacramento:", reply_markup=kb)
    return IG_SACRAMENT

async def ig_sacrament(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = update.message.text.lower().replace(" ", "_")
    if s not in SACRAMENTS:
        await update.message.reply_text("Sacramento non valido. Riprova.")
        return IG_SACRAMENT
    context.user_data["sacrament"] = s
    await update.message.reply_text("Aggiungi note (oppure scrivi 'no'):")
    return IG_NOTES

async def ig_notes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    notes = update.message.text.strip()
    if notes.lower() == "no":
        notes = ""
    context.user_data["notes"] = notes
    await update.message.reply_text(
        f"Confermi? RP: {context.user_data['rp_name']}, Nick: {context.user_data['nickname_mc']}, "
        f"Sacramento: {context.user_data['sacrament'].replace('_',' ')}, Note: {notes or '-'}",
        reply_markup=confirm_keyboard()
    )
    return IG_CONFIRM

async def ig_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "cancel":
        await query.edit_message_text("Prenotazione annullata.")
        return ConversationHandler.END
    if query.data != "confirm":
        return
    user_id = update.effective_user.id
    if not is_secretary(user_id):
        await query.edit_message_text("Permesso negato.")
        return ConversationHandler.END
    session = SessionLocal()
    try:
        booking = Booking(
            source="ingame",
            rp_name=context.user_data["rp_name"],
            nickname_mc=context.user_data["nickname_mc"],
            sacrament=context.user_data["sacrament"],
            notes=context.user_data["notes"],
            status="pending",
        )
        session.add(booking)
        session.commit()
        session.add(EventLog(booking_id=booking.id, actor_id=user_id, action="create", details="ingame"))
        session.commit()
        await query.edit_message_text(f"Prenotazione in-game registrata con ID #{booking.id}.")
        # Notifica alla Direzione
        await context.bot.send_message(DIRECTORS_GROUP_ID, f"Nuova prenotazione in-game #{booking.id} (solo Direzione).")
        return ConversationHandler.END
    finally:
        session.close()
# ---- DIREZIONE: ASSEGNAZIONE ----
@role_required(is_director, "Solo la Direzione può assegnare.")
async def assegna(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /assegna <booking_id> <@username|telegram_id>
    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text("Uso: /assegna <booking_id> <@username|telegram_id>")
        return
    booking_id = int(args[1])
    target = args[2]
    priest_id = None
    if target.startswith("@"):
        await update.message.reply_text("Specificare telegram_id del sacerdote (consigliato) oppure assicurarsi sia registrato.")
        return
    else:
        priest_id = int(target)
    if not is_priest(priest_id):
        await update.message.reply_text("L'utente indicato non è registrato come sacerdote.")
        return

    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if not booking:
            await update.message.reply_text("Prenotazione inesistente.")
            return
        booking.status = "assigned"
        booking.updated_at = datetime.now(timezone.utc)
        session.add(booking)

        assign = Assignment(
            booking_id=booking.id,
            priest_telegram_id=priest_id,
            assigned_by=update.effective_user.id,
        )
        session.add(assign)
        session.add(EventLog(booking_id=booking.id, actor_id=update.effective_user.id, action="assign", details=f"to {priest_id}"))
        session.commit()
        await update.message.reply_text(f"Prenotazione #{booking.id} assegnata a {priest_id}.")
        await context.bot.send_message(priest_id, f"Ti è stata assegnata la prenotazione #{booking.id}. Usa /mie_assegnazioni per i dettagli.")
    finally:
        session.close()

# ---- SACERDOTE: LISTA E COMPLETAMENTO ----
@role_required(is_priest, "Solo i sacerdoti possono visualizzare le assegnazioni.")
async def mie_assegnazioni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        assigns = session.query(Assignment).filter(Assignment.priest_telegram_id == priest_id).all()
        if not assigns:
            await update.message.reply_text("Nessuna assegnazione.")
            return
        msgs = []
        for a in assigns:
            b = session.query(Booking).get(a.booking_id)
            if not b:
                continue
            msgs.append(
                f"#{b.id} [{b.status}] - {b.sacrament.replace('_',' ')}\n"
                f"Cliente TG: {b.client_telegram_id or '-'} | RP: {b.rp_name or '-'} | Nick: {b.nickname_mc or '-'}\n"
                f"Note: {b.notes or '-'}"
            )
        await update.message.reply_text("\n\n".join(msgs))
    finally:
        session.close()

@role_required(is_priest, "Solo i sacerdoti possono completare.")
async def completa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /completa <booking_id>
    args = update.message.text.split()
    if len(args) != 2:
        await update.message.reply_text("Uso: /completa <booking_id>")
        return
    booking_id = int(args[1])
    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        b = session.query(Booking).get(booking_id)
        if not b:
            await update.message.reply_text("Prenotazione inesistente.")
            return
        a = session.query(Assignment).filter(
            Assignment.booking_id == booking_id,
            Assignment.priest_telegram_id == priest_id
        ).first()
        if not a:
            await update.message.reply_text("Questa prenotazione non ti è assegnata.")
            return
        b.status = "completed"
        b.updated_at = datetime.now(timezone.utc)
        session.add(b)
        session.add(EventLog(booking_id=b.id, actor_id=priest_id, action="complete", details=""))
        session.commit()
        await update.message.reply_text(f"Prenotazione #{b.id} contrassegnata come completata.")
        await context.bot.send_message(DIRECTORS_GROUP_ID, f"Sacramento completato #{b.id} da {priest_id}.")
    finally:
        session.close()

# ---- CANCEL ----
async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Processo annullato.")
    return ConversationHandler.END
# ---- DIREZIONE: ASSEGNAZIONE ----
@role_required(is_director, "Solo la Direzione può assegnare.")
async def assegna(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /assegna <booking_id> <@username|telegram_id>
    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text("Uso: /assegna <booking_id> <@username|telegram_id>")
        return
    booking_id = int(args[1])
    target = args[2]
    priest_id = None
    if target.startswith("@"):
        await update.message.reply_text("Specificare telegram_id del sacerdote (consigliato) oppure assicurarsi sia registrato.")
        return
    else:
        priest_id = int(target)
    if not is_priest(priest_id):
        await update.message.reply_text("L'utente indicato non è registrato come sacerdote.")
        return

    session = SessionLocal()
    try:
        booking = session.query(Booking).get(booking_id)
        if not booking:
            await update.message.reply_text("Prenotazione inesistente.")
            return
        booking.status = "assigned"
        booking.updated_at = datetime.now(timezone.utc)
        session.add(booking)

        assign = Assignment(
            booking_id=booking.id,
            priest_telegram_id=priest_id,
            assigned_by=update.effective_user.id,
        )
        session.add(assign)
        session.add(EventLog(booking_id=booking.id, actor_id=update.effective_user.id, action="assign", details=f"to {priest_id}"))
        session.commit()
        await update.message.reply_text(f"Prenotazione #{booking.id} assegnata a {priest_id}.")
        await context.bot.send_message(priest_id, f"Ti è stata assegnata la prenotazione #{booking.id}. Usa /mie_assegnazioni per i dettagli.")
    finally:
        session.close()

# ---- SACERDOTE: LISTA E COMPLETAMENTO ----
@role_required(is_priest, "Solo i sacerdoti possono visualizzare le assegnazioni.")
async def mie_assegnazioni(update: Update, context: ContextTypes.DEFAULT_TYPE):
    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        assigns = session.query(Assignment).filter(Assignment.priest_telegram_id == priest_id).all()
        if not assigns:
            await update.message.reply_text("Nessuna assegnazione.")
            return
        msgs = []
        for a in assigns:
            b = session.query(Booking).get(a.booking_id)
            if not b:
                continue
            msgs.append(
                f"#{b.id} [{b.status}] - {b.sacrament.replace('_',' ')}\n"
                f"Cliente TG: {b.client_telegram_id or '-'} | RP: {b.rp_name or '-'} | Nick: {b.nickname_mc or '-'}\n"
                f"Note: {b.notes or '-'}"
            )
        await update.message.reply_text("\n\n".join(msgs))
    finally:
        session.close()

@role_required(is_priest, "Solo i sacerdoti possono completare.")
async def completa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /completa <booking_id>
    args = update.message.text.split()
    if len(args) != 2:
        await update.message.reply_text("Uso: /completa <booking_id>")
        return
    booking_id = int(args[1])
    priest_id = update.effective_user.id
    session = SessionLocal()
    try:
        b = session.query(Booking).get(booking_id)
        if not b:
            await update.message.reply_text("Prenotazione inesistente.")
            return
        a = session.query(Assignment).filter(
            Assignment.booking_id == booking_id,
            Assignment.priest_telegram_id == priest_id
        ).first()
        if not a:
            await update.message.reply_text("Questa prenotazione non ti è assegnata.")
            return
        b.status = "completed"
        b.updated_at = datetime.now(timezone.utc)
        session.add(b)
        session.add(EventLog(booking_id=b.id, actor_id=priest_id, action="complete", details=""))
        session.commit()
        await update.message.reply_text(f"Prenotazione #{b.id} contrassegnata come completata.")
        await context.bot.send_message(DIRECTORS_GROUP_ID, f"Sacramento completato #{b.id} da {priest_id}.")
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
                    f"ALERT: Prenotazione #{b.id} in lista di {a.priest_telegram_id} da oltre 48h."
                )
    finally:
        session.close()

async def weekly_report(app):
    session = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        completed = session.query(Booking).filter(
            Booking.status == "completed",
            Booking.updated_at >= start
        ).all()
        total = len(completed)
        per_priest = {}
        for b in completed:
            a = session.query(Assignment).filter(Assignment.booking_id == b.id).first()
            pid = a.priest_telegram_id if a else "N/A"
            per_priest[pid] = per_priest.get(pid, 0) + 1
        lines = [f"Report settimanale (da {start.date()}):"]
        lines.append(f"- Totale sacramenti completati: {total}")
        for pid, num in sorted(per_priest.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- Sacerdote {pid}: {num}")
        open_items = session.query(Booking).filter(Booking.status.in_(["pending","assigned","in_progress"])).count()
        lines.append(f"- Prenotazioni ancora aperte: {open_items}")
        await app.bot.send_message(DIRECTORS_GROUP_ID, "\n".join(lines))
    finally:
        session.close()

# ---- BUILD APPLICATION ----
def build_application():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Client booking conversation
    conv_client = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            START_SACRAMENT: [CallbackQueryHandler(choose_sacrament)],
            ENTER_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_notes)],
            CONFIRM_BOOKING: [CallbackQueryHandler(confirm_booking)],
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

    # Direzione
    app.add_handler(CommandHandler("assegna", assegna))

    # Sacerdoti
    app.add_handler(CommandHandler("mie_assegnazioni", mie_assegnazioni))
    app.add_handler(CommandHandler("completa", completa))

    # Scheduler jobs
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(check_sla, "interval", hours=1, args=[app])
    scheduler.add_job(weekly_report, "cron", day_of_week="sun", hour=23, minute=55, args=[app])
    scheduler.start()

    return app
