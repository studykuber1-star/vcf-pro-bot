# pro_vcf_bot_final.py
import os
import re
import json
import math
import zipfile
import tempfile
from io import BytesIO
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from telegram import Update, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters
)

# ---------------- CONFIG - CHANGE THESE ----------------
BOT_TOKEN = "8471394707:AAHNXbIWk2cBMh_jtsI6fAWv1T6AH0PGIEY"  # <<--- replace with your token
ADMIN_IDS = [6075893329]               # <<--- you've provided this
ADMIN_USERNAME = "@CEROLCETTO"         # <<--- you've provided this
CHANNEL_USERNAME = "@CEROLCETTO"       # default channel (can be changed by admin with /set_channel)
DATA_FILE = "bot_data.json"
FREE_TRIAL_COUNT = 1
# -------------------------------------------------------

# Conversation states
(
    STATE_WAIT_FILE,
    STATE_WAIT_FORMAT,
    STATE_WAIT_BASE_NAME,
    STATE_WAIT_STYLE,
    STATE_WAIT_COUNT_OR_SIZE,
    STATE_WAIT_START_NUM,
    STATE_WAIT_ZIP_PASSWORD,
    STATE_WAIT_LANG,
    STATE_WAIT_JOIN_CONFIRM
) = range(9)

# Supported languages
LANGUAGES = {
    "en": "English",
    "hi": "Hindi",
    "cn": "Chinese",
    "id": "Indonesian"
}

# default per-user settings structure
DEFAULT_USER_SETTINGS = {
    "lang": "en",
    "name_style": "style1",  # style1=ARADHY 1, style2=ARADHY_0001, style3=ARADHY-001, style4=CONTACT_001_ARADHY
    "start_number": 1,
    "country_default": "+91",  # fallback when can't detect
    "auto_country_detect": True,
    "dedupe": True,
    "validate_numbers": True,
    "split_mode": "count",  # "count" or "size"
    "per_file_count": 100,
    "per_file_size_kb": 100, # used if split_mode == size
    "zip_password": None
}

# Multi-language messages (a concise set — you can expand later)
MESSAGES = {
    "en": {
        "lang_select": "Which language do you want to use?\nReply with: en / hi / cn / id",
        "welcome": "Hello! 👋\nWhat can I do for you?",
        "join_channel": "Please join our channel first: {channel}\nAfter joining, type 'I Joined' to continue.",
        "free_exhausted": "❌ Your free trial has been consumed. Buy this bot from the admin: {admin}",
        "ask_file": "Please upload your file (.txt, .csv, .xlsx).",
        "ask_base": "Enter the base name for contacts (example: ARADHY). This will be used as 'ARADHY 1', etc.",
        "ask_style": "Choose name style: style1 / style2 / style3 / style4",
        "ask_split_mode": "Split mode? Type 'count' or 'size'.",
        "ask_count": "Enter how many contacts per VCF file (example: 100).",
        "ask_size": "Enter max VCF file size in KB per file (example: 100).",
        "ask_start": "Enter starting number for naming (example: 1).",
        "ask_zip_pw": "Optional: Enter ZIP password or type 'no' for no password.",
        "processing": "Processing, please wait...",
        "done": "✅ Done! Created {files} VCF file(s), {total} contacts in total.",
        "not_admin": "Unauthorized. This command is for admins only.",
        "set_channel_ok": "Channel updated to {channel}.",
        "help": "Commands: /to_vcf, /txt_to_zip, /vcf_to_txt, /split_vcf, /merge_vcf, /buy, /status"
    },
    "hi": {
        "lang_select": "कौन सी भाषा उपयोग करना चाहते हैं? जवाब दें: en / hi / cn / id",
        "welcome": "नमस्ते! 👋\nमैं आपकी क्या मदद कर सकता हूँ?",
        "join_channel": "कृपया पहले हमारे चैनल से जुड़ें: {channel}\nजुड़ने के बाद 'I Joined' लिखें।",
        "free_exhausted": "❌ आपका फ्री ट्रायल उपयोग हो चुका है। बॉट खरीदने के लिए एडमिन से संपर्क करें: {admin}",
        "ask_file": "कृपया अपनी फाइल अपलोड करें (.txt, .csv, .xlsx).",
        "ask_base": "Contacts के लिए base नाम दें (उदा: ARADHY).",
        "ask_style": "नाम की शैली चुनें: style1 / style2 / style3 / style4",
        "ask_split_mode": "Split mode चुनें: 'count' या 'size'.",
        "ask_count": "प्रति VCF फ़ाइल कितने контак्ट होने चाहिए? (उदा: 100).",
        "ask_size": "प्रति फ़ाइल अधिकतम साइज KB में डालें (उदा: 100).",
        "ask_start": "नामिंग के लिए starting number दें (उदा: 1).",
        "ask_zip_pw": "Optional: ZIP password डालें या 'no' लिखें.",
        "processing": "प्रोसेस कर रहे हैं, कृपया प्रतीक्षा करें...",
        "done": "✅ पूरा हुआ! बनाए गए VCF फ़ाइलें: {files}, कुल संपर्क: {total}.",
        "not_admin": "अनधिकृत। यह कमांड केवल एडमिन के लिए है।",
        "set_channel_ok": "चैनल अपडेट हुआ: {channel}.",
        "help": "Commands: /to_vcf, /txt_to_zip, /vcf_to_txt, /split_vcf, /merge_vcf, /buy, /status"
    },
    "cn": {
        "lang_select": "您想使用哪种语言？回复: en / hi / cn / id",
        "welcome": "您好！👋 我能为您做什么？",
        "join_channel": "请先加入我们的频道: {channel}\n加入后请输入 'I Joined' 继续。",
        "free_exhausted": "❌ 您的免费试用已用完。请联系管理员购买：{admin}",
        "ask_file": "请上传您的文件 (.txt, .csv, .xlsx)。",
        "ask_base": "请输入联系人基础名称 (例如：ARADHY)。",
        "ask_style": "选择名称样式: style1 / style2 / style3 / style4",
        "ask_split_mode": "拆分方式? 输入 'count' 或 'size'.",
        "ask_count": "每个 VCF 文件包含多少联系人？（例如：100）。",
        "ask_size": "请输入每个文件的最大大小 (KB)。",
        "ask_start": "请输入起始编号 (例如：1)。",
        "ask_zip_pw": "可选: 输入 ZIP 密码 或 输入 'no' 表示无密码。",
        "processing": "处理中，请稍候...",
        "done": "✅ 完成！生成了 {files} 个 VCF 文件，共 {total} 个联系人。",
        "not_admin": "未授权。本命令仅限管理员使用。",
        "set_channel_ok": "频道已更新为: {channel}.",
        "help": "Commands: /to_vcf, /txt_to_zip, /vcf_to_txt, /split_vcf, /merge_vcf, /buy, /status"
    },
    "id": {
        "lang_select": "Bahasa apa yang ingin Anda gunakan? Balas: en / hi / cn / id",
        "welcome": "Halo! 👋\nApa yang bisa saya bantu?",
        "join_channel": "Silakan bergabung dengan channel kami dulu: {channel}\nSetelah bergabung, ketik 'I Joined' untuk melanjutkan.",
        "free_exhausted": "❌ Percobaan gratis Anda telah habis. Silakan hubungi admin untuk membeli: {admin}",
        "ask_file": "Silakan unggah file Anda (.txt, .csv, .xlsx).",
        "ask_base": "Masukkan nama dasar untuk kontak (misal: ARADHY).",
        "ask_style": "Pilih gaya nama: style1 / style2 / style3 / style4",
        "ask_split_mode": "Mode split? Ketik 'count' atau 'size'.",
        "ask_count": "Masukkan berapa kontak per VCF (misal: 100).",
        "ask_size": "Masukkan ukuran maksimum VCF per file (KB).",
        "ask_start": "Masukkan nomor awal untuk penamaan (misal: 1).",
        "ask_zip_pw": "Opsional: Masukkan password ZIP atau ketik 'no'.",
        "processing": "Memproses, silakan tunggu...",
        "done": "✅ Selesai! Dibuat {files} file VCF, total {total} kontak.",
        "not_admin": "Tidak berwenang. Perintah ini khusus admin.",
        "set_channel_ok": "Channel diubah menjadi: {channel}.",
        "help": "Commands: /to_vcf, /txt_to_zip, /vcf_to_txt, /split_vcf, /merge_vcf, /buy, /status"
    }
}

# ----------------- Helper persistence -----------------
def load_data():
    if not os.path.exists(DATA_FILE):
        base = {
            "used_users": {},   # user_id -> times_used (int)
            "premium_users": [], # list of user_ids
            "admins": ADMIN_IDS.copy(),
            "settings": {},     # user_id -> settings
            "users": {},        # user_id -> basic info (lang, first_seen)
            "logs": []          # activity logs
        }
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(base, f, indent=2)
        return base
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

DATA = load_data()

def is_admin(user_id: int) -> bool:
    return user_id in DATA.get("admins", []) or user_id in ADMIN_IDS

def has_premium(user_id: int) -> bool:
    return user_id in DATA.get("premium_users", [])

def increment_usage(user_id: int):
    used = DATA["used_users"].get(str(user_id), 0)
    used += 1
    DATA["used_users"][str(user_id)] = used
    save_data(DATA)
    return used

def get_usage(user_id: int) -> int:
    return DATA["used_users"].get(str(user_id), 0)

def grant_premium(user_id: int):
    if user_id not in DATA["premium_users"]:
        DATA["premium_users"].append(user_id)
        save_data(DATA)

def revoke_premium(user_id: int):
    if user_id in DATA["premium_users"]:
        DATA["premium_users"].remove(user_id)
        save_data(DATA)

def set_user_settings(user_id: int, settings: dict):
    DATA["settings"][str(user_id)] = settings
    save_data(DATA)

def get_user_settings(user_id: int) -> dict:
    return DATA["settings"].get(str(user_id), DEFAULT_USER_SETTINGS.copy())

def log_activity(entry: str):
    DATA.setdefault("logs", [])
    ts = datetime.utcnow().isoformat()
    DATA["logs"].insert(0, f"[{ts}] {entry}")
    # keep last 1000 logs
    DATA["logs"] = DATA["logs"][:1000]
    save_data(DATA)

def register_user_basic(user_id: int, lang: str):
    users = DATA.setdefault("users", {})
    if str(user_id) not in users:
        users[str(user_id)] = {
            "first_seen": datetime.utcnow().isoformat(),
            "lang": lang,
            "last_active": datetime.utcnow().isoformat()
        }
    else:
        users[str(user_id)]["last_active"] = datetime.utcnow().isoformat()
    save_data(DATA)

# --------------- Phone utilities ---------------------
def clean_number(num: str) -> str:
    if not isinstance(num, str):
        num = str(num)
    s = num.strip()
    s = re.sub(r"[^\d\+]", "", s)
    if s.count("+") > 1:
        s = s.replace("+", "")
    return s

def detect_country_code(num: str, default="+91") -> Tuple[str, str]:
    n = re.sub(r"\D", "", num)
    if num.startswith("+"):
        return ("+" + n, "+" + (n[:1] if len(n) >= 1 else default))
    if len(n) == 10:
        return ("+91" + n, "+91")
    if len(n) == 11:
        return ("+62" + n[-9:], "+62")
    if 7 <= len(n) <= 12:
        return (default + n, default)
    return (default + n, default)

def validate_number(num: str) -> bool:
    s = re.sub(r"\D", "", num)
    return 8 <= len(s) <= 15

def format_name(base: str, counter: int, style: str, padding: int = 0) -> str:
    if style == "style1":
        return f"{base} {counter}"
    if style == "style2":
        pad = str(counter).zfill(padding or 4)
        return f"{base}_{pad}"
    if style == "style3":
        pad = str(counter).zfill(padding or 3)
        return f"{base}-{pad}"
    if style == "style4":
        pad = str(counter).zfill(padding or 3)
        return f"CONTACT_{pad}_{base}"
    return f"{base} {counter}"

def vcard_for(name: str, phone: str) -> str:
    return f"BEGIN:VCARD\nVERSION:3.0\nN:{name}\nFN:{name}\nTEL;TYPE=CELL:{phone}\nEND:VCARD\n"

def split_by_count(phones: List[str], per_file: int) -> List[List[str]]:
    return [phones[i:i+per_file] for i in range(0, len(phones), per_file)]

def split_by_size(phones: List[str], max_kb: int, base_name: str, style: str, start_counter: int) -> List[List[str]]:
    kb = max_kb
    chunks = []
    curr = []
    curr_bytes = 0
    counter = start_counter
    for ph in phones:
        name = format_name(base_name, counter, style)
        card = vcard_for(name, ph)
        b = len(card.encode("utf-8"))
        if curr and (curr_bytes + b) > (kb * 1024):
            chunks.append(curr)
            curr = []
            curr_bytes = 0
        curr.append(ph)
        curr_bytes += b
        counter += 1
    if curr:
        chunks.append(curr)
    return chunks

# --------------- Language helpers -------------------
def msg_for(user_id: int, key: str, **kwargs) -> str:
    settings = get_user_settings(user_id)
    lang = settings.get("lang", "en")
    template = MESSAGES.get(lang, MESSAGES["en"]).get(key, "")
    return template.format(**kwargs)

# --------------- Bot handlers -----------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # Show language selection
    text = "Choose language / भाषा चुनें / 选择语言 / Pilih bahasa:\n"
    for k, v in LANGUAGES.items():
        text += f"{k} - {v}\n"
    await update.message.reply_text(text)
    return STATE_WAIT_LANG

async def receive_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip().lower()
    if text not in LANGUAGES:
        await update.message.reply_text("Invalid choice. Reply with: en / hi / cn / id")
        return STATE_WAIT_LANG
    # save user setting
    settings = get_user_settings(user_id)
    settings["lang"] = text
    set_user_settings(user_id, settings)
    register_user_basic(user_id, text)
    # require channel join
    await update.message.reply_text(msg_for(user_id, "welcome"))
    await update.message.reply_text(msg_for(user_id, "join_channel", channel=CHANNEL_USERNAME))
    return STATE_WAIT_JOIN_CONFIRM

async def receive_join_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # if user types "I Joined" (case-insensitive) proceed
    if update.message.text.strip().lower() not in ("i joined", "i joined.", "joined", "yes", "i joined✅"):
        await update.message.reply_text("Please type 'I Joined' after joining the channel.")
        return STATE_WAIT_JOIN_CONFIRM
    user_id = update.effective_user.id
    await update.message.reply_text(msg_for(user_id, "ask_file"))
    return STATE_WAIT_FILE

async def to_vcf_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = get_user_settings(user_id)
    # Check free/premium usage
    if not has_premium(user_id):
        used = get_usage(user_id)
        if used >= FREE_TRIAL_COUNT:
            await update.message.reply_text(msg_for(user_id, "free_exhausted", admin=ADMIN_USERNAME))
            return ConversationHandler.END
    await update.message.reply_text(msg_for(user_id, "ask_file"))
    return STATE_WAIT_FILE

async def receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc:
        await update.message.reply_text("Please send a document file.")
        return STATE_WAIT_FILE

    file_name = doc.file_name or f"{doc.file_unique_id}.txt"
    local_path = f"{update.effective_user.id}_{int(datetime.utcnow().timestamp())}_{file_name}"
    file_obj = await doc.get_file()
    await file_obj.download_to_drive(local_path)
    context.user_data["upload_path"] = local_path

    settings = get_user_settings(update.effective_user.id)
    context.user_data["settings"] = settings

    await update.message.reply_text(msg_for(update.effective_user.id, "ask_base"))
    return STATE_WAIT_BASE_NAME

async def receive_base_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    base_name = update.message.text.strip()
    if not base_name:
        await update.message.reply_text("Please send a valid base name.")
        return STATE_WAIT_BASE_NAME
    context.user_data["base_name"] = base_name
    await update.message.reply_text(msg_for(update.effective_user.id, "ask_style"))
    return STATE_WAIT_STYLE

async def receive_style(update: Update, context: ContextTypes.DEFAULT_TYPE):
    style = update.message.text.strip()
    if style not in ("style1", "style2", "style3", "style4"):
        await update.message.reply_text("Invalid style. Please type style1, style2, style3 or style4.")
        return STATE_WAIT_STYLE
    context.user_data["style"] = style
    await update.message.reply_text(msg_for(update.effective_user.id, "ask_split_mode"))
    return STATE_WAIT_FORMAT

async def receive_split_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = update.message.text.strip().lower()
    if mode not in ("count", "size"):
        await update.message.reply_text("Invalid mode. Type 'count' or 'size'.")
        return STATE_WAIT_FORMAT
    context.user_data["split_mode"] = mode
    if mode == "count":
        await update.message.reply_text(msg_for(update.effective_user.id, "ask_count"))
    else:
        await update.message.reply_text(msg_for(update.effective_user.id, "ask_size"))
    return STATE_WAIT_COUNT_OR_SIZE

async def receive_count_or_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        val = int(text)
        if val <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide a valid positive integer.")
        return STATE_WAIT_COUNT_OR_SIZE
    context.user_data["split_value"] = val
    await update.message.reply_text(msg_for(update.effective_user.id, "ask_start"))
    return STATE_WAIT_START_NUM

async def receive_start_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    try:
        start_num = int(text)
    except ValueError:
        await update.message.reply_text("Please provide an integer start number.")
        return STATE_WAIT_START_NUM
    context.user_data["start_num"] = start_num
    await update.message.reply_text(msg_for(update.effective_user.id, "ask_zip_pw"))
    return STATE_WAIT_ZIP_PASSWORD

async def receive_zip_password_and_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    zip_pw = update.message.text.strip()
    if zip_pw.lower() in ("no", "none", "n"):
        zip_pw = None
    context.user_data["zip_password_user"] = zip_pw

    await update.message.reply_text(msg_for(update.effective_user.id, "processing"))

    path = context.user_data.get("upload_path")
    if not path or not os.path.exists(path):
        await update.message.reply_text("Upload file not found. Please /to_vcf again.")
        return ConversationHandler.END

    _, ext = os.path.splitext(path.lower())
    phones_raw = []
    try:
        if ext in (".txt", ):
            with open(path, "r", encoding="utf-8") as f:
                phones_raw = [line.strip() for line in f if line.strip()]
        elif ext in (".csv",):
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            if 'Phone' in df.columns:
                phones_raw = df['Phone'].astype(str).tolist()
            else:
                phones_raw = df.iloc[:,0].astype(str).tolist()
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(path, dtype=str, engine="openpyxl")
            if 'Phone' in df.columns:
                phones_raw = df['Phone'].astype(str).tolist()
            else:
                phones_raw = df.iloc[:,0].astype(str).tolist()
        else:
            with open(path, "r", encoding="utf-8") as f:
                phones_raw = [line.strip() for line in f if line.strip()]
    except Exception as e:
        await update.message.reply_text(f"Failed to read file: {e}")
        return ConversationHandler.END

    # Clean, validate, dedupe
    settings = get_user_settings(update.effective_user.id)
    phones_cleaned = []
    for p in phones_raw:
        c = clean_number(p)
        if settings.get("auto_country_detect", True):
            c, _ = detect_country_code(c, default=settings.get("country_default", "+91"))
        if not c.startswith("+"):
            c = "+" + re.sub(r"\D", "", c)
        if settings.get("validate_numbers", True) and not validate_number(c):
            continue
        phones_cleaned.append(c)

    if settings.get("dedupe", True):
        seen = set()
        unique = []
        for ph in phones_cleaned:
            if ph not in seen:
                seen.add(ph)
                unique.append(ph)
        phones_cleaned = unique

    total_contacts = len(phones_cleaned)
    if total_contacts == 0:
        await update.message.reply_text("No valid phone numbers found after cleaning/validation.")
        return ConversationHandler.END

    # Enforce free trial usage: increment only on successful processing
    user_id = update.effective_user.id
    if not has_premium(user_id):
        used_after = increment_usage(user_id)
        if used_after > FREE_TRIAL_COUNT:
            # revert increment
            DATA["used_users"][str(user_id)] = DATA["used_users"].get(str(user_id), 1) - 1
            save_data(DATA)
            await update.message.reply_text(msg_for(user_id, "free_exhausted", admin=ADMIN_USERNAME))
            return ConversationHandler.END

    # splitting into parts
    split_mode = context.user_data.get("split_mode", settings.get("split_mode", "count"))
    split_val = context.user_data.get("split_value", settings.get("per_file_count", 100))
    base_name = context.user_data.get("base_name")
    style = context.user_data.get("style")
    start_num = context.user_data.get("start_num", settings.get("start_number", 1))
    zip_password = context.user_data.get("zip_password_user", None)

    if split_mode == "count":
        parts = split_by_count(phones_cleaned, split_val)
    else:
        parts = split_by_size(phones_cleaned, split_val, base_name, style, start_num)

    # create temp vcf files and zip them
    created = []
    counter = start_num
    tmpdir = tempfile.mkdtemp(prefix="vcf_")
    try:
        for i, chunk in enumerate(parts, start=1):
            fname = f"{base_name}_{i}.vcf"
            fpath = os.path.join(tmpdir, fname)
            with open(fpath, "w", encoding="utf-8") as vf:
                for ph in chunk:
                    name = format_name(base_name, counter, style)
                    vf.write(vcard_for(name, ph))
                    counter += 1
            created.append(fpath)

        zip_name = f"{base_name}_vcf_{int(datetime.utcnow().timestamp())}.zip"
        zip_path = os.path.join(tmpdir, zip_name)
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fpath in created:
                arcname = os.path.basename(fpath)
                zf.write(fpath, arcname=arcname)

        await update.message.reply_document(document=InputFile(open(zip_path, "rb"), filename=zip_name))
    finally:
        for f in created:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        if os.path.exists(path):
            os.remove(path)
        try:
            os.rmdir(tmpdir)
        except Exception:
            pass

    log_activity(f"User {user_id} used TXT->VCF, processed {total_contacts} contacts")
    await update.message.reply_text(msg_for(user_id, "done", files=len(parts), total=total_contacts))
    return ConversationHandler.END

# ---------------- Bulk generator -------------------
async def bulk_gen_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not has_premium(update.effective_user.id):
        used = get_usage(update.effective_user.id)
        if used >= FREE_TRIAL_COUNT:
            await update.message.reply_text(msg_for(update.effective_user.id, "free_exhausted", admin=ADMIN_USERNAME))
            return
    await update.message.reply_text(
        "Bulk Generator: Provide start and end numbers separated by space.\nExample: 9000000000 9000001999"
    )

async def bulk_gen_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    if len(parts) < 2:
        await update.message.reply_text("Please provide two numbers: start end.")
        return
    try:
        start = int(re.sub(r"\D", "", parts[0]))
        end = int(re.sub(r"\D", "", parts[1]))
    except ValueError:
        await update.message.reply_text("Invalid numbers.")
        return
    if end < start or (end - start) > 200000:
        await update.message.reply_text("Invalid range or too big (limit 200k).")
        return

    numbers = [f"+{n}" for n in range(start, end+1)]
    base = f"GEN{start}"
    style = "style1"
    parts_v = split_by_count(numbers, 1000)
    tmpdir = tempfile.mkdtemp(prefix="bulk_")
    created = []
    try:
        counter = 1
        for i, chunk in enumerate(parts_v, start=1):
            fname = f"{base}_{i}.vcf"
            fpath = os.path.join(tmpdir, fname)
            with open(fpath, "w", encoding="utf-8") as vf:
                for ph in chunk:
                    name = format_name(base, counter, style)
                    vf.write(vcard_for(name, ph))
                    counter += 1
            created.append(fpath)
        zip_name = f"bulk_{int(datetime.utcnow().timestamp())}.zip"
        zip_path = os.path.join(tmpdir, zip_name)
        with zipfile.ZipFile(zip_path, "w") as zf:
            for f in created:
                zf.write(f, os.path.basename(f))
        await update.message.reply_document(InputFile(open(zip_path, "rb"), filename=zip_name))
    finally:
        for f in created:
            if os.path.exists(f):
                os.remove(f)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        try:
            os.remove(tmpdir)
        except Exception:
            pass

# --------------- Admin commands ---------------------
async def give_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /give_access <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user id.")
        return
    grant_premium(uid)
    await update.message.reply_text(f"Granted premium access to {uid}.")
    log_activity(f"Admin {update.effective_user.id} granted premium to {uid}")

async def revoke_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /revoke_access <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user id.")
        return
    revoke_premium(uid)
    await update.message.reply_text(f"Revoked premium access for {uid}.")
    log_activity(f"Admin {update.effective_user.id} revoked premium from {uid}")

async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /add_admin <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user id.")
        return
    if uid not in DATA.get("admins", []):
        DATA.setdefault("admins", []).append(uid)
        save_data(DATA)
    await update.message.reply_text(f"Added admin {uid}.")

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /remove_admin <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user id.")
        return
    if uid in DATA.get("admins", []):
        DATA["admins"].remove(uid)
        save_data(DATA)
    await update.message.reply_text(f"Removed admin {uid}.")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    used = get_usage(uid)
    premium = has_premium(uid)
    settings = get_user_settings(uid)
    await update.message.reply_text(
        f"User ID: {uid}\nUsed conversions: {used}\nPremium: {premium}\nSettings: {settings}"
    )

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    total_users = len(DATA.get("users", {}))
    free_used = sum(1 for v in DATA.get("used_users", {}).values() if v > 0)
    premium = len(DATA.get("premium_users", []))
    await update.message.reply_text(
        f"Total users: {total_users}\nUsers used free trial: {free_used}\nPremium users: {premium}"
    )

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    users = DATA.get("users", {})
    lines = []
    for uid, info in users.items():
        used = DATA.get("used_users", {}).get(uid, 0)
        premium = int(uid) in DATA.get("premium_users", [])
        lines.append(f"{uid} - lang:{info.get('lang')} - used:{used} - premium:{premium}")
    await update.message.reply_text("\n".join(lines[:200]) or "No users yet.")

async def logs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    logs = DATA.get("logs", [])[:50]
    await update.message.reply_text("\n".join(logs) or "No logs yet.")

async def userlog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /userlog <user_id>")
        return
    uid = args[0]
    logs = [l for l in DATA.get("logs", []) if f"User {uid}" in l]
    await update.message.reply_text("\n".join(logs[:200]) or "No logs for this user.")

async def set_channel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /set_channel @channelusername")
        return
    ch = args[0]
    global CHANNEL_USERNAME
    CHANNEL_USERNAME = ch
    await update.message.reply_text(msg_for(update.effective_user.id, "set_channel_ok", channel=ch))

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(msg_for(update.effective_user.id, "not_admin"))
        return
    text = " ".join(context.args)
    count = 0
    for uid in DATA.get("users", {}).keys():
        try:
            await context.bot.send_message(int(uid), text)
            count += 1
        except Exception:
            pass
    await update.message.reply_text(f"Broadcast sent to {count} users.")
    log_activity(f"Admin {update.effective_user.id} broadcasted: {text}")

# ---------------- Main ------------------------------
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd), CommandHandler("to_vcf", to_vcf_start)],
        states={
            STATE_WAIT_LANG: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_lang)],
            STATE_WAIT_JOIN_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_join_confirm)],
            STATE_WAIT_FILE: [MessageHandler(filters.Document.ALL, receive_file)],
            STATE_WAIT_BASE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_base_name)],
            STATE_WAIT_STYLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_style)],
            STATE_WAIT_FORMAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_split_mode)],
            STATE_WAIT_COUNT_OR_SIZE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_count_or_size)],
            STATE_WAIT_START_NUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_start_number)],
            STATE_WAIT_ZIP_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_zip_password_and_process)],
        },
        fallbacks=[CommandHandler("cancel", lambda u,c: u.message.reply_text("Cancelled."))]
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("help", lambda u,c: c.bot.send_message(u.effective_chat.id, MESSAGES[get_user_settings(u.effective_user.id).get("lang","en")]["help"])))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("bulk_gen", bulk_gen_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bulk_gen_receive), group=1)
    app.add_handler(CommandHandler("give_access", give_access))
    app.add_handler(CommandHandler("revoke_access", revoke_access))
    app.add_handler(CommandHandler("add_admin", add_admin))
    app.add_handler(CommandHandler("remove_admin", remove_admin))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler("logs", logs_cmd))
    app.add_handler(CommandHandler("userlog", userlog_cmd))
    app.add_handler(CommandHandler("set_channel", set_channel_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    print("PRO VCF Bot running...")
    app.run_polling()

if __name__ == "__main__":
    main()
