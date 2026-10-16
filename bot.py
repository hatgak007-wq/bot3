# -*- coding: utf-8 -*-
"""
Bot Telegram pour des QCM organisés en arbre de profondeur libre sous chaque
module (ex: module -> leçon -> questions, ou module -> chapitre -> sous-thème
-> année -> questions). Supporte les réponses correctes multiples. La
navigation se fait via un clavier persistant (Reply Keyboard) qui parcourt
l'arbre dynamiquement : un niveau de menu par dossier traversé, jusqu'à
tomber sur une liste de questions.

Toutes les questions d'une feuille sont envoyées d'un coup sous forme de
messages texte normaux (question + options A/B/C...), avec la ou les
bonnes réponses affichées directement sous forme de spoiler (<tg-spoiler>)
à la fin du message (plus de sondages/polls : l'utilisateur lit la
question puis appuie sur le spoiler pour révéler la réponse, à son
rythme). Un bouton « ⬅️ Retour » reste affiché pendant toute la lecture
pour revenir au niveau précédent (ex : choisir une autre année) sans
repasser par le menu principal.

Chaque question est accompagnée d'un bouton inline « 🚩 Signaler »
permettant à l'utilisateur de signaler une question à revoir.

Les questions sont chargées depuis le dossier data/ (un fichier JSON par
module, via qcm_storage.py) — voir qcm_storage.py pour le détail
(load_qcm_data, get_node).

=== EXAMEN HTML AUTONOME ===
Depuis la liste racine des modules, un bouton « 📝 Examen HTML » permet de :
  1. Choisir un module.
  2. Indiquer un nombre de questions (limité au nombre de questions valides
     disponibles dans ce module, à N'IMPORTE QUEL niveau de son arbre).
  3. Recevoir un fichier .html autonome (aucun serveur requis) contenant ce
     nombre de questions tirées AU HASARD parmi tout le module (options de
     chaque question également mélangées), avec correction en JavaScript
     directement dans le navigateur et une note finale calculée sur 20.

=== MODE DE CORRECTION (tout ou rien / partiel) ===
L'examen HTML autonome ne demande plus à l'utilisateur de choisir un mode
de correction : les deux notes (tout ou rien ET partielle) sont calculées
en même temps et affichées ensemble à la fin de l'examen.

=== SIGNALEMENT D'UNE QUESTION ===
Sous chaque question envoyée, un bouton
« 🚩 Signaler » permet à l'utilisateur de signaler une question dont
l'énoncé, les options ou la bonne réponse lui semblent à revoir. Chaque
signalement est ajouté à reported_questions.json (question, options, bonne(s)
réponse(s), module/leçon, utilisateur, date). La commande /reports renvoie ce
fichier (réservée aux identifiants listés dans ADMIN_USER_IDS si celui-ci
n'est pas vide) ; si ADMIN_USER_IDS contient des identifiants Telegram, ils
reçoivent aussi une notification immédiate à chaque signalement.

=== ROBUSTESSE / CHARGE (300+ utilisateurs) ===
1. concurrent_updates=True : les updates de différents utilisateurs sont
   traités en parallèle au lieu d'un traitement strictement séquentiel.
2. Rate limiter global + retry automatique sur RetryAfter/TimedOut pour
   tous les envois de messages.
3. Nettoyage périodique de question_context (TTL) pour éviter une fuite mémoire.
4. Cache de QCM_DATA avec throttle de rechargement (pas de lecture disque
   à chaque navigation de menu).

Installation :
    pip install "python-telegram-bot[job-queue]" --upgrade

Lancement :
    python bot.py
"""

import asyncio
import html
import io
import json
import logging
import os
import random
import re
import time
import uuid

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputFile,
)
from telegram.error import RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

from qcm_storage import load_qcm_data, get_node

# ============ CONFIGURATION ============
# Le secret du bot n'est JAMAIS écrit ici en clair : il est lu depuis la
# variable d'environnement BOT_TOKEN. Sur PythonAnywhere, définis-la tout en
# haut de ton fichier WSGI (voir DEPLOY.md), par exemple :
#   os.environ["BOT_TOKEN"] = "xxxx"
# En local, tu peux la définir dans ton terminal avant de lancer le script :
#   export BOT_TOKEN="xxxx"        (Linux/Mac)
#   $env:BOT_TOKEN="xxxx"          (Windows PowerShell)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

SEND_DELAY_SECONDS = 0.35       # petit délai entre l'envoi de chaque question pour éviter les limites Telegram

TG_MESSAGE_MAX = 3900   # marge de sécurité sous la limite Telegram de 4096 caractères par message

BTN_BACK = "⬅️ Retour"
BTN_HOME = "🏠 Retour à l'accueil"
BTN_EXAM = "📝 Examen HTML"
LETTERS = ["A", "B", "C", "D", "E"]

EXAM_TOTAL_POINTS = 20   # note finale de l'examen HTML généré

# ============ SIGNALEMENT DE QUESTION (l'utilisateur signale une question à revoir) ============
REPORT_CALLBACK_PREFIX = "report:"
REPORT_BUTTON_LABEL = "🚩 Signaler"
REPORT_BUTTON_LABEL_DONE = "✅ Signalé"
REPORTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reported_questions.json")
# Renseigne ici ton identifiant Telegram numérique (celui de @userinfobot par exemple)
# pour recevoir une notification à chaque signalement, et/ou pour restreindre la
# commande /reports. Laisse le set vide pour que /reports reste ouverte à tout le monde.
ADMIN_USER_IDS: set[int] = set()

# ============ CONFIGURATION CHARGE / ROBUSTESSE ============
GLOBAL_RATE_LIMIT_PER_SEC = 25      # marge sous la limite Telegram globale (~30 msg/s)
MAX_SEND_RETRIES = 5                # nombre max de tentatives sur erreur transitoire
QUESTION_CONTEXT_TTL_SECONDS = 3 * 3600   # durée de vie max d'un contexte avant nettoyage
QUESTION_CONTEXT_CLEANUP_INTERVAL = 1800  # fréquence du nettoyage périodique
QCM_RELOAD_THROTTLE_SECONDS = 10    # ne recharge le JSON que si + de N secondes depuis le dernier chargement

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

QCM_DATA = load_qcm_data()
_last_qcm_reload_ts = time.monotonic()

# Associe un token unique aux infos nécessaires pour permettre à
# l'utilisateur de signaler une question (via le bouton « 🚩 Signaler »).
# Chaque entrée porte "created_at" pour permettre un nettoyage TTL.
question_context: dict[str, dict] = {}

# Verrou pour éviter que deux signalements simultanés ne s'écrasent en
# écrivant en même temps dans le fichier reported_questions.json.
_reports_lock = asyncio.Lock()


# ============ LIMITEUR DE DÉBIT GLOBAL (anti flood-control Telegram) ============
class GlobalRateLimiter:
    """Limite le nombre d'appels API Telegram par seconde sur TOUT le bot
    (et pas seulement par utilisateur), pour éviter les erreurs 429 quand
    beaucoup d'utilisateurs envoient/reçoivent des messages en même temps."""

    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second
        self._lock = asyncio.Lock()
        self._last_call = 0.0

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            wait_time = self._last_call + self._min_interval - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_call = time.monotonic()


rate_limiter = GlobalRateLimiter(GLOBAL_RATE_LIMIT_PER_SEC)


async def call_with_retry(coro_factory, *, max_retries: int = MAX_SEND_RETRIES, label: str = "appel Telegram"):
    """Exécute un appel API Telegram (send_message, ...) en respectant
    le rate limiter global et en réessayant automatiquement en cas d'erreur
    transitoire (flood control, timeout réseau), au lieu d'abandonner l'action.

    coro_factory doit être une fonction sans argument qui RETOURNE une coroutine
    (pas la coroutine elle-même), pour pouvoir la relancer proprement à chaque essai.
    """
    attempt = 0
    while True:
        attempt += 1
        await rate_limiter.wait()
        try:
            return await coro_factory()
        except RetryAfter as e:
            wait_s = float(getattr(e, "retry_after", 1)) + 0.5
            logger.warning("Flood control Telegram sur %s, attente %.1fs (essai %s)", label, wait_s, attempt)
            await asyncio.sleep(wait_s)
        except (TimedOut, NetworkError) as e:
            if attempt >= max_retries:
                logger.error("Échec définitif de %s après %s essais : %s", label, attempt, e)
                raise
            backoff = min(2 ** attempt, 20)
            logger.warning("Erreur réseau sur %s (essai %s), retry dans %ss : %s", label, attempt, backoff, e)
            await asyncio.sleep(backoff)


def is_valid_question(q: dict) -> bool:
    """Validité structurelle de base (indépendante des limites de longueur Telegram)."""
    if not isinstance(q, dict):
        return False
    if not q.get("question") or not q.get("options") or not all(q["options"]):
        return False
    if not (2 <= len(q["options"]) <= 5):
        return False
    if not q.get("correct_option_ids"):
        return False
    return True


def build_question_message(idx: int, total: int, q: dict) -> str:
    """Construit le texte d'une question (question + options A/B/C... + bonne(s)
    réponse(s) cachée(s) sous spoiler + explication éventuelle).

    Le spoiler liste TOUJOURS toutes les lettres de la question avec un
    repère ✅/❌ pour chacune, plutôt que de ne lister que les bonnes
    réponses : sa largeur ne dépend donc que du nombre d'options (déjà
    visible dans la question elle-même), jamais du nombre de bonnes
    réponses. Impossible de deviner combien de réponses sont correctes
    juste en regardant la taille du spoiler flouté avant de l'ouvrir."""
    options_lines = "\n".join(
        f"{LETTERS[i]}. {html.escape(opt)}" for i, opt in enumerate(q["options"])
    )
    correct_ids = set(q["correct_option_ids"])
    marks = " ".join(
        f"{LETTERS[i]}{'✅' if i in correct_ids else '❌'}"
        for i in range(len(q["options"]))
    )
    text = (
        f"[{idx + 1}/{total}] {html.escape(q['question'])}\n\n"
        f"{options_lines}\n\n"
        f"Réponse : <tg-spoiler>{marks}</tg-spoiler>"
    )
    if q.get("explanation"):
        text += f"\n💡 {html.escape(q['explanation'])}"
    return text


# ============ EXAMEN HTML AUTONOME (génération à la demande) ============

EXAM_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="fr" dir="ltr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Examen - __MODULE__</title>
<style>
  :root {
    --bg: #f4f6fb;
    --card: #ffffff;
    --primary: #2f6f4f;
    --primary-dark: #234f38;
    --danger: #c0392b;
    --text: #1f2933;
    --muted: #6b7280;
    --border: #e2e8f0;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    font-family: "Segoe UI", Tahoma, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 24px 16px 80px;
  }
  .header {
    max-width: 760px;
    margin: 0 auto 24px;
    text-align: center;
  }
  .header h1 {
    margin: 0 0 6px;
    color: var(--primary-dark);
    font-size: 24px;
  }
  .header p {
    margin: 0;
    color: var(--muted);
    font-size: 14px;
  }
  .quiz {
    max-width: 760px;
    margin: 0 auto;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .question-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 18px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
  }
  .question-card h3 {
    margin: 0 0 12px;
    font-size: 16px;
    line-height: 1.5;
  }
  .badge {
    display: inline-block;
    background: var(--primary);
    color: #fff;
    font-size: 12px;
    padding: 2px 8px;
    border-radius: 999px;
    margin-inline-end: 8px;
  }
  .option {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 10px;
    border-radius: 8px;
    margin-bottom: 6px;
    cursor: pointer;
    border: 1px solid transparent;
  }
  .option:hover { background: #f1f5f9; }
  .option input { transform: scale(1.15); accent-color: var(--primary); }
  .option.correct { background: #e6f6ea; border-color: #34a853; }
  .option.wrong { background: #fdeaea; border-color: var(--danger); }
  .option.missing { border-color: #3b82f6; border-style: dashed; }
  .status { margin-top: 10px; font-size: 13px; font-weight: 600; }
  .status.ok { color: #1b7f3a; }
  .status.ko { color: var(--danger); }
  .status.partial { color: #b8860b; }
  .footer-bar {
    max-width: 760px;
    margin: 24px auto 0;
    text-align: center;
  }
  #submitBtn {
    background: var(--primary);
    color: #fff;
    border: none;
    padding: 12px 28px;
    font-size: 15px;
    border-radius: 999px;
    cursor: pointer;
  }
  #submitBtn:hover { background: var(--primary-dark); }
  #submitBtn:disabled { opacity: 0.5; cursor: default; }
  #result {
    max-width: 760px;
    margin: 20px auto 0;
    text-align: center;
    font-size: 18px;
    font-weight: 700;
    color: var(--primary-dark);
    line-height: 1.6;
  }
</style>
</head>
<body>
  <div class="header">
    <h1>📝 Examen : __MODULE__</h1>
    <p>Nombre de questions : __N__ — Note finale sur __TOTAL__</p>
  </div>

  <div class="quiz" id="quiz"></div>

  <div class="footer-bar">
    <button id="submitBtn" onclick="gradeExam()">Corriger l'examen</button>
  </div>
  <div id="result"></div>

<script>
const QUESTIONS = __DATA__;
const TOTAL_POINTS = __TOTAL__;

function renderQuiz() {
  const container = document.getElementById('quiz');
  QUESTIONS.forEach((q, qi) => {
    const card = document.createElement('div');
    card.className = 'question-card';
    card.id = 'card-' + qi;

    const title = document.createElement('h3');
    title.innerHTML = '<span class="badge">' + (qi + 1) + '</span>' + escapeHtml(q.question);
    card.appendChild(title);

    q.options.forEach((opt, oi) => {
      const label = document.createElement('label');
      label.className = 'option';
      label.id = 'opt-' + qi + '-' + oi;
      const input = document.createElement('input');
      // Toujours une case à cocher, y compris pour les questions à une
      // seule bonne réponse : ainsi rien dans l'interface ne trahit si la
      // question attend une ou plusieurs réponses.
      input.type = 'checkbox';
      input.name = 'q' + qi;
      input.value = oi;
      label.appendChild(input);
      const span = document.createElement('span');
      span.textContent = opt;
      label.appendChild(span);
      card.appendChild(label);
    });

    const status = document.createElement('div');
    status.className = 'status';
    status.id = 'status-' + qi;
    card.appendChild(status);

    container.appendChild(card);
  });
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function gradeExam() {
  const perQuestion = TOTAL_POINTS / QUESTIONS.length;
  let totalStrict = 0;
  let totalPartial = 0;

  QUESTIONS.forEach((q, qi) => {
    const inputs = document.querySelectorAll('input[name="q' + qi + '"]');
    const selected = [];
    inputs.forEach(inp => {
      if (inp.checked) selected.push(parseInt(inp.value, 10));
      inp.disabled = true;
    });
    selected.sort((a, b) => a - b);
    const correct = q.correct.slice().sort((a, b) => a - b);
    const wrongSelected = selected.filter(oi => !correct.includes(oi));
    const overlap = selected.filter(oi => correct.includes(oi));
    const isFullMatch = JSON.stringify(selected) === JSON.stringify(correct);

    // Note "tout ou rien" : point entier seulement si la réponse est
    // parfaitement exacte.
    const fractionStrict = isFullMatch ? 1 : 0;
    totalStrict += fractionStrict * perQuestion;

    // Note "partielle" : crédit proportionnel au nombre de bonnes options
    // cochées, à condition de n'avoir coché aucune mauvaise option.
    let fractionPartial = 0;
    if (wrongSelected.length === 0 && overlap.length > 0) {
      fractionPartial = overlap.length / correct.length;
    }
    totalPartial += fractionPartial * perQuestion;

    q.options.forEach((opt, oi) => {
      const label = document.getElementById('opt-' + qi + '-' + oi);
      if (correct.includes(oi)) {
        label.classList.add('correct');
        if (!selected.includes(oi)) label.classList.add('missing');
      } else if (selected.includes(oi)) {
        label.classList.add('wrong');
      }
    });

    const statusEl = document.getElementById('status-' + qi);
    let state;
    if (isFullMatch) {
      statusEl.textContent = '✅ Correct';
      state = 'ok';
    } else if (fractionPartial > 0) {
      const missingCount = correct.length - overlap.length;
      statusEl.textContent =
        '🟡 Partiellement correct (' + Math.round(fractionPartial * 100) + '%) — il te manque ' +
        missingCount + ' choix correct(s)';
      state = 'partial';
    } else {
      statusEl.textContent = '❌ Incorrect';
      state = 'ko';
    }
    statusEl.classList.add(state);
  });

  document.getElementById('submitBtn').disabled = true;
  document.getElementById('result').innerHTML =
    'Note (tout ou rien) : ' + totalStrict.toFixed(2) + ' / ' + TOTAL_POINTS + '<br>' +
    'Note (partielle) : ' + totalPartial.toFixed(2) + ' / ' + TOTAL_POINTS;
  document.getElementById('result').scrollIntoView({ behavior: 'smooth' });
}

renderQuiz();
</script>
</body>
</html>
"""


def collect_module_questions(module: str) -> list[dict]:
    """Rassemble toutes les questions valides d'un module, en parcourant
    récursivement TOUT son arbre (quelle que soit sa profondeur), pour
    l'examen HTML (les limites de longueur Telegram ne s'y appliquent pas)."""
    result: list[dict] = []

    def walk(node):
        if isinstance(node, list):
            for q in node:
                if is_valid_question(q):
                    result.append(q)
        elif isinstance(node, dict):
            for child in node.values():
                walk(child)

    walk(QCM_DATA.get(module, {}))
    return result


def build_exam_html(module: str, questions: list[dict]) -> str:
    """Construit un examen HTML autonome (correction en JavaScript, dans le
    navigateur, aucun serveur requis) à partir d'une liste de questions déjà
    tirées au hasard. Les options de chaque question sont également
    mélangées. Les deux notes finales (tout ou rien et partielle) sont
    calculées ensemble et affichées ensemble à la fin de l'examen, réparties
    également entre toutes les questions."""
    exam_questions = []
    for q in questions:
        order = list(range(len(q["options"])))
        random.shuffle(order)
        shuffled_options = [q["options"][i] for i in order]
        new_correct = sorted(order.index(i) for i in q["correct_option_ids"])
        exam_questions.append({
            "question": q["question"],
            "options": shuffled_options,
            "correct": new_correct,
        })

    out = EXAM_HTML_TEMPLATE
    out = out.replace("__MODULE__", html.escape(module))
    out = out.replace("__N__", str(len(exam_questions)))
    out = out.replace("__TOTAL__", str(EXAM_TOTAL_POINTS))
    out = out.replace("__DATA__", json.dumps(exam_questions, ensure_ascii=False))
    return out


# ============ SUIVI DES QUESTIONS ENVOYÉES (pour signalement) ============

def register_question_context(
    chat_id: int,
    question: dict,
    correct_ids: set,
    module: str | None = None,
    path: list | None = None,
    quiz_label: str | None = None,
) -> str:
    """Enregistre les infos d'une question sous un token court, pour permettre
    à l'utilisateur de signaler cette question si besoin."""
    token = uuid.uuid4().hex[:12]
    question_context[token] = {
        "chat_id": chat_id,
        "question": question["question"],
        "options": question["options"],
        "correct_option_ids": set(correct_ids),
        "message_id": None,         # rempli juste après l'envoi du message de correction
        "base_text": None,          # texte du message de correction
        "module": module,
        "path": list(path) if path else [],
        "quiz_label": quiz_label,
        "reported": False,
        "created_at": time.monotonic(),
    }
    return token


async def save_report(record: dict):
    """Ajoute un signalement au fichier reported_questions.json (liste JSON),
    protégé par un verrou pour éviter les écritures concurrentes conflictuelles."""
    async with _reports_lock:
        data = []
        try:
            if os.path.exists(REPORTS_FILE):
                with open(REPORTS_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, list):
                        data = loaded
        except Exception as e:
            logger.error("Impossible de lire %s, on repart d'une liste vide : %s", REPORTS_FILE, e)
        data.append(record)
        try:
            with open(REPORTS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error("Impossible d'écrire dans %s : %s", REPORTS_FILE, e)


def cleanup_question_context(context: ContextTypes.DEFAULT_TYPE = None):
    """Supprime les contextes de question trop anciens pour éviter une fuite
    mémoire sur un processus qui tourne longtemps avec beaucoup d'utilisateurs."""
    now = time.monotonic()
    expired = [t for t, info in question_context.items() if now - info.get("created_at", now) > QUESTION_CONTEXT_TTL_SECONDS]
    for t in expired:
        question_context.pop(t, None)
    if expired:
        logger.info("Nettoyage question_context : %s entrées expirées supprimées", len(expired))


def build_question_button_markup(token: str) -> InlineKeyboardMarkup | None:
    """Construit le clavier inline avec le bouton de signalement de la
    question (toujours présent, tant que le token existe)."""
    info = question_context.get(token)
    if info is None:
        return None

    report_label = REPORT_BUTTON_LABEL_DONE if info.get("reported") else REPORT_BUTTON_LABEL
    rows = [[InlineKeyboardButton(report_label, callback_data=f"{REPORT_CALLBACK_PREFIX}{token}")]]

    return InlineKeyboardMarkup(rows)


async def handle_report_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Déclenché quand l'utilisateur appuie sur « 🚩 Signaler » sous une question
    corrigée : enregistre la question (texte, options, bonne(s) réponse(s),
    module/leçon) dans reported_questions.json pour une relecture ultérieure,
    et prévient les admins configurés si ADMIN_USER_IDS n'est pas vide."""
    query = update.callback_query
    token = query.data[len(REPORT_CALLBACK_PREFIX):]
    info = question_context.get(token)

    if info is None:
        await query.answer("Cette question n'est plus disponible pour être signalée.", show_alert=True)
        return

    if info.get("reported"):
        await query.answer("Tu as déjà signalé cette question, merci !")
        return

    info["reported"] = True
    user = update.effective_user

    correct_texts = [info["options"][i] for i in sorted(info["correct_option_ids"])]
    record = {
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "chat_id": info["chat_id"],
        "user_id": user.id if user else None,
        "username": (user.username or user.full_name) if user else None,
        "module": info.get("module"),
        "path": info.get("path", []),
        "quiz_label": info.get("quiz_label"),
        "question": info["question"],
        "options": info["options"],
        "correct_options": correct_texts,
    }
    await save_report(record)

    await query.answer("🚩 Merci, la question a été signalée pour relecture.", show_alert=True)

    new_markup = build_question_button_markup(token)
    try:
        await call_with_retry(
            lambda: query.edit_message_reply_markup(reply_markup=new_markup),
            label="edit_message_reply_markup(report)",
        )
    except Exception:
        pass

    for admin_id in ADMIN_USER_IDS:
        try:
            options_lines = "\n".join(f"{LETTERS[i]}. {opt}" for i, opt in enumerate(info["options"]))
            admin_text = (
                "🚩 Nouvelle question signalée :\n\n"
                f"{info['question']}\n\n{options_lines}\n\n"
                f"✅ Bonne(s) réponse(s) : {', '.join(correct_texts)}\n\n"
                f"📂 {info.get('quiz_label') or info.get('module') or 'inconnu'}\n"
                f"👤 {record['username'] or record['user_id']}"
            )
            await call_with_retry(
                lambda: context.bot.send_message(admin_id, admin_text),
                label="send_message(notif admin signalement)",
            )
        except Exception as e:
            logger.error("Échec de notification admin (id=%s) pour un signalement : %s", admin_id, e)


async def cmd_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /reports : envoie le fichier des questions signalées (si
    ADMIN_USER_IDS est configuré, réservé à ces utilisateurs)."""
    user_id = update.effective_user.id if update.effective_user else None
    if ADMIN_USER_IDS and user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("⛔ Commande réservée.")
        return

    if not os.path.exists(REPORTS_FILE):
        await update.message.reply_text("Aucune question signalée pour le moment.")
        return

    try:
        with open(REPORTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        count = len(data) if isinstance(data, list) else 0
    except Exception:
        count = "?"

    with open(REPORTS_FILE, "rb") as f:
        await update.message.reply_document(
            document=InputFile(f, filename=REPORTS_FILE),
            caption=f"🚩 {count} question(s) signalée(s).",
        )


# ============ CONSTRUCTION DES CLAVIERS (Reply Keyboard) ============

def build_keys_keyboard(keys: list[str], with_back: bool, is_root: bool = False) -> ReplyKeyboardMarkup:
    """Clavier générique pour un niveau de l'arbre. is_root ajoute le bouton
    « 📝 Examen HTML » (uniquement pertinent à la racine, sur la liste des
    modules)."""
    buttons = [[KeyboardButton(k)] for k in keys]
    if is_root:
        buttons.append([KeyboardButton(BTN_EXAM)])
    if with_back:
        buttons.append([KeyboardButton(BTN_BACK)])
    buttons.append([KeyboardButton(BTN_HOME)])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def build_quiz_keyboard() -> ReplyKeyboardMarkup:
    """Clavier affiché pendant l'envoi/la lecture des questions d'une feuille
    (ou pendant la saisie du nombre de questions d'un examen) : « ⬅️ Retour »
    pour revenir choisir un autre dossier/une autre année sans attendre la
    fin, et « 🏠 » pour revenir directement à l'accueil."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton(BTN_BACK)], [KeyboardButton(BTN_HOME)]], resize_keyboard=True
    )


def build_exam_modules_keyboard() -> ReplyKeyboardMarkup:
    modules = list(QCM_DATA.keys())
    return build_keys_keyboard(modules, with_back=True)


def maybe_reload_qcm_data():
    """Recharge QCM_DATA depuis le disque, mais au maximum une fois toutes les
    QCM_RELOAD_THROTTLE_SECONDS secondes, pour éviter des lectures disque
    répétées à chaque navigation de menu quand beaucoup d'utilisateurs sont
    actifs en même temps."""
    global QCM_DATA, _last_qcm_reload_ts
    now = time.monotonic()
    if now - _last_qcm_reload_ts >= QCM_RELOAD_THROTTLE_SECONDS:
        QCM_DATA = load_qcm_data()
        _last_qcm_reload_ts = now


def abandon_quiz_if_any(context: ContextTypes.DEFAULT_TYPE):
    """Nettoie l'état de navigation/lecture en cours (via ⬅️/🏠), pour repartir
    sur une base propre."""
    chat_data = context.chat_data
    for key in (
        "module", "path", "quiz_questions", "quiz_label",
        "exam_module", "exam_available",
    ):
        chat_data.pop(key, None)


async def show_modules(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    maybe_reload_qcm_data()

    context.chat_data["menu_level"] = "browsing"
    context.chat_data["module"] = None
    context.chat_data["path"] = []

    if not QCM_DATA:
        await call_with_retry(
            lambda: context.bot.send_message(
                chat_id, "Aucune question enregistrée pour le moment.", reply_markup=ReplyKeyboardRemove()
            ),
            label="send_message(aucune question)",
        )
        return

    modules = list(QCM_DATA.keys())
    await call_with_retry(
        lambda: context.bot.send_message(
            chat_id, "📚 Choisis un module :", reply_markup=build_keys_keyboard(modules, with_back=False, is_root=True)
        ),
        label="send_message(modules)",
    )


async def show_node(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Affiche le menu du nœud courant (context.chat_data['module'] + ['path'])."""
    module = context.chat_data.get("module")
    path = context.chat_data.get("path", [])
    node = get_node(QCM_DATA.get(module, {}), path)

    if not isinstance(node, dict) or not node:
        await call_with_retry(
            lambda: context.bot.send_message(chat_id, "⚠️ Ce dossier est vide ou n'existe plus."),
            label="send_message(dossier vide)",
        )
        await show_modules(chat_id, context)
        return

    keys = list(node.keys())
    breadcrumb = " → ".join([module] + path)
    await call_with_retry(
        lambda: context.bot.send_message(
            chat_id, f"📂 {breadcrumb}", reply_markup=build_keys_keyboard(keys, with_back=True)
        ),
        label="send_message(node)",
    )


async def show_exam_module_choice(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    maybe_reload_qcm_data()
    modules = list(QCM_DATA.keys())
    if not modules:
        await call_with_retry(
            lambda: context.bot.send_message(chat_id, "Aucun module disponible pour le moment."),
            label="send_message(exam aucun module)",
        )
        return

    context.chat_data["menu_level"] = "exam_module"
    await call_with_retry(
        lambda: context.bot.send_message(
            chat_id, "📝 Choisis le module pour lequel générer un examen HTML :",
            reply_markup=build_exam_modules_keyboard(),
        ),
        label="send_message(exam choix module)",
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Bonjour 👋\nChoisis un module avec les boutons ci-dessous pour commencer.\n\n"
        "Les questions s'affichent avec leur bonne réponse cachée sous un spoiler "
        "(appuie dessus pour la révéler).\n"
        f"Sous chaque question, le bouton « {REPORT_BUTTON_LABEL} » permet de signaler une question à revoir."
    )
    await show_modules(update.effective_chat.id, context)


async def qcm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_modules(update.effective_chat.id, context)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gère les appuis sur les boutons du clavier persistant (Reply Keyboard)."""
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    if text == BTN_HOME:
        abandon_quiz_if_any(context)
        await show_modules(chat_id, context)
        return

    level = context.chat_data.get("menu_level", "browsing")

    if text == BTN_BACK:
        if level == "exam_module":
            await show_modules(chat_id, context)
            return

        if level == "exam_count":
            context.chat_data.pop("exam_module", None)
            context.chat_data.pop("exam_available", None)
            await show_exam_module_choice(chat_id, context)
            return

        # level == "browsing" ou "quiz" (retour depuis la lecture des
        # questions vers le dossier parent, ex: choisir une autre année)
        context.chat_data["menu_level"] = "browsing"
        context.chat_data.pop("quiz_questions", None)
        context.chat_data.pop("quiz_label", None)
        module = context.chat_data.get("module")
        path = context.chat_data.get("path", [])
        if path:
            context.chat_data["path"] = path[:-1]
            await show_node(chat_id, context)
        elif module is not None:
            context.chat_data["module"] = None
            await show_modules(chat_id, context)
        else:
            await show_modules(chat_id, context)
        return

    if level == "quiz":
        return  # on ignore le texte libre pendant l'envoi des questions

    if level == "exam_module":
        modules = list(QCM_DATA.keys())
        if text in modules:
            module = text
            available = collect_module_questions(module)
            if not available:
                await call_with_retry(
                    lambda: context.bot.send_message(
                        chat_id, f"⚠️ Aucune question valide dans le module « {module} »."
                    ),
                    label="send_message(exam module vide)",
                )
                await show_modules(chat_id, context)
                return

            context.chat_data["exam_module"] = module
            context.chat_data["exam_available"] = len(available)
            context.chat_data["menu_level"] = "exam_count"

            await call_with_retry(
                lambda: context.bot.send_message(
                    chat_id,
                    f"🔢 Combien de questions veux-tu dans l'examen « {module} » ?\n"
                    f"(maximum disponible : {len(available)})",
                    reply_markup=build_quiz_keyboard(),
                ),
                label="send_message(exam demande nombre)",
            )
        else:
            await call_with_retry(
                lambda: context.bot.send_message(chat_id, "Choisis un module parmi les boutons ci-dessous."),
                label="send_message(exam choix module invalide)",
            )
        return

    if level == "exam_count":
        module = context.chat_data.get("exam_module")
        max_available = context.chat_data.get("exam_available", 0)

        if not text.isdigit():
            await call_with_retry(
                lambda: context.bot.send_message(chat_id, "Merci d'entrer un nombre entier positif."),
                label="send_message(exam nombre invalide)",
            )
            return

        n = int(text)
        if n <= 0 or n > max_available:
            await call_with_retry(
                lambda: context.bot.send_message(
                    chat_id, f"Merci d'entrer un nombre entre 1 et {max_available}."
                ),
                label="send_message(exam nombre hors limites)",
            )
            return

        all_questions = collect_module_questions(module)
        selected = random.sample(all_questions, n)
        exam_html = build_exam_html(module, selected)
        file_bytes = exam_html.encode("utf-8")

        safe_name = re.sub(r"[^\w\-]+", "_", module, flags=re.UNICODE).strip("_") or "module"
        filename = f"examen_{safe_name}_{n}q.html"

        await call_with_retry(
            lambda: context.bot.send_document(
                chat_id,
                document=InputFile(io.BytesIO(file_bytes), filename=filename),
                caption=f"📝 Examen « {module} » — {n} question(s) — note sur {EXAM_TOTAL_POINTS}",
            ),
            label="send_document(exam)",
        )

        context.chat_data.pop("exam_module", None)
        context.chat_data.pop("exam_available", None)
        await show_modules(chat_id, context)
        return

    # level == "browsing" : navigation dans l'arbre (profondeur libre)
    module = context.chat_data.get("module")
    path = context.chat_data.get("path", [])

    if module is None:
        if text == BTN_EXAM:
            await show_exam_module_choice(chat_id, context)
            return
        if text in QCM_DATA:
            context.chat_data["module"] = text
            context.chat_data["path"] = []
            await show_node(chat_id, context)
        else:
            await call_with_retry(
                lambda: context.bot.send_message(chat_id, "Choisis un module parmi les boutons ci-dessous."),
                label="send_message(choix module invalide)",
            )
        return

    node = get_node(QCM_DATA.get(module, {}), path)
    if not isinstance(node, dict) or text not in node:
        await call_with_retry(
            lambda: context.bot.send_message(chat_id, "Choisis une option parmi les boutons ci-dessous."),
            label="send_message(choix option invalide)",
        )
        return

    child = node[text]
    new_path = path + [text]

    if isinstance(child, list):
        # feuille atteinte : on envoie toutes les questions de cette liste
        context.chat_data["path"] = new_path
        context.chat_data["quiz_questions"] = child
        context.chat_data["quiz_label"] = " / ".join([module] + new_path)
        context.chat_data["menu_level"] = "quiz"

        await call_with_retry(
            lambda: context.bot.send_message(
                chat_id,
                f"🚀 « {context.chat_data['quiz_label']} » — envoi de toutes les questions...",
                reply_markup=build_quiz_keyboard(),
            ),
            label="send_message(debut quiz)",
        )
        await send_all_questions(chat_id, context)
    else:
        # encore un dossier : on descend d'un niveau
        context.chat_data["path"] = new_path
        await show_node(chat_id, context)


async def send_all_questions(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Envoie toutes les questions valides d'une feuille d'un coup, sous forme
    de messages texte (question + options + bonne réponse en spoiler). Les
    envois passent par le rate limiter global et réessaient automatiquement en
    cas d'erreur transitoire (flood control Telegram), ce qui évite de perdre
    des questions quand beaucoup d'utilisateurs sont actifs en même temps."""
    chat_data = context.chat_data
    questions = chat_data.get("quiz_questions", [])

    if not questions:
        await call_with_retry(
            lambda: context.bot.send_message(chat_id, "⚠️ Aucune question ici."),
            label="send_message(quiz vide)",
        )
        await show_modules(chat_id, context)
        return

    skipped = []
    failed = []
    sent_count = 0

    for idx, q in enumerate(questions):
        if not is_valid_question(q):
            skipped.append(idx + 1)
            logger.error("Question invalide dans '%s' index %s : %s", chat_data.get("quiz_label"), idx, q)
            continue

        text_q = build_question_message(idx, len(questions), q)
        if len(text_q) > TG_MESSAGE_MAX:
            # Message trop long pour Telegram (rare, en général à cause d'une
            # longue explication) : on l'envoie sans l'explication.
            q_sans_explication = dict(q)
            q_sans_explication.pop("explanation", None)
            text_q = build_question_message(idx, len(questions), q_sans_explication)

        report_token = register_question_context(
            chat_id, q, set(q["correct_option_ids"]),
            module=chat_data.get("module"),
            path=chat_data.get("path", []),
            quiz_label=chat_data.get("quiz_label"),
        )
        markup = build_question_button_markup(report_token)

        try:
            sent = await call_with_retry(
                lambda: context.bot.send_message(
                    chat_id, text_q, parse_mode="HTML", reply_markup=markup,
                ),
                label=f"send_message(question q{idx + 1})",
            )
        except Exception as e:
            failed.append(idx + 1)
            logger.error(
                "Échec d'envoi de la question '%s' index %s : %s",
                chat_data.get("quiz_label"), idx, e,
            )
            continue

        info = question_context.get(report_token)
        if info is not None:
            info["message_id"] = sent.message_id
            info["base_text"] = text_q

        sent_count += 1
        await asyncio.sleep(SEND_DELAY_SECONDS)

    if skipped:
        await call_with_retry(
            lambda: context.bot.send_message(
                chat_id,
                "⚠️ Questions ignorées à cause d'un problème dans leurs données : " + ", ".join(map(str, skipped)),
            ),
            label="send_message(skipped)",
        )

    if failed:
        await call_with_retry(
            lambda: context.bot.send_message(
                chat_id,
                "⚠️ Questions non envoyées (échec d'envoi) : " + ", ".join(map(str, failed)),
            ),
            label="send_message(failed)",
        )

    if sent_count == 0:
        await call_with_retry(
            lambda: context.bot.send_message(chat_id, "⚠️ Aucune question valide ici."),
            label="send_message(aucune valide)",
        )
        await show_modules(chat_id, context)
        return

    # Fin de la lecture : on repasse en navigation normale (⬅️ Retour permet
    # de choisir un autre dossier/une autre année, 🏠 ramène à l'accueil).
    context.chat_data["menu_level"] = "browsing"
    await call_with_retry(
        lambda: context.bot.send_message(
            chat_id,
            f"✅ « {chat_data.get('quiz_label', '')} » — {sent_count} question(s) envoyée(s).\n"
            "Appuie sur le spoiler de chaque question pour voir la bonne réponse.",
            reply_markup=build_quiz_keyboard(),
        ),
        label="send_message(fin envoi questions)",
    )


async def periodic_question_context_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """Job récurrent : purge les entrées question_context expirées pour éviter
    une fuite mémoire quand le bot tourne longtemps avec beaucoup d'utilisateurs."""
    cleanup_question_context()


def build_application() -> Application:
    """Construit l'Application PTB (handlers + job queue), sans la démarrer.
    Utilisée à la fois par le mode polling (test local) et le mode webhook
    (déploiement PythonAnywhere)."""
    if not BOT_TOKEN:
        raise RuntimeError(
            "⚠️  BOT_TOKEN manquant : définis la variable d'environnement BOT_TOKEN "
            "avant de lancer le bot (voir DEPLOY.md)."
        )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)   # traite les updates de différents utilisateurs en parallèle
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .get_updates_connect_timeout(30)
        .get_updates_read_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("qcm", qcm_command))
    app.add_handler(CommandHandler("reports", cmd_reports))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_report_button, pattern=rf"^{REPORT_CALLBACK_PREFIX}"))

    app.job_queue.run_repeating(periodic_question_context_cleanup, interval=QUESTION_CONTEXT_CLEANUP_INTERVAL, first=QUESTION_CONTEXT_CLEANUP_INTERVAL)
    return app


def main():
    """Mode polling : local + Render Web Service (avec petit serveur health
    en tâche de fond pour satisfaire le port requis par Render)."""
    # Render exige que le service écoute sur $PORT, sinon le déploiement
    # échoue. On lance donc un mini serveur Flask en thread daemon,
    # et le bot continue en polling dans le thread principal.
    port = int(os.environ.get("PORT", "10000"))
    try:
        from flask import Flask as _HealthFlask
        import threading as _threading

        _health = _HealthFlask("health")

        @_health.route("/", methods=["GET"])
        def _ok():
            return "OK - bot running (polling)", 200

        _threading.Thread(
            target=lambda: _health.run(host="0.0.0.0", port=port, use_reloader=False),
            daemon=True,
        ).start()
        print(f"Health server listening on port {port}")
    except Exception as e:
        logger.warning("Impossible de démarrer le health server : %s", e)

    app = build_application()
    print("🤖 Le bot est lancé en mode polling... (Ctrl+C pour arrêter)")
    app.run_polling()


# ============ MODE WEBHOOK (déploiement PythonAnywhere / tout serveur WSGI) ============
# Ce bloc n'est exécuté QUE si bot.py est importé (pas lancé directement),
# ce qui est le cas quand le fichier WSGI de PythonAnywhere fait
# "from bot import flask_app as application". En local (python bot.py),
# on reste en mode polling classique, plus simple pour tester.
if __name__ != "__main__":
    from flask import Flask, request

    tg_app = build_application()

    # PTB (asyncio) a besoin d'une boucle d'événements persistante : on en crée
    # une une seule fois au chargement du module, et on la réutilise pour
    # chaque requête webhook (au lieu d'asyncio.run() qui en recrée une à
    # chaque fois et casserait les objets internes de l'Application).
    _webhook_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_webhook_loop)
    _webhook_loop.run_until_complete(tg_app.initialize())
    _webhook_loop.run_until_complete(tg_app.start())  # démarre aussi le job_queue

    flask_app = Flask(__name__)

    # Le token dans l'URL sert de "secret" basique : seul quelqu'un connaissant
    # le token peut deviner l'URL du webhook.
    WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

    @flask_app.route(WEBHOOK_PATH, methods=["POST"])
    def telegram_webhook():
        update = Update.de_json(request.get_json(force=True), tg_app.bot)
        _webhook_loop.run_until_complete(tg_app.process_update(update))
        return "OK"

    @flask_app.route("/", methods=["GET"])
    def health_check():
        return "✅ Le bot QCM est en ligne (mode webhook)."


if __name__ == "__main__":
    main()