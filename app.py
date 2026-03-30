from __future__ import annotations

import csv
import os
import re
import secrets
import smtplib
import sqlite3
import textwrap
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from functools import wraps
from io import StringIO
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, Response, g, jsonify, redirect, render_template, request, send_file, session, url_for
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

load_dotenv()


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value or default


BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = Path(env_or_default("DATABASE_PATH", str(BASE_DIR / "database.sqlite3"))).expanduser()
PDF_DIR = Path(env_or_default("PDF_DIR", str(BASE_DIR / "storage" / "pdfs"))).expanduser()

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.config["DATABASE"] = str(DATABASE_PATH)
app.config["JSON_AS_ASCII"] = False
app.secret_key = os.getenv("SECRET_KEY", "local-dev-secret")

CLIENT_STATUSES = {"active", "prospect", "onboarding", "vip"}
SALE_STATUSES = {"new", "proposal", "negotiation", "won"}
DOCUMENT_STATUSES = {"draft", "sent", "approved"}
TASK_STATUSES = {"open", "in_progress", "done"}
PRIORITIES = {"low", "medium", "high"}
USER_ROLES = {"admin", "manager", "analyst", "client"}
MANAGE_ROLES = {"admin", "manager"}
EXPORT_ROLES = {"admin", "manager", "analyst"}
ROOM_LABELS = {
    "general": "Общий канал",
    "contracts": "Договоры",
    "support": "Поддержка",
}
ROLE_LABELS_RU = {
    "admin": "Руководитель",
    "manager": "Менеджер",
    "analyst": "Аналитик",
    "client": "Клиент",
}
STATUS_LABELS_RU = {
    "active": "Активный",
    "admin": "Руководитель",
    "analyst": "Аналитик",
    "approved": "Согласован",
    "client": "Клиент",
    "contract": "Договор",
    "crm": "CRM",
    "documents": "Документы",
    "done": "Готово",
    "draft": "Черновик",
    "general": "Общий канал",
    "high": "Высокий",
    "in_progress": "В работе",
    "invoice": "Счет",
    "low": "Низкий",
    "manager": "Менеджер",
    "medium": "Средний",
    "negotiation": "Переговоры",
    "new": "Новая",
    "onboarding": "Подключение",
    "open": "Открыта",
    "proposal": "Предложение",
    "prospect": "Потенциальный",
    "sent": "Отправлен",
    "support": "Поддержка",
    "vip": "VIP",
    "won": "Успешно",
}
DOCUMENT_TEMPLATES = [
    {
        "id": "contract_launch",
        "title": "Договор на внедрение",
        "type": "contract",
        "amount": 780000,
        "description": "Для запуска нового клиента с этапами внедрения и SLA.",
        "content": (
            "Предмет договора: внедрение продукта, настройка ролей, импорт базы клиентов, "
            "обучение сотрудников и сопровождение запуска в течение 30 дней."
        ),
    },
    {
        "id": "proposal_growth",
        "title": "Коммерческое предложение",
        "type": "proposal",
        "amount": 640000,
        "description": "Для продажи аналитики, чат-модуля и документооборота.",
        "content": (
            "Предлагаем подключение панели аналитики продаж, базы клиентов, электронного "
            "согласования документов и общего клиентского чата с поддержкой команды."
        ),
    },
    {
        "id": "invoice_monthly",
        "title": "Ежемесячный счет",
        "type": "invoice",
        "amount": 250000,
        "description": "Для регулярной оплаты доступа к продукту.",
        "content": (
            "Счет включает ежемесячный доступ к системе, хранение документов в PDF, "
            "почтовые уведомления и поддержку по рабочим вопросам."
        ),
    },
    {
        "id": "act_delivery",
        "title": "Акт выполненных работ",
        "type": "act",
        "amount": 320000,
        "description": "Для закрытия этапа внедрения или консультационных работ.",
        "content": (
            "Настоящим подтверждается выполнение работ по настройке аналитики, "
            "загрузке клиентской базы, подготовке документов и обучению команды."
        ),
    },
]


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'manager',
    client_id INTEGER,
    created_at TEXT NOT NULL,
    last_login_at TEXT,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    company TEXT,
    email TEXT NOT NULL,
    phone TEXT,
    segment TEXT,
    city TEXT,
    tags TEXT,
    status TEXT NOT NULL DEFAULT 'prospect',
    notes TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    sale_date TEXT NOT NULL,
    source TEXT,
    owner TEXT,
    region TEXT,
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    amount REAL DEFAULT 0,
    due_date TEXT,
    content TEXT NOT NULL,
    approval_token TEXT NOT NULL UNIQUE,
    pdf_path TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS email_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER,
    client_id INTEGER,
    recipient TEXT NOT NULL,
    subject TEXT NOT NULL,
    status TEXT NOT NULL,
    body TEXT,
    preview_text TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE SET NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    room_name TEXT NOT NULL,
    client_id INTEGER,
    sender_name TEXT NOT NULL,
    sender_role TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER,
    title TEXT NOT NULL,
    description TEXT,
    due_date TEXT,
    priority TEXT NOT NULL DEFAULT 'medium',
    status TEXT NOT NULL DEFAULT 'open',
    owner TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL
);
"""


def utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat(sep=" ")


def today_iso() -> str:
    return date.today().isoformat()


def slugify(value: str) -> str:
    value = re.sub(r"[^\w\s-]", "", value, flags=re.UNICODE)
    value = re.sub(r"[-\s]+", "-", value, flags=re.UNICODE).strip("-")
    return value.lower() or "document"


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def clean_float(value: Any, default: float = 0.0) -> float:
    raw = clean_text(value).replace(",", ".")
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def clean_int(value: Any, default: int = 0) -> int:
    raw = clean_text(value)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def validate_email(email: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email))


def normalize_choice(value: Any, allowed: set[str], fallback: str) -> str:
    normalized = clean_text(value).lower()
    return normalized if normalized in allowed else fallback


def label_for_status(value: str) -> str:
    return STATUS_LABELS_RU.get(value, value)


def label_for_role(value: str) -> str:
    return ROLE_LABELS_RU.get(value, value)


def initials_for_name(value: str) -> str:
    words = [part for part in clean_text(value).split() if part]
    if not words:
        return "?"
    if len(words) == 1:
        return words[0][:2].upper()
    return f"{words[0][0]}{words[1][0]}".upper()


def build_permissions(user: dict[str, Any] | None) -> dict[str, bool | str]:
    role = clean_text(user.get("role") if user else "guest") or "guest"
    return {
        "role": role,
        "can_manage_clients": role in MANAGE_ROLES,
        "can_manage_sales": role in MANAGE_ROLES,
        "can_manage_documents": role in MANAGE_ROLES,
        "can_manage_tasks": role in MANAGE_ROLES,
        "can_send_email": role in MANAGE_ROLES,
        "can_export": role in EXPORT_ROLES,
        "can_view_finance": role in {"admin", "manager", "analyst", "client"},
        "can_chat": role in USER_ROLES,
        "read_only": role in {"analyst", "client"},
        "client_scoped": role == "client",
    }


def safe_next_url(value: Any) -> str:
    target = clean_text(value)
    if target.startswith("/") and not target.startswith("//"):
        return target
    return url_for("workspace")


def demo_login_profiles() -> list[dict[str, str]]:
    return [
        {
            "username": "director",
            "password": "demo12345",
            "title": "Руководитель",
            "description": "Полный доступ к продажам, документам, экспорту и настройкам команды.",
        },
        {
            "username": "manager",
            "password": "demo12345",
            "title": "Менеджер",
            "description": "Работа со сделками, клиентами, письмами и задачами без лишней бюрократии.",
        },
        {
            "username": "analyst",
            "password": "demo12345",
            "title": "Аналитик",
            "description": "Просмотр дашборда, поиск инсайтов, экспорт и контроль динамики воронки.",
        },
        {
            "username": "client",
            "password": "demo12345",
            "title": "Клиент",
            "description": "Свой кабинет клиента: документы, чат и прозрачный статус работ.",
        },
    ]


def sum_money(rows: list[dict[str, Any]]) -> float:
    return round(sum(float(row.get("total") or 0) for row in rows), 1)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        database_path = Path(app.config["DATABASE"])
        database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(app.config["DATABASE"])
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        g.db = connection
    return g.db


@app.teardown_appcontext
def close_db(_: Any) -> None:
    connection = g.pop("db", None)
    if connection is not None:
        connection.close()


@app.before_request
def load_current_user() -> None:
    user_id = session.get("user_id")
    if not user_id:
        g.current_user = None
        return

    user = fetch_user_by_id(clean_int(user_id))
    if not user:
        session.clear()
    g.current_user = user


@app.context_processor
def inject_template_context() -> dict[str, Any]:
    user = current_user()
    return {
        "current_user": user,
        "current_permissions": build_permissions(user),
        "role_label": label_for_role,
    }


def query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    rows = get_db().execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def query_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    row = get_db().execute(sql, params).fetchone()
    return dict(row) if row else None


def execute(sql: str, params: tuple[Any, ...] = ()) -> int:
    cursor = get_db().execute(sql, params)
    get_db().commit()
    return cursor.lastrowid


def public_user_record(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    user = dict(row)
    user["role_label"] = label_for_role(user["role"])
    user["initials"] = initials_for_name(user["display_name"])
    return user


def fetch_user_by_id(user_id: int) -> dict[str, Any] | None:
    row = query_one(
        """
        SELECT
            u.id,
            u.username,
            u.display_name,
            u.email,
            u.role,
            u.client_id,
            u.created_at,
            u.last_login_at,
            c.name AS client_name,
            c.company AS client_company
        FROM users u
        LEFT JOIN clients c ON c.id = u.client_id
        WHERE u.id = ?
        """,
        (user_id,),
    )
    return public_user_record(row)


def fetch_login_user(identity: str) -> dict[str, Any] | None:
    normalized = clean_text(identity).lower()
    return query_one(
        """
        SELECT
            u.id,
            u.username,
            u.display_name,
            u.email,
            u.password_hash,
            u.role,
            u.client_id,
            c.name AS client_name,
            c.company AS client_company
        FROM users u
        LEFT JOIN clients c ON c.id = u.client_id
        WHERE lower(u.username) = ? OR lower(u.email) = ?
        """,
        (normalized, normalized),
    )


def current_user() -> dict[str, Any] | None:
    return getattr(g, "current_user", None)


def is_api_request() -> bool:
    return request.path.startswith("/api/")


def unauthorized_response(message: str = "Требуется вход в систему.") -> Response:
    if is_api_request():
        return jsonify({"message": message, "code": "auth_required"}), 401
    return redirect(url_for("login", next=request.full_path if request.query_string else request.path))


def forbidden_response(message: str = "Недостаточно прав для этого действия.") -> Response:
    if is_api_request():
        return jsonify({"message": message, "code": "forbidden"}), 403
    return redirect(url_for("workspace"))


def login_required(view: Any) -> Any:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if not current_user():
            return unauthorized_response()
        return view(*args, **kwargs)

    return wrapped


def roles_required(*roles: str) -> Any:
    def decorator(view: Any) -> Any:
        @wraps(view)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            user = current_user()
            if not user:
                return unauthorized_response()
            if user["role"] not in roles:
                return forbidden_response()
            return view(*args, **kwargs)

        return wrapped

    return decorator


def client_scope_id(user: dict[str, Any] | None) -> int | None:
    if user and user.get("role") == "client":
        return clean_int(user.get("client_id")) or None
    return None


def document_access_allowed(document: dict[str, Any], user: dict[str, Any] | None) -> bool:
    scope_id = client_scope_id(user)
    return not scope_id or clean_int(document.get("client_id")) == scope_id


def sale_access_allowed(sale: dict[str, Any], user: dict[str, Any] | None) -> bool:
    scope_id = client_scope_id(user)
    return not scope_id or clean_int(sale.get("client_id")) == scope_id


def task_access_allowed(task: dict[str, Any], user: dict[str, Any] | None) -> bool:
    scope_id = client_scope_id(user)
    return not scope_id or clean_int(task.get("client_id")) == scope_id


def ensure_storage() -> None:
    PDF_DIR.mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    get_db().executescript(SCHEMA_SQL)
    get_db().commit()


def seed_demo_data() -> None:
    existing = query_one("SELECT COUNT(*) AS count FROM clients")
    if existing and existing["count"]:
        return

    current = date.today()
    client_seed = [
        {
            "name": "Alem Retail",
            "company": "Alem Retail Group",
            "email": "partners@alem-retail.kz",
            "phone": "+7 700 111 20 10",
            "segment": "Retail",
            "city": "Almaty",
            "tags": "vip,analytics",
            "status": "active",
            "notes": "Ключевой клиент по розничной сети.",
        },
        {
            "name": "Steppe Logistics",
            "company": "Steppe Logistics",
            "email": "sales@steppe-logistics.kz",
            "phone": "+7 700 210 12 45",
            "segment": "Logistics",
            "city": "Astana",
            "tags": "contract,delivery",
            "status": "onboarding",
            "notes": "Ожидается масштабирование на 3 филиала.",
        },
        {
            "name": "Samal Medical",
            "company": "Samal Medical Center",
            "email": "procurement@samalmed.kz",
            "phone": "+7 701 777 00 21",
            "segment": "Healthcare",
            "city": "Shymkent",
            "tags": "pdf,approval",
            "status": "active",
            "notes": "Нужны ежемесячные отчеты в PDF.",
        },
        {
            "name": "Qala Build",
            "company": "Qala Build",
            "email": "office@qalabuild.kz",
            "phone": "+7 702 454 19 88",
            "segment": "Construction",
            "city": "Karaganda",
            "tags": "crm,new",
            "status": "prospect",
            "notes": "Просили демо по воронке продаж.",
        },
        {
            "name": "Nomad Edu",
            "company": "Nomad Education Hub",
            "email": "director@nomadedu.kz",
            "phone": "+7 705 600 55 66",
            "segment": "Education",
            "city": "Kyzylorda",
            "tags": "chat,pdf",
            "status": "active",
            "notes": "Сильный интерес к внутреннему чату с клиентами.",
        },
        {
            "name": "Orken Foods",
            "company": "Orken Foods",
            "email": "commercial@orkenfoods.kz",
            "phone": "+7 707 320 44 18",
            "segment": "FMCG",
            "city": "Aktobe",
            "tags": "invoice,analytics",
            "status": "vip",
            "notes": "Важен контроль маржинальности и согласований.",
        },
    ]

    client_ids: dict[str, int] = {}
    for index, client in enumerate(client_seed):
        created_at = datetime.combine(
            current - timedelta(days=45 - index * 4),
            datetime.min.time(),
        ).isoformat(sep=" ")
        client_id = execute(
            """
            INSERT INTO clients (name, company, email, phone, segment, city, tags, status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client["name"],
                client["company"],
                client["email"],
                client["phone"],
                client["segment"],
                client["city"],
                client["tags"],
                client["status"],
                client["notes"],
                created_at,
            ),
        )
        client_ids[client["name"]] = client_id

    sales_seed = [
        ("Alem Retail", "Sales Pulse", "Analytics", 12, 85000, "won", current - timedelta(days=82), "Referral", "Aruzhan", "Almaty", "Запуск сети магазинов."),
        ("Alem Retail", "Sign Flow", "Documents", 18, 45000, "won", current - timedelta(days=38), "Upsell", "Aruzhan", "Almaty", "Расширение документооборота."),
        ("Steppe Logistics", "Client Hub", "CRM", 8, 78000, "negotiation", current - timedelta(days=12), "Inbound", "Nursultan", "Astana", "Финальный этап согласования бюджета."),
        ("Samal Medical", "PDF Vault", "Documents", 10, 62000, "won", current - timedelta(days=26), "Referral", "Aliya", "Shymkent", "Требуются акты и счета в PDF."),
        ("Qala Build", "Pipeline Board", "Sales", 7, 54000, "proposal", current - timedelta(days=5), "Event", "Madi", "Karaganda", "Ожидают коммерческое предложение."),
        ("Nomad Edu", "Dialogue Chat", "Communication", 14, 33000, "won", current - timedelta(days=15), "Inbound", "Aliya", "Kyzylorda", "Внутренний чат для клиентов и кураторов."),
        ("Orken Foods", "Forecast Lens", "Analytics", 20, 91000, "won", current - timedelta(days=58), "Outbound", "Dana", "Aktobe", "Аналитика повторных продаж."),
        ("Orken Foods", "Approval Flow", "Documents", 12, 47000, "new", current - timedelta(days=3), "Upsell", "Dana", "Aktobe", "Запросили автоматизацию согласования."),
        ("Samal Medical", "Client Hub", "CRM", 5, 78000, "won", current - timedelta(days=71), "Referral", "Aliya", "Shymkent", "Новый поток заявок с портала."),
    ]

    for item in sales_seed:
        execute(
            """
            INSERT INTO sales (
                client_id, product_name, category, quantity, unit_price, status,
                sale_date, source, owner, region, notes, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_ids[item[0]],
                item[1],
                item[2],
                item[3],
                item[4],
                item[5],
                item[6].isoformat(),
                item[7],
                item[8],
                item[9],
                item[10],
                datetime.combine(item[6], datetime.min.time()).isoformat(sep=" "),
            ),
        )

    document_seed = [
        (
            "Alem Retail",
            "Договор на модуль Sign Flow",
            "contract",
            "sent",
            810000,
            current + timedelta(days=7),
            "Предмет договора: внедрение модуля Sign Flow, настройка маршрутов согласования, обучение команды и сопровождение запуска.",
        ),
        (
            "Steppe Logistics",
            "Коммерческое предложение для Steppe Logistics",
            "proposal",
            "draft",
            624000,
            current + timedelta(days=10),
            "Предлагаем внедрение Client Hub и Sales Pulse для филиалов в Астане, Алматы и Шымкенте с единым кабинетом аналитики.",
        ),
        (
            "Samal Medical",
            "Счет на пакет PDF Vault",
            "invoice",
            "approved",
            620000,
            current - timedelta(days=1),
            "Счет включает генерацию PDF, архив документов, шаблоны актов и централизованный реестр подписания.",
        ),
        (
            "Nomad Edu",
            "Акт выполненных работ",
            "act",
            "approved",
            462000,
            current - timedelta(days=5),
            "Подтверждение запуска корпоративного чата, загрузки базы клиентов и активации панели аналитики.",
        ),
        (
            "Orken Foods",
            "Договор на пакет Forecast Lens",
            "contract",
            "sent",
            1820000,
            current + timedelta(days=4),
            "Подписание договора на внедрение аналитики продаж, прогноза выручки и мониторинга согласований по документам.",
        ),
    ]

    for client_name, title, doc_type, status, amount, due_date, content in document_seed:
        timestamp = utc_now()
        document_id = execute(
            """
            INSERT INTO documents (
                client_id, title, type, status, amount, due_date, content,
                approval_token, pdf_path, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_ids[client_name],
                title,
                doc_type,
                status,
                amount,
                due_date.isoformat(),
                content,
                secrets.token_urlsafe(18),
                "",
                timestamp,
                timestamp,
            ),
        )
        generate_pdf(document_id)

    task_seed = [
        ("Подготовить повторную презентацию по воронке продаж", "С фокусом на региональную аналитику.", current + timedelta(days=2), "high", "open", "Madi", client_ids["Qala Build"]),
        ("Проверить статус подписания договора", "Созвон с юридическим отделом клиента.", current + timedelta(days=1), "high", "in_progress", "Dana", client_ids["Orken Foods"]),
        ("Обновить клиентскую базу из формы саморегистрации", "Проверить новые контакты и источники.", current + timedelta(days=4), "medium", "open", "Aruzhan", None),
        ("Сформировать пакет ежемесячных PDF-отчетов", "Отправка в Samal Medical и Nomad Edu.", current + timedelta(days=5), "medium", "open", "Aliya", client_ids["Samal Medical"]),
        ("Подготовить upsell-предложение", "Доп. модуль документооборота для Alem Retail.", current + timedelta(days=8), "low", "open", "Aruzhan", client_ids["Alem Retail"]),
    ]

    for title, description, due_date, priority, status, owner, client_id in task_seed:
        execute(
            """
            INSERT INTO tasks (client_id, title, description, due_date, priority, status, owner, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                title,
                description,
                due_date.isoformat(),
                priority,
                status,
                owner,
                utc_now(),
            ),
        )

    chat_seed = [
        ("general", client_ids["Alem Retail"], "Менеджер проекта", "manager", "Коллеги, обновили панель аналитики и добавили прогноз продаж."),
        ("general", client_ids["Alem Retail"], "Alem Retail", "client", "Отлично, отдельно нужна сводка по магазинам Алматы."),
        ("contracts", client_ids["Orken Foods"], "Юрист", "manager", "Договор загружен в PDF, отправляю на согласование сегодня."),
        ("contracts", client_ids["Orken Foods"], "Orken Foods", "client", "Просьба добавить приложение по SLA."),
        ("support", client_ids["Nomad Edu"], "Куратор", "manager", "Чат для клиентов активирован, можно вести переписку в одной комнате."),
        ("support", client_ids["Nomad Edu"], "Nomad Edu", "client", "Спасибо, удобно видеть все обращения в одном окне."),
        ("general", client_ids["Steppe Logistics"], "Менеджер продаж", "manager", "Готовы обсудить внедрение по филиалам на этой неделе."),
        ("general", client_ids["Steppe Logistics"], "Steppe Logistics", "client", "Да, пришлите финальный PDF и письмо на согласование."),
    ]

    for room_name, client_id, sender_name, sender_role, message in chat_seed:
        execute(
            """
            INSERT INTO chat_messages (room_name, client_id, sender_name, sender_role, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (room_name, client_id, sender_name, sender_role, message, utc_now()),
        )

    email_seed = [
        {
            "title": "Счет на пакет PDF Vault",
            "recipient": "procurement@samalmed.kz",
            "subject": "Счет PDF Vault",
            "status": "approved",
            "body": "Счет и PDF были отправлены, клиент подтвердил оплату.",
        },
        {
            "title": "Договор на пакет Forecast Lens",
            "recipient": "commercial@orkenfoods.kz",
            "subject": "Договор Forecast Lens",
            "status": "sent",
            "body": "Письмо отправлено, ожидается согласование по ссылке.",
        },
    ]

    document_lookup = {
        item["title"]: item
        for item in query_all("SELECT id, client_id, title FROM documents")
    }
    for item in email_seed:
        document = document_lookup[item["title"]]
        execute(
            """
            INSERT INTO email_logs (document_id, client_id, recipient, subject, status, body, preview_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document["id"],
                document["client_id"],
                item["recipient"],
                item["subject"],
                item["status"],
                item["body"],
                "",
                utc_now(),
            ),
        )


def seed_demo_users() -> None:
    existing = query_one("SELECT COUNT(*) AS count FROM users")
    if existing and existing["count"]:
        return

    client_lookup = {
        row["name"]: row["id"]
        for row in query_all("SELECT id, name FROM clients ORDER BY id")
    }
    demo_users = [
        {
            "username": "director",
            "display_name": "Дамир Жандосов",
            "email": "director@sales-hub.local",
            "role": "admin",
            "client_id": None,
        },
        {
            "username": "manager",
            "display_name": "Аружан Сейткалиева",
            "email": "manager@sales-hub.local",
            "role": "manager",
            "client_id": None,
        },
        {
            "username": "analyst",
            "display_name": "Тимур Байкенов",
            "email": "analyst@sales-hub.local",
            "role": "analyst",
            "client_id": None,
        },
        {
            "username": "client",
            "display_name": "Alem Retail",
            "email": "client@sales-hub.local",
            "role": "client",
            "client_id": client_lookup.get("Alem Retail"),
        },
    ]

    for item in demo_users:
        execute(
            """
            INSERT INTO users (username, display_name, email, password_hash, role, client_id, created_at, last_login_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["username"],
                item["display_name"],
                item["email"],
                generate_password_hash("demo12345"),
                item["role"],
                item["client_id"],
                utc_now(),
                None,
            ),
        )


def build_public_url(endpoint: str, **values: Any) -> str:
    public_base = clean_text(os.getenv("PUBLIC_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL")).rstrip("/")
    relative = url_for(endpoint, **values)
    if public_base:
        return f"{public_base}{relative}"
    return url_for(endpoint, _external=True, **values)


def fetch_document(document_id: int) -> dict[str, Any] | None:
    return query_one(
        """
        SELECT
            d.*,
            c.name AS client_name,
            c.company AS client_company,
            c.email AS client_email,
            c.phone AS client_phone,
            c.city AS client_city
        FROM documents d
        JOIN clients c ON c.id = d.client_id
        WHERE d.id = ?
        """,
        (document_id,),
    )


def fetch_sale(sale_id: int) -> dict[str, Any] | None:
    return query_one("SELECT * FROM sales WHERE id = ?", (sale_id,))


def fetch_task(task_id: int) -> dict[str, Any] | None:
    return query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))


def draw_wrapped_text(pdf: canvas.Canvas, text: str, x: int, y: int, width: int = 88, line_height: int = 16) -> int:
    cursor_y = y
    for paragraph in text.splitlines() or [""]:
        wrapped = textwrap.wrap(paragraph, width=width) or [""]
        for line in wrapped:
            pdf.drawString(x, cursor_y, line)
            cursor_y -= line_height
    return cursor_y


def generate_pdf(document_id: int) -> Path:
    ensure_storage()
    document = fetch_document(document_id)
    if not document:
        raise ValueError(f"Document {document_id} not found")

    file_name = f"document_{document_id}_{slugify(document['title'])}.pdf"
    pdf_path = PDF_DIR / file_name

    doc = canvas.Canvas(str(pdf_path), pagesize=A4)
    width, height = A4

    doc.setFillColor(HexColor("#0d5c63"))
    doc.rect(0, height - 130, width, 130, fill=1, stroke=0)
    doc.setFillColor(HexColor("#ffffff"))
    doc.setFont("Helvetica-Bold", 24)
    doc.drawString(42, height - 70, "Sales Signature Workspace")
    doc.setFont("Helvetica", 12)
    doc.drawString(42, height - 95, f"Документ: {document['title']}")
    doc.drawString(42, height - 112, f"Статус: {document['status']} | Тип: {document['type']}")

    doc.setFillColor(HexColor("#102a43"))
    doc.setFont("Helvetica-Bold", 13)
    doc.drawString(42, height - 165, "Данные клиента")
    doc.setFont("Helvetica", 11)
    client_info = [
        f"Клиент: {document['client_name']}",
        f"Компания: {document['client_company']}",
        f"Email: {document['client_email']}",
        f"Город: {document['client_city']}",
        f"Сумма: {document['amount']:,.0f} KZT".replace(",", " "),
        f"Срок: {document['due_date'] or 'не указан'}",
    ]
    pointer = height - 188
    for line in client_info:
        doc.drawString(42, pointer, line)
        pointer -= 17

    doc.setStrokeColor(HexColor("#d9e2ec"))
    doc.line(42, pointer - 8, width - 42, pointer - 8)

    doc.setFont("Helvetica-Bold", 13)
    doc.drawString(42, pointer - 32, "Содержание")
    doc.setFont("Helvetica", 11)
    content_y = draw_wrapped_text(doc, document["content"], 42, pointer - 54, width=90)

    doc.setFont("Helvetica-Bold", 13)
    doc.drawString(42, content_y - 18, "Подписание / согласование")
    doc.setFont("Helvetica", 11)
    signing_lines = [
        "1. Документ сформирован автоматически на основе данных CRM и продаж.",
        "2. Для согласования документа можно использовать ссылку из email-письма.",
        "3. После подтверждения статус обновляется в панели аналитики.",
    ]
    draw_wrapped_text(doc, "\n".join(signing_lines), 42, content_y - 40, width=90)

    doc.setStrokeColor(HexColor("#0d5c63"))
    doc.line(42, 118, 250, 118)
    doc.line(330, 118, width - 42, 118)
    doc.setFillColor(HexColor("#486581"))
    doc.setFont("Helvetica", 10)
    doc.drawString(42, 102, "Подпись исполнителя")
    doc.drawString(330, 102, "Подпись клиента")

    doc.setFillColor(HexColor("#7b8794"))
    doc.drawString(42, 72, f"Сгенерировано: {utc_now()} UTC")
    doc.save()

    execute(
        "UPDATE documents SET pdf_path = ?, updated_at = ? WHERE id = ?",
        (str(pdf_path), utc_now(), document_id),
    )
    return pdf_path


def bootstrap_app() -> None:
    ensure_storage()
    init_db()
    seed_demo_data()
    seed_demo_users()


def last_month_keys(count: int = 6) -> list[str]:
    keys: list[str] = []
    cursor = date.today().replace(day=1)
    for _ in range(count):
        keys.append(cursor.strftime("%Y-%m"))
        previous = cursor - timedelta(days=1)
        cursor = previous.replace(day=1)
    return list(reversed(keys))


def format_month_label(key: str) -> str:
    year, month = key.split("-")
    month_names = {
        "01": "Янв",
        "02": "Фев",
        "03": "Мар",
        "04": "Апр",
        "05": "Май",
        "06": "Июн",
        "07": "Июл",
        "08": "Авг",
        "09": "Сен",
        "10": "Окт",
        "11": "Ноя",
        "12": "Дек",
    }
    return f"{month_names.get(month, month)} {year[-2:]}"


def percent_delta(current_value: float, previous_value: float) -> float:
    if not previous_value:
        return 100.0 if current_value else 0.0
    return round((current_value - previous_value) / previous_value * 100, 1)


def build_stage_board(sales: list[dict[str, Any]]) -> list[dict[str, Any]]:
    columns: list[dict[str, Any]] = []
    status_order = ["new", "proposal", "negotiation", "won"]
    for status in status_order:
        items = [sale for sale in sales if sale["status"] == status]
        columns.append(
            {
                "status": status,
                "label": label_for_status(status),
                "count": len(items),
                "total": sum_money(items),
                "items": [
                    {
                        "id": sale["id"],
                        "title": sale["product_name"],
                        "client_name": sale["client_name"],
                        "owner": sale["owner"],
                        "total": sale["total"],
                        "region": sale["region"],
                    }
                    for sale in items[:4]
                ],
            }
        )
    return columns


def build_due_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    today = date.today()
    upcoming: list[dict[str, Any]] = []
    for item in documents:
        if item["status"] == "approved" or not item.get("due_date"):
            continue
        try:
            due = date.fromisoformat(item["due_date"])
        except ValueError:
            continue
        days_left = (due - today).days
        urgency = "high" if days_left <= 2 else "medium" if days_left <= 7 else "low"
        upcoming.append(
            {
                "id": item["id"],
                "title": item["title"],
                "client_name": item["client_name"],
                "due_date": item["due_date"],
                "days_left": days_left,
                "status": item["status"],
                "urgency": urgency,
            }
        )
    return sorted(upcoming, key=lambda item: (item["days_left"], item["title"]))[:6]


def build_team_load(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for task in tasks:
        owner = clean_text(task.get("owner")) or "Без ответственного"
        if owner not in grouped:
            grouped[owner] = {
                "owner": owner,
                "open_count": 0,
                "in_progress_count": 0,
                "high_priority": 0,
            }
        if task["status"] == "open":
            grouped[owner]["open_count"] += 1
        if task["status"] == "in_progress":
            grouped[owner]["in_progress_count"] += 1
        if task["priority"] == "high" and task["status"] != "done":
            grouped[owner]["high_priority"] += 1
    return sorted(
        grouped.values(),
        key=lambda item: (-item["high_priority"], -item["in_progress_count"], item["owner"]),
    )


def build_client_health(
    clients: list[dict[str, Any]],
    sales: list[dict[str, Any]],
    documents: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    today = date.today()
    health_cards: list[dict[str, Any]] = []
    for client in clients:
        client_sales = [sale for sale in sales if sale["client_id"] == client["id"]]
        client_documents = [doc for doc in documents if doc["client_id"] == client["id"]]
        client_tasks = [task for task in tasks if task.get("client_id") == client["id"]]
        open_tasks = [task for task in client_tasks if task["status"] != "done"]
        overdue_tasks: list[dict[str, Any]] = []
        for task in open_tasks:
            if not task.get("due_date"):
                continue
            try:
                if date.fromisoformat(task["due_date"]) < today:
                    overdue_tasks.append(task)
            except ValueError:
                continue
        waiting_documents = [doc for doc in client_documents if doc["status"] != "approved"]

        score = 55
        revenue = float(client.get("revenue") or 0)
        if revenue >= 1_500_000:
            score += 22
        elif revenue > 0:
            score += 12
        else:
            score -= 8

        if client["status"] in {"active", "vip"}:
            score += 12
        elif client["status"] == "onboarding":
            score += 5

        score -= len(waiting_documents) * 4
        score -= len(overdue_tasks) * 8
        score = max(8, min(100, score))

        if score >= 80:
            tone = "excellent"
            next_step = "Подходит для upsell или расширения пакета."
        elif score >= 60:
            tone = "stable"
            next_step = "Нужен регулярный контакт и контроль задач."
        else:
            tone = "risk"
            next_step = "Требуется внимание: закрыть задачи и ускорить документы."

        health_cards.append(
            {
                "client_id": client["id"],
                "name": client["name"],
                "score": score,
                "tone": tone,
                "revenue": revenue,
                "waiting_documents": len(waiting_documents),
                "open_tasks": len(open_tasks),
                "next_step": next_step,
            }
        )

    return sorted(health_cards, key=lambda item: (-item["score"], -item["revenue"]))[:6]


def build_insights(
    summary: dict[str, Any],
    sales: list[dict[str, Any]],
    documents: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    month_keys: list[str],
    month_map: dict[str, float],
) -> list[dict[str, Any]]:
    insights: list[dict[str, Any]] = []
    current_key = month_keys[-1]
    previous_key = month_keys[-2] if len(month_keys) > 1 else month_keys[-1]
    growth = percent_delta(month_map.get(current_key, 0), month_map.get(previous_key, 0))
    pipeline_share = round((summary["pipeline"] / summary["revenue"] * 100), 1) if summary["revenue"] else 0
    hottest_sale = max(
        (sale for sale in sales if sale["status"] != "won"),
        key=lambda item: float(item["total"]),
        default=None,
    )
    urgent_task = next((task for task in tasks if task["status"] != "done"), None)
    waiting_document = next((document for document in documents if document["status"] != "approved"), None)

    insights.append(
        {
            "title": "Темп выручки",
            "value": f"{growth:+.1f}%",
            "description": "Сравнение текущего месяца с предыдущим по закрытым продажам.",
            "tone": "positive" if growth >= 0 else "attention",
        }
    )
    insights.append(
        {
            "title": "Потенциал pipeline",
            "value": f"{pipeline_share}%",
            "description": "Доля активного pipeline относительно уже закрытой выручки.",
            "tone": "calm" if pipeline_share < 50 else "positive",
        }
    )
    if hottest_sale:
        insights.append(
            {
                "title": "Главная сделка",
                "value": hottest_sale["client_name"],
                "description": (
                    f"{hottest_sale['product_name']} на {int(hottest_sale['total']):,} KZT"
                    .replace(",", " ")
                ),
                "tone": "attention",
            }
        )
    if urgent_task:
        insights.append(
            {
                "title": "Фокус дня",
                "value": urgent_task["title"],
                "description": f"Ответственный: {urgent_task.get('owner') or 'не назначен'}.",
                "tone": "attention",
            }
        )
    if waiting_document:
        insights.append(
            {
                "title": "Документ на контроле",
                "value": waiting_document["title"],
                "description": f"Клиент: {waiting_document['client_name']}.",
                "tone": "calm",
            }
        )

    return insights[:5]


def sort_iso_desc(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda item: clean_text(item.get(key)), reverse=True)


def sale_value(row: dict[str, Any]) -> float:
    return round(float(row.get("total") or (clean_int(row.get("quantity"), 0) * clean_float(row.get("unit_price"), 0))), 1)


def build_summary_from_lists(
    clients: list[dict[str, Any]],
    sales: list[dict[str, Any]],
    documents: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[str], dict[str, float]]:
    won_sales = [item for item in sales if item["status"] == "won"]
    pipeline_sales = [item for item in sales if item["status"] in {"new", "proposal", "negotiation"}]
    revenue = round(sum(sale_value(item) for item in won_sales), 1)
    pipeline = round(sum(sale_value(item) for item in pipeline_sales), 1)
    active_clients = len([item for item in clients if item["status"] in CLIENT_STATUSES])
    approved_documents = len([item for item in documents if item["status"] == "approved"])
    documents_waiting = len([item for item in documents if item["status"] in {"draft", "sent"}])
    conversion = round(len(won_sales) / len(sales) * 100, 1) if sales else 0
    avg_deal = round(revenue / len(won_sales), 1) if won_sales else 0

    urgent_cutoff = date.today() + timedelta(days=7)
    urgent_tasks = 0
    for task in tasks:
        if task["status"] == "done" or not task.get("due_date"):
            continue
        try:
            if date.fromisoformat(task["due_date"]) <= urgent_cutoff:
                urgent_tasks += 1
        except ValueError:
            continue

    month_keys = last_month_keys()
    month_map: dict[str, float] = {key: 0 for key in month_keys}
    for sale in won_sales:
        month_key = clean_text(sale.get("sale_date"))[:7]
        if month_key in month_map:
            month_map[month_key] += sale_value(sale)

    forecast = round(revenue + pipeline * 0.35, 1)
    summary = {
        "revenue": revenue,
        "pipeline": pipeline,
        "active_clients": active_clients,
        "approval_rate": round(approved_documents / len(documents) * 100, 1) if documents else 0,
        "avg_deal": avg_deal,
        "urgent_tasks": urgent_tasks,
        "conversion": conversion,
        "documents_waiting": documents_waiting,
        "forecast": forecast,
        "approved_documents": approved_documents,
    }
    return summary, month_keys, month_map


def aggregate_top_pairs(rows: list[dict[str, Any]], key: str, value_key: str, limit: int = 5) -> list[dict[str, Any]]:
    grouped: dict[str, float] = {}
    for row in rows:
        label = clean_text(row.get(key)) or "Без названия"
        grouped[label] = grouped.get(label, 0) + float(row.get(value_key) or 0)
    return [
        {"label": label, "value": round(total, 1)}
        for label, total in sorted(grouped.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def build_notifications(
    user: dict[str, Any],
    due_documents: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    sales: list[dict[str, Any]],
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    personal_name = clean_text(user.get("display_name"))

    for item in due_documents[:3]:
        items.append(
            {
                "tone": "urgent" if item["days_left"] <= 2 else "focus",
                "title": "Срок документа близко",
                "description": f"{item['title']} · {item['client_name']}",
                "meta": f"Осталось: {max(item['days_left'], 0)} дн." if item["days_left"] >= 0 else "Есть просрочка",
                "panel": "documents",
            }
        )

    personal_tasks = [
        task
        for task in tasks
        if task["status"] != "done"
        and (
            user["role"] == "client"
            or not personal_name
            or clean_text(task.get("owner")) == personal_name
            or task["priority"] == "high"
        )
    ]
    for task in personal_tasks[:2]:
        items.append(
            {
                "tone": "focus" if task["priority"] == "high" else "calm",
                "title": "Задача требует внимания",
                "description": task["title"],
                "meta": f"{label_for_status(task['priority'])} · {task.get('due_date') or 'Без срока'}",
                "panel": "communications",
            }
        )

    hot_sales = [sale for sale in sales if sale["status"] == "negotiation"]
    hot_sales = sorted(hot_sales, key=sale_value, reverse=True)
    if hot_sales and user["role"] != "client":
        top_sale = hot_sales[0]
        items.append(
            {
                "tone": "positive",
                "title": "Сделка близка к закрытию",
                "description": f"{top_sale['product_name']} · {top_sale['client_name']}",
                "meta": f"{int(sale_value(top_sale)):,} KZT".replace(",", " "),
                "panel": "sales",
            }
        )

    if messages:
        latest = sort_iso_desc(messages, "created_at")[0]
        items.append(
            {
                "tone": "calm",
                "title": "Последний сигнал из чата",
                "description": f"{latest['sender_name']} · {label_for_status(latest['room_name'])}",
                "meta": clean_text(latest.get("client_name")) or "Системная комната",
                "panel": "communications",
            }
        )

    tone_rank = {"urgent": 0, "focus": 1, "positive": 2, "calm": 3}
    return sorted(items, key=lambda item: (tone_rank.get(item["tone"], 4), item["title"]))[:6]


def build_spotlight(
    user: dict[str, Any],
    summary: dict[str, Any],
    notifications: list[dict[str, Any]],
    tasks: list[dict[str, Any]],
    documents: list[dict[str, Any]],
    sales: list[dict[str, Any]],
) -> dict[str, Any]:
    completed_today = 0
    recent_cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat(sep=" ")
    for row in documents:
        if row["status"] == "approved" and clean_text(row.get("updated_at")) >= recent_cutoff:
            completed_today += 1
    for row in sales:
        if row["status"] == "won" and clean_text(row.get("created_at")) >= recent_cutoff:
            completed_today += 1
    for row in tasks:
        if row["status"] == "done" and clean_text(row.get("created_at")) >= recent_cutoff:
            completed_today += 1

    momentum = min(
        98,
        max(
            24,
            54
        + min(summary["conversion"], 30)
        + min(summary["approval_rate"], 20)
        - min(summary["urgent_tasks"] * 3, 18)
        + min(completed_today * 4, 18),
        ),
    )
    streak = max(1, min(9, completed_today or 1))

    if user["role"] == "admin":
        headline = "Главная панель руководителя"
        mission = "Смотрите на деньги, узкие места и скорость команды в одном ритме."
    elif user["role"] == "manager":
        headline = "Сегодня есть шанс ускорить закрытие"
        mission = "Сфокусируйтесь на переговорах, документах и задачах с самым коротким дедлайном."
    elif user["role"] == "analyst":
        headline = "Данные готовы к сильному разбору"
        mission = "Проверьте отклонения, регионы роста и клиентов с риском просадки."
    else:
        headline = "Ваш личный кабинет клиента"
        mission = "Здесь видно ваши документы, чат с командой и ближайшие шаги по проекту."

    top_notice = notifications[0]["title"] if notifications else "Система работает стабильно"
    badge = "На волне" if momentum >= 80 else "Под контролем" if momentum >= 65 else "Нужен фокус"
    return {
        "headline": headline,
        "mission": mission,
        "badge": badge,
        "score": momentum,
        "streak": streak,
        "focus": top_notice,
        "name": user["display_name"],
        "role_label": label_for_role(user["role"]),
    }


def build_quick_actions(user: dict[str, Any], permissions: dict[str, Any]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    if permissions["can_manage_clients"]:
        actions.append(
            {
                "title": "Новый клиент",
                "description": "Быстро добавить контакт в базу.",
                "panel": "clients",
                "focus": "#clientForm [name='name']",
            }
        )
        actions.append(
            {
                "title": "Новая сделка",
                "description": "Открыть форму сделки и сразу перейти к сумме.",
                "panel": "sales",
                "focus": "#saleForm [name='product_name']",
            }
        )
    if permissions["can_manage_documents"]:
        actions.append(
            {
                "title": "Создать PDF",
                "description": "Сформировать документ и отправить на согласование.",
                "panel": "documents",
                "focus": "#documentForm [name='title']",
            }
        )
    actions.append(
        {
            "title": "Открыть чат",
            "description": "Перейти в коммуникации и написать клиенту.",
            "panel": "communications",
            "focus": "#chatForm [name='message']",
        }
    )
    return actions[:4]


def build_dashboard_payload(user: dict[str, Any]) -> dict[str, Any]:
    permissions = build_permissions(user)
    scope_client = client_scope_id(user)

    clients = query_all(
        """
        SELECT
            c.*,
            COALESCE(SUM(CASE WHEN s.status = 'won' THEN s.quantity * s.unit_price ELSE 0 END), 0) AS revenue
        FROM clients c
        LEFT JOIN sales s ON s.client_id = c.id
        GROUP BY c.id
        ORDER BY c.created_at DESC
        """
    )
    sales = query_all(
        """
        SELECT
            s.*,
            c.name AS client_name,
            (s.quantity * s.unit_price) AS total
        FROM sales s
        JOIN clients c ON c.id = s.client_id
        ORDER BY s.sale_date DESC, s.id DESC
        """
    )
    docs = query_all(
        """
        SELECT
            d.*,
            c.name AS client_name,
            c.email AS client_email
        FROM documents d
        JOIN clients c ON c.id = d.client_id
        ORDER BY d.updated_at DESC, d.id DESC
        """
    )
    tasks = query_all(
        """
        SELECT
            t.*,
            c.name AS client_name
        FROM tasks t
        LEFT JOIN clients c ON c.id = t.client_id
        ORDER BY
            CASE t.priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
            COALESCE(t.due_date, '9999-12-31') ASC,
            t.id DESC
        """
    )
    email_logs = query_all(
        """
        SELECT
            e.*,
            d.title AS document_title
        FROM email_logs e
        LEFT JOIN documents d ON d.id = e.document_id
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT 12
        """
    )
    messages = query_all(
        """
        SELECT
            m.*,
            c.name AS client_name
        FROM chat_messages m
        LEFT JOIN clients c ON c.id = m.client_id
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT 60
        """
    )

    if scope_client:
        clients = [item for item in clients if item["id"] == scope_client]
        sales = [item for item in sales if clean_int(item.get("client_id")) == scope_client]
        docs = [item for item in docs if clean_int(item.get("client_id")) == scope_client]
        tasks = [item for item in tasks if clean_int(item.get("client_id")) == scope_client]
        email_logs = [item for item in email_logs if clean_int(item.get("client_id")) == scope_client]
        messages = [item for item in messages if clean_int(item.get("client_id")) == scope_client]

    summary, month_keys, month_map = build_summary_from_lists(clients, sales, docs, tasks)
    won_sales = [item for item in sales if item["status"] == "won"]
    revenue_by_product = aggregate_top_pairs(
        [{"product_name": item["product_name"], "total": sale_value(item)} for item in won_sales],
        "product_name",
        "total",
    )
    region_mix = aggregate_top_pairs(
        [{"region": clean_text(item.get("region")) or "Без региона", "total": sale_value(item)} for item in won_sales],
        "region",
        "total",
    )

    top_client_rows: list[dict[str, Any]] = []
    for client in clients:
        top_client_rows.append({"name": client["name"], "revenue": float(client.get("revenue") or 0)})
    top_client_rows = sorted(top_client_rows, key=lambda item: (-item["revenue"], item["name"]))[:5]

    sales_funnel = [
        {"status": status, "count": len([item for item in sales if item["status"] == status])}
        for status in ["new", "proposal", "negotiation", "won"]
    ]
    document_status = [
        {"status": status, "count": len([item for item in docs if item["status"] == status])}
        for status in ["draft", "sent", "approved"]
    ]

    due_documents = build_due_documents(docs)
    team_load = build_team_load(tasks)
    client_health = build_client_health(clients, sales, docs, tasks)
    stage_board = build_stage_board(sales)
    insights = build_insights(summary, sales, docs, tasks, month_keys, month_map)

    activity: list[dict[str, Any]] = []
    for item in docs:
        activity.append({"type": "document", "label": item["title"], "meta": item["status"], "moment": item["updated_at"]})
    for item in sales:
        activity.append({"type": "sale", "label": item["product_name"], "meta": item["status"], "moment": item["created_at"]})
    for item in messages:
        activity.append({"type": "chat", "label": item["sender_name"], "meta": item["room_name"], "moment": item["created_at"]})
    activity = sort_iso_desc(activity, "moment")[:10]

    notifications = build_notifications(user, due_documents, tasks, sales, messages)
    spotlight = build_spotlight(user, summary, notifications, tasks, docs, sales)
    quick_actions = build_quick_actions(user, permissions)

    return {
        "summary": summary,
        "charts": {
            "monthly_revenue": {
                "labels": [format_month_label(item) for item in month_keys],
                "values": [month_map.get(item, 0) for item in month_keys],
            },
            "revenue_by_product": {
                "labels": [row["label"] for row in revenue_by_product],
                "values": [row["value"] for row in revenue_by_product],
            },
            "sales_funnel": {
                "labels": [row["status"] for row in sales_funnel],
                "values": [row["count"] for row in sales_funnel],
            },
            "document_status": {
                "labels": [row["status"] for row in document_status],
                "values": [row["count"] for row in document_status],
            },
            "region_mix": {
                "labels": [row["label"] for row in region_mix],
                "values": [row["value"] for row in region_mix],
            },
            "top_clients": {
                "labels": [row["name"] for row in top_client_rows],
                "values": [row["revenue"] for row in top_client_rows],
            },
        },
        "session": {
            "user": user,
            "permissions": permissions,
            "notification_count": len(notifications),
        },
        "clients": clients,
        "sales": sales,
        "documents": docs,
        "tasks": tasks,
        "email_logs": email_logs[:8],
        "activity": activity,
        "rooms": [
            {"id": room_id, "label": ROOM_LABELS[room_id]}
            for room_id in ["general", "contracts", "support"]
        ],
        "stage_board": stage_board,
        "due_documents": due_documents,
        "team_load": team_load,
        "client_health": client_health,
        "insights": insights,
        "document_templates": DOCUMENT_TEMPLATES,
        "notifications": notifications,
        "spotlight": spotlight,
        "quick_actions": quick_actions,
    }


def smtp_config() -> dict[str, Any]:
    port_raw = clean_text(os.getenv("SMTP_PORT", "587"))
    return {
        "host": clean_text(os.getenv("SMTP_HOST")),
        "port": int(port_raw) if port_raw.isdigit() else 587,
        "user": clean_text(os.getenv("SMTP_USER")),
        "password": clean_text(os.getenv("SMTP_PASSWORD")),
        "sender": clean_text(os.getenv("SMTP_SENDER")) or clean_text(os.getenv("SMTP_USER")),
        "use_tls": bool_env("SMTP_USE_TLS", True),
    }


def smtp_is_configured(config: dict[str, Any]) -> bool:
    return all([config["host"], config["sender"]])


def send_document_email(document_id: int, recipient: str, subject: str, body: str) -> dict[str, Any]:
    document = fetch_document(document_id)
    if not document:
        raise ValueError("Документ не найден.")

    pdf_path = Path(document["pdf_path"]) if document["pdf_path"] else generate_pdf(document_id)
    approval_url = build_public_url("approve_document", token=document["approval_token"])
    message_body = (
        f"{body}\n\n"
        f"Ссылка для согласования: {approval_url}\n"
        f"Документ в PDF: {build_public_url('download_document_pdf', document_id=document_id)}"
    )
    config = smtp_config()

    delivery_status = "demo-preview"
    preview_text = message_body

    if smtp_is_configured(config):
        email_message = EmailMessage()
        email_message["Subject"] = subject
        email_message["From"] = config["sender"]
        email_message["To"] = recipient
        email_message.set_content(message_body)
        with open(pdf_path, "rb") as file_handle:
            email_message.add_attachment(
                file_handle.read(),
                maintype="application",
                subtype="pdf",
                filename=pdf_path.name,
            )

        with smtplib.SMTP(config["host"], config["port"], timeout=20) as server:
            if config["use_tls"]:
                server.starttls()
            if config["user"] and config["password"]:
                server.login(config["user"], config["password"])
            server.send_message(email_message)
        delivery_status = "sent"
        preview_text = ""

    execute(
        """
        INSERT INTO email_logs (document_id, client_id, recipient, subject, status, body, preview_text, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            document_id,
            document["client_id"],
            recipient,
            subject,
            delivery_status,
            body,
            preview_text,
            utc_now(),
        ),
    )

    if document["status"] == "draft":
        execute(
            "UPDATE documents SET status = ?, updated_at = ? WHERE id = ?",
            ("sent", utc_now(), document_id),
        )

    return {
        "delivery_status": delivery_status,
        "approval_url": approval_url,
        "message_body": message_body,
    }


def search_everywhere(term: str, scope: str, scope_client_id: int | None = None) -> list[dict[str, Any]]:
    like = f"%{term.lower()}%"
    results: list[dict[str, Any]] = []

    if scope in {"all", "clients"}:
        sql = """
            SELECT id, name, company, email, city, status
            FROM clients
            WHERE (
                lower(name) LIKE ? OR lower(company) LIKE ? OR lower(email) LIKE ? OR lower(city) LIKE ?
                OR lower(tags) LIKE ? OR lower(notes) LIKE ?
            )
            ORDER BY created_at DESC
            LIMIT 8
        """
        params: tuple[Any, ...] = (like, like, like, like, like, like)
        if scope_client_id:
            sql = sql.replace("ORDER BY created_at DESC", "AND id = ? ORDER BY created_at DESC")
            params += (scope_client_id,)
        rows = query_all(sql, params)
        results.extend(
            {
                "type": "client",
                "title": row["name"],
                "subtitle": f"{row['company']} | {row['email']}",
                "meta": f"{row['city']} | {row['status']}",
                "id": row["id"],
            }
            for row in rows
        )

    if scope in {"all", "sales"}:
        sql = """
            SELECT s.id, s.product_name, s.status, c.name AS client_name, (s.quantity * s.unit_price) AS total
            FROM sales s
            JOIN clients c ON c.id = s.client_id
            WHERE (
                lower(s.product_name) LIKE ? OR lower(s.category) LIKE ? OR lower(c.name) LIKE ?
                OR lower(s.owner) LIKE ? OR lower(s.region) LIKE ? OR lower(s.notes) LIKE ?
            )
            ORDER BY s.sale_date DESC
            LIMIT 8
        """
        params = (like, like, like, like, like, like)
        if scope_client_id:
            sql = sql.replace("ORDER BY s.sale_date DESC", "AND s.client_id = ? ORDER BY s.sale_date DESC")
            params += (scope_client_id,)
        rows = query_all(sql, params)
        results.extend(
            {
                "type": "sale",
                "title": row["product_name"],
                "subtitle": row["client_name"],
                "meta": f"{row['status']} | {row['total']:,.0f} KZT".replace(",", " "),
                "id": row["id"],
            }
            for row in rows
        )

    if scope in {"all", "documents"}:
        sql = """
            SELECT d.id, d.title, d.status, c.name AS client_name, d.type
            FROM documents d
            JOIN clients c ON c.id = d.client_id
            WHERE (
                lower(d.title) LIKE ? OR lower(d.type) LIKE ? OR lower(c.name) LIKE ?
                OR lower(d.content) LIKE ?
            )
            ORDER BY d.updated_at DESC
            LIMIT 8
        """
        params = (like, like, like, like)
        if scope_client_id:
            sql = sql.replace("ORDER BY d.updated_at DESC", "AND d.client_id = ? ORDER BY d.updated_at DESC")
            params += (scope_client_id,)
        rows = query_all(sql, params)
        results.extend(
            {
                "type": "document",
                "title": row["title"],
                "subtitle": row["client_name"],
                "meta": f"{row['type']} | {row['status']}",
                "id": row["id"],
            }
            for row in rows
        )

    if scope in {"all", "tasks"}:
        sql = """
            SELECT t.id, t.title, t.status, t.priority, c.name AS client_name
            FROM tasks t
            LEFT JOIN clients c ON c.id = t.client_id
            WHERE (
                lower(t.title) LIKE ? OR lower(t.description) LIKE ? OR lower(t.owner) LIKE ?
                OR lower(COALESCE(c.name, '')) LIKE ?
            )
            ORDER BY t.id DESC
            LIMIT 8
        """
        params = (like, like, like, like)
        if scope_client_id:
            sql = sql.replace("ORDER BY t.id DESC", "AND t.client_id = ? ORDER BY t.id DESC")
            params += (scope_client_id,)
        rows = query_all(sql, params)
        results.extend(
            {
                "type": "task",
                "title": row["title"],
                "subtitle": row["client_name"] or "Без клиента",
                "meta": f"{label_for_status(row['priority'])} | {label_for_status(row['status'])}",
                "id": row["id"],
            }
            for row in rows
        )

    return results


def export_rows_as_csv(filename: str, rows: list[dict[str, Any]]) -> Response:
    output = StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    csv_data = "\ufeff" + output.getvalue()
    return Response(
        csv_data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login() -> str | Response:
    if request.method == "GET" and current_user():
        return redirect(safe_next_url(request.values.get("next")))

    error = ""
    next_url = safe_next_url(request.values.get("next"))
    if request.method == "POST":
        identity = clean_text(request.form.get("username"))
        password = clean_text(request.form.get("password"))
        user = fetch_login_user(identity)

        if not user or not check_password_hash(user["password_hash"], password):
            error = "Неверный логин или пароль."
        else:
            session.clear()
            session["user_id"] = user["id"]
            execute("UPDATE users SET last_login_at = ? WHERE id = ?", (utc_now(), user["id"]))
            return redirect(next_url)

    return render_template("login.html", error=error, next_url=next_url, demo_users=demo_login_profiles())


@app.post("/logout")
@login_required
def logout() -> Response:
    session.clear()
    return redirect(url_for("login"))


@app.get("/workspace")
@login_required
def workspace() -> str:
    return render_template("workspace.html")


@app.get("/health")
def health() -> Response:
    return jsonify({"status": "ok", "date": today_iso()})


@app.get("/api/dashboard")
@login_required
def dashboard() -> Response:
    return jsonify(build_dashboard_payload(current_user()))


@app.get("/api/clients")
@login_required
def get_clients() -> Response:
    return jsonify({"items": build_dashboard_payload(current_user())["clients"]})


@app.post("/api/clients")
@roles_required(*MANAGE_ROLES)
def create_client() -> Response:
    payload = request.get_json(force=True)
    name = clean_text(payload.get("name"))
    email = clean_text(payload.get("email"))
    if not name or not email:
        return jsonify({"message": "Имя и email обязательны."}), 400
    if not validate_email(email):
        return jsonify({"message": "Укажите корректный email."}), 400

    client_id = execute(
        """
        INSERT INTO clients (name, company, email, phone, segment, city, tags, status, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            clean_text(payload.get("company")) or name,
            email,
            clean_text(payload.get("phone")),
            clean_text(payload.get("segment")),
            clean_text(payload.get("city")),
            clean_text(payload.get("tags")),
            normalize_choice(payload.get("status"), CLIENT_STATUSES, "prospect"),
            clean_text(payload.get("notes")),
            utc_now(),
        ),
    )
    return jsonify({"message": "Клиент добавлен.", "client": query_one("SELECT * FROM clients WHERE id = ?", (client_id,))})


@app.post("/api/sales")
@roles_required(*MANAGE_ROLES)
def create_sale() -> Response:
    payload = request.get_json(force=True)
    client_id = clean_int(payload.get("client_id"))
    product_name = clean_text(payload.get("product_name"))
    category = clean_text(payload.get("category"))
    if not client_id or not product_name or not category:
        return jsonify({"message": "Заполните клиента, продукт и категорию."}), 400

    quantity = max(clean_int(payload.get("quantity"), 1), 1)
    unit_price = clean_float(payload.get("unit_price"), 0)
    if unit_price <= 0:
        return jsonify({"message": "Цена должна быть больше нуля."}), 400
    sale_date = clean_text(payload.get("sale_date")) or today_iso()
    sale_id = execute(
        """
        INSERT INTO sales (
            client_id, product_name, category, quantity, unit_price, status,
            sale_date, source, owner, region, notes, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_id,
            product_name,
            category,
            quantity,
            unit_price,
            normalize_choice(payload.get("status"), SALE_STATUSES, "new"),
            sale_date,
            clean_text(payload.get("source")),
            clean_text(payload.get("owner")),
            clean_text(payload.get("region")),
            clean_text(payload.get("notes")),
            utc_now(),
        ),
    )
    return jsonify({"message": "Сделка сохранена.", "sale": query_one("SELECT * FROM sales WHERE id = ?", (sale_id,))})


@app.patch("/api/sales/<int:sale_id>")
@roles_required(*MANAGE_ROLES)
def update_sale(sale_id: int) -> Response:
    sale = fetch_sale(sale_id)
    if not sale:
        return jsonify({"message": "Сделка не найдена."}), 404

    payload = request.get_json(force=True)
    status = normalize_choice(payload.get("status"), SALE_STATUSES, sale["status"])
    execute("UPDATE sales SET status = ? WHERE id = ?", (status, sale_id))
    return jsonify({"message": "Статус сделки обновлен.", "sale": fetch_sale(sale_id)})


@app.post("/api/documents")
@roles_required(*MANAGE_ROLES)
def create_document() -> Response:
    payload = request.get_json(force=True)
    client_id = clean_int(payload.get("client_id"))
    title = clean_text(payload.get("title"))
    doc_type = clean_text(payload.get("type"))
    content = clean_text(payload.get("content"))
    if not client_id or not title or not doc_type or not content:
        return jsonify({"message": "Заполните клиента, название, тип и содержание."}), 400
    if doc_type not in {"contract", "proposal", "invoice", "act"}:
        return jsonify({"message": "Неизвестный тип документа."}), 400

    timestamp = utc_now()
    document_id = execute(
        """
        INSERT INTO documents (
            client_id, title, type, status, amount, due_date, content,
            approval_token, pdf_path, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            client_id,
            title,
            doc_type,
            "draft",
            max(clean_float(payload.get("amount"), 0), 0),
            clean_text(payload.get("due_date")),
            content,
            secrets.token_urlsafe(18),
            "",
            timestamp,
            timestamp,
        ),
    )
    generate_pdf(document_id)
    return jsonify({"message": "Документ создан и сохранен в PDF.", "document": fetch_document(document_id)})


@app.get("/api/documents/<int:document_id>/pdf")
@login_required
def download_document_pdf(document_id: int) -> Response:
    document = fetch_document(document_id)
    if not document:
        return jsonify({"message": "Документ не найден."}), 404
    if not document_access_allowed(document, current_user()):
        return jsonify({"message": "Документ недоступен для вашего профиля."}), 403
    pdf_path = Path(document["pdf_path"]) if document["pdf_path"] else generate_pdf(document_id)
    if not pdf_path.exists():
        pdf_path = generate_pdf(document_id)
    return send_file(pdf_path, as_attachment=True, download_name=pdf_path.name)


@app.post("/api/documents/<int:document_id>/email")
@roles_required(*MANAGE_ROLES)
def email_document(document_id: int) -> Response:
    payload = request.get_json(force=True)
    document = fetch_document(document_id)
    if not document:
        return jsonify({"message": "Документ не найден."}), 404

    recipient = clean_text(payload.get("recipient")) or clean_text(document.get("client_email"))
    subject = clean_text(payload.get("subject")) or f"Согласование документа: {document['title']}"
    body = clean_text(payload.get("body")) or (
        f"Здравствуйте.\n\n"
        f"Направляем документ «{document['title']}» для согласования.\n"
        f"После просмотра вы можете подтвердить документ по ссылке ниже."
    )
    if not recipient:
        return jsonify({"message": "Укажите email получателя."}), 400
    if not validate_email(recipient):
        return jsonify({"message": "Email получателя указан неверно."}), 400

    try:
        result = send_document_email(document_id, recipient, subject, body)
    except Exception as error:  # noqa: BLE001
        execute(
            """
            INSERT INTO email_logs (document_id, client_id, recipient, subject, status, body, preview_text, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                document["client_id"],
                recipient,
                subject,
                "error",
                body,
                str(error),
                utc_now(),
            ),
        )
        return jsonify({"message": f"Не удалось отправить письмо: {error}"}), 500

    return jsonify(
        {
            "message": "Письмо подготовлено." if result["delivery_status"] == "demo-preview" else "Письмо отправлено.",
            "delivery_status": result["delivery_status"],
            "approval_url": result["approval_url"],
            "preview": result["message_body"] if result["delivery_status"] == "demo-preview" else "",
        }
    )


@app.patch("/api/documents/<int:document_id>")
@roles_required(*MANAGE_ROLES)
def update_document(document_id: int) -> Response:
    document = fetch_document(document_id)
    if not document:
        return jsonify({"message": "Документ не найден."}), 404

    payload = request.get_json(force=True)
    status = normalize_choice(payload.get("status"), DOCUMENT_STATUSES, document["status"])
    execute(
        "UPDATE documents SET status = ?, updated_at = ? WHERE id = ?",
        (status, utc_now(), document_id),
    )
    if status == "approved":
        generate_pdf(document_id)
    return jsonify({"message": "Статус документа обновлен.", "document": fetch_document(document_id)})


@app.route("/approve/<token>", methods=["GET", "POST"])
def approve_document(token: str) -> str:
    document = query_one(
        """
        SELECT d.*, c.name AS client_name, c.company AS client_company
        FROM documents d
        JOIN clients c ON c.id = d.client_id
        WHERE d.approval_token = ?
        """,
        (token,),
    )
    if not document:
        return render_template("approve.html", not_found=True)

    if request.method == "POST" and document["status"] != "approved":
        execute(
            "UPDATE documents SET status = ?, updated_at = ? WHERE approval_token = ?",
            ("approved", utc_now(), token),
        )
        refreshed = query_one("SELECT * FROM documents WHERE approval_token = ?", (token,))
        if refreshed:
            generate_pdf(refreshed["id"])
        document = query_one(
            """
            SELECT d.*, c.name AS client_name, c.company AS client_company
            FROM documents d
            JOIN clients c ON c.id = d.client_id
            WHERE d.approval_token = ?
            """,
            (token,),
        )

    return render_template("approve.html", document=document, not_found=False)


@app.get("/api/chat/messages")
@login_required
def get_chat_messages() -> Response:
    room = clean_text(request.args.get("room")) or "general"
    rows = query_all(
        """
        SELECT
            m.*,
            c.name AS client_name
        FROM chat_messages m
        LEFT JOIN clients c ON c.id = m.client_id
        WHERE m.room_name = ?
        ORDER BY m.id ASC
        LIMIT 50
        """,
        (room,),
    )
    scope_id = client_scope_id(current_user())
    if scope_id:
        rows = [row for row in rows if clean_int(row.get("client_id")) == scope_id]
    return jsonify({"room": room, "items": rows})


@app.post("/api/chat/messages")
@login_required
def post_chat_message() -> Response:
    payload = request.get_json(force=True)
    message = clean_text(payload.get("message"))
    user = current_user()
    sender_name = clean_text(payload.get("sender_name")) or clean_text(user.get("display_name"))
    if not message or not sender_name:
        return jsonify({"message": "Введите имя отправителя и текст сообщения."}), 400

    scoped_client_id = client_scope_id(user)
    client_id = clean_int(payload.get("client_id")) or None
    if scoped_client_id:
        client_id = scoped_client_id

    execute(
        """
        INSERT INTO chat_messages (room_name, client_id, sender_name, sender_role, message, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            clean_text(payload.get("room_name")) or "general",
            client_id,
            sender_name,
            clean_text(user.get("role")) or clean_text(payload.get("sender_role")) or "manager",
            message,
            utc_now(),
        ),
    )
    return jsonify({"message": "Сообщение отправлено."})


@app.post("/api/tasks")
@roles_required(*MANAGE_ROLES)
def create_task() -> Response:
    payload = request.get_json(force=True)
    title = clean_text(payload.get("title"))
    if not title:
        return jsonify({"message": "Название задачи обязательно."}), 400

    task_id = execute(
        """
        INSERT INTO tasks (client_id, title, description, due_date, priority, status, owner, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            clean_int(payload.get("client_id")) or None,
            title,
            clean_text(payload.get("description")),
            clean_text(payload.get("due_date")),
            normalize_choice(payload.get("priority"), PRIORITIES, "medium"),
            normalize_choice(payload.get("status"), TASK_STATUSES, "open"),
            clean_text(payload.get("owner")),
            utc_now(),
        ),
    )
    return jsonify({"message": "Задача добавлена.", "task": query_one("SELECT * FROM tasks WHERE id = ?", (task_id,))})


@app.patch("/api/tasks/<int:task_id>")
@roles_required(*MANAGE_ROLES)
def update_task(task_id: int) -> Response:
    task = fetch_task(task_id)
    if not task:
        return jsonify({"message": "Задача не найдена."}), 404

    payload = request.get_json(force=True)
    status = normalize_choice(payload.get("status"), TASK_STATUSES, task["status"])
    execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
    return jsonify({"message": "Статус задачи обновлен.", "task": fetch_task(task_id)})


@app.get("/api/search")
@login_required
def search() -> Response:
    term = clean_text(request.args.get("q"))
    scope = clean_text(request.args.get("scope")) or "all"
    if not term:
        return jsonify({"items": []})
    return jsonify({"items": search_everywhere(term, scope, client_scope_id(current_user()))})


@app.get("/api/export/<dataset>.csv")
@roles_required(*EXPORT_ROLES)
def export_dataset(dataset: str) -> Response:
    if dataset == "clients":
        rows = query_all(
            "SELECT id, name, company, email, phone, segment, city, tags, status, notes, created_at FROM clients ORDER BY id"
        )
        return export_rows_as_csv("clients_export.csv", rows)
    if dataset == "sales":
        rows = query_all(
            """
            SELECT
                s.id,
                c.name AS client_name,
                s.product_name,
                s.category,
                s.quantity,
                s.unit_price,
                (s.quantity * s.unit_price) AS total,
                s.status,
                s.sale_date,
                s.source,
                s.owner,
                s.region
            FROM sales s
            JOIN clients c ON c.id = s.client_id
            ORDER BY s.id
            """
        )
        return export_rows_as_csv("sales_export.csv", rows)
    if dataset == "documents":
        rows = query_all(
            """
            SELECT
                d.id,
                c.name AS client_name,
                d.title,
                d.type,
                d.status,
                d.amount,
                d.due_date,
                d.created_at,
                d.updated_at
            FROM documents d
            JOIN clients c ON c.id = d.client_id
            ORDER BY d.id
            """
        )
        return export_rows_as_csv("documents_export.csv", rows)
    if dataset == "tasks":
        rows = query_all(
            """
            SELECT
                t.id,
                COALESCE(c.name, '') AS client_name,
                t.title,
                t.description,
                t.priority,
                t.status,
                t.owner,
                t.due_date,
                t.created_at
            FROM tasks t
            LEFT JOIN clients c ON c.id = t.client_id
            ORDER BY t.id
            """
        )
        return export_rows_as_csv("tasks_export.csv", rows)
    return jsonify({"message": "Неизвестный набор данных."}), 404


with app.app_context():
    bootstrap_app()


if __name__ == "__main__":
    debug_mode = os.getenv("APP_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    app.run(
        host="127.0.0.1",
        port=int(os.getenv("PORT", "5000")),
        debug=debug_mode,
        use_reloader=debug_mode,
    )
