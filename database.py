import sqlite3
import os
from datetime import datetime

DB_PATH = os.getenv("SQLITE_DB_PATH", "bot_data.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            referred_by INTEGER REFERENCES users(user_id),
            referral_count INTEGER DEFAULT 0,
            is_lifetime_member INTEGER DEFAULT 0,
            has_free_access INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(user_id),
            telegram_payment_charge_id TEXT UNIQUE,
            provider_payment_charge_id TEXT,
            amount INTEGER NOT NULL,
            currency TEXT DEFAULT 'XTR',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER REFERENCES users(user_id),
            referred_id INTEGER UNIQUE REFERENCES users(user_id),
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);
        CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);
    """)
    conn.commit()
    conn.close()


def _row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def get_or_create_user(user_id, username=None, first_name=None, last_name=None):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cur.fetchone()
    if not user:
        cur.execute(
            "INSERT INTO users (user_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
            (user_id, username, first_name, last_name),
        )
        conn.commit()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = cur.fetchone()
    conn.close()
    return _row_to_dict(user)


def register_referral(referrer_id, referred_id):
    """Register a referral. Returns True if new, 'threshold_reached' if referrer earned access, False if duplicate."""
    if referrer_id == referred_id:
        return False
    conn = get_connection()
    cur = conn.cursor()

    # Check if referred user was already referred by someone
    cur.execute("SELECT id FROM referrals WHERE referred_id = ?", (referred_id,))
    if cur.fetchone():
        conn.close()
        return False

    try:
        cur.execute(
            "INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
            (referrer_id, referred_id),
        )
        cur.execute(
            "UPDATE users SET referral_count = referral_count + 1, updated_at = datetime('now') WHERE user_id = ?",
            (referrer_id,),
        )
        conn.commit()

        # Check if referrer has reached the threshold
        cur.execute("SELECT referral_count FROM users WHERE user_id = ?", (referrer_id,))
        row = cur.fetchone()
        referrals_needed = int(os.getenv("REFERRALS_NEEDED", "3"))
        if row and row["referral_count"] >= referrals_needed:
            cur.execute(
                "UPDATE users SET has_free_access = 1, updated_at = datetime('now') WHERE user_id = ?",
                (referrer_id,),
            )
            conn.commit()
            conn.close()
            return "threshold_reached"

        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        conn.close()
        return False


def record_payment(user_id, telegram_charge_id, provider_charge_id, amount):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO payments (user_id, telegram_payment_charge_id, provider_payment_charge_id, amount) VALUES (?, ?, ?, ?)",
        (user_id, telegram_charge_id, provider_charge_id, amount),
    )
    cur.execute(
        "UPDATE users SET is_lifetime_member = 1, updated_at = datetime('now') WHERE user_id = ?",
        (user_id,),
    )
    conn.commit()
    conn.close()


def get_user(user_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cur.fetchone()
    conn.close()
    return _row_to_dict(user)


def get_top_referrers(limit=20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, username, first_name, referral_count FROM users WHERE referral_count > 0 ORDER BY referral_count DESC LIMIT ?",
        (limit,),
    )
    rows = [_row_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_payment_history(limit=50):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT p.*, u.username, u.first_name
           FROM payments p JOIN users u ON p.user_id = u.user_id
           ORDER BY p.created_at DESC LIMIT ?""",
        (limit,),
    )
    rows = [_row_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_total_revenue():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM payments")
    row = cur.fetchone()
    conn.close()
    return row[0], row[1]


def get_lifetime_members(limit=50):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, username, first_name, created_at FROM users WHERE is_lifetime_member = 1 ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    )
    rows = [_row_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_all_lifetime_member_count():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE is_lifetime_member = 1")
    count = cur.fetchone()[0]
    conn.close()
    return count


def grant_access(user_id, access_type):
    conn = get_connection()
    cur = conn.cursor()
    if access_type == "premium":
        cur.execute(
            "UPDATE users SET is_lifetime_member = 1, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
    else:
        cur.execute(
            "UPDATE users SET has_free_access = 1, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
    conn.commit()
    affected = cur.rowcount
    conn.close()
    return affected > 0


def revoke_access(user_id, access_type):
    conn = get_connection()
    cur = conn.cursor()
    if access_type == "premium":
        cur.execute(
            "UPDATE users SET is_lifetime_member = 0, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
    else:
        cur.execute(
            "UPDATE users SET has_free_access = 0, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
    conn.commit()
    affected = cur.rowcount
    conn.close()
    return affected > 0


def get_stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE is_lifetime_member = 1")
    premium_members = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE has_free_access = 1")
    free_members = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM payments")
    row = cur.fetchone()
    total_revenue, total_payments = row[0], row[1]
    cur.execute("SELECT COALESCE(SUM(referral_count), 0) FROM users")
    total_referrals = cur.fetchone()[0]
    conn.close()
    return {
        "total_users": total_users,
        "premium_members": premium_members,
        "free_members": free_members,
        "total_revenue": total_revenue,
        "total_payments": total_payments,
        "total_referrals": total_referrals,
    }
