import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime


def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            referred_by BIGINT REFERENCES users(user_id),
            referral_count INTEGER DEFAULT 0,
            is_lifetime_member BOOLEAN DEFAULT FALSE,
            has_free_access BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(user_id),
            telegram_payment_charge_id TEXT UNIQUE,
            provider_payment_charge_id TEXT,
            amount INTEGER NOT NULL,
            currency TEXT DEFAULT 'XTR',
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT REFERENCES users(user_id),
            referred_id BIGINT REFERENCES users(user_id) UNIQUE,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);
        CREATE INDEX IF NOT EXISTS idx_payments_user ON payments(user_id);
    """)
    conn.commit()
    cur.close()
    conn.close()


def get_or_create_user(user_id, username=None, first_name=None, last_name=None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
    user = cur.fetchone()
    if not user:
        cur.execute(
            """INSERT INTO users (user_id, username, first_name, last_name)
               VALUES (%s, %s, %s, %s) RETURNING *""",
            (user_id, username, first_name, last_name),
        )
        user = cur.fetchone()
        conn.commit()
    cur.close()
    conn.close()
    return user


def register_referral(referrer_id, referred_id):
    """Register a referral. Returns True if newly registered, False if duplicate."""
    if referrer_id == referred_id:
        return False
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Check if referred user was already referred by someone
        cur.execute("SELECT id FROM referrals WHERE referred_id = %s", (referred_id,))
        if cur.fetchone():
            return False

        cur.execute(
            "INSERT INTO referrals (referrer_id, referred_id) VALUES (%s, %s)",
            (referrer_id, referred_id),
        )
        cur.execute(
            "UPDATE users SET referral_count = referral_count + 1, updated_at = NOW() WHERE user_id = %s",
            (referrer_id,),
        )
        conn.commit()

        # Check if referrer has reached the threshold
        cur.execute("SELECT referral_count FROM users WHERE user_id = %s", (referrer_id,))
        row = cur.fetchone()
        referrals_needed = int(os.getenv("REFERRALS_NEEDED", "3"))
        if row and row[0] >= referrals_needed:
            cur.execute(
                "UPDATE users SET has_free_access = TRUE, updated_at = NOW() WHERE user_id = %s",
                (referrer_id,),
            )
            conn.commit()
            cur.close()
            conn.close()
            return "threshold_reached"

        cur.close()
        conn.close()
        return True
    except psycopg2.IntegrityError:
        conn.rollback()
        cur.close()
        conn.close()
        return False


def record_payment(user_id, telegram_charge_id, provider_charge_id, amount):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO payments (user_id, telegram_payment_charge_id, provider_payment_charge_id, amount)
           VALUES (%s, %s, %s, %s)""",
        (user_id, telegram_charge_id, provider_charge_id, amount),
    )
    cur.execute(
        "UPDATE users SET is_lifetime_member = TRUE, updated_at = NOW() WHERE user_id = %s",
        (user_id,),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_user(user_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    return user


def get_top_referrers(limit=20):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT user_id, username, first_name, referral_count
           FROM users WHERE referral_count > 0
           ORDER BY referral_count DESC LIMIT %s""",
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_payment_history(limit=50):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT p.*, u.username, u.first_name
           FROM payments p JOIN users u ON p.user_id = u.user_id
           ORDER BY p.created_at DESC LIMIT %s""",
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_total_revenue():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM payments")
    total, count = cur.fetchone()
    cur.close()
    conn.close()
    return total, count


def get_lifetime_members(limit=50):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT user_id, username, first_name, created_at
           FROM users WHERE is_lifetime_member = TRUE
           ORDER BY updated_at DESC LIMIT %s""",
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_all_lifetime_member_count():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE is_lifetime_member = TRUE")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def grant_access(user_id, access_type):
    """access_type: 'premium' or 'free'"""
    conn = get_connection()
    cur = conn.cursor()
    if access_type == "premium":
        cur.execute(
            "UPDATE users SET is_lifetime_member = TRUE, updated_at = NOW() WHERE user_id = %s",
            (user_id,),
        )
    else:
        cur.execute(
            "UPDATE users SET has_free_access = TRUE, updated_at = NOW() WHERE user_id = %s",
            (user_id,),
        )
    conn.commit()
    affected = cur.rowcount
    cur.close()
    conn.close()
    return affected > 0


def revoke_access(user_id, access_type):
    """access_type: 'premium' or 'free'"""
    conn = get_connection()
    cur = conn.cursor()
    if access_type == "premium":
        cur.execute(
            "UPDATE users SET is_lifetime_member = FALSE, updated_at = NOW() WHERE user_id = %s",
            (user_id,),
        )
    else:
        cur.execute(
            "UPDATE users SET has_free_access = FALSE, updated_at = NOW() WHERE user_id = %s",
            (user_id,),
        )
    conn.commit()
    affected = cur.rowcount
    cur.close()
    conn.close()
    return affected > 0


def get_stats():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE is_lifetime_member = TRUE")
    premium_members = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE has_free_access = TRUE")
    free_members = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(amount), 0), COUNT(*) FROM payments")
    total_revenue, total_payments = cur.fetchone()
    cur.execute("SELECT COALESCE(SUM(referral_count), 0) FROM users")
    total_referrals = cur.fetchone()[0]
    cur.close()
    conn.close()
    return {
        "total_users": total_users,
        "premium_members": premium_members,
        "free_members": free_members,
        "total_revenue": total_revenue,
        "total_payments": total_payments,
        "total_referrals": total_referrals,
    }
