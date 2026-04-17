# Telegram XTR Payment & Referral Bot

Telegram bot that manages access to two channels (free + premium) using Telegram Stars (XTR) payments and a referral system.

## Features

- **XTR Stars Payment**: One-time payment for lifetime premium channel access
- **Referral System**: Earn free channel access by referring 3 users
- **Admin Panel**: In-bot admin menu with leaderboard, payment history, revenue stats, member management, and grant/revoke access
- **Anti-abuse**: Unique referrals only, self-referral prevention, duplicate protection

## Setup

### 1. Prerequisites
- Python 3.9+
- PostgreSQL database
- Telegram Bot Token (from @BotFather)
- Two private Telegram channels (free + premium) with bot added as admin

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env with your values
```

### 4. Run
```bash
python bot.py
```

## Configuration (.env)

| Variable | Description |
|---|---|
| BOT_TOKEN | Telegram bot token from @BotFather |
| ADMIN_USER_ID | Your Telegram user ID (admin) |
| FREE_CHANNEL_ID | Free channel ID (e.g., -100xxxx) |
| PREMIUM_CHANNEL_ID | Premium channel ID (e.g., -100xxxx) |
| STARS_PRICE | Price in Stars for premium (default: 500) |
| DATABASE_URL | PostgreSQL connection string |
| REFERRALS_NEEDED | Referrals needed for free access (default: 3) |

## Bot Commands

- `/start` — Main menu (also handles referral deep links)
- `/cancel` — Cancel current admin action

## Admin Features

- Top Referrers Leaderboard
- Payment History
- Revenue & Statistics
- Lifetime Members List
- Grant/Revoke Premium or Free Access
