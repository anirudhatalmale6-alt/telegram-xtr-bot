import os
import logging
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import database as db

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
FREE_CHANNEL_ID = int(os.getenv("FREE_CHANNEL_ID", "0"))
PREMIUM_CHANNEL_ID = int(os.getenv("PREMIUM_CHANNEL_ID", "0"))


def get_stars_price():
    return int(db.get_setting("stars_price", "500"))


def get_referrals_needed():
    return int(db.get_setting("referrals_needed", "3"))


# ─── Helpers ───────────────────────────────────────────────────────────────────

def display_name(user_data):
    """Get display name from a user dict or similar."""
    if isinstance(user_data, dict):
        return user_data.get("first_name") or user_data.get("username") or str(user_data.get("user_id", "Unknown"))
    return str(user_data)


async def create_invite_link(bot, channel_id, user_id):
    """Create a single-use invite link for a channel."""
    link = await bot.create_chat_invite_link(
        chat_id=channel_id,
        member_limit=1,
        name=f"user_{user_id}",
    )
    return link.invite_link


def is_admin(user_id):
    return user_id == ADMIN_USER_ID


# ─── /start ───────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = db.get_or_create_user(user.id, user.username, user.first_name, user.last_name)

    # Handle referral deep link: /start ref_12345
    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0].replace("ref_", ""))
            result = db.register_referral(referrer_id, user.id)
            if result == "threshold_reached":
                # Notify the referrer they earned free access
                try:
                    free_link = await create_invite_link(context.bot, FREE_CHANNEL_ID, referrer_id)
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=(
                            f"🎉 Congratulations! You've reached {get_referrals_needed()} referrals!\n\n"
                            f"You've earned FREE access to our channel.\n"
                            f"Here's your invite link (single use):\n{free_link}"
                        ),
                    )
                except Exception as e:
                    logger.error(f"Failed to notify referrer {referrer_id}: {e}")
            elif result is True:
                # Notify referrer of new referral
                try:
                    referrer = db.get_user(referrer_id)
                    remaining = get_referrals_needed() - referrer["referral_count"]
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=(
                            f"👤 New referral! {user.first_name} joined using your link.\n"
                            f"You now have {referrer['referral_count']}/{get_referrals_needed()} referrals."
                            + (f"\n{remaining} more to go!" if remaining > 0 else "")
                        ),
                    )
                except Exception as e:
                    logger.error(f"Failed to notify referrer {referrer_id}: {e}")
        except (ValueError, IndexError):
            pass

    # Build main menu
    keyboard = [
        [InlineKeyboardButton("⭐ Buy Premium Access", callback_data="buy_premium")],
        [InlineKeyboardButton("👥 My Referral Link", callback_data="my_referral")],
        [InlineKeyboardButton("📊 My Status", callback_data="my_status")],
    ]
    if is_admin(user.id):
        keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_text = (
        f"Welcome, {user.first_name}! 👋\n\n"
        f"This bot manages access to our channels:\n\n"
        f"📢 Free Channel — Earn access by referring {get_referrals_needed()} friends\n"
        f"⭐ Premium Channel — One-time purchase of {get_stars_price()} Stars for lifetime access\n\n"
        f"Choose an option below:"
    )
    await update.message.reply_text(welcome_text, reply_markup=reply_markup)


# ─── Callback Query Router ───────────────────────────────────────────────────

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "buy_premium":
        await handle_buy_premium(query, context)
    elif data == "my_referral":
        await handle_my_referral(query, context)
    elif data == "my_status":
        await handle_my_status(query, context)
    elif data == "admin_menu":
        await handle_admin_menu(query, context)
    elif data == "admin_leaderboard":
        await handle_admin_leaderboard(query, context)
    elif data == "admin_payments":
        await handle_admin_payments(query, context)
    elif data == "admin_revenue":
        await handle_admin_revenue(query, context)
    elif data == "admin_members":
        await handle_admin_members(query, context)
    elif data == "admin_stats":
        await handle_admin_stats(query, context)
    elif data == "admin_setprice":
        await handle_admin_setprice_start(query, context)
    elif data == "admin_setreferrals":
        await handle_admin_setreferrals_start(query, context)
    elif data.startswith("admin_grant_"):
        await handle_admin_grant_start(query, context)
    elif data.startswith("admin_revoke_"):
        await handle_admin_revoke_start(query, context)
    elif data == "back_main":
        await handle_back_main(query, context)
    elif data == "back_admin":
        await handle_admin_menu(query, context)


# ─── Buy Premium ──────────────────────────────────────────────────────────────

async def handle_buy_premium(query, context):
    user_id = query.from_user.id
    user = db.get_user(user_id)

    if user and user["is_lifetime_member"]:
        await query.edit_message_text(
            "✅ You already have lifetime premium access!\n\n"
            "If you need a new invite link, please contact the admin.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("« Back", callback_data="back_main")]]
            ),
        )
        return

    # Update the current message to show payment is being processed
    await query.edit_message_text(
        f"⭐ Sending payment invoice for {get_stars_price()} Stars...\n\n"
        f"Please check below for the payment button.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back", callback_data="back_main")]]
        ),
    )

    # Send Stars invoice as a new message
    try:
        await context.bot.send_invoice(
            chat_id=query.message.chat_id,
            title="Premium Channel - Lifetime Access",
            description=(
                f"One-time payment of {get_stars_price()} Stars for lifetime access "
                f"to the premium ad-free channel."
            ),
            payload=f"premium_access_{user_id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice("Lifetime Premium Access", get_stars_price())],
        )
    except Exception as e:
        logger.error(f"Failed to send invoice to {user_id}: {e}")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=f"❌ Failed to send invoice: {e}\n\nPlease try again later.",
        )


# ─── Payment Handlers ─────────────────────────────────────────────────────────

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    # Always approve — Telegram Stars handles the actual payment
    await query.answer(ok=True)


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment = update.message.successful_payment
    user = update.effective_user

    # Record in database
    db.record_payment(
        user_id=user.id,
        telegram_charge_id=payment.telegram_payment_charge_id,
        provider_charge_id=payment.provider_payment_charge_id,
        amount=payment.total_amount,
    )

    # Generate invite link
    try:
        invite_link = await create_invite_link(context.bot, PREMIUM_CHANNEL_ID, user.id)
        await update.message.reply_text(
            f"🎉 Payment successful! Thank you, {user.first_name}!\n\n"
            f"You are now a lifetime premium member.\n\n"
            f"Here's your invite link to the premium channel (single use):\n"
            f"{invite_link}\n\n"
            f"Welcome aboard! ⭐"
        )
    except Exception as e:
        logger.error(f"Failed to create invite link for {user.id}: {e}")
        await update.message.reply_text(
            f"🎉 Payment successful! You are now a lifetime member.\n\n"
            f"⚠️ There was an issue generating your invite link. "
            f"Please contact the admin and they will provide it manually."
        )

    # Notify admin
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                f"💰 New payment received!\n"
                f"User: {user.first_name} (@{user.username or 'N/A'})\n"
                f"User ID: {user.id}\n"
                f"Amount: {payment.total_amount} Stars"
            ),
        )
    except Exception:
        pass


# ─── My Referral Link ─────────────────────────────────────────────────────────

async def handle_my_referral(query, context):
    user_id = query.from_user.id
    bot_username = (await context.bot.get_me()).username
    referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    user = db.get_user(user_id)
    count = user["referral_count"] if user else 0
    remaining = max(0, get_referrals_needed() - count)

    text = (
        f"👥 Your Referral Link:\n\n"
        f"{referral_link}\n\n"
        f"Share this link with friends. When they start the bot, "
        f"you'll get credit for the referral.\n\n"
        f"📊 Your referrals: {count}/{get_referrals_needed()}\n"
    )
    if remaining > 0:
        text += f"You need {remaining} more to earn free channel access!"
    else:
        text += "✅ You've already earned free channel access!"

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back", callback_data="back_main")]]
        ),
    )


# ─── My Status ────────────────────────────────────────────────────────────────

async def handle_my_status(query, context):
    user_id = query.from_user.id
    user = db.get_user(user_id)

    if not user:
        await query.edit_message_text("User not found. Please /start again.")
        return

    premium_status = "✅ Lifetime Member" if user["is_lifetime_member"] else "❌ Not purchased"
    free_status = "✅ Access earned" if user["has_free_access"] else "❌ Not yet earned"
    referrals = user["referral_count"]

    text = (
        f"📊 Your Status\n\n"
        f"⭐ Premium Channel: {premium_status}\n"
        f"📢 Free Channel: {free_status}\n"
        f"👥 Referrals: {referrals}/{get_referrals_needed()}\n"
        f"📅 Member since: {str(user['created_at'])[:10]}"
    )

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back", callback_data="back_main")]]
        ),
    )


# ─── Back to Main ─────────────────────────────────────────────────────────────

async def handle_back_main(query, context):
    user = query.from_user
    keyboard = [
        [InlineKeyboardButton("⭐ Buy Premium Access", callback_data="buy_premium")],
        [InlineKeyboardButton("👥 My Referral Link", callback_data="my_referral")],
        [InlineKeyboardButton("📊 My Status", callback_data="my_status")],
    ]
    if is_admin(user.id):
        keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_menu")])

    await query.edit_message_text(
        f"Welcome back, {user.first_name}! Choose an option:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ─── Admin Menu ───────────────────────────────────────────────────────────────

async def handle_admin_menu(query, context):
    if not is_admin(query.from_user.id):
        await query.edit_message_text("⛔ Unauthorized.")
        return

    keyboard = [
        [InlineKeyboardButton("🏆 Top Referrers", callback_data="admin_leaderboard")],
        [InlineKeyboardButton("💰 Payment History", callback_data="admin_payments")],
        [InlineKeyboardButton("📈 Revenue & Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("👑 Lifetime Members", callback_data="admin_members")],
        [InlineKeyboardButton("✅ Grant Premium Access", callback_data="admin_grant_premium")],
        [InlineKeyboardButton("✅ Grant Free Access", callback_data="admin_grant_free")],
        [InlineKeyboardButton("❌ Revoke Premium Access", callback_data="admin_revoke_premium")],
        [InlineKeyboardButton("❌ Revoke Free Access", callback_data="admin_revoke_free")],
        [InlineKeyboardButton(f"💲 Set Price (current: {get_stars_price()})", callback_data="admin_setprice")],
        [InlineKeyboardButton(f"🔢 Set Referrals Needed (current: {get_referrals_needed()})", callback_data="admin_setreferrals")],
        [InlineKeyboardButton("« Back to Main", callback_data="back_main")],
    ]

    await query.edit_message_text(
        "🔧 Admin Panel\n\nSelect an option:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_admin_leaderboard(query, context):
    if not is_admin(query.from_user.id):
        return

    top = db.get_top_referrers(20)
    if not top:
        text = "🏆 Top Referrers\n\nNo referrals yet."
    else:
        lines = ["🏆 Top Referrers\n"]
        for i, r in enumerate(top, 1):
            name = r["first_name"] or r["username"] or str(r["user_id"])
            lines.append(f"{i}. {name} — {r['referral_count']} referrals")
        text = "\n".join(lines)

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back to Admin", callback_data="back_admin")]]
        ),
    )


async def handle_admin_payments(query, context):
    if not is_admin(query.from_user.id):
        return

    payments = db.get_payment_history(20)
    if not payments:
        text = "💰 Payment History\n\nNo payments yet."
    else:
        lines = ["💰 Recent Payments\n"]
        for p in payments:
            name = p["first_name"] or p["username"] or str(p["user_id"])
            date = str(p["created_at"])[:16]
            lines.append(f"• {name} — {p['amount']} Stars — {date}")
        text = "\n".join(lines)

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back to Admin", callback_data="back_admin")]]
        ),
    )


async def handle_admin_stats(query, context):
    if not is_admin(query.from_user.id):
        return

    stats = db.get_stats()
    text = (
        f"📈 Bot Statistics\n\n"
        f"👤 Total Users: {stats['total_users']}\n"
        f"👑 Premium Members: {stats['premium_members']}\n"
        f"📢 Free Access Members: {stats['free_members']}\n"
        f"💰 Total Revenue: {stats['total_revenue']} Stars\n"
        f"🧾 Total Payments: {stats['total_payments']}\n"
        f"👥 Total Referrals: {stats['total_referrals']}"
    )

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back to Admin", callback_data="back_admin")]]
        ),
    )


async def handle_admin_members(query, context):
    if not is_admin(query.from_user.id):
        return

    members = db.get_lifetime_members(20)
    total = db.get_all_lifetime_member_count()
    if not members:
        text = "👑 Lifetime Members\n\nNo lifetime members yet."
    else:
        lines = [f"👑 Lifetime Members ({total} total)\n"]
        for m in members:
            name = m["first_name"] or m["username"] or str(m["user_id"])
            date = str(m["created_at"])[:10]
            lines.append(f"• {name} (ID: {m['user_id']}) — since {date}")
        text = "\n".join(lines)

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Back to Admin", callback_data="back_admin")]]
        ),
    )


# ─── Admin Settings ───────────────────────────────────────────────────────────

async def handle_admin_setprice_start(query, context):
    if not is_admin(query.from_user.id):
        return
    context.user_data["admin_action"] = "setprice"
    await query.edit_message_text(
        f"💲 Set Stars Price\n\n"
        f"Current price: {get_stars_price()} Stars\n\n"
        f"Send the new price (a number, e.g. 300):",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Cancel", callback_data="back_admin")]]
        ),
    )


async def handle_admin_setreferrals_start(query, context):
    if not is_admin(query.from_user.id):
        return
    context.user_data["admin_action"] = "setreferrals"
    await query.edit_message_text(
        f"🔢 Set Referrals Needed\n\n"
        f"Current requirement: {get_referrals_needed()} referrals\n\n"
        f"Send the new number of referrals needed (e.g. 5):",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Cancel", callback_data="back_admin")]]
        ),
    )


# ─── Admin Grant/Revoke ───────────────────────────────────────────────────────

async def handle_admin_grant_start(query, context):
    if not is_admin(query.from_user.id):
        return

    access_type = query.data.replace("admin_grant_", "")
    context.user_data["admin_action"] = f"grant_{access_type}"

    await query.edit_message_text(
        f"✅ Grant {access_type.title()} Access\n\n"
        f"Please send the Telegram User ID (number) of the user you want to grant access to.\n\n"
        f"Send /cancel to cancel.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Cancel", callback_data="back_admin")]]
        ),
    )


async def handle_admin_revoke_start(query, context):
    if not is_admin(query.from_user.id):
        return

    access_type = query.data.replace("admin_revoke_", "")
    context.user_data["admin_action"] = f"revoke_{access_type}"

    await query.edit_message_text(
        f"❌ Revoke {access_type.title()} Access\n\n"
        f"Please send the Telegram User ID (number) of the user you want to revoke access from.\n\n"
        f"Send /cancel to cancel.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Cancel", callback_data="back_admin")]]
        ),
    )


async def handle_admin_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages from admin for grant/revoke user ID input."""
    if not is_admin(update.effective_user.id):
        return

    action = context.user_data.get("admin_action")
    if not action:
        return

    text = update.message.text.strip()

    # Handle settings changes
    if action == "setprice":
        try:
            new_price = int(text)
            if new_price < 1:
                await update.message.reply_text("Price must be at least 1 Star.")
                return
            db.set_setting("stars_price", new_price)
            await update.message.reply_text(f"✅ Stars price updated to {new_price} Stars.")
        except ValueError:
            await update.message.reply_text("Please send a valid number.")
            return
        context.user_data.pop("admin_action", None)
        return

    if action == "setreferrals":
        try:
            new_count = int(text)
            if new_count < 1:
                await update.message.reply_text("Referrals needed must be at least 1.")
                return
            db.set_setting("referrals_needed", new_count)
            await update.message.reply_text(f"✅ Referrals needed updated to {new_count}.")
        except ValueError:
            await update.message.reply_text("Please send a valid number.")
            return
        context.user_data.pop("admin_action", None)
        return

    try:
        target_user_id = int(text)
    except ValueError:
        await update.message.reply_text("Please send a valid numeric User ID.")
        return

    parts = action.split("_")
    operation = parts[0]  # grant or revoke
    access_type = parts[1]  # premium or free

    target_user = db.get_user(target_user_id)
    if not target_user:
        await update.message.reply_text(
            f"User ID {target_user_id} not found in database. "
            f"The user must have started the bot first."
        )
        context.user_data.pop("admin_action", None)
        return

    if operation == "grant":
        success = db.grant_access(target_user_id, access_type)
        if success:
            # Generate invite link
            channel_id = PREMIUM_CHANNEL_ID if access_type == "premium" else FREE_CHANNEL_ID
            try:
                invite_link = await create_invite_link(context.bot, channel_id, target_user_id)
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=(
                        f"🎉 You've been granted {access_type} channel access by the admin!\n\n"
                        f"Here's your invite link (single use):\n{invite_link}"
                    ),
                )
                await update.message.reply_text(
                    f"✅ Granted {access_type} access to user {target_user_id} "
                    f"({display_name(target_user)}). Invite link sent."
                )
            except Exception as e:
                await update.message.reply_text(
                    f"✅ Granted {access_type} access to user {target_user_id} in database, "
                    f"but failed to send invite link: {e}"
                )
        else:
            await update.message.reply_text(f"Failed to grant access. User may not exist.")
    else:
        success = db.revoke_access(target_user_id, access_type)
        if success:
            try:
                await context.bot.send_message(
                    chat_id=target_user_id,
                    text=f"⚠️ Your {access_type} channel access has been revoked by the admin.",
                )
            except Exception:
                pass
            await update.message.reply_text(
                f"❌ Revoked {access_type} access from user {target_user_id} "
                f"({display_name(target_user)}).\n\n"
                f"Note: The user may still be in the channel. You may need to "
                f"remove them manually from the channel."
            )
        else:
            await update.message.reply_text(f"Failed to revoke access.")

    context.user_data.pop("admin_action", None)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("admin_action", None)
    await update.message.reply_text("Action cancelled.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    db.init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))

    # Payment handlers
    app.add_handler(PreCheckoutQueryHandler(pre_checkout))
    app.add_handler(
        MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment)
    )

    # Callback (button) handler
    app.add_handler(CallbackQueryHandler(button_handler))

    # Admin text input handler (for grant/revoke user IDs)
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text_input)
    )

    logger.info("Bot started!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
