CHOOSE_PLAN_MESSAGE = "Выберите подходящий тариф:"
CHOOSE_PAYMENT_METHOD_MESSAGE = "Выберите удобный способ оплаты:"
VPN_INACTIVE_TEXT = "❌ <b>Статус VPN:</b> Неактивен (срок истек)"
VPN_NO_DATA_TEXT = "ℹ️ <b>Статус VPN:</b> У вас пока нет активных ключей."

def get_profile_text(username, total_spent, total_months, vpn_status_text):
    return (
        f"👤 <b>Профиль:</b> {username}\n\n"
        f"💰 <b>Потрачено всего:</b> {total_spent:.0f} RUB\n"
        f"📅 <b>Приобретено месяцев:</b> {total_months}\n\n"
        f"{vpn_status_text}"
    )

def get_vpn_active_text(days_left, hours_left):
    return (
        f"✅ <b>Статус VPN:</b> Активен\n"
        f"⏳ <b>Осталось:</b> {days_left} д. {hours_left} ч."
    )

def get_key_info_text(key_number, expiry_date, created_date, connection_string, subscription_url=None):
    expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')
    created_formatted = created_date.strftime('%d.%m.%Y в %H:%M')
    
    text = (
        f"<b>🔑 Информация о ключе #{key_number}</b>\n\n"
        f"<b>➕ Приобретён:</b> {created_formatted}\n"
        f"<b>⏳ Действителен до:</b> {expiry_formatted}\n\n"
    )
    
    if subscription_url:
        text += f"<b>📡 Subscription URL:</b>\n<code>{subscription_url}</code>\n\n"
    
    text += f"<b>🔗 VLESS:</b>\n<code>{connection_string}</code>"
    return text

def get_purchase_success_text(action: str, key_number: int, expiry_date, connection_string: str, subscription_url=None):
    action_text = "обновлен" if action == "extend" else "готов"
    expiry_formatted = expiry_date.strftime('%d.%m.%Y в %H:%M')

    text = (
        f"🎉 <b>Ваш ключ #{key_number} {action_text}!</b>\n\n"
        f"⏳ <b>Он будет действовать до:</b> {expiry_formatted}\n\n"
    )
    
    if subscription_url:
        text += f"<b>📡 Subscription URL:</b>\n<code>{subscription_url}</code>\n\n"
    
    text += f"<b>🔗 VLESS:</b>\n<code>{connection_string}</code>"
    return text