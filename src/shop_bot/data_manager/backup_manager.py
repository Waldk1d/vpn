"""
Модуль для резервного копирования данных в JSON и восстановления из него
"""
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from shop_bot.data_manager.database import DB_FILE, get_all_hosts, get_plans_for_host

logger = logging.getLogger(__name__)

# Определяем корень проекта
if Path("/app/project").exists():
    PROJECT_ROOT = Path("/app/project")
else:
    PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

BACKUP_FILE = PROJECT_ROOT / "backup.json"

def create_backup() -> Dict[str, Any]:
    """Создает резервную копию всех данных в формате JSON"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Получаем всех пользователей
            cursor.execute("SELECT * FROM users")
            users = [dict(row) for row in cursor.fetchall()]
            
            # Получаем все ключи
            cursor.execute("SELECT * FROM vpn_keys")
            keys = []
            for row in cursor.fetchall():
                key_dict = dict(row)
                # Преобразуем datetime в строку
                if key_dict.get('expiry_date'):
                    if isinstance(key_dict['expiry_date'], str):
                        key_dict['expiry_date'] = key_dict['expiry_date']
                    else:
                        key_dict['expiry_date'] = key_dict['expiry_date'].isoformat() if hasattr(key_dict['expiry_date'], 'isoformat') else str(key_dict['expiry_date'])
                if key_dict.get('created_date'):
                    if isinstance(key_dict['created_date'], str):
                        key_dict['created_date'] = key_dict['created_date']
                    else:
                        key_dict['created_date'] = key_dict['created_date'].isoformat() if hasattr(key_dict['created_date'], 'isoformat') else str(key_dict['created_date'])
                keys.append(key_dict)
            
            # Получаем все subscription ссылки
            cursor.execute("SELECT * FROM subscription_links")
            subscription_links = []
            for row in cursor.fetchall():
                link_dict = dict(row)
                # Преобразуем datetime в строку
                for date_field in ['expiry_date', 'created_date', 'assigned_date']:
                    if link_dict.get(date_field):
                        if isinstance(link_dict[date_field], str):
                            continue
                        link_dict[date_field] = link_dict[date_field].isoformat() if hasattr(link_dict[date_field], 'isoformat') else str(link_dict[date_field])
                subscription_links.append(link_dict)
            
            # Получаем все транзакции
            cursor.execute("SELECT * FROM transactions")
            transactions = []
            for row in cursor.fetchall():
                trans_dict = dict(row)
                if trans_dict.get('created_date'):
                    if isinstance(trans_dict['created_date'], str):
                        continue
                    trans_dict['created_date'] = trans_dict['created_date'].isoformat() if hasattr(trans_dict['created_date'], 'isoformat') else str(trans_dict['created_date'])
                transactions.append(trans_dict)
            
            # Получаем все тарифы
            cursor.execute("SELECT * FROM plans")
            plans = [dict(row) for row in cursor.fetchall()]
            
            # Получаем все хосты
            hosts = get_all_hosts()
            
            # Получаем все настройки
            cursor.execute("SELECT key, value FROM bot_settings")
            settings = {row['key']: row['value'] for row in cursor.fetchall()}
            
            backup_data = {
                "backup_timestamp": datetime.now().isoformat(),
                "users": users,
                "keys": keys,
                "subscription_links": subscription_links,
                "transactions": transactions,
                "plans": plans,
                "hosts": hosts,
                "settings": settings
            }
            
            return backup_data
            
    except Exception as e:
        logger.error(f"Error creating backup: {e}", exc_info=True)
        return {}

def save_backup_to_file() -> bool:
    """Сохраняет резервную копию в JSON файл"""
    try:
        backup_data = create_backup()
        if not backup_data:
            logger.error("Failed to create backup data")
            return False
        
        with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Backup saved to {BACKUP_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error saving backup to file: {e}", exc_info=True)
        return False

def restore_from_backup(backup_data: Dict[str, Any]) -> bool:
    """Восстанавливает данные из резервной копии"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # Восстанавливаем пользователей
            if 'users' in backup_data:
                cursor.execute("DELETE FROM users")
                for user in backup_data['users']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO users 
                        (telegram_id, username, total_spent, total_months, trial_used, 
                         agreed_to_terms, registration_date, is_banned, referred_by, 
                         referral_balance, referral_balance_all)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        user.get('telegram_id'),
                        user.get('username'),
                        user.get('total_spent', 0),
                        user.get('total_months', 0),
                        user.get('trial_used', 0),
                        user.get('agreed_to_terms', 0),
                        user.get('registration_date'),
                        user.get('is_banned', 0),
                        user.get('referred_by'),
                        user.get('referral_balance', 0),
                        user.get('referral_balance_all', 0)
                    ))
            
            # Восстанавливаем ключи
            if 'keys' in backup_data:
                cursor.execute("DELETE FROM vpn_keys")
                for key in backup_data['keys']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO vpn_keys 
                        (key_id, user_id, host_name, xui_client_uuid, key_email, expiry_date, created_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        key.get('key_id'),
                        key.get('user_id'),
                        key.get('host_name'),
                        key.get('xui_client_uuid'),
                        key.get('key_email'),
                        key.get('expiry_date'),
                        key.get('created_date')
                    ))
            
            # Восстанавливаем subscription ссылки
            if 'subscription_links' in backup_data:
                cursor.execute("DELETE FROM subscription_links")
                for link in backup_data['subscription_links']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO subscription_links 
                        (link_id, subscription_url, status, user_id, key_id, expiry_date, created_date, assigned_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        link.get('link_id'),
                        link.get('subscription_url'),
                        link.get('status', 'free'),
                        link.get('user_id'),
                        link.get('key_id'),
                        link.get('expiry_date'),
                        link.get('created_date'),
                        link.get('assigned_date')
                    ))
            
            # Восстанавливаем транзакции
            if 'transactions' in backup_data:
                cursor.execute("DELETE FROM transactions")
                for trans in backup_data['transactions']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO transactions 
                        (transaction_id, username, payment_id, user_id, status, amount_rub, 
                         amount_currency, currency_name, payment_method, metadata, created_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        trans.get('transaction_id'),
                        trans.get('username'),
                        trans.get('payment_id'),
                        trans.get('user_id'),
                        trans.get('status'),
                        trans.get('amount_rub'),
                        trans.get('amount_currency'),
                        trans.get('currency_name'),
                        trans.get('payment_method'),
                        trans.get('metadata'),
                        trans.get('created_date')
                    ))
            
            # Восстанавливаем тарифы
            if 'plans' in backup_data:
                cursor.execute("DELETE FROM plans")
                for plan in backup_data['plans']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO plans 
                        (plan_id, host_name, plan_name, months, price)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        plan.get('plan_id'),
                        plan.get('host_name'),
                        plan.get('plan_name'),
                        plan.get('months'),
                        plan.get('price')
                    ))
            
            # Восстанавливаем настройки (опционально, чтобы не перезаписать текущие)
            # Можно добавить флаг для полного восстановления настроек
            
            conn.commit()
            logger.info("Backup restored successfully")
            return True
            
    except Exception as e:
        logger.error(f"Error restoring from backup: {e}", exc_info=True)
        return False

def load_backup_from_file(file_path: str = None) -> Dict[str, Any] | None:
    """Загружает резервную копию из JSON файла"""
    try:
        backup_path = Path(file_path) if file_path else BACKUP_FILE
        if not backup_path.exists():
            logger.error(f"Backup file not found: {backup_path}")
            return None
        
        with open(backup_path, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        logger.info(f"Backup loaded from {backup_path}")
        return backup_data
    except Exception as e:
        logger.error(f"Error loading backup from file: {e}", exc_info=True)
        return None
