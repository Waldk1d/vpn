import uuid
from datetime import datetime, timedelta
import logging
from urllib.parse import urlparse
from typing import List, Dict
import hashlib
import base64

from py3xui import Api, Client, Inbound

from shop_bot.data_manager.database import get_host, get_key_by_email

logger = logging.getLogger(__name__)

def login_to_host(host_url: str, username: str, password: str, inbound_id: int) -> tuple[Api | None, Inbound | None]:
    try:
        api = Api(host=host_url, username=username, password=password)
        api.login()
        inbounds: List[Inbound] = api.inbound.get_list()
        target_inbound = next((inbound for inbound in inbounds if inbound.id == inbound_id), None)
        
        if target_inbound is None:
            logger.error(f"Inbound with ID '{inbound_id}' not found on host '{host_url}'")
            return api, None
        return api, target_inbound
    except Exception as e:
        logger.error(f"Login or inbound retrieval failed for host '{host_url}': {e}", exc_info=True)
        return None, None

def get_subscription_token(api: Api, inbound: Inbound, password: str, host_data: dict = None) -> str | None:
    """Получает subscription token из настроек панели 3x-ui
    
    В 3x-ui subscription token обычно хранится в настройках inbound или панели.
    Пробуем несколько способов получения.
    """
    try:
        # Способ 1: Используем токен из базы данных (если задан вручную)
        if host_data and host_data.get('subscription_token'):
            logger.debug(f"Using subscription token from database: {host_data['subscription_token']}")
            return host_data['subscription_token']
        
        # Способ 2: Пытаемся получить из настроек inbound
        if inbound and hasattr(inbound, 'settings'):
            settings = inbound.settings
            # Проверяем различные возможные поля для subscription token
            if hasattr(settings, 'subscription_token') and settings.subscription_token:
                return settings.subscription_token
            if hasattr(settings, 'subId') and settings.subId:
                return settings.subId
            if hasattr(settings, 'sub_id') and settings.sub_id:
                return settings.sub_id
        
        # Способ 3: Пытаемся получить через API системных настроек
        if hasattr(api, 'system'):
            try:
                if hasattr(api.system, 'get_settings'):
                    system_settings = api.system.get_settings()
                    if system_settings:
                        if hasattr(system_settings, 'subscription_token') and system_settings.subscription_token:
                            return system_settings.subscription_token
                        if hasattr(system_settings, 'subId') and system_settings.subId:
                            return system_settings.subId
            except Exception as e:
                logger.debug(f"Could not get subscription token from system settings: {e}")
        
        logger.warning("Could not determine subscription token. Please set it manually in host settings.")
        return None
    except Exception as e:
        logger.warning(f"Could not get subscription token: {e}")
        return None

def get_subscription_url(host_url: str, inbound: Inbound, email: str, api: Api = None, password: str = None, host_data: dict = None, use_subscription_keyword: bool = True) -> str:
    """Генерирует Subscription URL для клиента в формате 3x-ui
    
    Формат: https://panel-url/{token}/{identifier}
    Где token - это subscription token панели/inbound
    identifier - это либо "Subscription" (стандартный формат), либо email клиента
    """
    parsed_url = urlparse(host_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}".rstrip('/')
    
    # Пытаемся получить токен из настроек inbound или панели
    token = None
    if api and inbound and password:
        token = get_subscription_token(api, inbound, password, host_data)
        logger.debug(f"Subscription token for email {email}: {token}")
    
    # Если токен не получен, возвращаем None (чтобы показать ошибку)
    if not token:
        logger.error(f"Subscription token not found for host. Please set it in host settings.")
        return None
    
    # В 3x-ui в конце URL может использоваться либо "Subscription", либо email клиента
    # По умолчанию используем "Subscription" (стандартный формат панели)
    if use_subscription_keyword:
        identifier = "Subscription"
    else:
        # Используем email клиента (только часть до @, если есть)
        identifier = email.split('@')[0] if '@' in email else email
    
    subscription_url = f"{base_url}/{token}/{identifier}"
    logger.info(f"Generated subscription URL: {subscription_url}")
    
    return subscription_url

def get_connection_string(inbound: Inbound, user_uuid: str, host_url: str, remark: str) -> str | None:
    if not inbound: return None
    settings = inbound.stream_settings.reality_settings.get("settings")
    if not settings: return None
    
    public_key = settings.get("publicKey")
    fp = settings.get("fingerprint")
    server_names = inbound.stream_settings.reality_settings.get("serverNames")
    short_ids = inbound.stream_settings.reality_settings.get("shortIds")
    port = inbound.port
    
    if not all([public_key, server_names, short_ids]): return None
    
    parsed_url = urlparse(host_url)
    short_id = short_ids[0]
    
    connection_string = (
        f"vless://{user_uuid}@{parsed_url.hostname}:{port}"
        f"?type=tcp&security=reality&pbk={public_key}&fp={fp}&sni={server_names[0]}"
        f"&sid={short_id}&spx=%2F&flow=xtls-rprx-vision#{remark}"
    )
    return connection_string

def update_or_create_client_on_panel(api: Api, inbound_id: int, email: str, days_to_add: int, password: str = None) -> tuple[str | None, int | None]:
    try:
        inbound_to_modify = api.inbound.get_by_id(inbound_id)
        if not inbound_to_modify:
            raise ValueError(f"Could not find inbound with ID {inbound_id}")

        if inbound_to_modify.settings.clients is None:
            inbound_to_modify.settings.clients = []
            
        client_index = -1
        for i, client in enumerate(inbound_to_modify.settings.clients):
            if client.email == email:
                client_index = i
                break
        
        if client_index != -1:
            existing_client = inbound_to_modify.settings.clients[client_index]
            if existing_client.expiry_time > int(datetime.now().timestamp() * 1000):
                current_expiry_dt = datetime.fromtimestamp(existing_client.expiry_time / 1000)
                new_expiry_dt = current_expiry_dt + timedelta(days=days_to_add)
            else:
                new_expiry_dt = datetime.now() + timedelta(days=days_to_add)
        else:
            new_expiry_dt = datetime.now() + timedelta(days=days_to_add)

        new_expiry_ms = int(new_expiry_dt.timestamp() * 1000)

        if client_index != -1:
            inbound_to_modify.settings.clients[client_index].reset = days_to_add
            inbound_to_modify.settings.clients[client_index].enable = True
            
            client_uuid = inbound_to_modify.settings.clients[client_index].id
        else:
            client_uuid = str(uuid.uuid4())
            new_client = Client(
                id=client_uuid,
                email=email,
                enable=True,
                flow="xtls-rprx-vision",
                expiry_time=new_expiry_ms
            )
            inbound_to_modify.settings.clients.append(new_client)

        api.inbound.update(inbound_id, inbound_to_modify)

        return client_uuid, new_expiry_ms

    except Exception as e:
        logger.error(f"Error in update_or_create_client_on_panel: {e}", exc_info=True)
        return None, None

async def create_or_update_key_on_host(host_name: str, email: str, days_to_add: int) -> Dict | None:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Workflow failed: Host '{host_name}' not found in the database.")
        return None

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )
    if not api or not inbound:
        logger.error(f"Workflow failed: Could not log in or find inbound on host '{host_name}'.")
        return None
        
    client_uuid, new_expiry_ms = update_or_create_client_on_panel(api, inbound.id, email, days_to_add, host_data['host_pass'])
    if not client_uuid:
        logger.error(f"Workflow failed: Could not create/update client '{email}' on host '{host_name}'.")
        return None
    
    # Обновляем inbound после создания клиента, чтобы получить актуальные данные
    inbound = api.inbound.get_by_id(inbound.id)
    
    connection_string = get_connection_string(inbound, client_uuid, host_data['host_url'], remark=host_name)
    subscription_url = get_subscription_url(host_data['host_url'], inbound, email, api, host_data['host_pass'], host_data)
    
    logger.info(f"Successfully processed key for '{email}' on host '{host_name}'.")
    
    return {
        "client_uuid": client_uuid,
        "email": email,
        "expiry_timestamp_ms": new_expiry_ms,
        "connection_string": connection_string,
        "subscription_url": subscription_url,
        "host_name": host_name
    }

async def get_key_details_from_host(key_data: dict) -> dict | None:
    host_name = key_data.get('host_name')
    if not host_name:
        logger.error(f"Could not get key details: host_name is missing for key_id {key_data.get('key_id')}")
        return None

    host_db_data = get_host(host_name)
    if not host_db_data:
        logger.error(f"Could not get key details: Host '{host_name}' not found in the database.")
        return None

    api, inbound = login_to_host(
        host_url=host_db_data['host_url'],
        username=host_db_data['host_username'],
        password=host_db_data['host_pass'],
        inbound_id=host_db_data['host_inbound_id']
    )
    if not api or not inbound: return None

    connection_string = get_connection_string(inbound, key_data['xui_client_uuid'], host_db_data['host_url'], remark=host_name)
    email = key_data.get('key_email', '')
    subscription_url = get_subscription_url(host_db_data['host_url'], inbound, email, api, host_db_data['host_pass'], host_db_data)
    return {
        "connection_string": connection_string,
        "subscription_url": subscription_url
    }

async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Cannot delete client: Host '{host_name}' not found.")
        return False

    api, inbound = login_to_host(
        host_url=host_data['host_url'],
        username=host_data['host_username'],
        password=host_data['host_pass'],
        inbound_id=host_data['host_inbound_id']
    )

    if not api or not inbound:
        logger.error(f"Cannot delete client: Login or inbound lookup failed for host '{host_name}'.")
        return False
        
    try:
        client_to_delete = get_key_by_email(client_email)
        if client_to_delete:
            api.client.delete(inbound.id, client_to_delete['xui_client_uuid'])
            logger.info(f"Successfully deleted client '{client_to_delete['xui_client_uuid']}' from host '{host_name}'.")
            return True
        else:
            logger.warning(f"Client '{client_to_delete['xui_client_uuid']}' not found on host '{host_name}' for deletion (already gone).")
            return True
            
    except Exception as e:
        logger.error(f"Failed to delete client '{client_to_delete['xui_client_uuid']}' from host '{host_name}': {e}", exc_info=True)
        return False