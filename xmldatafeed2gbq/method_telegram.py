import requests
from notifiers.logging import NotificationHandler


def send_messages(chat_id, tocken, text):
    headers = {"Content-Type": "Content-Type: application/json"}
    uri = f'https://api.telegram.org/bot{tocken}/sendMessage?chat_id={chat_id}&text="{text}"'
    res = requests.post(headers=headers)


# прописываем параметры телеграм бота, от чьего имени и куда слать, где их взять думаю сами разберетесь
def get_loguru_telegramm_notification_handler(logger, chat_id, tocken):
    params = {"token": tocken, "chat_id": chat_id}
    tg_handler = NotificationHandler("telegram", defaults=params)
    return tg_handler
