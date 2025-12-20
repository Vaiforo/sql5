import time
import uuid
import json
import re
from typing import List, Dict, Optional

import requests
import urllib3
import pandas as pd

from config import AUTH_KEY


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
API_BASE_URL = "https://gigachat.devices.sberbank.ru/api/v1"


class GigaChatRESTClient:
    def __init__(self,
                 auth_key: str,
                 scope: str = "GIGACHAT_API_PERS",
                 model: str = "GigaChat-2",
                 timeout: int = 30,
                 verify_ssl: bool = False):
        self.auth_key = auth_key
        self.scope = scope
        self.model = model
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def _request_new_token(self):
        rq_uid = str(uuid.uuid4())

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "RqUID": rq_uid,
            "Authorization": f"Basic {self.auth_key}",
        }

        data = {"scope": self.scope}

        resp = requests.post(
            AUTH_URL,
            headers=headers,
            data=data,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._access_token = payload["access_token"]
        self._expires_at = float(payload["expires_at"])

    def _get_token(self) -> str:
        now = time.time()
        if self._access_token is None or now > self._expires_at - 10:
            self._request_new_token()
        return self._access_token

    def chat(self,
             messages: List[Dict[str, str]],
             temperature: float = 0.7,
             top_p: float = 0.9,
             max_tokens: int = 2048,
             repetition_penalty: float = 1.0,
             stream: bool = False
             ) -> str:
        token = self._get_token()

        url = f"{API_BASE_URL}/chat/completions"

        body = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "n": 1,
            "stream": stream,
            "max_tokens": max_tokens,
            "repetition_penalty": repetition_penalty,
            "update_interval": 0,
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        resp = requests.post(
            url,
            json=body,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


def parse_json_array_from_text(text: str):
    text = text.strip()
    if not text:
        raise ValueError("Неверный формат ответа модели/пустой овтет")

    text = re.sub(r',\s*""', '', text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        fragment = text[start:end + 1].strip()
        fragment = re.sub(r',\s*""', '', fragment)
        try:
            return json.loads(fragment)
        except json.JSONDecodeError:
            text = fragment

    objs = re.findall(r'\{.*?\}', text, flags=re.DOTALL)
    if objs:
        arr_text = "[" + ",".join(objs) + "]"
        try:
            return json.loads(arr_text)
        except json.JSONDecodeError:
            pass

    raise ValueError(
        "Не удалось распарсить JSON из ответа. "
        f"Ответ:\n{text[:500]}"
    )


def llm_generate_raw(batch_size: int = 10) -> str:
    client = GigaChatRESTClient(
        auth_key=AUTH_KEY,
        scope="GIGACHAT_API_PERS",
        model="GigaChat-2",
        verify_ssl=False,
    )

    prompt = f"""
    Сгенерируй JSON-массив из {batch_size} записей.
    Тематика: автомобили в автосервисе.

    Каждая запись — один автомобиль владельца, который обслуживался в сервисе.
    У КАЖДОГО объекта должны быть СТРОГО такие поля (10 штук):

    - "car_id" — идентификатор автомобиля (строка или число)
    - "owner_name" — имя владельца
    - "brand" — марка авто (Toyota, BMW, Lada, или любая другая марка, нужно брать постоянно разные)
    - "model" — модель авто
    - "year" — год выпуска (можно строкой)
    - "vin" — VIN-номер (может быть с ошибками, лишними символами, но длина 17 символов)
    - "mileage" — пробег (иногда числом, иногда строкой: "xxxxxx", "xxx xxx", "xxxk" - любое количество разрядов из любых цифр, должно быть и более 100 тысяч, и более 200 тысяч, и менее 100 тысяч)
    - "last_service_date" — дата последнего обслуживания (в разных форматах: "yyyy-mm-dd", "dd.mm.yyyy", "yyyy-mm-ddThh:mm:ss" ДАТА ЛЮБАЯ)
    - "issue_description" — описание проблемы/неисправности (с опечатками, разными языками)
    - "service_cost" — стоимость обслуживания (иногда числом, иногда строкой: "xxxxx", "xx,xxx", "xx xxx ₽", "$xxx" - любое количество разрядов)

    ДАННЫЕ ДОЛЖНЫ БЫТЬ "ГРЯЗНЫМИ":
    - пропуски (null, "", "NaN") в некоторых полях
    - странные форматы чисел и дат
    - VIN с лишними символами
    - пробег и стоимость как строки с пробелами и символами
    - опечатки в тексте

    Требования к формату ответа:
    - Ответ ТОЛЬКО в виде JSON-массива.
    - Каждый элемент массива — объект с РОВНО этими 10 полями.
    - БЕЗ текста до или после.
    - БЕЗ ``` и других код-блоков.
    """

    messages = [
        {
            "role": "system",
            "content": (
                "Ты генерируешь синтетический 'грязный' датасет автомобилей в автосервисе для тестов очистки данных"
            ),
        },
        {"role": "user", "content": prompt},
    ]

    return client.chat(messages)


def get_dataset(n: int = 50, batch_size: int = 10) -> pd.DataFrame:
    records: list[dict] = []

    while len(records) < n:
        raw = llm_generate_raw(batch_size)
        data = parse_json_array_from_text(raw)

        if not isinstance(data, list):
            raise ValueError("Ожидался JSON объектов от модели.")

        for obj in data:
            if isinstance(obj, dict):
                records.append(obj)
            if len(records) >= n:
                break

        print(f"[I] get_dataset - Записей: {len(records)} / {n}")

        if len(data) == 0:
            raise RuntimeError(
                "Модель вернула пустой массив.")

    records = records[:n]

    return pd.DataFrame(records)
