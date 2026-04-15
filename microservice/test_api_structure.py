#!/usr/bin/env python3
"""Тестовый скрипт для проверки структуры данных API"""
import json

# Моковые данные для проверки структуры
mock_response = {
    "source": "database",
    "period": {"start_date": "2025-09-01", "end_date": "2025-09-30"},
    "free_slots_count": 100,
    "detailed_count": 50,
    "free": [
        {
            "Аудитория": "А-101",
            "Тип помещения": "Лекционная",
            "День недели": "Пн",
            "Пара": 1,
            "Тип недели": "числитель",
            "Время начала": "08:30:00",
            "Статус": "Свободна"
        }
    ],
    "detailed": [
        {
            "Аудитория": "А-102",
            "Тип помещения": "Практическая",
            "Дата/время занятия": "2025-09-02 10:10:00",
            "День недели": "Вт",
            "Пара": 2,
            "Тип недели": "числитель",
            "Общее количество студентов с учетом подгрупп (ПЛАН)": 30.0,
            "Фактическое посещение (ФАКТ)": 25.0,
            "Процент посещаемости (факт/план * 100%)": 83.33
        }
    ],
    "pattern": [
        {
            "Аудитория": "А-101",
            "День недели": "Пн",
            "Пара": 1,
            "Тип недели": "числитель",
            "Тип помещения": "Лекционная",
            "Всего недель": 4,
            "Занятые недели": 2,
            "Процент занятости": 50.0
        }
    ]
}

print("=== Проверка структуры ответа API ===\n")
print(f"Источник: {mock_response['source']}")
print(f"Период: {mock_response['period']['start_date']} - {mock_response['period']['end_date']}")
print(f"Свободных слотов: {mock_response['free_slots_count']}")
print(f"Занятых слотов: {mock_response['detailed_count']}")
print(f"Записей паттернов: {len(mock_response['pattern'])}")

print("\n=== Проверка полей pattern ===")
if mock_response['pattern']:
    p = mock_response['pattern'][0]
    print(f"Ключи pattern: {list(p.keys())}")
    print(f"Тип 'Пара': {type(p['Пара'])}")
    print(f"Тип 'Процент занятости': {type(p['Процент занятости'])}")
    
    # Проверка на возможность итерации
    try:
        for item in mock_response['pattern']:
            pass
        print("✅ pattern поддерживает итерацию (forEach)")
    except Exception as e:
        print(f"❌ Ошибка итерации: {e}")

print("\n=== Проверка числовых полей ===")
for key, value in mock_response['pattern'][0].items():
    print(f"{key}: {value} (тип: {type(value).__name__})")

print("\n=== Экспорт в JSON ===")
try:
    json_str = json.dumps(mock_response, ensure_ascii=False, indent=2)
    print("✅ Успешная сериализация в JSON")
    print(f"Размер JSON: {len(json_str)} байт")
except Exception as e:
    print(f"❌ Ошибка сериализации: {e}")

print("\n=== Тест завершён ===")
