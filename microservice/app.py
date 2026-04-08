"""
Микросервис для анализа эффективности использования аудиторного фонда университета.
FastAPI + кэширование + веб-интерфейс.
"""
import json
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import urllib.parse

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

# === Загрузка конфигурации ===
with open('config.json', 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

DB_CONFIG = CONFIG['db_config']
DEFAULT_START_DATE = CONFIG['period']['start_date']
DEFAULT_END_DATE = CONFIG['period']['end_date']
EXCLUDED_BUILDINGS = CONFIG['excluded_buildings']

# === Подключение к базе данных ===
password_encoded = urllib.parse.quote_plus(DB_CONFIG['password'])
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{password_encoded}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset=cp1251"

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)

# === Кэш данных ===
# Структура: {cache_key: {"data": {...}, "timestamp": datetime}}
DATA_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 3600 * 24  # 24 часа


def get_cache_key(start_date: str, end_date: str) -> str:
    """Генерирует уникальный ключ кэша на основе периода."""
    period_str = f"{start_date}_{end_date}"
    return hashlib.md5(period_str.encode()).hexdigest()


def is_cache_valid(cache_entry: Dict[str, Any]) -> bool:
    """Проверяет, действителен ли кэш."""
    if cache_entry is None:
        return False
    age = (datetime.now() - cache_entry["timestamp"]).total_seconds()
    return age < CACHE_TTL_SECONDS


# === Временные слоты ===
TIME_SLOTS = {
    '08:30:00': 1,
    '10:10:00': 2,
    '12:10:00': 3,
    '13:50:00': 4,
    '15:30:00': 5,
    '17:10:00': 6,
    '18:50:00': 7,
    '20:30:00': 8,
    '22:10:00': 9,
}

DAYS_OF_WEEK = {
    2: 'Пн',
    3: 'Вт',
    4: 'Ср',
    5: 'Чт',
    6: 'Пт',
    7: 'Сб',
}

DAY_ORDER = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб']


def get_week_type_from_date(date_str) -> str:
    """Определяет тип недели (числитель/знаменатель) по дате."""
    date = pd.to_datetime(date_str, format='mixed', dayfirst=True)
    start_date = datetime(2025, 9, 1)
    start_offset = (date.weekday()) % 7
    start_monday = date - pd.Timedelta(days=start_offset)
    monday_of_week = date - pd.Timedelta(days=(date.weekday()) % 7)
    weeks_passed = int((monday_of_week - start_monday).days / 7)
    return 'числитель' if weeks_passed % 2 == 0 else 'знаменатель'


def fetch_data_from_db(start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """Загружает все необходимые данные из базы данных."""
    
    # Формируем запросы
    excluded_str = ','.join(map(str, EXCLUDED_BUILDINGS))
    
    query_auditories = f"""
    SELECT a.id, a.name, at.name as type_name
    FROM auditories a
    JOIN aud_types at ON a.id_type = at.id
    JOIN building b ON a.building_id = b.id
    JOIN departments d ON b.dep_id = d.id
    WHERE d.filial_id = 1
      AND b.id NOT IN ({excluded_str});
    """
    
    query_occupied = f"""
    SELECT
        a.id as auditory_id,
        a.name as auditory_name,
        at.name as type_name,
        b.name as building_name,
        d.name as department_name,
        sd.id as sched_done_id,
        sd.lec_date,
        sd.title,
        sd.group_id,
        sd.subgroup,
        sd.lec_type,
        sd.aud_id,
        sd.is_present,
        sd.all_absence,
        sd.sched_error,
        sd.special_type,
        sg.name as group_name,
        lt.name as lesson_type_name
    FROM sched_done sd
    JOIN auditories a ON sd.aud_id = a.id
    JOIN aud_types at ON a.id_type = at.id
    JOIN building b ON a.building_id = b.id
    JOIN departments d ON b.dep_id = d.id
    JOIN stud_groups sg ON sd.group_id = sg.id
    JOIN lec_types lt ON sd.lec_type = lt.id
    WHERE d.filial_id = 1
      AND b.id NOT IN ({excluded_str})
      AND sd.lec_date >= '{start_date}'
      AND sd.lec_date <= '{end_date}';
    """
    
    query_presence = """
    SELECT sched_done_id, COUNT(person_id) as present_count
    FROM sched_presence
    GROUP BY sched_done_id;
    """
    
    query_courses = f"""
    SELECT
        c.group_id,
        c.subgroup,
        COUNT(*) as plan_count
    FROM courses c
    WHERE c.s_year = 2025
      AND (c.semestr %% 2) = 1
      AND c.active IN (0, 1)
      AND c.archived_id NOT IN (4, 5)
      AND c.date_create <= '{end_date}'
      AND (c.date_end IS NULL OR c.date_end >= '{start_date}')
    GROUP BY c.group_id, c.subgroup;
    """
    
    with engine.connect() as conn:
        # Загружаем аудитории
        df_auditories = pd.read_sql(query_auditories, conn)
        
        # Загружаем занятые слоты
        df_occupied = pd.read_sql(query_occupied, conn)
        
        # Загружаем посещаемость
        try:
            df_presence = pd.read_sql(query_presence, conn)
        except Exception:
            df_presence = pd.DataFrame({'sched_done_id': [], 'present_count': []})
        
        # Загружаем количество студентов в группах
        try:
            df_courses = pd.read_sql(query_courses, conn)
        except Exception:
            df_courses = pd.DataFrame({'group_id': [], 'subgroup': [], 'plan_count': []})
    
    return {
        'auditories': df_auditories,
        'occupied': df_occupied,
        'presence': df_presence,
        'courses': df_courses
    }


def process_data(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Обрабатывает сырые данные: объединяет, рассчитывает метрики."""
    
    df_occupied = raw_data['occupied'].copy()
    df_presence = raw_data['presence']
    df_courses = raw_data['courses']
    df_auditories = raw_data['auditories']
    
    # Сопоставляем посещаемость с занятиями
    if len(df_presence) > 0:
        df_occupied = df_occupied.merge(df_presence, left_on='sched_done_id', right_on='sched_done_id', how='left')
        df_occupied['present_count'] = df_occupied['present_count'].fillna(0)
    else:
        df_occupied['present_count'] = 0
    
    # Сопоставляем плановые студенты из courses
    if len(df_courses) > 0:
        df_occupied = df_occupied.merge(
            df_courses[['group_id', 'subgroup', 'plan_count']],
            left_on=['group_id', 'subgroup'],
            right_on=['group_id', 'subgroup'],
            how='left'
        )
        df_occupied['plan_count'] = df_occupied['plan_count'].fillna(0)
    else:
        df_occupied['plan_count'] = 0
    
    # Обработка подгрупп
    df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] = 0
    
    for idx, row in df_occupied.iterrows():
        group_id = row['group_id']
        sched_subgroup = row['subgroup']
        if sched_subgroup == 0:
            total_plan = df_courses[
                (df_courses['group_id'] == group_id) &
                (df_courses['subgroup'] >= 0)
            ]['plan_count'].sum() if len(df_courses) > 0 else 0
            df_occupied.at[idx, 'Общее количество студентов с учетом подгрупп (ПЛАН)'] = total_plan
        else:
            plan_row = df_courses[
                (df_courses['group_id'] == group_id) &
                (df_courses['subgroup'] == sched_subgroup)
            ]
            if not plan_row.empty:
                df_occupied.at[idx, 'Общее количество студентов с учетом подгрупп (ПЛАН)'] = plan_row.iloc[0]['plan_count']
    
    # Вычисляем метрики
    df_occupied['Фактическое посещение (ФАКТ)'] = df_occupied['present_count']
    
    # Защита от деления на ноль
    df_occupied['Процент посещаемости (факт/план * 100%)'] = np.where(
        df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] > 0,
        (df_occupied['Фактическое посещение (ФАКТ)'] / df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] * 100).round(2),
        0
    )
    
    df_occupied['Разница между планом и фактом'] = (
        df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] - df_occupied['Фактическое посещение (ФАКТ)']
    )
    
    # Добавляем временные колонки
    df_occupied['datetime_parsed'] = pd.to_datetime(df_occupied['lec_date'], format='mixed', dayfirst=True)
    df_occupied['time_part'] = df_occupied['datetime_parsed'].dt.time.astype(str)
    df_occupied['Пара'] = df_occupied['time_part'].apply(lambda x: TIME_SLOTS.get(x, None))
    df_occupied['Тип недели'] = df_occupied['datetime_parsed'].apply(lambda x: get_week_type_from_date(x).lower().strip())
    df_occupied['День недели'] = df_occupied['datetime_parsed'].dt.day_name()
    
    day_map = {
        'Monday': 'Пн',
        'Tuesday': 'Вт',
        'Wednesday': 'Ср',
        'Thursday': 'Чт',
        'Friday': 'Пт',
        'Saturday': 'Сб',
        'Sunday': 'Вс'
    }
    df_occupied['День недели'] = df_occupied['День недели'].map(day_map)
    
    # Генерация всех возможных слотов
    start_dt = pd.to_datetime(CONFIG['period']['start_date'] if 'period' in CONFIG else df_occupied['datetime_parsed'].min())
    end_dt = pd.to_datetime(CONFIG['period']['end_date'] if 'period' in CONFIG else df_occupied['datetime_parsed'].max())
    
    all_combinations = []
    for _, row in df_auditories.iterrows():
        auditory_id = row['id']
        auditory_name = row['name']
        type_name = row['type_name']
        current = start_dt
        while current <= end_dt:
            week_type = get_week_type_from_date(current)
            day_of_week_num = current.weekday() + 2
            if day_of_week_num in DAYS_OF_WEEK:
                day_name = DAYS_OF_WEEK[day_of_week_num]
                for time_str, pair_num in TIME_SLOTS.items():
                    all_combinations.append({
                        'auditory_id': auditory_id,
                        'auditory_name': auditory_name,
                        'type_name': type_name,
                        'day_of_week_num': day_of_week_num,
                        'day_name': day_name,
                        'pair_number': pair_num,
                        'time_start': time_str,
                        'week_type': week_type,
                    })
            current += pd.Timedelta(days=1)
    
    df_all_slots = pd.DataFrame(all_combinations)
    df_all_slots = df_all_slots.drop_duplicates(subset=['auditory_id', 'day_of_week_num', 'time_start', 'week_type'])
    
    # Сопоставляем занятые слоты
    occupied_keys = df_occupied[['auditory_id', 'day_of_week_num', 'time_start', 'week_type']].drop_duplicates()
    df_all_slots = df_all_slots.merge(
        occupied_keys,
        on=['auditory_id', 'day_of_week_num', 'time_start', 'week_type'],
        how='left',
        indicator=True
    )
    
    # Фильтруем свободные слоты
    df_free_slots = df_all_slots[df_all_slots['_merge'] == 'left_only'].copy()
    df_free_slots.drop(columns=['_merge'], inplace=True)
    df_free_slots.rename(columns={'time_start': 'время начала'}, inplace=True)
    df_free_slots['статус'] = 'Свободна'
    
    # === ИСПРАВЛЕНИЕ ТИПОВ ДАННЫХ ===
    # Преобразуем числовые колонки в numeric, обрабатывая ошибки
    numeric_cols_detailed = [
        'Общее количество студентов с учетом подгрупп (ПЛАН)',
        'Фактическое посещение (ФАКТ)',
        'Процент посещаемости (факт/план * 100%)',
        'Разница между планом и фактом',
        'Пара',
        'subgroup',
        'group_id',
        'auditory_id',
        'sched_done_id',
        'present_count',
        'plan_count',
        'is_present',
        'all_absence',
        'sched_error',
        'special_type',
        'aud_id',
        'lec_type'
    ]
    
    for col in numeric_cols_detailed:
        if col in df_occupied.columns:
            df_occupied[col] = pd.to_numeric(df_occupied[col], errors='coerce').fillna(0)
    
    # Для df_free_slots
    if 'pair_number' in df_free_slots.columns:
        df_free_slots['pair_number'] = pd.to_numeric(df_free_slots['pair_number'], errors='coerce').fillna(0)
    
    # Переименовываем колонки для отображения
    df_occupied.rename(columns={
        'auditory_name': 'Аудитория',
        'type_name': 'Тип помещения',
        'building_name': 'Корпус',
        'department_name': 'Факультет',
        'lec_date': 'Дата/время занятия',
        'title': 'Дисциплина',
        'group_name': 'Список групп на этом занятии',
        'subgroup': 'Информация о подгруппах',
        'lesson_type_name': 'Тип занятия',
        'aud_id': 'ID аудитории',
        'is_present': 'Присутствие преподавателя',
        'all_absence': 'Полное отсутствие студентов',
        'sched_error': 'Ошибка расписания',
        'special_type': 'Специальный тип',
    }, inplace=True)
    
    df_free_slots.rename(columns={
        'auditory_name': 'Аудитория',
        'type_name': 'Тип помещения',
        'day_name': 'День недели',
        'pair_number': 'Пара',
        'время начала': 'Время начала',
        'week_type': 'Тип недели',
        'статус': 'Статус',
    }, inplace=True)
    
    # Упорядочиваем дни недели
    if 'День недели' in df_free_slots.columns:
        df_free_slots['День недели'] = pd.Categorical(df_free_slots['День недели'], categories=DAY_ORDER, ordered=True)
    if 'День недели' in df_occupied.columns:
        df_occupied['День недели'] = pd.Categorical(df_occupied['День недели'], categories=DAY_ORDER, ordered=True)
    
    return {
        'free': df_free_slots,
        'detailed': df_occupied
    }


def generate_pattern_data(df_detailed: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """Генерирует данные паттернов использования."""
    
    min_date = pd.to_datetime(start_date)
    max_date = pd.to_datetime(end_date)
    
    start_monday = min_date - pd.Timedelta(days=min_date.weekday())
    end_monday = max_date - pd.Timedelta(days=max_date.weekday())
    weeks = pd.date_range(start=start_monday, end=end_monday, freq='7D')
    
    def week_type_for_date(date):
        start_sem = datetime(2025, 9, 1)
        start_monday_sem = start_sem - pd.Timedelta(days=start_sem.weekday())
        weeks_passed = int((date - start_monday_sem).days / 7)
        return 'числитель' if weeks_passed % 2 == 0 else 'знаменатель'
    
    auditoriums = df_detailed['Аудитория'].unique() if 'Аудитория' in df_detailed.columns else []
    all_slots = []
    
    for w in weeks:
        wt = week_type_for_date(w).lower().strip()
        for aud in auditoriums:
            for day_name in DAY_ORDER:
                for pair_num in range(1, 10):
                    all_slots.append({
                        'Аудитория': aud,
                        'День недели': day_name,
                        'Пара': pair_num,
                        'Тип недели': wt,
                        'week_start': w.strftime('%Y-%m-%d').strip(),
                    })
    
    df_all_slots = pd.DataFrame(all_slots)
    
    if len(df_detailed) > 0 and 'Аудитория' in df_detailed.columns:
        df_detailed_for_pattern = df_detailed.copy()
        
        def round_to_nearest_monday(date):
            mon = date - pd.Timedelta(days=date.weekday())
            available_mondays = weeks.normalize()
            diff = (available_mondays - mon.normalize()).to_series().abs()
            idx = diff.argmin()
            closest_monday = available_mondays[idx]
            return closest_monday.strftime('%Y-%m-%d')
        
        df_detailed_for_pattern['week_start'] = df_detailed_for_pattern['datetime_parsed'].apply(round_to_nearest_monday)
        
        slots_in_use = df_detailed_for_pattern[['Аудитория', 'День недели', 'Пара', 'Тип недели', 'week_start']].drop_duplicates()
        
        merged = df_all_slots.merge(
            slots_in_use,
            on=['Аудитория', 'День недели', 'Пара', 'Тип недели', 'week_start'],
            how='left',
            indicator=True
        )
        merged['is_occupied'] = merged['_merge'] == 'both'
        
        merged = merged.merge(
            df_detailed_for_pattern[['Аудитория', 'Тип помещения']].drop_duplicates(),
            on='Аудитория',
            how='left'
        )
        
        pattern = merged.groupby(['Аудитория', 'День недели', 'Пара', 'Тип недели', 'Тип помещения']).agg(
            total_weeks=('week_start', 'count'),
            occupied_weeks=('is_occupied', 'sum')
        ).reset_index()
        pattern['Процент занятости'] = (pattern['occupied_weeks'] / pattern['total_weeks'] * 100).round(1)
        
        pattern = pattern[['Аудитория', 'День недели', 'Пара', 'Тип недели', 'Тип помещения', 'total_weeks', 'occupied_weeks', 'Процент занятости']]
        pattern.rename(columns={
            'total_weeks': 'Всего недель',
            'occupied_weeks': 'Занятые недели',
        }, inplace=True)
    else:
        pattern = pd.DataFrame(columns=['Аудитория', 'День недели', 'Пара', 'Тип недели', 'Тип помещения', 'Всего недель', 'Занятые недели', 'Процент занятости'])
    
    return pattern


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    print("🚀 Микросервис запускается...")
    yield
    print("🛑 Микросервис останавливается...")


app = FastAPI(
    title="📊 Сервис анализа аудиторного фонда",
    description="Микросервис для построения отчетов по эффективности использования аудиторий",
    version="1.0.0",
    lifespan=lifespan
)

# Шаблоны и статика
templates = Jinja2Templates(directory="templates", context_processors=[])


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Главная страница с интерфейсом выбора периода."""
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "default_start": DEFAULT_START_DATE,
            "default_end": DEFAULT_END_DATE
        }
    )


@app.get("/api/reports")
async def get_reports(
    start_date: str = Query(DEFAULT_START_DATE, description="Дата начала периода (YYYY-MM-DD)"),
    end_date: str = Query(DEFAULT_END_DATE, description="Дата конца периода (YYYY-MM-DD)"),
    force_refresh: bool = Query(False, description="Принудительно обновить данные из БД")
):
    """
    Получает отчетные данные за указанный период.
    
    - Если период уже есть в кэше и кэш действителен, возвращает кэшированные данные
    - Если force_refresh=True, загружает данные заново из БД
    """
    cache_key = get_cache_key(start_date, end_date)
    
    # Проверяем кэш
    if not force_refresh and cache_key in DATA_CACHE and is_cache_valid(DATA_CACHE[cache_key]):
        print(f"✅ Данные найдены в кэше для периода {start_date} - {end_date}")
        cached_data = DATA_CACHE[cache_key]["data"]
        return {
            "source": "cache",
            "period": {"start_date": start_date, "end_date": end_date},
            "free_slots_count": len(cached_data["free"]),
            "detailed_count": len(cached_data["detailed"]),
            "free": cached_data["free"].to_dict(orient='records'),
            "detailed": cached_data["detailed"].to_dict(orient='records'),
            "pattern": cached_data["pattern"].to_dict(orient='records')
        }
    
    # Загружаем из БД
    print(f"📥 Загрузка данных из БД для периода {start_date} - {end_date}...")
    try:
        raw_data = fetch_data_from_db(start_date, end_date)
        processed_data = process_data(raw_data)
        pattern_data = generate_pattern_data(processed_data['detailed'], start_date, end_date)
        
        # Сохраняем в кэш
        DATA_CACHE[cache_key] = {
            "data": {
                "free": processed_data['free'],
                "detailed": processed_data['detailed'],
                "pattern": pattern_data
            },
            "timestamp": datetime.now()
        }
        
        print(f"✅ Данные загружены и закэшированы. Свободных слотов: {len(processed_data['free'])}, Занятых: {len(processed_data['detailed'])}")
        
        return {
            "source": "database",
            "period": {"start_date": start_date, "end_date": end_date},
            "free_slots_count": len(processed_data['free']),
            "detailed_count": len(processed_data['detailed']),
            "free": processed_data['free'].to_dict(orient='records'),
            "detailed": processed_data['detailed'].to_dict(orient='records'),
            "pattern": pattern_data.to_dict(orient='records')
        }
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка при загрузке данных: {str(e)}")


@app.get("/api/reports/summary")
async def get_summary(
    start_date: str = Query(DEFAULT_START_DATE),
    end_date: str = Query(DEFAULT_END_DATE),
    force_refresh: bool = Query(False)
):
    """Получает краткую сводку по периоду."""
    cache_key = get_cache_key(start_date, end_date)
    
    if not force_refresh and cache_key in DATA_CACHE and is_cache_valid(DATA_CACHE[cache_key]):
        cached_data = DATA_CACHE[cache_key]["data"]
        detailed = cached_data["detailed"]
        free = cached_data["free"]
    else:
        raw_data = fetch_data_from_db(start_date, end_date)
        processed_data = process_data(raw_data)
        detailed = processed_data['detailed']
        free = processed_data['free']
    
    # Рассчитываем метрики
    total_auditories = detailed['Аудитория'].nunique() if len(detailed) > 0 else 0
    avg_attendance = detailed['Процент посещаемости (факт/план * 100%)'].mean() if len(detailed) > 0 else 0
    total_slots = len(free) + len(detailed)
    free_percentage = (len(free) / total_slots * 100) if total_slots > 0 else 0
    
    return {
        "period": {"start_date": start_date, "end_date": end_date},
        "total_auditories": int(total_auditories),
        "occupied_slots": int(len(detailed)),
        "free_slots": int(len(free)),
        "free_percentage": round(free_percentage, 2),
        "avg_attendance": round(avg_attendance, 2)
    }


@app.get("/api/cache/status")
async def get_cache_status():
    """Получает статус кэша."""
    status = {
        "cache_entries": len(DATA_CACHE),
        "entries": []
    }
    
    for key, entry in DATA_CACHE.items():
        age = (datetime.now() - entry["timestamp"]).total_seconds()
        status["entries"].append({
            "key": key[:8] + "...",
            "age_seconds": round(age, 2),
            "valid": is_cache_valid(entry),
            "timestamp": entry["timestamp"].isoformat()
        })
    
    return status


@app.delete("/api/cache/clear")
async def clear_cache():
    """Очищает весь кэш."""
    DATA_CACHE.clear()
    return {"message": "Кэш очищен", "entries_cleared": len(DATA_CACHE)}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
