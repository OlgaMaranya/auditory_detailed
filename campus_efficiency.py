import json
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime, timedelta
import os

# === Загрузка конфигурации ===
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

START_DATE = config['period']['start_date']
END_DATE = config['period']['end_date']
EXCLUDED_BUILDINGS = config['excluded_buildings']
DB_CONFIG = config['db_config']

# === Подключение к базе данных ===
password_encoded = urllib.parse.quote_plus(DB_CONFIG['password'])
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{password_encoded}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset=cp1251"

engine = create_engine(DATABASE_URL)

# === Запросы ===
QUERY_AUDITORIES = f"""
SELECT a.id, a.name, at.name as type_name
FROM auditories a
JOIN aud_types at ON a.id_type = at.id
JOIN building b ON a.building_id = b.id
JOIN departments d ON b.dep_id = d.id
WHERE d.filial_id = 1
  AND b.id NOT IN ({','.join(map(str, EXCLUDED_BUILDINGS))});
"""

# === Запрос: получить занятые слоты (с датой, именами групп и типов) ===
QUERY_OCCUPIED_SLOTS = f"""
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
  AND b.id NOT IN ({','.join(map(str, EXCLUDED_BUILDINGS))})
  AND sd.lec_date >= '{START_DATE}'
  AND sd.lec_date <= '{END_DATE}';
"""

# === Запрос: получить посещаемость ===
QUERY_PRESENCE = """
SELECT sched_done_id, COUNT(person_id) as present_count
FROM sched_presence
GROUP BY sched_done_id;
"""

# === Запрос: получить количество студентов в группе на дату из courses ===
QUERY_COURSE_STUDENTS = f"""
SELECT
    c.group_id,
    c.subgroup,
    COUNT(*) as plan_count
FROM courses c
WHERE c.s_year = 2025
  AND (c.semestr %% 2) = 1
  AND c.active IN (0, 1)
  AND c.archived_id NOT IN (4, 5)
  AND c.date_create <= '{END_DATE}'
  AND (c.date_end IS NULL OR c.date_end >= '{START_DATE}')
GROUP BY c.group_id, c.subgroup;
"""

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

def get_week_type_from_date(date_str):
    date = pd.to_datetime(date_str, format='mixed', dayfirst=True)
    start_date = datetime(2025, 9, 1)
    start_offset = (date.weekday()) % 7
    start_monday = date - pd.Timedelta(days=start_offset)
    monday_of_week = date - pd.Timedelta(days=(date.weekday()) % 7)
    weeks_passed = int((monday_of_week - start_monday).days / 7)
    return 'числитель' if weeks_passed % 2 == 0 else 'знаменатель'

def main():
    print(f"🔍 Подключение к базе данных...")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Подключение успешно.")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    try:
        print(f"📥 Загрузка аудиторий...")
        df_auditories = pd.read_sql(QUERY_AUDITORIES, engine)
        print(f"✅ Загружено {len(df_auditories)} аудиторий (теперь с типами).")

        print(f"📥 Загрузка занятых слотов...")
        df_occupied = pd.read_sql(QUERY_OCCUPIED_SLOTS, engine)
        print(f"✅ Загружено {len(df_occupied)} занятых слотов.")

        print(f"📥 Загрузка посещаемости...")
        try:
            df_presence = pd.read_sql(QUERY_PRESENCE, engine)
            print(f"✅ Загружено {len(df_presence)} записей посещаемости.")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки посещаемости: {e}")
            df_presence = pd.DataFrame({'sched_done_id': [], 'present_count': []})

        print(f"📥 Загрузка количества студентов в группах из courses...")
        try:
            df_courses = pd.read_sql(QUERY_COURSE_STUDENTS, engine)
            print(f"✅ Загружено {len(df_courses)} записей из courses.")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки courses: {e}")
            df_courses = pd.DataFrame({'group_id': [], 'subgroup': [], 'plan_count': []})

        # === Сопоставляем посещаемость с занятиями ===
        df_occupied = df_occupied.merge(df_presence, left_on='sched_done_id', right_on='sched_done_id', how='left')
        df_occupied['present_count'] = df_occupied['present_count'].fillna(0)

        # === Сопоставляем плановые студенты из courses (с учётом подгрупп) ===
        # Если sched_done.subgroup = 0 (вся группа), то ищем в courses.subgroup = 0 (вся группа) или 1,2 (подгруппы)
        # Если sched_done.subgroup = 1 или 2, то ищем только ту же подгруппу

        # Создадим временную колонку для совпадения
        df_occupied = df_occupied.merge(
            df_courses[['group_id', 'subgroup', 'plan_count']],
            left_on=['group_id', 'subgroup'],
            right_on=['group_id', 'subgroup'],
            how='left'
        )
        df_occupied['plan_count'] = df_occupied['plan_count'].fillna(0)

        # === Обработка случая: sched_done.subgroup = 0 (вся группа) → нужно сложить plan_count всех подгрупп ===
        # Сбросим plan_count, чтобы пересчитать правильно
        df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] = 0

        for idx, row in df_occupied.iterrows():
            group_id = row['group_id']
            sched_subgroup = row['subgroup']
            if sched_subgroup == 0:
                # Если sched_done.subgroup = 0 → суммируем plan_count всех подгрупп этой группы
                total_plan = df_courses[
                    (df_courses['group_id'] == group_id) &
                    (df_courses['subgroup'] >= 0)  # любая подгруппа
                ]['plan_count'].sum()
                df_occupied.at[idx, 'Общее количество студентов с учетом подгрупп (ПЛАН)'] = total_plan
            else:
                # Если sched_done.subgroup = 1 или 2 → берем только ту подгруппу
                plan_row = df_courses[
                    (df_courses['group_id'] == group_id) &
                    (df_courses['subgroup'] == sched_subgroup)
                ]
                if not plan_row.empty:
                    df_occupied.at[idx, 'Общее количество студентов с учетом подгрупп (ПЛАН)'] = plan_row.iloc[0]['plan_count']

        # === Вычисляем Факт, Посещаемость ===
        df_occupied['Фактическое посещение (ФАКТ)'] = df_occupied['present_count']
        df_occupied['Процент посещаемости (факт/план * 100%)'] = (
            df_occupied['Фактическое посещение (ФАКТ)'] / df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] * 100
        ).round(2)
        df_occupied['Разница между планом и фактом'] = (
            df_occupied['Общее количество студентов с учетом подгрупп (ПЛАН)'] - df_occupied['Фактическое посещение (ФАКТ)']
        )

        # === Добавляем столбцы: числитель или знаменатель, день недели, пара ===
        df_occupied['datetime_parsed'] = pd.to_datetime(df_occupied['lec_date'], format='mixed', dayfirst=True)
        df_occupied['time_part'] = df_occupied['datetime_parsed'].dt.time.astype(str)
        df_occupied['Пара'] = df_occupied['time_part'].apply(
            lambda x: TIME_SLOTS.get(x, None)
        )
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

        # === Добавляем day_of_week_num, time_start, week_type для совместимости с merge ===
        df_occupied['day_of_week_num'] = df_occupied['datetime_parsed'].dt.dayofweek + 2  # Monday=2, ..., Sunday=8
        df_occupied['time_start'] = df_occupied['time_part']
        df_occupied['week_type'] = df_occupied['Тип недели']

        print("\n📋 Примеры из df_occupied (с рассчитанными колонками):")
        print(df_occupied[['auditory_name', 'lec_date', 'group_name', 'subgroup', 'Общее количество студентов с учетом подгрупп (ПЛАН)', 'Фактическое посещение (ФАКТ)', 'Процент посещаемости (факт/план * 100%)']].head(10))

        # === Генерация всех возможных слотов (только в пределах периода) ===
        print(f"\n🔄 Генерация всех возможных слотов (с типом помещения)...")

        start_dt = datetime.strptime(START_DATE, '%Y-%m-%d')
        end_dt = datetime.strptime(END_DATE, '%Y-%m-%d')

        all_combinations = []
        for _, row in df_auditories.iterrows():
            auditory_id = row['id']
            auditory_name = row['name']
            type_name = row['type_name']
            current = start_dt
            while current <= end_dt:
                week_type = get_week_type_from_date(current)
                day_of_week_num = current.weekday() + 2  # 0=пн → 2, ..., 6=вс → 8 (но 8 не входит)
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
                current += timedelta(days=1)

        df_all_slots = pd.DataFrame(all_combinations)

        # === Удаляем дубликаты перед merge ===
        df_all_slots = df_all_slots.drop_duplicates(subset=['auditory_id', 'day_of_week_num', 'time_start', 'week_type'])

        # === Сопоставляем занятые слоты ===
        print(f"\n🔍 Объединение с занятыми слотами...")
        df_all_slots = df_all_slots.merge(
            df_occupied[['auditory_id', 'day_of_week_num', 'time_start', 'week_type']],
            how='left',
            left_on=['auditory_id', 'day_of_week_num', 'time_start', 'week_type'],
            right_on=['auditory_id', 'day_of_week_num', 'time_start', 'week_type'],
            indicator=True
        )

        # === Фильтруем только свободные слоты ===
        df_free_slots = df_all_slots[df_all_slots['_merge'] == 'left_only'].copy()
        df_free_slots.drop(columns=['_merge'], inplace=True)
        df_free_slots.rename(columns={'time_start': 'время начала'}, inplace=True)
        df_free_slots['статус'] = 'Свободна'

        print(f"✅ Найдено {len(df_free_slots)} свободных слотов (с учётом типов помещений).")

        # === Сохраняем в CSV ===
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        free_slots_file = f"free_auditory_slots_{timestamp}.csv"

        df_free_slots[['auditory_name', 'type_name', 'day_name', 'pair_number', 'время начала', 'week_type', 'статус']].to_csv(
            free_slots_file,
            sep=';',
            decimal=',',
            index=False,
            encoding='utf-8-sig',
            quoting=1
        )

        detailed_file = f"auditory_detailed_{timestamp}.csv"

        df_occupied_to_save = df_occupied.copy()
        df_occupied_to_save.rename(columns={
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

        # Убедимся, что колонки в правильном порядке
        cols_order = [
            'auditory_id',
            'Аудитория',
            'Тип помещения',
            'Корпус',
            'Факультет',
            'Дата/время занятия',
            'Дисциплина',
            'Список групп на этом занятии',
            'Информация о подгруппах',
            'Тип занятия',
            'ID аудитории',
            'Присутствие преподавателя',
            'Полное отсутствие студентов',
            'Ошибка расписания',
            'Специальный тип',
            'datetime_parsed',
            'time_part',
            'Пара',
            'Тип недели',
            'День недели',
            'Общее количество студентов с учетом подгрупп (ПЛАН)',
            'Фактическое посещение (ФАКТ)',
            'Процент посещаемости (факт/план * 100%)',
            'Разница между планом и фактом',
        ]

        for col in cols_order:
            if col not in df_occupied_to_save.columns:
                df_occupied_to_save[col] = 0

        df_occupied_to_save = df_occupied_to_save[cols_order]

        df_occupied_to_save.to_csv(
            detailed_file,
            sep=';',
            decimal=',',
            index=False,
            encoding='utf-8-sig',
            quoting=1
        )

        print(f"💾 Сохранено:")
        print(f"   → {free_slots_file}")
        print(f"   → {detailed_file}")

        print("\n📋 Пример занятых слотов (с рассчитанными колонками):")
        print(df_occupied_to_save.head(10))

    except Exception as e:
        print(f"❌ Ошибка при выполнении: {e}")
        raise

if __name__ == "__main__":
    main()