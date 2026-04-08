import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, dash_table, callback
import os
import glob
from datetime import datetime, timedelta

# === Загрузка конфигурации ===
with open('config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

START_DATE = config['period']['start_date']
END_DATE = config['period']['end_date']

# === Загрузка CSV ===
def load_latest_csv(pattern):
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"Файл по шаблону '{pattern}' не найден!")
    print(f"Загружаю файл: {max(files, key=os.path.getctime)}")
    return pd.read_csv(max(files, key=os.path.getctime), sep=';', decimal=',')

df_free  = load_latest_csv("free_auditory_slots_*.csv")
df_detailed = load_latest_csv("auditory_detailed_*.csv")

# === Нормализуем имена колонок ===
df_detailed.columns = [col.strip().replace('\n', '').replace('\r', '') for col in df_detailed.columns]
df_free.columns = [col.strip().replace('\n', '').replace('\r', '') for col in df_free.columns]

# === Переименуем колонки в df_free ===
df_free.rename(columns={
    'auditory_name': 'Аудитория',
    'type_name': 'Тип помещения',
    'day_name': 'День недели',
    'pair_number': 'Пара',
    'время начала': 'Время начала',
    'week_type': 'Тип недели',
    'статус': 'Статус',
}, inplace=True)

# === Порядок дней недели ===
day_order = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб']
df_free['День недели'] = pd.Categorical(df_free['День недели'], categories=day_order, ordered=True)

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

def time_to_pair(time_str):
    return TIME_SLOTS.get(time_str, None)

def get_week_type_from_date(date_str):
    date = pd.to_datetime(date_str, format='mixed', dayfirst=True)
    start_date = datetime(2025, 9, 1)
    start_offset = (start_date.weekday()) % 7
    start_monday = start_date - pd.Timedelta(days=start_offset)
    monday_of_week = date - pd.Timedelta(days=(date.weekday()) % 7)
    weeks_passed = int((monday_of_week - start_monday).days / 7)
    return 'числитель' if weeks_passed % 2 == 0 else 'знаменатель'

# === Добавляем колонки в df_detailed ===
df_detailed['datetime_parsed'] = pd.to_datetime(df_detailed['Дата/время занятия'], format='mixed', dayfirst=True)
df_detailed['time_part'] = df_detailed['datetime_parsed'].dt.time.astype(str)
df_detailed['Пара'] = df_detailed['time_part'].apply(time_to_pair)
df_detailed['Тип недели'] = df_detailed['datetime_parsed'].apply(lambda x: get_week_type_from_date(x).lower().strip())
df_detailed['День недели'] = df_detailed['datetime_parsed'].dt.day_name()
day_map = {
    'Monday': 'Пн',
    'Tuesday': 'Вт',
    'Wednesday': 'Ср',
    'Thursday': 'Чт',
    'Friday': 'Пт',
    'Saturday': 'Сб',
    'Sunday': 'Вс'
}
df_detailed['День недели'] = df_detailed['День недели'].map(day_map)

# === Генерация паттерна использования ===
min_date = pd.to_datetime(START_DATE)
max_date = pd.to_datetime(END_DATE)

start_monday = min_date - pd.Timedelta(days=min_date.weekday())
end_monday = max_date - pd.Timedelta(days=max_date.weekday())
weeks = pd.date_range(start=start_monday, end=end_monday, freq='7D')

def week_type_for_date(date):
    start_sem = datetime(2025, 9, 1)
    start_monday_sem = start_sem - pd.Timedelta(days=start_sem.weekday())
    weeks_passed = int((date - start_monday_sem).days / 7)
    return 'числитель' if weeks_passed % 2 == 0 else 'знаменатель'

# Создаём полную сетку слотов
auditories = df_detailed['Аудитория'].unique()
all_slots = []
for w in weeks:
    wt = week_type_for_date(w).lower().strip()
    for aud in auditories:
        for day_name in day_order:
            for pair_num in range(1, 10):
                all_slots.append({
                    'Аудитория': aud,
                    'День недели': day_name,
                    'Пара': pair_num,
                    'Тип недели': wt,
                    'week_start': w.strftime('%Y-%m-%d').strip(),
                })

df_all_slots = pd.DataFrame(all_slots)

# Подготовка реальных занятий
df_detailed_for_pattern = df_detailed.copy()

# === Безопасная функция ===
def round_to_nearest_monday(date):
    mon = date - pd.Timedelta(days=date.weekday())
    available_mondays = weeks.normalize()
    diff = (available_mondays - mon.normalize()).to_series().abs()
    idx = diff.argmin()
    closest_monday = available_mondays[idx]
    return closest_monday.strftime('%Y-%m-%d')

df_detailed_for_pattern['week_start'] = df_detailed_for_pattern['datetime_parsed'].apply(round_to_nearest_monday)

slots_in_use = df_detailed_for_pattern[['Аудитория', 'День недели', 'Пара', 'Тип недели', 'week_start']].drop_duplicates()

# Агрегируем
merged = df_all_slots.merge(
    slots_in_use,
    on=['Аудитория', 'День недели', 'Пара', 'Тип недели', 'week_start'],
    how='left',
    indicator=True
)
merged['is_occupied'] = merged['_merge'] == 'both'

# === Добавляем Тип помещения в merged из df_detailed_for_pattern ===
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

print("=== pattern.columns ===")
print(pattern.columns.tolist())

# === Инициализация приложения ===
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "📊 Отчёт по аудиториям"

# === Layout ===
app.layout = html.Div([
    html.H1("📊 Отчёт по аудиториям", style={'textAlign': 'center'}),

    dcc.Tabs(id="tabs", value='tab-free', children=[
        dcc.Tab(label='Свободные слоты', value='tab-free'),
        dcc.Tab(label='Загрузка аудиторий', value='tab-load'),
        dcc.Tab(label='Паттерны использования', value='tab-pattern')
    ]),

    html.Div(id='tab-content')
])

# === Callback для переключения вкладок ===
@app.callback(
    Output('tab-content', 'children'),
    Input('tabs', 'value')
)
def render_tab(tab):
    if tab == 'tab-free':
        # Порядок колонок: Аудитория, Тип помещения, День недели, Пара, Тип недели, Время начала, Статус
        cols_order = ['Аудитория', 'Тип помещения', 'День недели', 'Пара', 'Тип недели', 'Время начала', 'Статус']
        cols_order += [c for c in df_free.columns if c not in cols_order]
        df_free_ordered = df_free[cols_order]

        # Опции для фильтра по типу помещения
        type_options = [{'label': t, 'value': t} for t in sorted(df_free['Тип помещения'].dropna().unique())]

        return html.Div([
            html.Div([
                dcc.Dropdown(id='auditory-dropdown-free',
                             options=[{'label': a, 'value': a} for a in sorted(df_free['Аудитория'].unique())],
                             placeholder="Аудитория...", multi=True),
                dcc.Dropdown(id='type-dropdown-free',
                             options=type_options,
                             placeholder="Тип помещения...", multi=True),
                dcc.Dropdown(id='day-dropdown-free',
                             options=[{'label': d, 'value': d} for d in day_order],
                             placeholder="День недели...", multi=True),
                dcc.Dropdown(id='week-type-dropdown-free',
                             options=[{'label': 'Числитель', 'value': 'числитель'},
                                      {'label': 'Знаменатель', 'value': 'знаменатель'}],
                             placeholder="Тип недели...", multi=True),
            ], style={'padding': '10px'}),
            html.H3("Свободные слоты"),
            dash_table.DataTable(
                id='table-free',
                columns=[{"name": col, "id": col} for col in df_free_ordered.columns],
                data=df_free_ordered.to_dict('records'),
                sort_action="native",
                filter_action="native",
                page_size=15,
                style_cell={'textAlign': 'left', 'whiteSpace': 'normal', 'height': 'auto'},
                style_header={'fontWeight': 'bold'},
            ),
            dcc.Graph(id='bar-free'),
            dcc.Graph(id='heatmap-free'),
            dcc.Graph(id='treemap-free')
        ])

    elif tab == 'tab-load':
        # Порядок: Дата/время, Аудитория, Тип помещения, День недели, Пара, Тип недели, остальные
        cols_order = ['Дата/время занятия', 'Аудитория', 'Тип помещения', 'День недели', 'Пара', 'Тип недели']
        cols_order += [c for c in df_detailed.columns if c not in cols_order and c != 'datetime_parsed']
        df_detailed_ordered = df_detailed[cols_order]

        # Опции для фильтра по типу помещения
        type_options = [{'label': t, 'value': t} for t in sorted(df_detailed['Тип помещения'].dropna().unique())]

        return html.Div([
            html.Div([
                dcc.Dropdown(id='auditory-dropdown-detailed',
                             options=[{'label': a, 'value': a} for a in sorted(df_detailed['Аудитория'].unique())],
                             placeholder="Аудитория...", multi=True),
                dcc.Dropdown(id='type-dropdown-detailed',
                             options=type_options,
                             placeholder="Тип помещения...", multi=True),
                dcc.Dropdown(id='lesson-type-dropdown',
                             options=[{'label': t, 'value': t} for t in sorted(df_detailed['Тип занятия'].dropna().unique())],
                             placeholder="Тип занятия...", multi=True),
            ], style={'padding': '10px'}),
            html.H3("Загрузка аудиторий"),
            dash_table.DataTable(
                id='table-detailed',
                columns=[{"name": col, "id": col} for col in df_detailed_ordered.columns],
                data=df_detailed_ordered.to_dict('records'),
                sort_action="native",
                filter_action="native",
                page_size=15,
                style_cell={'textAlign': 'left', 'whiteSpace': 'normal', 'height': 'auto'},
                style_header={'fontWeight': 'bold'},
            ),
            dcc.Graph(id='scatter-load'),
            dcc.Graph(id='hist-attendance'),
            dcc.Graph(id='bar-loaded')
        ])

    elif tab == 'tab-pattern':
        # Опции для фильтра по типу помещения
        type_options = [{'label': t, 'value': t} for t in sorted(pattern['Тип помещения'].dropna().unique())]

        return html.Div([
            html.Div([
                dcc.Dropdown(id='auditory-dropdown-pattern',
                             options=[{'label': a, 'value': a} for a in sorted(pattern['Аудитория'].dropna().unique())],
                             placeholder="Аудитория...", multi=True),
                dcc.Dropdown(id='type-dropdown-pattern',
                             options=type_options,
                             placeholder="Тип помещения...", multi=True),
                dcc.Dropdown(id='day-dropdown-pattern',
                             options=[{'label': d, 'value': d} for d in day_order],
                             placeholder="День недели...", multi=True),
                dcc.Dropdown(id='week-type-dropdown-pattern',
                             options=[{'label': 'Числитель', 'value': 'числитель'},
                                      {'label': 'Знаменатель', 'value': 'знаменатель'}],
                             placeholder="Тип недели...", multi=True),
            ], style={'padding': '10px'}),
            html.H3("Паттерны использования аудиторий"),
            dash_table.DataTable(
                id='table-pattern',
                columns=[{"name": col, "id": col} for col in pattern.columns],
                data=pattern.to_dict('records'),
                sort_action="native",
                filter_action="native",
                page_size=15,
                style_cell={'textAlign': 'left', 'whiteSpace': 'normal', 'height': 'auto'},
                style_header={'fontWeight': 'bold'},
            ),
            dcc.Graph(id='heatmap-pattern')
        ])

# === Callback для вкладки "Свободные слоты" ===
@app.callback(
    [Output('table-free', 'data'), Output('bar-free', 'figure'), Output('heatmap-free', 'figure'), Output('treemap-free', 'figure')],
    [Input('auditory-dropdown-free', 'value'), Input('type-dropdown-free', 'value'), Input('day-dropdown-free', 'value'), Input('week-type-dropdown-free', 'value')]
)
def update_free(aud, room_types, day, week):
    dff = df_free.copy()
    if aud: dff = dff[dff['Аудитория'].isin(aud)]
    if room_types: dff = dff[dff['Тип помещения'].isin(room_types)]
    if day: dff = dff[dff['День недели'].isin(day)]
    if week: dff = dff[dff['Тип недели'].isin(week)]

    # Переставим колонки
    cols_order = ['Аудитория', 'Тип помещения', 'День недели', 'Пара', 'Тип недели', 'Время начала', 'Статус']
    cols_order += [c for c in dff.columns if c not in cols_order]
    dff_ordered = dff[cols_order]

    top = dff['Аудитория'].value_counts().head(10).reset_index()
    top.columns = ['Аудитория', 'Кол-во']
    fig_bar = px.bar(top, x='Кол-во', y='Аудитория', orientation='h', title="Топ-10 свободных аудиторий")

    # === Тепловая карта: Свободные слоты ===
    heatmap = dff.groupby(['День недели', 'Пара']).size().reset_index(name='count')
    if heatmap.empty:
        fig_heat = go.Figure()
        fig_heat.update_layout(title="Нет данных для отображения", height=300)
    else:
        heatmap['День недели'] = pd.Categorical(heatmap['День недели'], categories=day_order, ordered=True)
        heatmap = heatmap.sort_values('День недели')
        pivot = heatmap.pivot(index='День недели', columns='Пара', values='count').fillna(0)

        # Убедимся, что pivot имеет правильные колонки (1–9)
        full_range = pd.RangeIndex(1, 10)
        pivot = pivot.reindex(columns=full_range, fill_value=0)

        # Проверка: если pivot пустой или имеет 0 строк — создаем пустой график
        if pivot.shape[0] == 0 or pivot.shape[1] == 0:
            fig_heat = go.Figure()
            fig_heat.update_layout(title="Нет данных для отображения", height=300)
        else:
            fig_heat = px.imshow(
                pivot.values,
                x=list(range(1, 10)),
                y=pivot.index,
                color_continuous_scale='Blues',
                title="Свободные слоты по дням и парам"
            )

    treemap = dff.groupby('Аудитория').size().reset_index(name='count')
    fig_tree = px.treemap(treemap, path=['Аудитория'], values='count', title="Распределение свободных слотов")

    return dff_ordered.to_dict('records'), fig_bar, fig_heat, fig_tree

# === Callback для вкладки "Загрузка" ===
@app.callback(
    [Output('table-detailed', 'data'), Output('scatter-load', 'figure'), Output('hist-attendance', 'figure'), Output('bar-loaded', 'figure')],
    [Input('auditory-dropdown-detailed', 'value'), Input('type-dropdown-detailed', 'value'), Input('lesson-type-dropdown', 'value')]
)
def update_load(aud, room_types, typ):
    dff = df_detailed.copy()
    if aud: dff = dff[dff['Аудитория'].isin(aud)]
    if room_types: dff = dff[dff['Тип помещения'].isin(room_types)]
    if typ: dff = dff[dff['Тип занятия'].isin(typ)]

    # Переставим колонки
    cols_order = ['Дата/время занятия', 'Аудитория', 'Тип помещения', 'День недели', 'Пара', 'Тип недели']
    cols_order += [c for c in dff.columns if c not in cols_order and c != 'datetime_parsed']
    dff_ordered = dff[cols_order]

    plan_col = 'Общее количество студентов с учетом подгрупп (ПЛАН)'
    fact_col = 'Фактическое посещение (ФАКТ)'
    rate_col = 'Процент посещаемости (факт/план * 100%)'

    fig_scatter = px.scatter(dff, x=plan_col, y=fact_col, color='Аудитория', title="План vs Факт")
    fig_scatter.update_layout(xaxis_title="План", yaxis_title="Факт")

    fig_hist = px.histogram(dff, x=rate_col, nbins=50, title="Распределение посещаемости (%)")
    fig_hist.update_layout(xaxis_title="Посещаемость (%)", yaxis_title="Количество")

    overloaded = dff[dff[fact_col] > dff[plan_col]]
    top_over = overloaded['Аудитория'].value_counts().head(10).reset_index()
    top_over.columns = ['Аудитория', 'Кол-во']
    fig_bar_loaded = px.bar(top_over, x='Кол-во', y='Аудитория', orientation='h', title="Топ-10 аудиторий с превышением плана")

    return dff_ordered.to_dict('records'), fig_scatter, fig_hist, fig_bar_loaded

# === Callback для вкладки "Паттерны использования" ===
@app.callback(
    [Output('table-pattern', 'data'), Output('heatmap-pattern', 'figure')],
    [Input('auditory-dropdown-pattern', 'value'), Input('type-dropdown-pattern', 'value'), Input('day-dropdown-pattern', 'value'), Input('week-type-dropdown-pattern', 'value')]
)
def update_pattern(aud, room_types, day, week):
    dff = pattern.copy()
    if aud: dff = dff[dff['Аудитория'].isin(aud)]
    if room_types: dff = dff[dff['Тип помещения'].isin(room_types)]
    if day: dff = dff[dff['День недели'].isin(day)]
    if week: dff = dff[dff['Тип недели'].isin(week)]

    # Тепловая карта: разделённая по типу недели
    fig_heat = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Числитель", "Знаменатель"],
        shared_yaxes=True,
        horizontal_spacing=0.05
    )

    for i, wt in enumerate(['числитель', 'знаменатель']):
        subset = dff[dff['Тип недели'] == wt]
        if subset.empty:
            continue
        heatmap_data = subset.groupby(['День недели', 'Пара'])['Процент занятости'].mean().reset_index()
        heatmap_data['День недели'] = pd.Categorical(heatmap_data['День недели'], categories=day_order, ordered=True)
        heatmap_data = heatmap_data.sort_values('День недели')
        pivot = heatmap_data.pivot(index='День недели', columns='Пара', values='Процент занятости')
        full_range = pd.RangeIndex(1, 10)
        pivot = pivot.reindex(columns=full_range, fill_value=0)

        fig_heat.add_trace(
            go.Heatmap(
                z=pivot.values,
                x=list(range(1, 10)),
                y=pivot.index,
                name=wt,
                showscale=True if i == 0 else False,
                coloraxis="coloraxis"
            ),
            row=1, col=i+1
        )

    fig_heat.update_layout(
        title="Занятость по дням и парам (%)",
        coloraxis=dict(colorscale='Reds', cmin=0, cmax=100),
        height=500
    )
    fig_heat.update_xaxes(title_text="Пара", row=1, col=1)
    fig_heat.update_xaxes(title_text="Пара", row=1, col=2)
    fig_heat.update_yaxes(title_text="День недели", row=1, col=1)

    return dff.to_dict('records'), fig_heat

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8050)