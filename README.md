# Fraud Scoring Monitoring & Observability

Сервис скоринга фродовых транзакций с полным контуром наблюдаемости: Kafka, Scoring, PostgreSQL, Prometheus, Grafana.

Проект реализует потоковую обработку данных, автоматическое переобучение (подхват модели без перезапуска контейнера) и детальный мониторинг метрик, включая алертинг и BI-дашборды.

## Быстрый старт

Для запуска всей инфраструктуры выполните в корне проекта:

    docker compose up -d --build

Первый запуск может занять 3–5 минут (скачивание образов). После старта подождите 30 секунд, пока Kafka и PostgreSQL полностью инициализируются, а сервисы начнут обмен сообщениями.

## Адреса и доступы

| Сервис | URL |
|---|---|
| **Grafana** | http://localhost:3000 (admin/admin) | 
| **Prometheus** | http://localhost:9090 |
| **Scoring /metrics** | http://localhost:8000/metrics | 
| **Writer /metrics** | http://localhost:8001/metrics | 
| **Node Exporter** | http://localhost:9100/metrics |
| **PostgreSQL** | postgres:5432 (db/user/pass = fraud) |

## Дашборды Grafana

В проекте настроен автоматический provisioning. После запуска Grafana в меню **Dashboards** будут доступны 4 дашборда:

1. **scoring** — Основной дашборд скоринга:
   - TPS (транзакций в секунду), время обработки (p50/p95/p99).
   - Доля фрода (с цветовыми порогами).
   - Текущая версия модели (всегда показывает только актуальную).
   - Kafka consumer lag (отставание консьюмера).
   - Heatmap распределения скоров.
   - **Аннотация смены версии модели** (оранжевая линия на графиках).
2. **writer** — Запись в PostgreSQL:
   - Скорость записи, время записи (p50/p95/p99), число записанного фрода.
   - Heatmap записанных скоров.
3. **system** — Системные метрики (Node Exporter):
   - CPU, RAM, Сеть (RX/TX), Занятость диска.
4. **business** — BI-дашборд на данных из PostgreSQL:
   - Таблица: количество скоров и доля фрода в разрезе версий модели.
   - Barplot: плотность распределения скоров с фильтрами по Штату и Мерчанту.
   - Timeseries: TPS по выбранным транзакциям.
   - Barplot: средняя доля фрода по cat_id (за последние 1000 транзакций).
   - Barplot (бизнес-инсайт): доля фрода по суммовым корзинам.

## Метрики

### Scoring-сервис
* scoring_transactions_total — Counter. Общее число обработанных транзакций.
* scoring_processing_seconds — Histogram. Время обработки одной транзакции.
* scoring_score — Histogram. Распределение скоров (0.0 – 1.0).
* scoring_fraud_total — Counter. Число транзакций, помеченных как фрод.
* scoring_fraud_share — Gauge. Доля фрода среди всех обработанных.
* scoring_model_reload_total{version} — Counter. Сколько раз подхватывалась модель.
* scoring_model_info{version} — Gauge. Равен 1 только для текущей активной версии модели.
* scoring_model_switch_annotation{version} — Gauge. Равен 1 в течение 30 секунд после смены модели (используется для аннотаций).
* scoring_kafka_consumer_lag_messages — **Gauge (Доп. метрика)**. Показывает отставание Kafka consumer от последнего оффсета.

### Writer-сервис
* writer_scores_written_total — Counter. Число записанных строк в БД.
* writer_write_seconds — Histogram. Время записи одной строки.
* writer_fraud_total — Counter. Число записанных фрод-транзакций.
* writer_score — Histogram. Распределение записанных скоров.

## Алерты

Правила алертов подключаются автоматически (provisioning) и лежат в grafana/provisioning/alerting/rules.yml. 
Смотреть: **Grafana/Alerting/Alert rules/Alerts**.

*Примечание: у правил установлен noDataState: OK, так как операторы сравнения в PromQL (>, ==) возвращают пустой результат, когда условие не выполнено.*

| Правило | Условие (PromQL) | Порог (for) |
|---|---|---|
| **HighFraudShare** | scoring_fraud_share > 0.25 | 1 минута |
| **ScoringStreamStopped** | rate(scoring_transactions_total[1m]) == 0 | 30 секунд |
| **SlowScoringP95** | histogram_quantile(0.95, rate(...)) > 0.5 | 1 минута |

### Как проверить срабатывание алертов:
1. **Проверка остановки потока**:
       docker compose stop producer
       sleep 120
   Откройте Grafana/Alerting. Правило ScoringStreamStopped перейдет в статус **Firing** (красный). 
   Вернуть обратно: docker compose start producer.
2. **Проверка высокого фрода**:
   Временно измените порог в rules.yml на > 0.01, перезапустите Grafana (docker compose restart grafana). Через минуту алерт сработает. Верните порог обратно.

## Как проверить, что всё работает

### 1. Метрики растут вместе с потоком
    A=$(curl -s http://localhost:8000/metrics | awk '/^scoring_transactions_total /{print $2}')
    sleep 3
    B=$(curl -s http://localhost:8000/metrics | awk '/^scoring_transactions_total /{print $2}')
    echo "Было: $A, Стало: $B"

Число должно увеличиваться. Также на дашборде scoring панель **TPS во времени** будет показывать стабильный график (по умолчанию 20 ops/s).

### 2. Подхват новой версии модели (без перезапуска)
Сервис следит за mtime файла scoring/model.pkl. Чтобы сымитировать переобучение:
1. Сгенерируйте новую модель (выполняется внутри контейнера, чтобы не ставить зависимости на хост):
       docker compose exec -T scoring python3 -c "
       import joblib, numpy as np
       from sklearn.linear_model import LogisticRegression
       X = np.random.rand(100, 3)
       y = (X[:, 0] > 0.5).astype(int)
       joblib.dump(LogisticRegression().fit(X, y), '/app/model.pkl')
       "
2. Подождите 3–5 секунд. Проверьте health:
       curl -s http://localhost:8000/health
   Поле model_version изменится на новый таймстамп (например, v1794...).
3. Откройте дашборд scoring в течение 30 секунд. На всех графиках появится **оранжевая вертикальная линия** с подписью Model v...
4. В дашборде business в таблице появится новая строка с новой версией и пересчитанным fraud_pct.

### 3. Фильтры и BI-графики (PostgreSQL)
Откройте дашборд business:
- Сверху есть выпадающие списки **US State** и **Merchant**. Выберите CA и amazon — панели «Плотность распределения» и «TPS» пересчитаются только по этим транзакциям.
- Панель «Средняя доля фрода по cat_id» всегда показывает последние 1000 транзакций.
- Бизнес-инсайт по суммам показывает, что фрод чаще всего встречается в корзине 500+ (доходит до 30%).

## Структура проекта

    .
    ├── docker-compose.yml
    ├── prometheus/
    │   └── prometheus.yml
    ├── scoring/         
    ├── writer/           
    ├── producer/         
    └── grafana/
        ├── dashboards/   
        └── provisioning/
            ├── datasources/  
            ├── dashboards/   
            └── alerting/     
