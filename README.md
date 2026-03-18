# from_postgresql_to_mysql

Перенос витрины из PostgreSQL в MySQL

СУБД подняты в Docker(см. docker-compose)

Ссылка на базу данных: https://postgrespro.ru/education/demodb

Скачиваем файл и переносим его в контейнер с PostgreSQL:

```bash
docker cp demo-20250901-3m.sql postgres:/tmp/demo-20250901-3m.sql

```
Далее в контейнере разворачиваем базу:
```bash
psql -U postgres -f demo-20250901-3m.sql
```
Создаем витрину:
```sql
create table if not exists passenger_mart(
    passenger_id text,
    passenger_name text,
    city_to text,
    total_revenue_by_city int4,
    cnt_of_flights_to_city int4,
    total_revenue int4,
    cnt_of_flights int4,
    percent_of_revenue_by_cities decimal(10, 1),
    percent_of_flights_by_cities decimal(10, 1)
);

insert into passenger_mart
select *, sum(total_revenue_by_city) over(PARTITION BY passenger_id) as total_revenue,
       sum(cnt_of_flights_to_city) over(PARTITION BY passenger_id) as cnt_of_flights,
       ROUND(total_revenue_by_city * 100 / sum(total_revenue_by_city) over(PARTITION BY passenger_id), 1) as percent_of_revenue_by_cities,
       ROUND(cnt_of_flights_to_city::numeric * 100 / sum(cnt_of_flights_to_city) over(PARTITION BY passenger_id), 1) as percent_of_flights_by_cities
from (select passenger_id,
       passenger_name,
       a.city as city_to,
       sum(price) as total_revenue_by_city,
       count(*) as cnt_of_flights_to_city
from tickets as t
join segments as sgm on t.ticket_no = sgm.ticket_no
join timetable as tmtbl on sgm.flight_id = tmtbl.flight_id and tmtbl.actual_arrival is not null
join airports as a on tmtbl.arrival_airport = a.airport_code
group by passenger_id, passenger_name, city) as t1;
```

Устанавливаем необохдимые зависимости в venv(см. req.txt) и запускаем скрипт Python.
