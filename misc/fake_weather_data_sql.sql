
select * from weather_copy_2;


-- Creating fake data
create table weather_main_list (word varchar(50));

insert into weather_main_list (word)
values ('Clear'), ('Clouds'), ('Drizzle'), ('Snow'), ('Mist'), ('Fog'), ('Rain');

WITH RandomDate AS (
  SELECT
    ('2024-02-16'::DATE - FLOOR(RANDOM() * 10)::INTEGER) AS random_date
)

INSERT INTO weather_copy_2 (longitude, latitude, country_id, country, city, main_temp, main_feels_like, main_temp_min,
                          main_temp_max, date_vilnius, date_local, timezone, sunrise_local, sunset_local, weather_id,
                          weather_main, weather_description, weather_icon, pressure, humidity, wind_speed,
                          wind_deg, clouds, visibility, base, sys_type, sys_id, cod)
SELECT
    distinct on (public.weather_data.longitude)
    public.weather_data.longitude,
    public.weather_data.latitude,
    public.weather_data.country_id,
    public.weather_data.country, -- Copy 'country' from source_data
    public.weather_data.city, -- Copy 'city' from source_data
    (RANDOM() * 30 + 10)::NUMERIC(5, 2) AS main_temp,
    (RANDOM() * 10)::NUMERIC(5, 2) AS main_feels_like,
    (RANDOM() * 5)::NUMERIC(5, 2) AS main_temp_min,
    (RANDOM() * 10 + 20)::NUMERIC(5, 2) AS main_temp_max,
    random_date AT TIME ZONE 'UTC+2' AS date_vilnius,
    random_date as date_local,
    timezone,
    (random_date + (RANDOM() * 2 + 5) * INTERVAL '1 hour') AS sunrise_local,
    (random_date + (RANDOM() * 2 + 18) * INTERVAL '1 hour') AS sunset_local,
    ROUND(RANDOM() * 1000)::INTEGER,  -- Random 'weather_id'
    (SELECT word FROM weather_main_list ORDER BY RANDOM() LIMIT 1) AS weather_main,
    weather_description,
    weather_icon,  -- Copy 'weather_icon' from source_data
    (RANDOM() * 10 + 990)::NUMERIC(5, 2) AS pressure,
    (RANDOM() * 50 + 50)::NUMERIC(5, 2) AS humidity,
    (RANDOM() * 10 + 1)::NUMERIC(5, 2) AS wind_speed,
    (RANDOM() * 360)::NUMERIC(5, 2) AS wind_deg,
    clouds,
    visibility,
    base,  -- Copy 'base' from source_data
    sys_type,
    sys_id,
    cod
FROM
    weather_data, RandomDate;






create table weather_copy_2
as
    select * from weather_data;


select * from weather_copy_2;

insert into weather_data (longitude, latitude, country_id, country, city, main_temp, main_feels_like, main_temp_min, main_temp_max, date_vilnius, date_local,
                            timezone, sunrise_local, sunset_local, weather_id, weather_main, weather_description, weather_icon, pressure, humidity, wind_speed,
                            wind_deg, clouds, visibility, base, sys_type, sys_id, cod)
select
    distinct on (public.weather_copy_2.date_local)
    public.weather_copy_2.longitude, public.weather_copy_2.latitude, public.weather_copy_2.country_id, public.weather_copy_2.country,
    public.weather_copy_2.city, public.weather_copy_2.main_temp, public.weather_copy_2.main_feels_like, public.weather_copy_2.main_temp_min,
    public.weather_copy_2.main_temp_max, public.weather_copy_2.date_vilnius, public.weather_copy_2.date_local, public.weather_copy_2.timezone,
    public.weather_copy_2.sunrise_local, public.weather_copy_2.sunset_local, public.weather_copy_2.weather_id, public.weather_copy_2.weather_main,
    public.weather_copy_2.weather_description, public.weather_copy_2.weather_icon, public.weather_copy_2.pressure, public.weather_copy_2.humidity,
    public.weather_copy_2.wind_speed, public.weather_copy_2.wind_deg, public.weather_copy_2.clouds, public.weather_copy_2.visibility,
    public.weather_copy_2.base, public.weather_copy_2.sys_type, public.weather_copy_2.sys_id, public.weather_copy_2.cod
from weather_copy_2;





update weather_copy
set date = extract(epoch from date)
where date
between 2024-02-14 and 2024-02-15;

select date, city, count(city)
from weather_copy
group by city, date
having count(city) > 1
order by city, date;

DELETE FROM weather_copy
WHERE date > '2024-02-15 07:00:00.000000';