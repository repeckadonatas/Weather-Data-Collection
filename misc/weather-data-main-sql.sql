select * from weather_data
order by longitude, latitude, country, date_vilnius, date_local;

select longitude, latitude, city, main_temp, date_vilnius, date_local
from weather_data
group by longitude, latitude, city, date_vilnius, date_local, main_temp
order by longitude, latitude, city, date_vilnius, date_local;

alter table weather_data
alter column date_vilnius type timestamp without time zone;

SELECT to_timestamp(timezone)
from weather_data;

truncate table weather_data;

-- alter table weather_data
-- ADD COLUMN id INT GENERATED ALWAYS AS IDENTITY UNIQUE;
--
-- alter table weather_data
-- ADD PRIMARY KEY (id);

-- drop table weather_data;

select date, city, count(city)
from weather_data
group by city, date
having count(city) > 1
order by city, date;

-- deleting duplicate rows

create table weather_copy
as
    select * from weather_data;

select * from weather_copy;

select date, city, count(city)
from weather_copy
group by city, date
having count(city) > 1
order by city, date;

DELETE FROM weather_copy
WHERE date > '2024-02-15 07:00:00.000000';

-- drop table weather_copy;
-- drop table weather_temp;


create table weather_temp (like weather_data);

select * from weather_temp;

insert into weather_temp (country_id, country, city, longitude, latitude, main_temp, main_feels_like, main_temp_min, main_temp_max, date, timezone, sunrise, sunset, weather_id, weather_main, weather_description, weather_icon, pressure, humidity, wind_speed, wind_deg, clouds, visibility, base, sys_type, sys_id, cod)
select
    distinct on (public.weather_data.date)
    public.weather_data.country_id, public.weather_data.country, public.weather_data.city, public.weather_data.longitude, public.weather_data.latitude,
    public.weather_data.main_temp, public.weather_data.main_feels_like, public.weather_data.main_temp_min, public.weather_data.main_temp_max, public.weather_data.date,
    public.weather_data.timezone, public.weather_data.sunrise, public.weather_data.sunset, public.weather_data.weather_id, public.weather_data.weather_main,
    public.weather_data.weather_description, public.weather_data.weather_icon, public.weather_data.pressure, public.weather_data.humidity, public.weather_data.wind_speed,
    public.weather_data.wind_deg, public.weather_data.clouds, public.weather_data.visibility, public.weather_data.base, public.weather_data.sys_type, public.weather_data.sys_id, public.weather_data.cod
from weather_data;

select date, city, count(city)
from weather_temp
group by city, date
having count(city) > 1
order by city, date;

