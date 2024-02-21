
-- creating a view to join real data with fake one
create or replace temporary view weather_data_real_fake
    as
    select * from weather_data
union distinct
select * from weather_copy_2;

select * from weather_data_real_fake;


--  A function to equalize cities names while querying the data
CREATE OR REPLACE FUNCTION equalize_cities_names(city varchar) RETURNS VARCHAR AS
$$
BEGIN
    CASE
        when city = 'Old Town' then return city = 'Prague';
        when city = 'Sol' then return city = 'Madrid';
        when city = 'Madrid City Center' then return city = 'Madrid';
        when city = 'Eixample' then return city = 'Barcelona';
        when city = 'Altstadt' then return city = 'Munich';
        when city = 'Pigna' then return city = 'Rome';
        when city = 'Trevi' then return city = 'Rome';
        when city = 'Mitte' then return city = 'Berlin';
        when city = 'Alt-KĆ¶lln' then return city = 'Berlin';
        when city = 'Novaya Gollandiya' then return city = 'Saint Petersburg';
        when city = 'Podil' then return city = 'Kyiv';
        when city = 'Pushcha-Vodytsya' then return city = 'Kyiv';
        when city = 'Innere Stadt' then return city = 'Vienna';
        when city = 'Inner city' then return city = 'Vienna';
        ELSE RETURN city;
    END CASE;
END
$$ LANGUAGE plpgsql;


-- Creating today's temperature view
create or replace temporary view temperature_view_today
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
       max(main_temp) as max_temp_today,
       min(main_temp) as min_temp_today,
       stddev(main_temp) as standard_deviation_temp_today,
       current_date as date_today
from weather_data
where date_local >= current_date
or date_local >= now()::date + interval '1h'
group by country,
         case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end
order by country, city;

select * from temperature_view_today;



create or replace temporary view temperature_view_today
    as
select country,
       equalize_cities_names(city) as city,
       max(main_temp) as max_temp_today,
       min(main_temp) as min_temp_today,
       stddev(main_temp) as standard_deviation_temp_today,
       current_date as date_today
from weather_data
where date_local >= current_date
or date_local >= now()::date + interval '1h'
group by country, city
order by country, city;

select * from temperature_view_today;



-- Creating yesterday's temperature view
create or replace temporary view temperature_view_yesterday
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
       max(main_temp) as max_temp_yesterday,
       min(main_temp) as min_temp_yesterday,
       stddev(main_temp) as standard_deviation_temp,
       current_date - 1 as date_yesterday
from weather_data
where date_local >= current_date - 1
group by country,
    case city
        when 'Old Town' then 'Prague'
        when 'Sol' then 'Madrid'
        when 'Madrid City Center' then 'Madrid'
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
        when 'Inner city' then 'Vienna'
        when 'Alt-KĆ¶lln' then 'Berlin'
    else city
    end
order by country, city;

select * from temperature_view_yesterday;



create or replace temporary view temperature_view_yesterday
    as
select country,
       equalize_cities_names(city) as city,
       max(main_temp) as max_temp_today,
       min(main_temp) as min_temp_today,
       stddev(main_temp) as standard_deviation_temp_today,
       current_date as date_today
from weather_data
where date_local >= current_date - 1
group by country, city
order by country, city;

select * from temperature_view_yesterday;




-- Creating temperature view for current week
create or replace temporary view temperature_view_this_week
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
       max(main_temp) as max_temp_current_week,
       min(main_temp) as min_temp_current_week,
       stddev(main_temp) as standard_deviation_temp,
       date_trunc('week', current_date) as start_of_current_week
from weather_data
where date_local >= date_trunc('week', current_date::date)
group by country,
    case city
        when 'Old Town' then 'Prague'
        when 'Sol' then 'Madrid'
        when 'Madrid City Center' then 'Madrid'
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
        when 'Inner city' then 'Vienna'
        when 'Alt-KĆ¶lln' then 'Berlin'
    else city
    end
order by country, city;

select * from temperature_view_this_week;




-- Creating temperature view for the last 7 days
create or replace temporary view temperature_view_last_7_days
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
       max(main_temp) as max_temp_current_week,
       min(main_temp) as min_temp_current_week,
       stddev(main_temp) as standard_deviation_temp,
       date_trunc('week', current_date - interval '7 days') as beginning_last_7_days
-- from weather_data
from weather_data_real_fake
where date_local >= current_date::date - interval '7 days'
and date_local <= current_date
group by country,
    case city
        when 'Old Town' then 'Prague'
        when 'Sol' then 'Madrid'
        when 'Madrid City Center' then 'Madrid'
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
        when 'Inner city' then 'Vienna'
        when 'Alt-KĆ¶lln' then 'Berlin'
    else city
    end
order by country, city;

select * from temperature_view_last_7_days;




-- creating a view for cities with the highest or lowest temperature for each hour
create or replace temporary view cities_min_max_temp_hourly
    as
with ranked_temperatures as (
  select
    case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp,
    date_local,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(HOUR FROM date_local) ORDER BY main_temp DESC) AS rank_highest,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(HOUR FROM date_local) ORDER BY main_temp) AS rank_lowest
  from
    weather_data
)

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
  max(main_temp) as temp_min_max_hourly,
  current_date as date,
  extract(hour from date_local) as hour
from
  ranked_temperatures
where
  rank_highest = 1
and
    date_local >= current_date + interval '1h'
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp, date_local

UNION

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
  min(main_temp) as temp_min_max_hourly,
  current_date as datetime_today,
  extract(hour from date_local) as hour
from
  ranked_temperatures
where
  rank_lowest = 1
and
    date_local >= current_date + interval '1h'
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp, date_local
order by temp_min_max_hourly, hour;

select * from cities_min_max_temp_hourly;




-- creating a view for cities with the highest or lowest temperature for each day
create or replace temporary view cities_min_max_temp_daily
    as
with ranked_temperatures as (
  select
    case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp,
    date_local,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(day FROM date_local) ORDER BY main_temp DESC) AS rank_highest,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(day FROM date_local) ORDER BY main_temp) AS rank_lowest
  from
    weather_data
)

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
  max(main_temp) as temp_min_max_daily,
  current_date as date,
  extract(day from date_local) as day
from
  ranked_temperatures
where
  rank_highest = 1
and
    date_local >= date_trunc('day', current_date)
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp, date_local

UNION

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
  min(main_temp) as temp_min_max_daily,
  current_date as date,
  extract(day from date_local) as day
from
  ranked_temperatures
where
  rank_lowest = 1
and
    date_local >= date_trunc('day', current_date)
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp, date_local
order by temp_min_max_daily, day;

select * from cities_min_max_temp_daily;




-- creating a view for cities with the highest or lowest temperature for each week
create or replace temporary view cities_min_max_temp_weekly
    as
with ranked_temperatures as (
  select
    case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp,
    date_local,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(week FROM date_local) ORDER BY main_temp DESC) AS rank_highest,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(week FROM date_local) ORDER BY main_temp) AS rank_lowest
  from
    weather_data
)

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
  max(main_temp) as temp_min_max_weekly,
  current_date as date,
  date_trunc('week', current_date) as start_of_week_day
from
  ranked_temperatures
where
  rank_highest = 1
and
    date_local >= date_trunc('week', current_date)
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp, date_local

UNION

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
  min(main_temp) as temp_min_max_weekly,
  current_date as date,
  date_trunc('week', current_date) as start_of_week_day
from
  ranked_temperatures
where
  rank_lowest = 1
and
    date_local >= date_trunc('week', current_date::date)
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Madrid City Center' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
           when 'Inner city' then 'Vienna'
           when 'Alt-KĆ¶lln' then 'Berlin'
       else city
       end,
    main_temp, date_local
order by temp_min_max_weekly, start_of_week_day;

select * from cities_min_max_temp_weekly;




-- creating a view to count the number of times (hours) it rained in the last day
create or replace temporary view rainy_hours_last_day
    as
select count(*) as rainy_hours_last_day
from weather_data
where weather_main like '%Drizzle'
or
    weather_main like '%Rain'
and
    date_local >= current_date - 1
and date_local <= current_date;

select * from rainy_hours_last_day;




-- creating a view to count the number of times (hours) it rained in the last week
create or replace temporary view rainy_hours_last_week
    as
select count(*) as rainy_hours_last_week
from weather_data
where weather_main like '%Drizzle'
or
    weather_main like '%Rain'
and
    date_local >= date_trunc('week', current_date) - interval '7 days'
and date_local <= current_date;

select * from rainy_hours_last_week;