
-- Creating today's temperature view
create or replace view temperature_view_today
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
    else city
    end
order by country, city;


-- Creating yesterday's temperature view
create or replace view temperature_view_yesterday
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
    else city
    end
order by country, city;


-- Creating temperature view for current week
create or replace view temperature_view_this_week
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
    else city
    end
order by country, city;


-- Creating temperature view for the last 7 days
create or replace view temperature_view_last_7_days
    as
select country,
       case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
       max(main_temp) as max_temp_current_week,
       min(main_temp) as min_temp_current_week,
       stddev(main_temp) as standard_deviation_temp,
       date_trunc('week', current_date - interval '7 days') as beginning_last_7_days
from weather_data
where date_local >= current_date - interval '7 days'
group by country,
    case city
        when 'Old Town' then 'Prague'
        when 'Sol' then 'Madrid'
        when 'Eixample' then 'Barcelona'
        when 'Altstadt' then 'Munich'
        when 'Pigna' then 'Rome'
        when 'Trevi' then 'Rome'
        when 'Mitte' then 'Berlin'
        when 'Novaya Gollandiya' then 'Saint Petersburg'
        when 'Podil' then 'Kyiv'
        when 'Pushcha-Vodytsya' then 'Kyiv'
        when 'Innere Stadt' then 'Vienna'
    else city
    end
order by country, city;


-- creating a view for cities with the highest or lowest temperature for each hour
create or replace view cities_min_max_temp_hourly
    as
with ranked_temperatures as (
  select
    case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
  max(main_temp) as temp_min_max_hourly,
  current_date as date,
  extract(hour from date_local) as Hour
from
  ranked_temperatures
where
  rank_highest = 1
and
    date_local >= current_date + interval '1h'
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
    main_temp, date_local

UNION

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
    main_temp, date_local
order by temp_min_max_hourly, hour;


-- creating a view for cities with the highest or lowest temperature for each day
create or replace view cities_min_max_temp_daily
    as
with ranked_temperatures as (
  select
    case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
    main_temp, date_local

UNION

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
    main_temp, date_local
order by temp_min_max_daily, day;



-- creating a view for cities with the highest or lowest temperature for each week
create or replace view cities_min_max_temp_weekly
    as
with ranked_temperatures as (
  select
    case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
    main_temp, date_local

UNION

select
  case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
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
    date_local >= date_trunc('week', current_date)
group by case city
           when 'Old Town' then 'Prague'
           when 'Sol' then 'Madrid'
           when 'Eixample' then 'Barcelona'
           when 'Altstadt' then 'Munich'
           when 'Pigna' then 'Rome'
           when 'Trevi' then 'Rome'
           when 'Mitte' then 'Berlin'
           when 'Novaya Gollandiya' then 'Saint Petersburg'
           when 'Podil' then 'Kyiv'
           when 'Pushcha-Vodytsya' then 'Kyiv'
           when 'Innere Stadt' then 'Vienna'
       else city
       end,
    main_temp, date_local
order by temp_min_max_weekly, start_of_week_day;


