-- 1. **Find the total number of trips for each day.**

select 
date(trip_start_timestamp) as trip_date,
count(*) as total_trips
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1

-- 2. **Calculate the average trip duration for each day.**

select 
date(trip_start_timestamp) as trip_date, 
avg(date_diff(trip_start_timestamp,trip_end_timestamp,'%h')) as avg_trip_duration
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1

-- 3. **Identify the top 5 stations with the highest number of trip starts.**

with cte as
(
select 
trip_start_location as station_name, 
count(distinct trip_id) as total_trips
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
),
cte2 as
(
select
*,
dense_rank() over(order by total_trips desc) as rnk
)
select
station_name,
total_trips
from
cte2
where rnk<=5



-- 4. **Find the average number of trips per hour of the day.**
select
date(trip_start_time) as date,
count(trip_id)/24 as avg_trips_per_hour
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
gorup by 1


-- 5. **Determine the most common trip route (start station to end station).**
with cte as
(
select 
trip_start_location, 
trip_end_location, 
count(trip_id) as trip_count
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1,2
),
cte2 as
(
select
*,
dense_rank() over(order by trip_count desc) as rnk
)
select
trip_start_location,
trip_end_location
from
cte2
where rnk=1

-- 6. **Calculate the number of trips each month.**
select 
date_format(trip_start_time,'%y-%m') as month_year, 
count(trip_id) as total_trips
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1

-- 7. **Find the station with the longest average trip duration.**
with cte
as
(
select 
trip_start_location, 
avg(date_diff(trip_start_timestamp,trip_end_timestamp,'%h')) as avg_duration
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1
),
cte2 as
(
select
*,
dense_rank() over(order by avg_duration desc) as rnk
)
select
trip_start_location
from
cte2
where rnk=1

-- 8. **Find the busiest hour of the day (most trips started).**
with cte as
(
select 
date(trip_start_time) as trip_date, 
extract(hour from trip_start_time) as hour_of_day, 
count(trip_id) as total_trips
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1,2
)
,cte2 as
(
select
*,
dense_rank() over(order by total_trips desc) as rnk
)
select
trip_date,
hour_of_day,
total_trips
from
cte2
where rnk=1

-- 9. **Identify the day with the highest number of trips.**
with cte as
(
select 
date(trip_start_time) as trip_date, 
count(trip_id) as total_trips
from `bigquery-public-data.austin_bikeshare.bikeshare_trips`
group by 1
)
,cte2 as
(
select
*,
dense_rank() over(order by total_trips desc) as rnk
)
select
trip_date,
total_trips
from
cte2
where rnk=1