-------QUERIES

-- 1. SHAPE OF THE DATASET
select count(1) from green_taxi_trip_data_2018;
Data Size : 8807303


-- 2. The column LPEP_PICKUP_DATETIME and LPEP_DROPOFF_DATETIME have date values but in two different format
--1. 	01-12-2018 21:12 [MM-DD-YYYY HH24:MI]
--2. 	01/13/2018 08:35:50 PM [MM/DD/YYYY HH12:MI:SS AM]
--	12/31/2017 11:13:44 PM [MM/DD/YYYY HH12:MI:SS AM]
	
	So we need to add 2 new columns which will contain LPEP_PICKUP_DATETIME and LPEP_DROPOFF_DATETIME values in proper datetime format.
	
	-- Step 1: Add a new temporary column to hold the new date values
	ALTER TABLE green_taxi_trip_data_2018 ADD (PICKUP_DATETIME DATE);
	ALTER TABLE green_taxi_trip_data_2018 ADD (DROPOFF_DATETIME DATE);
	
	-- Step 2: Convert the existing varchar2 values to date values and store them in the temp column
	UPDATE green_taxi_trip_data_2018 SET PICKUP_DATETIME = TO_DATE(LPEP_PICKUP_DATETIME, 'MM/DD/YYYY HH12:MI:SS AM');
	UPDATE green_taxi_trip_data_2018 SET DROPOFF_DATETIME = TO_DATE(LPEP_DROPOFF_DATETIME, 'MM/DD/YYYY HH12:MI:SS AM');
	
	-- Step 3: Drop the old varchar2 column
	ALTER TABLE green_taxi_trip_data_2018 DROP COLUMN LPEP_PICKUP_DATETIME;
        ALTER TABLE green_taxi_trip_data_2018 DROP COLUMN LPEP_DROPOFF_DATETIME;
	
-------------HOURLY DATA ANALYSIS-------------

--3. In which particular hour of the day does taxi drives get the most fare
select extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM')) as fare_hour,
sum(FARE_AMOUNT + TIP_AMOUNT) as driver_earnings
from green_taxi_trip_data_2018
group by extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM'))
order by DRIVER_EARNINGS desc;
-------------------------------------------------


--4. In which particular hour of the day does taxi drives get most number of trips
select extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM')) as fare_hour,
count(1) as num_trips
from green_taxi_trip_data_2018
group by extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM'))
order by num_trips desc;
-------------------------------------------------

--5. At what time of hour do driver get the most tripped and what is the tip percentage with select extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM')) as fare_hour,
round(avg(FARE_AMOUNT),2) as AVG_FARE_AMOUNT, round(avg(TIP_AMOUNT),2) as AVG_TIP_AMOUNT,
round((avg(TIP_AMOUNT)/avg(FARE_AMOUNT))*100,2) as tip_percentage
from green_taxi_trip_data_2018
group by extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM'))
order by tip_percentage desc;
-------------------------------------------------

--during 5pm,6pm and 7pm , drivers usually get most number of trips and fare amount
--People pay higher tip during night time

-------------WEEKLY DATA ANALYSIS-------------

--6. On which days of the week do drivers get the most no. of trips
select to_char(to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM') , 'DAY') as Week_day,
count(*) as num_trips
from green_taxi_trip_data_2018
group by to_char(to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM') , 'DAY')
order by num_trips desc;
-------------------------------------------------

--7. On which days of the week do drivers get the most tipped and the tip%
select to_char(to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM') , 'DAY') as Week_day,
round(avg(FARE_AMOUNT),2) as AVG_FARE_AMOUNT, round(avg(TIP_AMOUNT),2) as AVG_TIP_AMOUNT,
round((avg(TIP_AMOUNT)/avg(FARE_AMOUNT))*100,2) as tip_percentage
from green_taxi_trip_data_2018
group by to_char(to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM') , 'DAY')
order by AVG_TIP_AMOUNT desc;
--CONCLUSION: Saturdays and Sundays have higher trips and tips

-------------------------------------------------

--8. Fare lead
with temp1 as (
select extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM')) as hour,
round(avg(fare_amount),2) as fare_amount
from green_taxi_trip_data_2018
group by extract(hour from to_timestamp(PICKUP_DATETIME,'DD-MM-YYYY HH12:MI:SS AM'))
)
select hour,fare_amount,lead(fare_amount,1) over(order by fare_amount desc) as fare_amount_lead
from temp1
;

-------------------------------------------------

--9. Resampling the data into Day level as we has the data in hour,minute,second level
-- It hard to analyze data in hour, minute level
select trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD') as pickup_date ,
count(*) as num_trips
from green_taxi_trip_data_2018
group by trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD')
order by pickup_date asc;

-------------------------------------------------

--10. Calculate the change% in no of trips of current day and the day in previous week
with daily_trips as (
    select trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD') as pickup_date ,
    count(*) as num_trips
    from green_taxi_trip_data_2018
    group by trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD')
), lag_trips as (
    select PICKUP_DATE, NUM_TRIPS, 
    lag(NUM_TRIPS,7) over(order by PICKUP_DATE) PREVIOUS_WEEK_NUM_TRIPS 
    from daily_trips
)
select PICKUP_DATE, NUM_TRIPS, PREVIOUS_WEEK_NUM_TRIPS,
COALESCE(round(((NUM_TRIPS-PREVIOUS_WEEK_NUM_TRIPS)/PREVIOUS_WEEK_NUM_TRIPS)*100),0) as CHANGE
from lag_trips
order by PICKUP_DATE
;

-------------------------------------------------

--11. Calculate Moving Average for 7 days and 14 days
with daily_trips as (
    select trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD') as pickup_date ,
    count(*) as num_trips
    from green_taxi_trip_data_2018
    group by trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD')
)
select PICKUP_DATE, NUM_TRIPS, 
round(avg(NUM_TRIPS) over(order by PICKUP_DATE rows between 6 preceding and current row),2) as ma_7_days,
round(avg(NUM_TRIPS) over(order by PICKUP_DATE rows between 13 preceding and current row),2) as ma_14_days
from daily_trips
;

-------------------------------------------------

--12. Calculate Running total of 30 days for fare amount
with daily_trips as (
    select trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD') as pickup_date ,
    sum(fare_amount) as fare_amount
    from green_taxi_trip_data_2018
    group by trunc(to_timestamp(PICKUP_DATETIME, 'DD-MM-YYYY HH12:MI:SS AM'),'DD')
)
select PICKUP_DATE, fare_amount, 
sum(fare_amount) over(order by PICKUP_DATE rows between 29 preceding and current row) as running_total_30_days
from daily_trips
;

