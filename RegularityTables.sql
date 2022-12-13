--Create databse speed per route

CREATE TABLE regularity_trip_speed (
	route_id INT,
	route_short_name text,
	route_short_name_int INT,
	first_stop INT,
	last_stop INT,
	date_format date,
	delta_time FLOAT,
	distance FLOAT,
	speed FLOAT
);
select * from regularity_trip_speed order by route_short_name;

--setup load timezone
SET datestyle = dmy;

--create database retularity_trip_times data real arrival
create table regularity_trip_times as (select 
(TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels')::timestamp  as used_time,
vc.distancefrompoint distance,
r.speed  speed,
(TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels')::timestamp - (vc.distancefrompoint/r.speed) * interval '1 second'  as real_time,
((TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels')::timestamp - (vc.distancefrompoint/r.speed) * interval '1 second')::time  as real_arrival_time,
vc.lineid as route_short_name_int,
vc.pointid as stop_id
from vehiclepositioncomplete vc
inner join regularity_trip_speed r
on r.route_short_name_int=vc.lineid and r.last_stop=vc.directionid
order by route_short_name_int, time, last_stop);

select * from regularity_trip_times limit 5;

select count(*) from regularity_trip_times group by route_short_name_int, stop_id;

-- create table calculate headways
 create table regularity_trip_headways as (select route_short_name_int, 
stop_id,real_time,real_arrival_time,
case 
WHEN lag(route_short_name_int) over (order by route_short_name_int, stop_id,real_time, real_arrival_time)=route_short_name_int and lag (stop_id) over (order by route_short_name_int, stop_id,real_time, real_arrival_time)=stop_id
then EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) 
ELSE NULL END headway_s
from regularity_trip_times 
order by route_short_name_int , stop_id,real_time,real_arrival_time);

select * from regularity_trip_headways limit 10;	  

-- create table by timegroups
create table regularity_trip_timegroups as(select route_short_name_int, stop_id, min(real_time) as from_date, max(real_time) as to_date,
       grp, count(*) as elements_time_group,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median,
		avg(headway_s)  as headway_s_avg
from (select t.*,
      sum(case 
		  when headway_s>720 then 1 else 0 end) over (partition by route_short_name_int, stop_id order by route_short_name_int, stop_id,real_time, real_arrival_time,headway_s ) as grp
      from regularity_trip_headways t
     ) t
group by route_short_name_int, stop_id, grp
			  order by route_short_name_int, stop_id, grp);
			  
select * from regularity_trip_timegroups  limit 100 ;

-- create table summarize data headways by route and stopid
create table  regularity_trip_summary_line as(select 
		route_short_name_int,
		stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median, 
		avg(headway_s)  as headway_s_avg,
		(sum(headway_s^2)/2*sum(headway_s))::float as waiting_time	
		from regularity_trip_headways
		group by route_short_name_int , stop_id
		);
select * from regularity_trip_summary_line;




--SCHEDULED

--create table headways scheduled
create table regularity_trip_headways_scheduled as ((select r.route_short_name, 
r.stop_id, r.arrival_time,
case 
WHEN lag(r.route_short_name) over (order by r.route_short_name, r.stop_id, r.arrival_time)=r.route_short_name and lag (r.stop_id) over (order by r.route_short_name, r.stop_id,r.arrival_time)=r.stop_id
then EXTRACT(EPOCH FROM (r.arrival_time  - lag(r.arrival_time) over (order by r.route_short_name, r.stop_id,r.arrival_time))) 
ELSE NULL END headway_s
from (select REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')::INT AS route_short_name,
			   REGEXP_REPLACE(stop_id, '[^0-9]', '', 'g')::INT AS stop_id,
			  arrival_time as before_arrival_time,
			   case
			   when arrival_time ~ '^24:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'24:','00:'), 'HH24:MI:SS')::TIME 
		when arrival_time ~ '^25:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'25:','01:'), 'HH24:MI:SS')::TIME 	   	   
	  	when arrival_time ~ '^26:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'26:','02:'), 'HH24:MI:SS')::TIME 	   	   
	  	when arrival_time ~ '^27:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'27:','03:'), 'HH24:MI:SS')::TIME 	   	   
	  else  TO_TIMESTAMP(arrival_time, 'HH24:MI:SS')::TIME
			   end
			   as arrival_time 
			   from regularity_trip ) r
order by r.route_short_name , r.stop_id,r.arrival_time));

select * from regularity_trip_headways_scheduled limit 10;	  


-- create table timegroups
create table regularity_trip_timegroups_scheduled as(select route_short_name, stop_id, min(arrival_time) as from_date, max(arrival_time) as to_date,
       grp, count(*) as elements_time_group,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median,
		avg(headway_s)  as headway_s_avg
from (select t.*,
      sum(case 
		  when headway_s>720 then 1 else 0 end) over (partition by route_short_name, stop_id order by route_short_name, stop_id,arrival_time,headway_s ) as grp
      from regularity_trip_headways_scheduled t
     ) t
group by route_short_name, stop_id, grp
			  order by route_short_name, stop_id, grp);
			  
			  
--create table summarize
 create table  regularity_trip_summary_line_scheduled as(select 
		route_short_name,
		stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median, 
		avg(headway_s)  as headway_s_avg,
		(sum(headway_s^2)/2*sum(headway_s))::float as waiting_time	
		from regularity_trip_headways_scheduled
		group by route_short_name , stop_id
		);



-- get ETW
select s.route_short_name,r.stop_id, r.waiting_time-s.waiting_time as EWT from regularity_trip_summary_line r, regularity_trip_summary_line_scheduled s
where r.route_short_name_int= s.route_short_name and
r.stop_id= s.stop_id;

