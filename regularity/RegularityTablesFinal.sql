--speed
drop table regularity_trip_speed;
CREATE TABLE regularity_trip_speed (
	route_id INT,
	route_short_name text,
	route_short_name_int INT,
	first_stop INT,
	last_stop INT,
	direction INT,
	list_stops TEXT,
	date_format date,
	delta_time FLOAT,
	distance FLOAT,
	speed FLOAT
);


-- FILTER REGULARITY METHOD
-- t_used assessment_methods, regularity_filter

select * from assessment_methods where method='regularity' limit 10;
select count(*) from assessment_methods where method='regularity';
--9360

drop table regularity_filter;
create table regularity_filter as(select distinct 
	a.*,
	 case
	when REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')='' then -1
	else REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')::INT
	end AS route_short_name,
	REGEXP_REPLACE("stopId", '[^0-9]', '', 'g')::INT AS stop_id, 
	TO_TIMESTAMP(EXTRACT(EPOCH FROM (date_from +(
		case
	when time_from ~ '^24:(.*)' then TO_TIMESTAMP(REPLACE(time_from,'24:','00:'), 'HH24:MI:SS')::TIME 
		when time_from ~ '^25:(.*)' then TO_TIMESTAMP(REPLACE(time_from,'25:','01:'), 'HH24:MI:SS')::TIME 	   	   
	  	when time_from ~ '^26:(.*)' then TO_TIMESTAMP(REPLACE(time_from,'26:','02:'), 'HH24:MI:SS')::TIME 	   	   
	  	when time_from ~ '^27:(.*)' then TO_TIMESTAMP(REPLACE(time_from,'27:','03:'), 'HH24:MI:SS')::TIME 	   	   
	  else  TO_TIMESTAMP(time_from, 'HH24:MI:SS')::TIME
			   end
	))))::timestamp as from_date_time,
	TO_TIMESTAMP(EXTRACT(EPOCH FROM (date_to +(
		case
	when time_to ~ '^24:(.*)' then TO_TIMESTAMP(REPLACE(time_to,'24:','00:'), 'HH24:MI:SS')::TIME 
		when time_to ~ '^25:(.*)' then TO_TIMESTAMP(REPLACE(time_to,'25:','01:'), 'HH24:MI:SS')::TIME 	   	   
	  	when time_to ~ '^26:(.*)' then TO_TIMESTAMP(REPLACE(time_to,'26:','02:'), 'HH24:MI:SS')::TIME 	   	   
	  	when time_to ~ '^27:(.*)' then TO_TIMESTAMP(REPLACE(time_to,'27:','03:'), 'HH24:MI:SS')::TIME 	   	   
	  else  TO_TIMESTAMP(time_to, 'HH24:MI:SS')::TIME
			   end
	))))::timestamp as to_date_time
	from assessment_methods a inner join routes r on 
	r.route_id = a.route_id
	where method='regularity' and r.date_format='2022-09-03');

select count(*) from regularity_filter ;
--9360

-- Headways scheduled

-- 1. Create table whole information scheduled
drop table regularity_trip;
create table regularity_trip as(
	Select distinct 
	t.route_id,
    t.service_id,
    t.trip_id,
    t.trip_headsign,
    t.direction_id,
    st.arrival_time,
    st.departure_time,
    st.stop_id,
	st.stop_sequence,
	st.pickup_type,
	st.drop_off_type,
    r.route_type,
	r.route_short_name,
	t.date_format,
    c.start_date as date_from,
    c.end_date  as date_to,
    c.monday,
    c.tuesday,
    c.wednesday,
    c.thursday,
    c.friday,
    c.saturday,
    c.sunday
    from trips t
    inner join stop_times st
    on t.trip_id = st.trip_id 
    inner join calendar c
    on t.service_id = c.service_id
    inner join routes r
    on r.route_id = t.route_id
	where t.date_format='2022-09-03'
    order by t.route_id,t.direction_id,c.start_date,t.service_id,st.stop_id,st.arrival_time
);
select  * from regularity_trip limit 10;

-- 2. Calculted headways
drop table regularity_trip_headways_scheduled;
create table regularity_trip_headways_scheduled as (
select r.route_short_name, 
r.stop_id, r.direction_id, r.arrival_time, 
case 
WHEN lag(r.route_short_name) over (order by r.stop_id, r.route_short_name, r.direction_id,r.arrival_time)=r.route_short_name 
and lag (r.stop_id) over (order by r.stop_id, r.route_short_name, r.direction_id,r.arrival_time)=r.stop_id
and lag(r.direction_id) over (order by r.stop_id, r.route_short_name,r.direction_id, r.arrival_time)=r.direction_id
then EXTRACT(EPOCH FROM (r.arrival_time  - lag(r.arrival_time) over (order by r.stop_id, r.route_short_name, r.direction_id,r.arrival_time))) 
ELSE NULL END headway_s,
date_from, date_to
from(select 	case
	when REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')='' then -1
	else REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')::INT
	end AS route_short_name,
			   REGEXP_REPLACE(stop_id, '[^0-9]', '', 'g')::INT AS stop_id,
			  arrival_time as before_arrival_time,
			   case
			   when arrival_time ~ '^24:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'24:','00:'), 'HH24:MI:SS')::TIME 
		when arrival_time ~ '^25:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'25:','01:'), 'HH24:MI:SS')::TIME 	   	   
	  	when arrival_time ~ '^26:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'26:','02:'), 'HH24:MI:SS')::TIME 	   	   
	  	when arrival_time ~ '^27:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'27:','03:'), 'HH24:MI:SS')::TIME 	   	   
	  else  TO_TIMESTAMP(arrival_time, 'HH24:MI:SS')::TIME
			   end
			   as arrival_time ,direction_id,
	  date_from, date_to
			   from regularity_trip
	  order by stop_id,route_short_name,direction_id , arrival_time
) r
order by r.stop_id,r.route_short_name ,r.direction_id,r.arrival_time);


select * from regularity_trip_headways_scheduled 
order by stop_id,route_short_name ,arrival_time
limit 10;
select * from regularity_filter 
order by stop_id,route_short_name 
limit 10;

drop table regularity_trip_timegroups_scheduled;
create table regularity_trip_timegroups_scheduled as (
select 
f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type,
avg(s.headway_s) avg_headway,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as median_headway, 
sum(headway_s^2) sum_2_headway,
sum(headway_s) sum_headway,
min(s.arrival_time) minsarrival_time,
max(s.arrival_time) maxsarrival_time,
	min(s.date_from) minsarrival_date,
max(s.date_to) maxsarrival_date,
case
when sum(headway_s)=0 then 0 else sum(headway_s^2)/(2*sum(headway_s))
end waiting_time
from regularity_filter f left join regularity_trip_headways_scheduled s
on f.route_short_name=s.route_short_name
and f.stop_id= s.stop_id 
	and f.direction_id = s.direction_id
	and 
(s.date_from,s.date_to) OVERLAPS
	(f.date_from,f.date_to)
and s.arrival_time between f.from_date_time::time and f.to_date_time::time
group by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type
order by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time); 

select count(*) from regularity_trip_timegroups_scheduled;
select count(*) from regularity_filter;

select * from regularity_trip_timegroups_scheduled limit 10;
select * from regularity_trip_headways_scheduled limit 10;
select * from regularity_filter limit 10;
select * from regularity_trip limit 10;

select * FROM
regularity_trip_headways_scheduled
where route_short_name=1 and stop_id=8011;

select * FROM
regularity_trip
where route_short_name='1' and stop_id='8011';

select * FROM
regularity_filter
where route_short_name='1' and stop_id='8011';

select count(*) from regularity_filter f
group by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type;

select f.* from regularity_filter f
order by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time
limit 10; 






select distinct route_id , route_short_name
from regularity_trip 
where date_format='2022-09-03'
group by route_id , route_short_name 
order by route_id , route_short_name;

delete  from regularity_trip_speed
where route_short_name='76' and direction=0 and first_stop=1638
and last_stop=1629 and delta_time=660;

select * from regularity_trip_speed
where route_short_name='76';
select route_short_name, count(*) 
from regularity_trip_speed
group by route_short_name
order by route_short_name;
select  count(*) 
from regularity_trip_speed;
-- Calculate real data
SET datestyle = dmy;

select  *
from regularity_trip_speed where speed=0;

update regularity_trip_speed set speed=1 where speed=0;
select count(*) from vehiclepositioncomplete;

drop table regularity_trip_times_pre;
create table regularity_trip_times_pre as ( 
select 
(TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels')::timestamp  as used_time,
vc.distancefrompoint distance,
r.speed  speed,
(TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels')::timestamp - (vc.distancefrompoint/r.speed) * interval '1 second'  as real_arrival_date,
((TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels')::timestamp - (vc.distancefrompoint/r.speed) * interval '1 second')::time  as real_arrival_time,
vc.lineid as route_short_name_int,
vc.pointid as stop_id,
r.direction as direction_id
from vehiclepositioncomplete vc
	
inner join regularity_trip_speed r
on r.route_short_name_int=vc.lineid 
--and (r.last_stop=vc.directionid or r.first_stop=vc.directionid)
and 
	(case
	 when r.last_stop=vc.directionid then r.direction
	 when r.first_stop=vc.directionid and r.direction=0 then 1
	  when r.first_stop=vc.directionid and r.direction=1 then 0
	when(vc.directionid::text =ANY( string_to_array(r.list_stops, ','))
		and vc.pointid::text =ANY( string_to_array(r.list_stops, ',')))then r.direction else NULL
	end)=r.direction
);
--order by route_short_name_int, time, last_stop
select count(*) from regularity_trip_times_pre;
--6139025
--8m
--20m
--15234652
--13841990
select * from regularity_trip_speed limit 10;
select * from vehiclepositioncomplete limit 10;

--189464
select count(*) from vehiclepositioncomplete where directionid=8262
and lineid=5;
--405897
select count(*) from vehiclepositioncomplete where lineid=5;
--85373
select count(*) from vehiclepositioncomplete where directionid=8641
and lineid=5;
select count(*) from vehiclepositioncomplete where directionid=8231
and lineid=5;
select * from vehiclepositioncomplete where directionid=8641
and lineid=5 
limit 10;

select lineid,directionid, count (*) from vehiclepositioncomplete where lineid=5 
group by lineid, directionid
order by count (*) desc;

select lineid,pointid, count (*) from vehiclepositioncomplete where lineid=5 
group by lineid, pointid
order by count (*) desc;

select lineid,pointid, count (*) from vehiclepositioncomplete where lineid=5 
group by lineid, pointid
order by lineid, pointid
limit 10;

select lineid, directionid,count(*) 
from vehiclepositioncomplete where lineid=5
group by lineid, directionid;

select * from regularity_trip limit 10;
select  trip_id,stop_sequence, direction_id, stop_id, count(*) from regularity_trip where route_short_name='5'
group by  trip_id,stop_sequence, direction_id, stop_id
order by trip_id,stop_sequence, direction_id, stop_id
limit 1000;

select  trip_id,stop_sequence, direction_id, stop_id, count(*) from regularity_trip where route_short_name='12'
group by  trip_id,stop_sequence, direction_id, stop_id
order by trip_id,stop_sequence, direction_id, stop_id
limit 1000;

select  trip_id,stop_sequence, direction_id, stop_id, count(*) from regularity_trip where route_short_name='2'
and direction_id=0
group by  trip_id,stop_sequence, direction_id, stop_id
order by trip_id,stop_sequence, direction_id, stop_id
limit 1000;
select  route_short_name, count(*) from regularity_trip 
group by  route_short_name
order by route_short_name
limit 1000;

select * from routes where route_short_name='5';


select stop_id, count(*) from regularity_trip where route_short_name='5'
group by stop_id
order by stop_id
limit 1000;

select stop_id, count(*) from regularity_trip where route_short_name='5'
group by stop_id
order by stop_id
limit 1000;

drop table regularity_trip_times;
create table regularity_trip_times as(select route_short_name_int, stop_id, direction_id,
min(real_arrival_date) as real_arrival_date, 
min(real_arrival_date)::time as real_arrival_time, 
 max(real_arrival_date) as  real_depature_date,
 max(real_arrival_date)::time as  real_depature_time,
    grp as   grp_less, count(*) as elements_time_group_less
from (
	select t.*,
      sum(case 
		  when headway_s>60 then 1 else 0 end) over (partition by stop_id, route_short_name_int order by stop_id, route_short_name_int, real_arrival_date ) as grp
      from(select 
used_time,
distance,
speed,
real_arrival_date,
real_arrival_time,
route_short_name_int,
stop_id,
direction_id,
case 
WHEN lag(route_short_name_int) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date)=route_short_name_int 
and lag (stop_id) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date)=stop_id
and lag(direction_id) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date)=direction_id
then EXTRACT(EPOCH FROM (real_arrival_date - lag(real_arrival_date) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date))) 
ELSE NULL END headway_s
from regularity_trip_times_pre
order by stop_id, route_short_name_int,direction_id, real_arrival_date) t
     
) t
group by stop_id, route_short_name_int,direction_id, grp
order by stop_id, route_short_name_int,direction_id, grp);
			  
			  
select * from regularity_trip_times
limit 100;			  
			  	  
select count(*) from regularity_trip_times group by stop_id, route_short_name_int, real_arrival_date;

select max(headway_s) from regularity_trip_headways_scheduled;
select count(headway_s) from regularity_trip_headways
where headway_s<=12;
select count(headway_s) from regularity_trip_headways
where headway_s>12;

-- create table calculate headways
drop table regularity_trip_headways;
 create table regularity_trip_headways as (select route_short_name_int, 
stop_id,direction_id,real_arrival_date,real_arrival_time,real_depature_date,
case 
WHEN lag(route_short_name_int) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date)=route_short_name_int 
and lag (stop_id) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date)=stop_id
and lag(direction_id) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date)=direction_id
then EXTRACT(EPOCH FROM (real_arrival_date - lag(real_depature_date) over (order by stop_id, route_short_name_int,direction_id, real_arrival_date))) 
ELSE NULL END headway_s
from regularity_trip_times 
order by stop_id, route_short_name_int,direction_id, real_arrival_date);

select * from regularity_trip_headways limit 10;
select * from regularity_trip_headways where headway_s<0 
order by stop_id, route_short_name_int,direction_id, real_arrival_date
limit 10;

select * from regularity_trip_headways where route_short_name_int=39
and stop_id=89 
order by stop_id, route_short_name_int,direction_id, real_arrival_date;

drop table regularity_trip_timegroups;
create table regularity_trip_timegroups as (
select 
f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type,
avg(s.headway_s) avg_headway,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as median_headway, 
sum(headway_s^2) sum_2_headway,
sum(headway_s) sum_headway,
min(s.real_arrival_date)::time minsarrival_time,
max(s.real_depature_date)::time maxsarrival_time,
	min(s.real_arrival_date)::date minsarrival_date,
max(s.real_depature_date)::date maxsarrival_date,
case
when sum(headway_s)=0 then 0 else sum(headway_s^2)/(2*sum(headway_s))
end waiting_time
from regularity_filter f left join regularity_trip_headways s
on f.route_short_name=s.route_short_name_int
	and f.direction_id=s.direction_id 
and f.stop_id= s.stop_id and 
(s.real_arrival_date::date,s.real_depature_date::date) OVERLAPS
	(f.from_date_time::date,f.to_date_time::date) 
and s.real_arrival_date::time between f.from_date_time::time and f.to_date_time::time
group by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type
order by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time
); 

drop table regularity_trip_timegroups_results;
create table regularity_trip_timegroups_results as (select  
f.route_id,
f.route_short_name,
f.stop_id,f.direction_id, f.from_date_time,f.to_date_time,
f.from_date_time::time from_time,f.to_date_time::time to_time,
f.from_date_time::date from_date,f.to_date_time::date to_date,
f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type, 
f.waiting_time waiting_time_real,
s.waiting_time waiting_time_scheduled,
(f.waiting_time-s.waiting_time) as EWT,
(f.waiting_time-s.waiting_time)/60 as EWT_MINUTES,
CASE
when (f.waiting_time) is null or (s.waiting_time) is null then 'DATA NOT AVAILABLE'													
when (f.waiting_time-s.waiting_time)/60<=0 then '<=0'
when (f.waiting_time-s.waiting_time)/60>0 and (f.waiting_time-s.waiting_time)/60<=4 then '0-4MIN'
when (f.waiting_time-s.waiting_time)/60>4 and (f.waiting_time-s.waiting_time)/60<=8 then '4-8MIN'
when (f.waiting_time-s.waiting_time)/60>8 and (f.waiting_time-s.waiting_time)/60<=12 then'8-12MIN'
when (f.waiting_time-s.waiting_time)/60>12 then '>12MIN'									
end ewt_category
from regularity_trip_timegroups f, regularity_trip_timegroups_scheduled s
where f.route_short_name= s.route_short_name and
f.stop_id= s.stop_id and f.direction_id=s.direction_id
and f.from_date_time= s.from_date_time and 
f.to_date_time= s.to_date_time);

select * from regularity_trip_timegroups_results_test
where ewt is not null
and route_id=56 and stop_id=2269
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results_test
where ewt is not null
and route_id=1 and stop_id=8211
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results_test
where ewt is not null
and route_id=1 and stop_id=8211
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results
where ewt is not null
and route_id=1 and stop_id=8211
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results
where ewt is not null
and route_id=56 and stop_id=2269
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results_test
where ewt is not null
and route_id=56 and stop_id=2269
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results_test
where ewt is not null
order by ewt desc
limit 100;

select * from regularity_trip_timegroups_results
where ewt is not null
order by ewt desc
limit 100;

select count(*) from regularity_trip_timegroups_scheduled;

select count(*) from regularity_filter;

select * from regularity_trip_timegroups limit 10;
select count(*) from regularity_trip_timegroups;
select count(*) from regularity_trip_timegroups where waiting_time is NULL;
--9360
--6014
--5135
--1417

--2566

drop table regularity_trip_timegroups_results;
create table regularity_trip_timegroups_results as (select  
f.route_id,
f.route_short_name,
f.stop_id,f.direction_id, f.from_date_time,f.to_date_time,
f.from_date_time::time from_time,f.to_date_time::time to_time,
f.from_date_time::date from_date,f.to_date_time::date to_date,
f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type, 
f.waiting_time waiting_time_real,
s.waiting_time waiting_time_scheduled,
(f.waiting_time-s.waiting_time) as EWT,
(f.waiting_time-s.waiting_time)/60 as EWT_MINUTES,
CASE
when (f.waiting_time) is null or (s.waiting_time) is null then 'DATA NOT AVAILABLE'													
when (f.waiting_time-s.waiting_time)/60<=0 then '<=0'
when (f.waiting_time-s.waiting_time)/60>0 and (f.waiting_time-s.waiting_time)/60<=4 then '0-4MIN'
when (f.waiting_time-s.waiting_time)/60>4 and (f.waiting_time-s.waiting_time)/60<=8 then '4-8MIN'
when (f.waiting_time-s.waiting_time)/60>8 and (f.waiting_time-s.waiting_time)/60<=12 then'8-12MIN'
when (f.waiting_time-s.waiting_time)/60>12 then '>12MIN'									
end ewt_category
from regularity_trip_timegroups f, regularity_trip_timegroups_scheduled s
where f.route_short_name= s.route_short_name and
f.stop_id= s.stop_id and f.direction_id=s.direction_id
and f.from_date_time= s.from_date_time and 
f.to_date_time= s.to_date_time);


select * from regularity_trip_timegroups_results limit 10;


--Delete out of service >10000
--delet headways 12000





--- New intervals
select * from regularity_filter_30 limit 10;

drop table regularity_filter_30;
create table regularity_filter_30 as (select f.id, f.route_id, f.direction_id,f.stop_id,f.route_short_name,
f.route_type, f.method,
f.monday, f.tuesday, f.wednesday, f.thursday, f.friday,
f.saturday, f.sunday,
generate_series(f.from_date_time,f.to_date_time, interval '30 minutes')::timestamp as from_date_time ,
(generate_series(f.from_date_time,f.to_date_time, interval '30 minutes')::timestamp + interval '30 minutes')::timestamp to_date_time
from regularity_filter f );


drop table regularity_trip_timegroups_scheduled_30;
create table regularity_trip_timegroups_scheduled_30 as (
select 
f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type,
avg(s.headway_s) avg_headway,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as median_headway, 
sum(headway_s^2) sum_2_headway,
sum(headway_s) sum_headway,
min(s.arrival_time) minsarrival_time,
max(s.arrival_time) maxsarrival_time,
	min(s.date_from) minsarrival_date,
max(s.date_to) maxsarrival_date,
case
when sum(headway_s)=0 then 0 else sum(headway_s^2)/(2*sum(headway_s))
end waiting_time
from regularity_filter_30 f left join regularity_trip_headways_scheduled s
on f.route_short_name=s.route_short_name
and f.stop_id= s.stop_id and 
	f.direction_id=s.direction_id and
(s.date_from,s.date_to) OVERLAPS
	(f.from_date_time::date,f.to_date_time::date)
and s.arrival_time between f.from_date_time::time and f.to_date_time::time
group by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type
order by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time); 


drop table regularity_trip_timegroups_30;
create table regularity_trip_timegroups_30 as (
select 
f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type,
avg(s.headway_s) avg_headway,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as median_headway, 
sum(headway_s^2) sum_2_headway,
sum(headway_s) sum_headway,
min(s.real_arrival_date)::time minsarrival_time,
max(s.real_depature_date)::time maxsarrival_time,
	min(s.real_arrival_date)::date minsarrival_date,
max(s.real_depature_date)::date maxsarrival_date,
case
when sum(headway_s)=0 then 0 else sum(headway_s^2)/(2*sum(headway_s))
end waiting_time
from regularity_filter_30 f left join regularity_trip_headways s
on f.route_short_name=s.route_short_name_int
and f.stop_id= s.stop_id and 
	f.direction_id=s.direction_id and
(s.real_arrival_date,s.real_depature_date) OVERLAPS
	(f.from_date_time,f.to_date_time)
group by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time,
f.route_id, f.direction_id,f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type
order by f.route_short_name,f.stop_id, f.from_date_time,f.to_date_time
); 

drop table regularity_trip_timegroups_results_30;
create table regularity_trip_timegroups_results_30 as (select 
f.route_id,
f.route_short_name,
f.stop_id,f.direction_id, f.from_date_time,f.to_date_time,
f.from_date_time::time from_time,f.to_date_time::time to_time,
f.from_date_time::date from_date,f.to_date_time::date to_date,
f.monday, f.tuesday, f.wednesday, f.thursday,
f.friday, f.saturday, f.sunday, f.route_type, 
f.waiting_time waiting_time_real,
s.waiting_time waiting_time_scheduled,
(f.waiting_time-s.waiting_time) as EWT,
(f.waiting_time-s.waiting_time)/60 as EWT_MINUTES,
CASE
when (f.waiting_time) is null or (s.waiting_time) is null then 'DATA NOT AVAILABLE'													
when (f.waiting_time-s.waiting_time)/60<=0 then '<=0'
when (f.waiting_time-s.waiting_time)/60>0 and (f.waiting_time-s.waiting_time)/60<=4 then '0-4MIN'
when (f.waiting_time-s.waiting_time)/60>4 and (f.waiting_time-s.waiting_time)/60<=8 then '4-8MIN'
when (f.waiting_time-s.waiting_time)/60>8 and (f.waiting_time-s.waiting_time)/60<=12 then'8-12MIN'
when (f.waiting_time-s.waiting_time)/60>12 then '>12MIN'									
end ewt_category
from regularity_trip_timegroups_30 f, regularity_trip_timegroups_scheduled_30 s
where f.route_short_name= s.route_short_name and
f.direction_id=s.direction_id and
f.stop_id= s.stop_id and f.from_date_time= s.from_date_time and 
f.to_date_time= s.to_date_time);


select min(from_date) from regularity_trip_timegroups_results;
select max(to_date) from regularity_trip_timegroups_results;


create table defined_intervals as (
	select 
	generate_series('2021-09-01 00:00:00'::timestamp,'2021-09-19 23:30:00'::timestamp, interval '30 minutes')::timestamp from_time,
	(generate_series('2021-09-01 00:00:00'::timestamp,'2021-09-19 23:30:00'::timestamp, interval '30 minutes')::timestamp + interval '30 minutes')::timestamp to_time

);