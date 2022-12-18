
-- filter data by method
select 
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
	where method='regularity';

--Create databse speed per route
drop table regularity_trip_speed;
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

update regularity_trip_speed set speed=4
where speed=0;
drop table regularity_filter;
create table regularity_filter as(select distinct 
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

select lineId, directionId, count(*) from vehiclepositioncomplete
group by   lineId, directionId
order by lineId, directionId
limit 100;

--create database retularity_trip_times data real arrival
drop table regularity_trip_times_pre;
create table regularity_trip_times_pre as ( select * 
from (
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
vc.pointid as stop_id
from vehiclepositioncomplete vc
inner join regularity_trip_speed r
on r.route_short_name_int=vc.lineid and r.last_stop=vc.directionid
order by route_short_name_int, time, last_stop) i
where EXISTS(select *
			 from regularity_filter f
			 where i.route_short_name_int=f.route_short_name
			 and i.stop_id=f.stop_id
			 and i.real_arrival_date BETWEEN f.from_date_time 
			 and f.to_date_time	
));

select * from regularity_trip_times_pre
order by stop_id, route_short_name_int, real_arrival_date
limit 100000;

drop table regularity_trip_times;
create table regularity_trip_times as(select route_short_name_int, stop_id, 
min(real_arrival_date) as real_arrival_date, 
min(real_arrival_date)::time as real_arrival_time, 
 max(real_arrival_date) as  real_depature_date,
 max(real_arrival_date)::time as  real_depature_time,
    grp as   grp_less, count(*) as elements_time_group_less
from (
	select h.*,
	case
	when grp= lead(grp)  over (order by stop_id, route_short_name_int, real_arrival_date) 
	and grp != lag(grp) over (order by stop_id, route_short_name_int, real_arrival_date) 
	then NULL
	else headway_s
	end headway_s_new
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
case 
WHEN lag(route_short_name_int) over (order by stop_id, route_short_name_int, real_arrival_date)=route_short_name_int and lag (stop_id) over (order by stop_id, route_short_name_int, real_arrival_date)=stop_id
then EXTRACT(EPOCH FROM (real_arrival_date - lag(real_arrival_date) over (order by stop_id, route_short_name_int, real_arrival_date))) 
ELSE NULL END headway_s
from regularity_trip_times_pre) t
     )h
) t
group by stop_id, route_short_name_int, grp
order by stop_id, route_short_name_int, grp);
			  
select * from regularity_trip_times
order by stop_id, route_short_name_int, real_arrival_date
limit 100;			  
			  
			  
select count(*) from regularity_trip_times group by stop_id, route_short_name_int, real_arrival_date;


-- create table calculate headways
drop table regularity_trip_headways;
 create table regularity_trip_headways as (select route_short_name_int, 
stop_id,real_arrival_date,real_arrival_time,real_depature_date,
case 
WHEN lag(route_short_name_int) over (order by stop_id, route_short_name_int, real_arrival_date)=route_short_name_int and lag (stop_id) over (order by stop_id, route_short_name_int, real_arrival_date)=stop_id
then EXTRACT(EPOCH FROM (real_arrival_date - lag(real_depature_date) over (order by stop_id, route_short_name_int, real_arrival_date))) 
ELSE NULL END headway_s
from regularity_trip_times 
order by stop_id, route_short_name_int, real_arrival_date);


select * from regularity_trip_headways
order by stop_id, route_short_name_int, real_arrival_date
limit 1000;	  

select * from assessment_methods limit 100;

select count(*) from regularity_trip_headways
WHERE headway_s>120;
select count(*) from regularity_trip_headways
WHERE headway_s<=120;
select count(*) from regularity_trip_headways;
select count(*) from regularity_trip_headways
WHERE headway_s=0;
select count(*) from regularity_trip_headways
WHERE headway_s is null;
--update regularity_trip_headways set headway_s=0 where headway_s is null;

-- create table by timegroups
drop table regularity_trip_timegroups;
create table regularity_trip_timegroups as(select route_short_name_int,
stop_id, 
min(real_arrival_date) as from_date, 
min(real_arrival_date)::time as  from_time, 
 max(real_depature_date) as  to_date,
 max(real_depature_date)::time as  to_time,
EXTRACT(EPOCH FROM (max(real_depature_date)-min(real_arrival_date)))/60 as delta_interval,
grp, count(*) as elements_time_group,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s_new) as headway_s_median,
avg(headway_s_new)  as headway_s_avg,
			sum(headway_s_new^2) as headway_s_2	,
			sum(headway_s_new) as headway_s_sum,
										   case
										   when sum(headway_s_new)=0 THEN 0
										   else sum(headway_s_new^2)/(2*sum(headway_s_new))	 
										   end as waiting_time
from (
	select h.*,
	case
	when grp= lead(grp)  over (order by stop_id, route_short_name_int, real_arrival_date,grp) 
	and grp!= lag(grp) over (order by stop_id, route_short_name_int, real_arrival_date,grp) 
	then NULL
	else headway_s
	end headway_s_new
	from (
	select t.*,
      sum(case 
when delta_headway_s<120 then 1 else 0 end) over (partition by stop_id, route_short_name_int order by stop_id, route_short_name_int, real_arrival_date) as grp
      from (select 
r.*,
case 
WHEN lag(route_short_name_int) over (order by stop_id, route_short_name_int, real_arrival_date)=route_short_name_int and lag (stop_id) over (order by stop_id, route_short_name_int, real_arrival_date)=stop_id
then abs(headway_s - lag(headway_s) over (order by stop_id, route_short_name_int, real_arrival_date))
ELSE NULL END delta_headway_s
from regularity_trip_headways r) t
     )h 
) t
group by stop_id, route_short_name_int, grp
order by stop_id, route_short_name_int, grp );
			  
	
			
			
										   		  
			  
select * from regularity_trip_timegroups  
where headway_s_sum=0
order by stop_id, route_short_name_int,from_date 
limit 100 
;

select * from regularity_trip_timegroups  
order by stop_id, route_short_name_int,from_date 
limit 100 ;
select count(*) from regularity_trip_timegroups ;
select count(*) from regularity_trip_timegroups  where elements_time_group>1 ;



-- create table summarize data headways by route and stopid
drop table regularity_trip_summary_line;
create table  regularity_trip_summary_line as(select 
		route_short_name_int,
		stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median, 
		avg(headway_s)  as headway_s_avg,
		(sum(headway_s^2)/2*sum(headway_s))::float as waiting_time	
		from regularity_trip_headways
		group by stop_id, route_short_name_int 
		);
select * from regularity_trip_summary_line;








--SCHEDULED
select * from regularity_trip limit 100;

--create table headways scheduled
drop table regularity_trip_headways_scheduled;
create table regularity_trip_headways_scheduled as ((select r.route_short_name, 
r.stop_id, r.arrival_time,
case 
WHEN lag(r.route_short_name) over (order by r.stop_id, r.route_short_name,r.arrival_time)=r.route_short_name and lag (r.stop_id) over (order by r.stop_id, r.route_short_name,r.arrival_time)=r.stop_id
then EXTRACT(EPOCH FROM (r.arrival_time  - lag(r.arrival_time) over (order by r.stop_id, r.route_short_name,r.arrival_time))) 
ELSE NULL END headway_s,
date_from, date_to
from(select * from (select REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')::INT AS route_short_name,
			   REGEXP_REPLACE(stop_id, '[^0-9]', '', 'g')::INT AS stop_id,
			  arrival_time as before_arrival_time,
			   case
			   when arrival_time ~ '^24:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'24:','00:'), 'HH24:MI:SS')::TIME 
		when arrival_time ~ '^25:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'25:','01:'), 'HH24:MI:SS')::TIME 	   	   
	  	when arrival_time ~ '^26:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'26:','02:'), 'HH24:MI:SS')::TIME 	   	   
	  	when arrival_time ~ '^27:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'27:','03:'), 'HH24:MI:SS')::TIME 	   	   
	  else  TO_TIMESTAMP(arrival_time, 'HH24:MI:SS')::TIME
			   end
			   as arrival_time ,
	  date_from, date_to
			   from regularity_trip
	 		where date_format='2022-09-03'
	  and date_to>= '2021-09-01' and route_type = '3'
	  order by stop_id,route_short_name , arrival_time
	 ) i
where EXISTS(select *
			 from regularity_filter f
			 where i.route_short_name=f.route_short_name
			 and i.stop_id=f.stop_id
			 and TO_TIMESTAMP(EXTRACT(EPOCH FROM (i.date_from +(i.arrival_time))))::timestamp BETWEEN f.from_date_time 
			 and f.to_date_time	
			 and TO_TIMESTAMP(EXTRACT(EPOCH FROM (i.date_to +(i.arrival_time))))::timestamp BETWEEN f.from_date_time 
			 and f.to_date_time
)) r
order by r.stop_id,r.route_short_name ,r.arrival_time));


select * from regularity_trip
	 			where date_format='2022-09-03'
				and date_to>= '2021-09-01' and route_type = '3'
	  order by stop_id,route_short_name ,arrival_time
	  limit 10;	
	  
select * from assessment_methods a
order by a."stopId", a.route_id ,a.time_from
limit 20;

select * from stop_times limit 100;

select route_id, direction_id, count(*) from assessment_methods 
where method='regularity'
group by route_id, direction_id
order by route_id, direction_id
limit 20;

select * from regularity_trip_headways_scheduled 
order by stop_id,route_short_name ,arrival_time
limit 20;	  

select * 
from regularity_trip
where date_format='2022-09-03'
order by stop_id,route_short_name ,arrival_time
limit 10;	  

-- create table timegroups
drop table regularity_trip_timegroups_scheduled;

			  
			  
create table regularity_trip_timegroups_scheduled as(select route_short_name,
stop_id, 
min(arrival_time) as from_time, max(arrival_time) as to_time,
min(date_from) as from_date, max(date_to) as to_date,
TO_TIMESTAMP(EXTRACT(EPOCH FROM (min(date_from)+min(arrival_time))))::timestamp as from_date_time,
TO_TIMESTAMP(EXTRACT(EPOCH FROM ( max(date_to)+max(arrival_time))))::timestamp as to_date_time,

EXTRACT(EPOCH FROM (max(arrival_time)-min(arrival_time)))/60 as delta_interval,
grp, count(*) as elements_time_group,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s_new) as headway_s_median,
avg(headway_s_new)  as headway_s_avg,
			sum(headway_s_new^2) as headway_s_2	,
			sum(headway_s_new) as headway_s_sum,
										   case
										   when sum(headway_s_new)=0 THEN 0
										   else sum(headway_s_new^2)/(2*sum(headway_s_new))	 
										   end as waiting_time
from (
	select h.*,
	case
	when grp= lead(grp)  over (order by stop_id, route_short_name, arrival_time,grp) 
	and grp!= lag(grp) over (order by stop_id, route_short_name, arrival_time,grp) 
	then NULL
	else headway_s
	end headway_s_new
	from (
	select t.*,
      sum(case 
when delta_headway_s<120 then 1 else 0 end) over (partition by stop_id, route_short_name order by stop_id, route_short_name, arrival_time) as grp
      from (select 
r.*,
case 
WHEN lag(route_short_name) over (order by stop_id, route_short_name, arrival_time)=route_short_name and lag (stop_id) over (order by stop_id, route_short_name, arrival_time)=stop_id
then abs(headway_s - lag(headway_s) over (order by stop_id, route_short_name, arrival_time))
ELSE NULL END delta_headway_s
from regularity_trip_headways_scheduled r) t
     )h 
) t
group by stop_id, route_short_name, grp
order by stop_id, route_short_name, grp );
			  
	

	
	
	
	
	select count(*) from regularity_trip_timegroups_scheduled;
	select count(*) from regularity_trip_timegroups;
	select count(*) from assessment_methods where method='regularity';
	select count(*) from regularity_filter;
	
	select * from regularity_trip_timegroups_scheduled limit 10;
	select * from regularity_trip_timegroups limit 10;
	select * from regularity_filter limit 10;
	
	
	select r.route_short_name,count(*) from assessment_methods a inner join routes r on 
	r.route_id = a.route_id
	
	where method='regularity'
	group by r.route_short_name;
	
	select count(*) from assessment_methods
	where method='punctuality'
	limit 10;
			  
	select * from regularity_trip_timegroups
order by stop_id, route_short_name_int, grp
	limit 10;		  
			  
	select * from regularity_trip_timegroups_scheduled
	 where stop_id='1015' 
	order by stop_id, route_short_name, grp
	limit 10;	  
		  
		 select * from assessment_methods a
		 where a."stopId"='1015' 
order by a."stopId", a.route_id ,a.time_from
limit 20; 
		  
--create table summarize
drop table regularity_trip_summary_line_scheduled;
 create table  regularity_trip_summary_line_scheduled as(select 
		route_short_name,
		stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median, 
		avg(headway_s)  as headway_s_avg,
		(sum(headway_s^2)/2*sum(headway_s))::float as waiting_time	
		from regularity_trip_headways_scheduled
		group by route_short_name , stop_id
		);

	select * from regularity_trip_summary_line_scheduled limit 10;	  


-- get ETW
select s.route_short_name,r.stop_id, r.waiting_time-s.waiting_time as EWT from regularity_trip_summary_line r, regularity_trip_summary_line_scheduled s
where r.route_short_name_int= s.route_short_name and
r.stop_id= s.stop_id;


select count(*) from regularity_trip_timegroups_scheduled;
select count(*) from regularity_trip_timegroups;
select count(*) from assessment_methods where method='regularity';
select count(*) from regularity_filter;
	
	
	select * from regularity_trip_timegroups_scheduled limit 10;
	select * from regularity_trip_timegroups limit 10;
	select * from regularity_filter limit 10;
	select * from assessment_methods  where method='regularity'
	order by route_id , "stopId" limit 10;


drop table regularity_result_timegroups;
create table regularity_result_timegroups as (select s.route_short_name,r.stop_id, r.waiting_time-s.waiting_time as EWT,
r.waiting_time rw, s.waiting_time sw,
r.from_date rf, r.to_date rt, s.from_date_time sf, s.to_date_time st
from regularity_trip_timegroups r, regularity_trip_timegroups_scheduled s
where r.route_short_name_int= s.route_short_name and
r.stop_id= s.stop_id and (s.from_date_time::time,s.to_date_time::time)
overlaps(r.from_date::time,r.to_date::time)
order by r.stop_id,s.route_short_name,r.from_date);

select * from regularity_result_timegroups limit 200;
select count(*) from regularity_result_timegroups;
select count(*) from regularity_trip_timegroups;
select count(*) from regularity_trip_timegroups_scheduled;

select count(*) from regularity_result_timegroups where ewt>0;
select count(*) from regularity_result_timegroups where ewt<0;

select count(*) from regularity_result_timegroups where ewt>60;
select count(*) from regularity_result_timegroups where ewt>120;
select count(*) from regularity_result_timegroups where ewt>720;


--Analysis


