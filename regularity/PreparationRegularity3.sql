update stop_lines_regularity
set real_arrival_time=NULL;

update stop_lines_regularity
set real_departure_time=NULL;


update stop_lines_regularity
set speed_used=0;

select * from stop_lines_regularity
where route_id=2 order by trip_id, stop_sequence
limit 30;

select count(*) from stop_lines_regularity;

CREATE TABLE stop_lines_regularity_data (
	trip_id BIGINT,
	route_id INT,
	stop_id_1 TEXT,
	stop_id_2 TEXT,
	delta_time FLOAT,
	distance FLOAT,
	speed FLOAT
);

select * from stop_lines_regularity_data;

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
drop table regularity_trip_speed;
select * from regularity_trip_speed order by route_short_name;

SET datestyle = dmy;

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

select *
from vehiclepositioncomplete vc
inner join regularity_trip_speed r
on r.route_short_name_int=vc.lineid and r.last_stop=vc.directionid
order by route_short_name_int, time, last_stop
limit 5;

select *
from regularity_trip_times 
order by route_short_name_int, stop_id, real_arrival_time
limit 5;

select count(*) from regularity_trip_times group by route_short_name_int, stop_id;



CREATE TABLE regularity_trip_summary_line (
	route_short_name_int INT,
	stop_id INT,
	headway_s_median FLOAT,
	headway_s_avg FLOAT
);
drop table regularity_trip_summary_line;
select * from regularity_trip_summary_line;

CREATE OR REPLACE FUNCTION test()
RETURNS void AS 
$$
DECLARE temprow RECORD;
BEGIN 
	FOR temprow IN
        (SELECT route_short_name_int, stop_id FROM regularity_trip_times group by route_short_name_int, stop_id ORDER BY route_short_name_int, stop_id)
    LOOP
		
		INSERT INTO regularity_trip_summary_line (route_short_name_int,stop_id,headway_s_median,headway_s_avg) select 
		h.route_short_name_int,
		h.stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY h.headway_s) as headway_s_median, 
		avg(h.headway_s)  as headway_s_avg
		from (select route_short_name_int, 
			 stop_id,
			   real_time,
			   real_arrival_time,
			   EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
		from regularity_trip_times 
		where route_short_name_int=temprow.route_short_name_int and stop_id=temprow.stop_id
		) h
		group by h.route_short_name_int,h.stop_id ;
	END 
	END LOOP;
$$ LANGUAGE plpgsql;




WITH test as (FOR temprow IN
        (SELECT route_short_name_int, stop_id FROM regularity_trip_times group by route_short_name_int, stop_id ORDER BY route_short_name_int, stop_id)
    LOOP
		
		INSERT INTO regularity_trip_summary_line (route_short_name_int,stop_id,headway_s_median,headway_s_avg) select 
		h.route_short_name_int,
		h.stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY h.headway_s) as headway_s_median, 
		avg(h.headway_s)  as headway_s_avg
		from (select route_short_name_int, 
			 stop_id,
			   real_time,
			   real_arrival_time,
			   EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
		from regularity_trip_times 
		where route_short_name_int=temprow.route_short_name_int and stop_id=temprow.stop_id
		) h
		group by h.route_short_name_int,h.stop_id ;
	END LOOP);

explain ANALYSE (select test());

select * from regularity_trip_summary_line;

delete  from regularity_trip_summary_line;

INSERT INTO regularity_trip_summary_line (route_short_name_int,stop_id,headway_s_median,headway_s_avg) 
select 
		h.route_short_name_int,
		h.stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY h.headway_s) as headway_s_median, 
		avg(h.headway_s)  as headway_s_avg
		from (select route_short_name_int, 
			 stop_id,
			   real_time,
			   real_arrival_time,
			   EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
		from regularity_trip_times 
		where route_short_name_int=1 and stop_id=8012
		) h
		group by h.route_short_name_int,h.stop_id ;

select 
		h.route_short_name_int as route_short_name_int,
		h.stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY h.headway_s) as headway_s_median, 
		avg(h.headway_s)  as headway_s_avg
		from (select route_short_name_int, 
			 stop_id,
			   real_time,
			   real_arrival_time,
			   EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
		from regularity_trip_times 
		where route_short_name_int=temprow.route_short_name_int and stop_id=temprow.stop_id;

select 
		h.route_short_name_int as route_short_name_int,
		h.route_short_name,
		h.stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY h.headway_s) as headway_s_median, 
		avg(h.headway_s)  as headway_s_avg
		from (select route_short_name_int, 
			 stop_id,
			   real_time,
			   real_arrival_time,
			   EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
		from regularity_trip_times 
		where route_short_name_int=2 and stop_id=8244
		) h
		group by h.route_short_name_int,h.route_short_name,h.stop_id ;

select testfinal();	  
create or replace function testfinal()
returns void as $$
declare 
emp record;
count int;
begin 
count:=0;
FOR emp in SELECT route_short_name_int, stop_id FROM regularity_trip_times group by route_short_name_int, stop_id ORDER BY route_short_name_int, stop_id 
loop 
raise notice '%', count;
count:=count+1;
INSERT INTO regularity_trip_summary_line (route_short_name_int,stop_id,headway_s_median,headway_s_avg) 
select h.route_short_name_int, h.stop_id,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY h.headway_s) as headway_s_median, 
avg(h.headway_s)  as headway_s_avg
from (select route_short_name_int, stop_id,real_time,real_arrival_time,
EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
from regularity_trip_times 
where route_short_name_int=emp.route_short_name_int and stop_id=emp.stop_id) h
group by h.route_short_name_int,h.stop_id ;
end loop;
end;
$$language plpgsql;		
	
			  
create table  regularity_trip_summary_line as(select 
		route_short_name_int,
		stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median, 
		avg(headway_s)  as headway_s_avg
		from regularity_trip_headways
		group by route_short_name_int , stop_id);
		
select * from regularity_trip_summary_line;
			  create table regularity_trip_headways as (select route_short_name_int, stop_id,real_time,real_arrival_time,
EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) as headway_s
from regularity_trip_times 
group by route_short_name_int , stop_id,real_time,real_arrival_time
order by route_short_name_int , stop_id,real_time,real_arrival_time);
			 
select route_short_name_int, stop_id, min(real_time) as from_date, max(real_time) as to_date,
       grp, count(*) as elements_time_group,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median,
		avg(headway_s)  as headway_s_avg
from (select t.*,
      sum(case 
		  when headway_s>720 then 1 else 0 end) over (partition by route_short_name_int, stop_id order by route_short_name_int, stop_id,real_time, real_arrival_time,headway_s ) as grp
      from regularity_trip_headways t
     ) t
group by route_short_name_int, stop_id, grp
			  order by route_short_name_int, stop_id, grp
			  limit 10;	
			  
			  
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
			  
select * from regularity_trip_timegroups 
			  limit 100
			  ;
			  
select * from regularity_trip limit 10;
			  
select * from regularity_trip_headways 
			  limit 100
			  ;
			  
			  
			  
create table regularity_trip_headways as (select route_short_name_int, 
stop_id,real_time,real_arrival_time,
case 
WHEN lag(route_short_name_int) over (order by route_short_name_int, stop_id,real_time, real_arrival_time)=route_short_name_int and lag (stop_id) over (order by route_short_name_int, stop_id,real_time, real_arrival_time)=stop_id
then EXTRACT(EPOCH FROM (real_arrival_time - lag(real_arrival_time) over (order by route_short_name_int, stop_id,real_time, real_arrival_time))) 
ELSE NULL END headway_s
from regularity_trip_times 
order by route_short_name_int , stop_id,real_time,real_arrival_time);
			  
			  select * from regularity_trip limit 10;	  
			  
			  
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
			  
select * from regularity_trip_timegroups_scheduled limit 10;	  
	
			  drop table regularity_trip_timegroups_scheduled;
			  
			
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
			  
		select * from regularity_trip_summary_line limit 10;
			  drop table regularity_trip_summary_line;
			  
			  create table  regularity_trip_summary_line as(select 
		route_short_name_int,
		stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY headway_s) as headway_s_median, 
		avg(headway_s)  as headway_s_avg,
		(sum(headway_s^2)/2*sum(headway_s))::float as waiting_time	
		from regularity_trip_headways
		group by route_short_name_int , stop_id
		);
			  
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
	
			  select s.route_short_name,r.stop_id, r.waiting_time-s.waiting_time as EWT from regularity_trip_summary_line r, regularity_trip_summary_line_scheduled s
			  where r.route_short_name_int= s.route_short_name and
			  r.stop_id= s.stop_id
			  ;
			  
			  
			  create table  regularity_trip_summary_line_scheduled as(select 
		 r.route_short_name,
		 r.stop_id,
		PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY  r.headway_s) as headway_s_median, 
		avg( r.headway_s)  as headway_s_avg,
		(sum(r.headway_s_2)/2*sum( r.headway_s))::float as	waiting_time											  
		from (
			select 
			route_short_name,
			stop_id,
			headway_s_median as headway_s,
			headway_s_median^2 as headway_s_2
		
		from regularity_trip_timegroups_scheduled) r
		group by r.route_short_name ,  r.stop_id);
			  
			  select * from regularity_trip_timegroups_scheduled limit 10;
			  
			  
			 

			  
			  
			  
			  
			  (select REGEXP_REPLACE(route_short_name, '[^0-9]', '', 'g')::INT AS route_short_name,
			   REGEXP_REPLACE(stop_id, '[^0-9]', '', 'g')::INT AS stop_id,
			  arrival_time as before_arrival_time,
			   case
			   when arrival_time ~ '^24:(.*)' then TO_TIMESTAMP(REPLACE(arrival_time,'24:','00:'), 'HH24:MI:SS')::TIME 
			   else  TO_TIMESTAMP(arrival_time, 'HH24:MI:SS')::TIME
			   end
			   as arrival_time 
			   from regularity_trip limit 1000);
			  

select * from regularity_trip_headways order by route_short_name_int , stop_id,real_time,real_arrival_time limit 20000;
			  
			  
			  
			  
			  
			  
			  
			  
			  
			  
			  
			  
			  
			  
select *
from vehiclepositioncomplete
limit 5;

select *
from stop_times
limit 5;

select *
from routes
limit 5;
select * from regularity_trip_times limit 4;

select * from regularity_trip_speed where speed=0;


select * from regularity_trip_speed where speed=1;

select * from regularity_trip_speed ;

update regularity_trip_speed set speed=1 where speed=0;



SELECT TO_CHAR(TO_TIMESTAMP(time / 1000), 'DD/MM/YYYY HH24:MI:SS')::timestamp 
at time zone 'UTC' 
at time zone 'Europe/Brussels' 
FROM vehiclepositioncomplete
limit 5;

SELECT TO_TIMESTAMP(time / 1000)::date
FROM vehiclepositioncomplete
limit 5;

SELECT time
FROM vehiclepositioncomplete
limit 5;

CREATE TABLE stop_times (
    trip_id BIGINT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INT,
    pickup_type INT,
    drop_off_type INT,
    date_format DATE NOT NULL

);


Select distinct t.route_id,
    t.service_id,
    t.trip_id,
    t.trip_headsign,
    t.direction_id,
    st.arrival_time,
    st.departure_time,
    st.stop_id,
    r.route_type,
	r.route_type,
    c.start_date,
    c.end_date,
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
    on t.trip_headsign = SUBSTRING(r.route_long_name from POSITION('-' IN r.route_long_name)+2
                        for char_length(r.route_long_name)-POSITION('-' IN r.route_long_name)+1)
    and r.route_id = t.route_id
    where c.end_date >= '2021-09-01'
    and r.route_type = '3'
    order by t.route_id,t.direction_id,c.start_date,t.service_id,st.stop_id,st.arrival_time;

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
    on t.trip_id = st.trip_id and t.date_format = st.date_format
    inner join calendar c
    on t.service_id = c.service_id and t.date_format = c.date_format
    inner join routes r
    on t.trip_headsign = SUBSTRING(r.route_long_name from POSITION('-' IN r.route_long_name)+2
                        for char_length(r.route_long_name)-POSITION('-' IN r.route_long_name)+1)
    and r.route_id = t.route_id and r.date_format = t.date_format
    order by t.trip_id,t.route_id,st.stop_sequence,st.arrival_time
);

select * from stops limit 10;

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
    on t.trip_id = st.trip_id and t.date_format = st.date_format
    inner join calendar c
    on t.service_id = c.service_id and t.date_format = c.date_format
    inner join routes r
    on t.trip_headsign = SUBSTRING(r.route_long_name from POSITION('-' IN r.route_long_name)+2
                        for char_length(r.route_long_name)-POSITION('-' IN r.route_long_name)+1)
    and r.route_id = t.route_id and r.date_format = t.date_format
    order by t.trip_id,t.route_id,st.stop_sequence,st.arrival_time limit 10;


select distinct trip_id, arrival_time route_id,  stop_sequence, stop_id
from stop_lines_regularity
group by route_id,  stop_sequence, stop_id
order by  route_id,  stop_sequence, stop_id
limit 30;

select distinct route_id , route_short_name, date_format
from regularity_trip 
group by route_id , route_short_name, date_format 
order by route_id , route_short_name, date_format;

select * from regularity_trip limit 10;
select  *
from regularity_trip st,  
(select trip_id
from regularity_trip	
 where route_id=2 and route_short_name=1 and date_format ='2022-09-03'
limit 1) f
where st.trip_id=f.trip_id
order by stop_sequence;



create table regularity_stop_times as(
select *
from stop_lines_regularity st, (select route_id, route_short_name
							   from routes) r
	where st.route_id= r.route_id
);
select count(*) from stop_lines_regularity where date_format='2022-09-03';
select count(*) from stop_lines_regularity where date_format='2022-09-23';

select count(*) from stop_times where date_format='2022-09-23';
select count(*) from stop_times where date_format='2022-09-03';

2793425
select *
from stop_lines_regularity st inner join routes r
	on st.route_id= r.route_id and st.date_format=r.date_format limit 10;
	
select min(time) from vehiclepositioncomplete;

select max(time) from vehiclepositioncomplete;
select * from regularity_trip_speed;
drop table regularity_trip_speed;



select * from vehiclepositioncomplete vp 
inner join regularity_trip_speed r
on r.route_short_name = vp.lineid and 
r.date_format=vp.date_format limit 5;

