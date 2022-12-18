select * from regularity_trip_headways
where route_short_name_int=64
order by stop_id, route_short_name_int,direction_id, real_arrival_date
limit 100;

select count(*) from regularity_trip_headways
where headway_s>14400;

select count(*) from regularity_trip_headways
where headway_s>10000 and real_arrival_time::time between '04:00:00'::time and '06:00:00';

select * from regularity_trip_headways
where headway_s>10000 and real_arrival_time::time between '04:00:00'::time and '06:00:00';

select * from regularity_trip_headways
where headway_s>10000 ;


select * from regularity_trip_headways
where headway_s>10000
order by  real_arrival_date;

update regularity_trip_headways set headway_s=0
where headway_s>10000 and real_arrival_time::time between '04:00:00'::time and '06:00:00';

update regularity_trip_headways set headway_s=0
where headway_s>14400;


select count(*) from regularity_trip_headways
where headway_s>12000;
select count(*) from regularity_trip_headways
where headway_s>10000;

select * from regularity_trip_headways
where real_arrival_time between '23:00:00'::time and '05:40:00'::time
limit 100;