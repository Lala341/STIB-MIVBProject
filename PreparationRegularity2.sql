select * from vehiclepositioncomplete 
order by lineid, time limit 30;

select * from stop_lines_regularity_data
limit 10;

select * from vehiclepositioncomplete 
where  pointid=8272 and lineid=1
order by lineid, time limit 30;

select * from vehiclepositioncomplete 
where lineid=0
order by lineid, time limit 30;

select * from vehiclepositioncomplete 
where lineid=2 
order by time limit 30;

select * from stop_times
where trip_id= 112947202236269500
order by stop_sequence;

select * from stop_lines_regularity_data
where trip_id= 112947202236269500;
select * from stop_lines_regularity
where trip_id= 112947202236269500;

select * from trips
where trip_id= 112947202236269500;

select route_id,route_type, count(*) 
from routes where route_type='0' 
group by route_id, route_type 
order by route_id, route_type;

select route_id, count(*) 
from routes group by route_id 
order by route_id;

select * 
from routes limit 2;

select lineid, count(*) 
from vehiclepositioncomplete group by lineid 
order by lineid;

select count(*) from stop_times;

select count(*) from stop_times group by trip_id;