SELECT distancefrompoint, count(*) FROM vehicleposition group by distancefrompoint;

SELECT distancefrompoint, count(*) FROM vehicleposition where distancefrompoint is null group by distancefrompoint

SELECT count(*) FROM vehicleposition

SELECT count(*) FROM vehicleposition v where v.vehicle_position_id is null or
v.time is null or v.lineid is null or v.directionid is null or
v.distancefrompoint is null or v.pointid is null


SELECT lineid, avg(distancefrompoint) FROM vehicleposition group by lineid;


SELECT route_id FROM trips group by route_id;


SELECT pointid, avg(distancefrompoint) FROM vehicleposition group by pointid;
SELECT stop_id, count(*) FROM actu_stops group by stop_id order by count(*) desc;
SELECT stop_id, count(*) FROM stops group by stop_id order by count(*) desc;



SELECT code_ligne FROM actu_stops group by code_ligne;
SELECT id FROM actu_lines group by id;


SELECT route_short_name FROM routes group by route_short_name;
SELECT shape_id FROM shapes group by shape_id;






SELECT lineid, avg(distancefrompoint) FROM vehicleposition group by lineid;
SELECT * FROM actu_lines  order by date_fin desc limit 5 ;


SELECT * FROM stop_times limit 5 ;
SELECT * FROM trips limit 5 ;
SELECT * FROM vehicleposition limit 5 ;
SELECT * FROM routes limit 5 ;
SELECT * FROM shapes limit 5 ;
SELECT * FROM actu_stops limit 5 ;
SELECT * FROM actu_lines limit 5 ;

SELECT * FROM vehicleposition limit 10 ;


SELECT * FROM agency limit 5 ;

SELECT * FROM assessment_methods limit 5 ;

SELECT route_type,count(*) FROM assessment_methods group by route_type;


