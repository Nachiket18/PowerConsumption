

CREATE DATABASE powerconsumption;

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

GO

CREATE TABLE device_data (DateCaptured DATE,TimeStampCaptured TIME,Global_active_power FLOAT,Global_Reactive_Power FLOAT,Volatage FLOAT,Global_Intensity FLOAT,Sub_Metering_1 FLOAT,Sub_Metering_2 FLOAT,Sub_Metering_3 FLOAT);

GO

SELECT percentile_cont(0.5)
WITHIN GROUP (ORDER BY global_active_power)
FROM device_data;

GO


SELECT datecaptured, sum(sum(global_intensity)) OVER(ORDER BY datecaptured)
FROM device_data
GROUP BY datecaptured;


GO


SELECT datecaptured, AVG(volatage) OVER(ORDER BY datecaptured
ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) AS smooth_temp 
FROM device_data 
WHERE datecaptured > date '2009–09–12' - INTERVAL '1 day' 
ORDER BY datecaptured DESC;

GO


SELECT datecaptured, global_reactive_power,timestampcaptured 
FROM (
        SELECT datecaptured,
        global_reactive_power,
        timestampcaptured,
        global_reactive_power - LAG(global_reactive_power) OVER  (ORDER BY timestampcaptured) AS diff
        FROM device_data) ht
        
        
 GO

 
 SELECT datecaptured,timestampcaptured,global_active_power,global_reactive_power,volatage,((global_active_power * 1000 / 60) - (sub_metering_1 + sub_metering_2 + sub_metering_3)) as sub_metering_remainder,global_intensity,sub_metering_1,sub_metering_2,sub_metering_3 
 FROM device_data dd 
 ORDER BY datecaptured LIMIT 10000
 

 GO


SELECT datecaptured,timestampcaptured,global_active_power,global_reactive_power,volatage,((global_active_power * 1000 / 60) - (sub_metering_1 + sub_metering_2 + sub_metering_3)) as sub_metering_remainder,global_intensity,sub_metering_1,sub_metering_2,sub_metering_3 
FROM device_data dd 
ORDER BY datecaptured LIMIT 2000000


GO

SELECT date_part('month',datecaptured) as Month ,datecaptured,timestampcaptured,global_active_power,global_reactive_power,volatage,((global_active_power * 1000 / 60) - (sub_metering_1 + sub_metering_2 + sub_metering_3)) as sub_metering_remainder,global_intensity,sub_metering_1,sub_metering_2,sub_metering_3 
FROM device_data dd 
WHERE date_part('year',datecaptured) = '2008'


GO

SELECT regexp_replace(a.elem,'[{}]','') as global_active_power_hist,
regexp_replace(a.elem2,'[{}]','') as global_reactive_power_hist,
regexp_replace(a.elem3,'[{}]','') as volatage_hist,
regexp_replace(a.elem4,'[{}]','') as global_intensity_hist,
regexp_replace(a.elem5,'[{}]','') as sub_metering_1_hist,
regexp_replace(a.elem6,'[{}]','') as sub_metering_2_hist,
regexp_replace(a.elem7,'[{}]','') as sub_metering_3_hist 
FROM(
	SELECT cast (histogram(global_active_power,0.076,11.122,100) as varchar(3000)) as Hist_active,
	cast (histogram(global_reactive_power,0,1.39,100) as VARCHAR(3000)) as Hist_reactive,
	cast (histogram(volatage,223.2,254.15,100) as VARCHAR(3000)) as hist_volt,
	cast (histogram(global_intensity,0.2,48.4,100) as VARCHAR(3000) ) as Hist_intensity,
	cast (histogram(sub_metering_1,0,88,100) as VARCHAR(3000)) as Hist_submetering,
	cast (histogram(sub_metering_2,0,80,100) as VARCHAR(3000)) as Hist_submetering2,
	cast (histogram(sub_metering_3,0,31,100) as VARCHAR(3000)) as Hist_submetering3 
	FROM device_data dd 
	) stg left join lateral unnest(string_to_array(stg.Hist_active,','),string_to_array(stg.Hist_reactive,','),
	string_to_array(stg.hist_volt,','),string_to_array(stg.Hist_intensity,','),string_to_array(stg.Hist_submetering,','),
	string_to_array(stg.Hist_submetering2,','),string_to_array(stg.Hist_submetering3,','))  
WITH ORDINALITY AS a(elem,elem2,elem3,elem4,elem5,elem6,elem7,nr) ON TRUE

GO

DROP TABLE IF EXISTS ActivePower_average CASCADE;

CREATE TABLE ActivePower_average(
three_hour TIMESTAMP WITHOUT TIME ZONE NOT NULL,
average NUMERIC
);

SELECT create_hypertable('ActivePower_average', 'three_hour');WITH data AS (

SELECT time_bucket('3 hour', datetimecaptured) AS three_hour,avg(global_active_power) AS average
FROM
(
	SELECT cast(CONCAT (datecaptured,' ', timestampcaptured) as TIMESTAMP) as datetimecaptured,global_active_power
	FROM device_data
) stg
GROUP BY three_hour
ORDER BY three_hour
),
period AS (
SELECT time_bucket('3 hour', no_gaps) three_hour
FROM generate_series(TIMESTAMP '2007–01–01 00:00:00', TIMESTAMP '2007–01–31 23:59:59', INTERVAL '1 hour') no_gaps
)
INSERT INTO ActivePower_average
	SELECT period.three_hour, coalesce(data.average, 0)
	FROM period
	LEFT JOIN data ON period.three_hour = data.three_hour
	ORDER BY period.three_hour;


