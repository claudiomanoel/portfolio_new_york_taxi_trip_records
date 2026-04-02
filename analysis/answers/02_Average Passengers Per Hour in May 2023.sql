select day(pickup_date), pickup_hour, round(sum(passenger_count) / sum(quantity), 2) from workspace.`04_gold`.new_york_taxi_by_hour
where pickup_date between '2023-05-01' and '2023-05-31'
group by day(pickup_date), pickup_hour
order by 1 desc, 2 desc;