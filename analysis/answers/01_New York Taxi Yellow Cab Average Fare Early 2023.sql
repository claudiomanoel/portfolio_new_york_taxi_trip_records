select
  year(pickup_date) as year,
  month(pickup_date) as month,
  round(sum(total_amount) / sum(quantity), 2) as average_total_amount
from
  workspace.`04_gold`.new_york_taxi_by_hour as h
where
  year(pickup_date) = 2023
  and month(pickup_date) between 1 and 5
  and color = 'yellow'
group by
  month(pickup_date),
  year(pickup_date)
order by
  1 desc,
  2 desc;