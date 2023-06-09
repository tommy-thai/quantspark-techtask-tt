-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions

DROP TABLE IF EXISTS london_poc;
DROP TABLE IF EXISTS london_poc_hourly_recommendation;

CREATE TABLE london_poc AS
WITH glasses AS (
    SELECT DISTINCT drink_name,
                    glass_type
      FROM drinks
),
inventory AS (
    SELECT DISTINCT glass_type,
                    stock
      FROM bars
     WHERE bar = 'london'
)
SELECT hour,
       trans.glass_type,
       num_drinks_ordered,
       sum(stock) num_glasses_available
  FROM (
           SELECT date(timestamp) || 'T' || STRFTIME('%H', timestamp) || ':00:00' hour,
                  lower(glasses.glass_type) glass_type,
                  count(drink) num_drinks_ordered
             FROM transactions
                  LEFT JOIN
                  glasses ON transactions.drink = glasses.drink_name
            WHERE bar = 'london'
            GROUP BY date(timestamp) || 'T' || STRFTIME('%H', timestamp) || ':00:00',
                     lower(glasses.glass_type) 
       )
       trans
       LEFT JOIN
       inventory ON lower(trans.glass_type) = lower(inventory.glass_type) 
 GROUP BY hour,
          trans.glass_type,
          num_drinks_ordered;

CREATE TABLE london_poc_hourly_recommendation AS
SELECT glass_type,
       avg(num_drinks_ordered) avg_drinks_per_hour,
       max(num_drinks_ordered) max_drinks_per_hour,
       avg(num_glasses_available) num_glasses_available,
       max(avg(num_drinks_ordered) - avg(num_glasses_available), 0) glasses_to_purchase_avg,
       max(max(num_drinks_ordered) - avg(num_glasses_available), 0) glasses_to_purchase_max
  FROM london_poc
 GROUP BY glass_type;