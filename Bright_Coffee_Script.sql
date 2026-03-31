---Loading Bright Coffee Shop Table into Databricks
select * from `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1` limit 10;

--- Checking locations of stores from Bright_Coffee_Shop
Select Distinct store_location
From  `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;
/*We have observed that Bright Coffee Shop has Stores in 3 different locations
Which are: Lower Manhattan, Hell's Kitchen and Astoria.*/

---Checking differrent product category do Bright Coffee has
Select  Distinct product_category
From  `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;
/*We have observed that they have 9 differrent product categories available
Which are, Coffee, Tea, Drinking Chocolate, Bakery, Flavours, Loose Tea, Coffee beans, Packaged Chocolate, Branded */

--- Checking what types of products do they have
Select Distinct product_type
From  `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;
/* We have observed that they are selling 29 types of product*/

---Checking the details of product available
Select distinct product_detail
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;
/* We have observed that Bright coffee shop has 80 differrent products details*/

/*Data was collected from 2023/01/01*/
SELECT MIN(transaction_date) AS min_date 
FROM `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;

/*The last date of Data collection was 2023/06/30*/
/*This Data was collected in period of 6 months*/
SELECT MAX(transaction_date) AS latest_date 
FROM `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;

--- Checking Bright Coffee Shop Opetating hour

Select transaction_id, transaction_date,
Dayname(transaction_date) AS Day_name,
Monthname(transaction_date) AS Month_name,
---Calculating Revenue
transaction_qty*unit_price AS Revenue_Price
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;

/*Checking the minimum and Maximum product price*/
SELECT MIN(unit_price) As cheapest_price
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;

SELECT MAX(unit_price) As expensive_price
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;


Select transaction_date, Dayname(transaction_date) As Day_name,
Monthname(transaction_date) As Month_name, 
Count (Distinct transaction_id) As Num_of_Sale,
Sum (transaction_qty*unit_price) As Revenue_per_day
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`
Group by transaction_date, Day_name, Month_name;

/*Checking number of products available, number of Sale and Number of stores*/
SELECT 
COUNT(*) AS number_of_rows,
      COUNT(DISTINCT transaction_id) AS number_of_sales,
      COUNT(DISTINCT product_id) AS number_of_products,
      COUNT(DISTINCT store_id) AS number_of_stores
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;

/*Checking transections per day */
SELECT transaction_id,
      transaction_date,
      Dayname(transaction_date) AS Day_name,
      Monthname(transaction_date) AS Month_name,
      transaction_qty*unit_price AS revenue_per_Trans
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`;

/*Calculating revenue perday*/
SELECT 
      transaction_date,
      Dayname(transaction_date) AS Day_name,
      Monthname(transaction_date) AS Month_name,
      COUNT(DISTINCT transaction_id) AS Number_of_sales,
      SUM(transaction_qty*unit_price) AS revenue_per_day
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`
GROUP BY transaction_date,
         Day_name,
         Month_name;

select
---Dates
      transaction_date as purchase_date,
      Dayname(transaction_date) as day_name,
      Monthname(transaction_date) as month_name,
      Dayofmonth(transaction_date) as day_of_month,
      case
          when Dayname(transaction_date) in ('Sun' , 'Sat') then 'Weekend'
          else 'Weekday'
          end as day_classification,
      ---date_format(transaction_time, 'HH:mm:ss') as purchase_time,
      case
          when date_format(transaction_time, 'HH:mm:ss') between '00:00:00' and '11:59:59' then '0.1 Morning'
          when date_format(transaction_time, 'HH:mm:ss') between '12:00:00' and '16:59:59' then '0.2 Afternoon'
          when date_format(transaction_time, 'HH:mm:ss') >= '17:00:00' then '0.3 Evening'
          end as time_buckets,
---Counts of IDs
      count(*) as number_of_sales,
      count(distinct product_id) as number_of_products,
      count(distinct store_id) as number_stores,
---Revenue
      sum(transaction_qty*unit_price) as revenue_per_day,
---Categorical Columns
      store_location,
      product_category,
      product_detail
      
From `casestudy`.`bright`.`bright_coffee_shop_analysis_case_study_1`
group by transaction_date, 
        dayname(transaction_date), 
        monthname(transaction_date), 
        dayofmonth(transaction_date),
        case
            when Dayname(transaction_date) in ('Sun' , 'Sat') then 'Weekend'
            else 'Weekday'
            end,
        case
            when date_format(transaction_time, 'HH:mm:ss') between '00:00:00' and '11:59:59' then '0.1 Morning'
            when date_format(transaction_time, 'HH:mm:ss') between '12:00:00' and '16:59:59' then '0.2 Afternoon'
            when date_format(transaction_time, 'HH:mm:ss') >= '17:00:00' then '0.3 Evening'
            end,
        store_location, 
        product_category,
        product_detail;
