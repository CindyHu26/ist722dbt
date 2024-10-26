with f_sales as (
    select *
    from {{ ref('fact_sales') }}
),
d_customer as (
    select *
    from {{ ref('dim_customer') }}
),
d_employee as (
    select *
    from {{ ref('dim_employee') }}
),
d_product as (
    select *
    from {{ ref('dim_product') }}
),
d_date as (
    select *
    from {{ ref('dim_date') }}
)
select 
    d_customer.*,
    d_employee.*,
    d_product.*,
    d_date.*,
    f_sales.OrderID,
    f_sales.quantity, 
    f_sales.extendedpriceamount, 
    f_sales.discountamount, 
    f_sales.soldamount
from f_sales
left join d_customer on f_sales.customerkey = d_customer.customerkey
left join d_employee on f_sales.employeekey = d_employee.employeekey
left join d_product on f_sales.productkey = d_product.productkey
left join d_date on f_sales.orderdatekey = d_date.datekey