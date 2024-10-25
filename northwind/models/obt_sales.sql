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
    f_sales.orderid, 
    d_customer.companyname, 
    d_customer.contactname, 
    d_customer.contacttitle, 
    d_customer.address, 
    d_customer.city, 
    d_customer.country,
    d_employee.employeenamefirstlast, 
    d_employee.employeetitle,
    d_product.productname, 
    d_product.categoryname,
    d_date.date as orderdate,
    f_sales.quantity, 
    f_sales.extendedpriceamount, 
    f_sales.discountamount, 
    f_sales.soldamount
from f_sales
left join d_customer on f_sales.customerkey = d_customer.customerkey
left join d_employee on f_sales.employeekey = d_employee.employeekey
left join d_product on f_sales.productkey = d_product.productkey
left join d_date on f_sales.orderdatekey = d_date.datekey
