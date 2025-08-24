with payment as 
    (
    select * from {{ ref('stg_stripe__payments') }}
    where payment_status = 'success'
)
,pivoted as 
(   
    Select order_id,
        {%- set payment_methods = ['bank_transfer','coupon','credit_card','gift_card'] -%}

        {%- for payment_method in payment_methods -%}

        sum(case when payment_method = '{{payment_method}}' then payment_amount else 0 end) as {{payment_method}},  

        {% endfor %}

       
    from payment
    group by 1

)

select * 
from pivoted