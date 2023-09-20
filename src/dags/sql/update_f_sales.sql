
-- for 3 task in project
DELETE FROM mart.f_sales WHERE exists(SELECT 1
									  FROM mart.d_calendar AS dc
									  WHERE dc.date_id = mart.f_sales.date_id AND
									  		dc.date_actual = '{{ds}}'
									  );

insert into  mart.f_sales (date_id,item_id,customer_id,city_id,quantity,payment_amount,status)
select dc.date_id,
	   uol.item_id,
	   uol.customer_id,
	   uol.city_id,
	   uol.quantity,
	   case 	
	   		when status = 'refunded' then payment_amount * -1
	   		else payment_amount 
	   end as payment_amount,
	   uol.status
from staging.user_order_log  as uol
left join mart.d_calendar  as dc 
	on uol.date_time::date = dc.date_actual 
where uol.date_time::date = '{{ds}}';