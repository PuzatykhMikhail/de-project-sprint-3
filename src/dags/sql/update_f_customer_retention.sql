create table if not exists mart.f_customer_retention(
 new_customers_count          bigint,
 returning_customers_count    bigint,
 refunded_customer_count	  bigint,
 period_name				  varchar(30),
 period_id				      int,
 item_id					  bigint,
 new_customers_revenue 	      numeric(14,2),
 returning_customers_revenue  numeric(14,2),
 customers_refunded    		  int
);


delete from mart.f_customer_retention where period_id in(
	select date_id
	from staging.user_order_log as uol
	left join mart.d_calendar  as dc 
	on uol.date_time::date = dc.date_actual 
	where uol.date_time::date = '{{ds}}'

);


insert into mart.f_customer_retention (new_customers_count,returning_customers_count,refunded_customer_count,period_name,period_id,item_id,new_customers_revenue,returning_customers_revenue,customers_refunded)
select 
	   count(distinct case
						when status = '1'
						then customer_id
					  end
			) as new_customer_count,
	   count(distinct case
			 			when status <> '1'
			 			then customer_id
	  			     end
	  	    ) as returning_customers_count ,
	   count(distinct case
			 			when status = 'refunded'
			 			then customer_id
	  			     end
	  	    ) as refunded_customer_count,
	  	'weekly' as period_name,
	  	weekly as period_id,
	  	item_id,
	  	sum(case
	  			when status = '1'
	  			then payment_amount
	  		end
	  	   ) as new_customers_revenue, 
	  	sum(case
	  			when status <> '1'
	  			then payment_amount
	  		end
	  	   ) as returning_customers_revenue, 
	  	sum(case
	  			when status = 'refunded'
	  			then quantity
	  		end
	  	   ) as customers_refunded  
from(
		select 
			date_part('week', dc.date_actual) as weekly,
			dcust.customer_id,
			sal.quantity,
			sal.payment_amount,
			case 
				when count(dcust.customer_id) over(partition by dcust.customer_id, date_part('week', dc.date_actual)) = 1
				then '1' 
				else sal.status
			end as  status,
			sal.item_id
		from mart.f_sales as sal
		join mart.d_customer dcust  
			on sal.customer_id  = dcust.customer_id 
		join mart.d_calendar dc 
			on sal.date_id  = dc.date_id
	 ) as  flag
group by weekly, item_id;