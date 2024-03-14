create view integration.dim_customer as 
select hub.cus_dtls_sr_key as cus_dtls_sr_key 
,pit.*
from integration.pit_customer pit 
join integration.hub_customer hub
on pit.dv_hkey_hub_customer = hub.dv_hkey_hub_customer
where 1=1 
