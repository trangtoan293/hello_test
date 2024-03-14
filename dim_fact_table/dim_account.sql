create view integration.dim_account as 
select hub.depst_act_sr_key as depst_act_sr_key 
,pit.*
from integration.pit_account pit 
join integration.hub_account hub
on pit.dv_hkey_hub_account = hub.dv_hkey_hub_account
where 1=1 
