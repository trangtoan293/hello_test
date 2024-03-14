create view integration.brd_customer_account as
select 
lnk.dv_hkey_hub_customer dv_hkey_hub_customer
,lnk.dv_hkey_hub_account dv_hkey_hub_account
,hub_acct.depst_act_sr_key depst_act_sr_key
,hub_cust.cus_dtls_sr_key cus_dtls_sr_key
from integration.lnk_customer_account lnk
join integration.hub_customer hub_cust
on lnk.dv_hkey_hub_customer = hub_cust.dv_hkey_hub_customer
join integration.hub_account hub_acct
on lnk.dv_hkey_hub_account = hub_acct.dv_hkey_hub_account
where 1=1