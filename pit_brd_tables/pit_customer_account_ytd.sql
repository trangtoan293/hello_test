create view integration.pit_customer_account_ytd as
with cte_get_rnk as (
select *,
row_number() over (PARTITION BY dv_hkey_lnk_customer_account order by dv_src_ldt) rnk
from integration.lsat_der_customer_account_ytd)
, cte_get_rnk_prev_hsh_dif as (
select *,
lag(dv_hsh_dif) over (PARTITION BY dv_hkey_lnk_customer_account order by dv_src_ldt) prev_hsh_dif
from cte_get_rnk )
,cte_get_latest_record (
select * from cte_get_rnk_prev_hsh_dif
where rnk =1 )
select * ,
coalesce(lead(dv_src_ldt) over (partition by dv_hkey_lnk_customer_account order by dv_src_ldt), cast('9999-12-31' as date)) as dv_src_ldt_end
from cte_get_latest_record sat
where prev_hsh_dif <> dv_hsh_dif
or prev_hsh_dif is null
;
