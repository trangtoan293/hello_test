create table source.depst_dtls_fact (
act_typ_cd string,
tm_sr_key int,
depst_act_nbr float,
depst_cnvrtd_act_nbr string,
prd_sr_key int,
brnch_sr_key int,
intrst_rte_typ_sr_key int,
mture_day_bnd_sr_key int,
act_bal_tier_sr_key int,
cus_bal_tier_sr_key int,
cus_brnch_tier_sr_key int,
inactv_day_bnd_sr_key int,
cus_dtls_sr_key int,
depst_act_sts_sr_key int,
trm_typ_sr_key int,
emp_sr_key int,
acum_sav_bnd_sr_key int,
gl_grp_sr_key int,
depst_prog_prd_sr_key int,
act_drmt_day_bnd_sr_key int,
trns_cur_cd string,
lcl_cur_cd string,
offcr_cd string,
depst_act_open_dt timestamp,
depst_act_clsd_dt timestamp,
mture_dt timestamp,
last_rdmptn_dt timestamp,
last_rnwl_dt timestamp,
next_sch_dt timestamp,
curr_sch_dt timestamp,
rte_rvw_dt timestamp,
sts_chng_dt timestamp,
last_depst_dt timestamp,
last_cus_initd_trns_dt timestamp,
gl_grp_cd string,
od_fac_flag int,
ermrk_flag int,
intrst_rte float,
od_exss_intrst_rte float,
secrd_ovr_draft_ind string,
intrst_accr_mthd_ind string,
rnwl_cntr int,
auto_rnwl_ind string,
tm_depst_fund_src_ind string,
nbr_of_day_to_mture int,
nbr_of_day_inactv int,
tms_od_this_cyc int,
ftp_rte float,
intrst_cyc_min_bal_in_lc float,
intrst_cyc_min_bal_in_tc float,
intrst_cyc_max_bal_in_lc float,
intrst_cyc_max_bal_in_tc float,
ldgr_bal_td_act_pmture_tdy_lc float,
ldgr_bal_td_act_pmture_tdy_tc float,
org_amt_in_lc float,
org_amt_in_tc float,
hld_amt_in_lc float,
hld_amt_in_tc float,
od_drwng_lmt_amt_in_lc float,
od_drwng_lmt_amt_in_tc float,
od_lmt_amt_in_lc float,
od_lmt_amt_in_tc float,
ctcd_intrst_amt_in_lc float,
ctcd_intrst_amt_in_tc float,
accr_intrst_amt_in_lc float,
accr_intrst_amt_in_tc float,
wtdle_intrst_amt_in_lc float,
wtdle_intrst_amt_in_tc float,
ovrdrwn_bal_amt_in_lc float,
ovrdrwn_bal_amt_in_tc float,
acum_sav_mth_depst_amt_in_lc float,
acum_sav_mth_depst_amt_in_tc float,
acum_sav_tot_depst_amt_in_lc float,
acum_sav_tot_depst_amt_in_tc float,
dly_actv_depst_act_flag int,
dly_ldgr_bal_amt_in_lc float,
dly_ldgr_bal_amt_in_tc float,
dly_intrst_accr_amt_in_lc float,
dly_intrst_pd_amt_in_lc float,
dly_intrst_pd_amt_in_tc float,
dly_debt_brnch_trns_amt_in_lc float,
dly_debt_brnch_trns_amt_in_tc float,
dly_debt_oth_trns_amt_in_lc float,
dly_debt_oth_trns_amt_in_tc float,
dly_crdt_brnch_trns_amt_in_lc float,
dly_crdt_brnch_trns_amt_in_tc float,
dly_crdt_oth_trns_amt_in_lc float,
dly_crdt_oth_trns_amt_in_tc float,
dly_nbr_debt_brnch_trns int,
dly_nbr_debt_oth_trns int,
dly_nbr_crdt_brnch_trns int,
dly_nbr_crdt_oth_trns int,
dly_nii_amt_in_lc float,
dly_nii_amt_in_tc float,
dly_fund_cst_amt_in_lc float,
dly_fund_cst_amt_in_tc float,
dly_fee_incm_amt_in_lc float,
dly_fee_incm_amt_in_tc float,
dly_net_intrst_mrgn_pct float,
dly_depst_statry_rsrv_in_lc float,
dly_depst_statry_rsrv_in_tc float,
dly_insrnce_prmum_amt_in_lc float,
dly_insrnce_prmum_amt_in_tc float,
dly_avg_intrst_amt_in_lc float,
dly_avg_intrst_amt_in_tc float,
dly_od_intrst_accr_amt_in_lc float,
dly_od_intrst_accr_amt_in_tc float,
dly_ftp_amt_in_lc float,
dly_ftp_amt_in_tc float,
dly_acum_sav_lte_depst_amt_in_lc float,
dly_acum_sav_lte_depst_amt_in_tc float,
dly_ldgrbal_act_opng_tdy_in_lc float,
dly_ldgrbal_act_opng_tdy_in_tc float,
dly_ldgr_bal_1m_6mth_in_lc float,
dly_ldgr_bal_1m_6mth_in_tc float,
dly_trmday_ostd_bal_amt_in_lc float,
dly_trmday_new_ostd_bal_amt_in_lc float,
dly_pnlty_intrst_amt_in_lc float,
dly_pnlty_intrst_amt_in_tc float,
mtd_actv_depst_act_flag int,
mtd_ldgr_bal_amt_in_lc float,
mtd_ldgr_bal_amt_in_tc float,
mtd_avg_ldgr_bal_amt_in_lc float,
mtd_avg_ldgr_bal_amt_in_tc float,
mtd_intrst_accr_amt_in_lc float,
mtd_intrst_pd_amt_in_lc float,
mtd_intrst_pd_amt_in_tc float,
mtd_debt_brnch_trns_amt_in_lc float,
mtd_debt_brnch_trns_amt_in_tc float,
mtd_debt_oth_trns_amt_in_lc float,
mtd_debt_oth_trns_amt_in_tc float,
mtd_crdt_brnch_trns_amt_in_lc float,
mtd_crdt_brnch_trns_amt_in_tc float,
mtd_crdt_oth_trns_amt_in_lc float,
mtd_crdt_oth_trns_amt_in_tc float,
mtd_nbr_debt_brnch_trns int,
mtd_nbr_debt_oth_trns int,
mtd_nbr_crdt_brnch_trns int,
mtd_nbr_crdt_oth_trns int,
mtd_nii_amt_in_lc float,
mtd_nii_amt_in_tc float,
mtd_fund_cst_amt_in_lc float,
mtd_fund_cst_amt_in_tc float,
mtd_fee_incm_amt_in_lc float,
mtd_fee_incm_amt_in_tc float,
mtd_depst_statry_rsrv_in_lc float,
mtd_depst_statry_rsrv_in_tc float,
mtd_insrnce_prmum_amt_in_lc float,
mtd_insrnce_prmum_amt_in_tc float,
mtd_avg_intrst_amt_in_lc float,
mtd_avg_intrst_amt_in_tc float,
mtd_od_intrst_accr_amt_in_lc float,
mtd_od_intrst_accr_amt_in_tc float,
mtd_avg_intrst_rte float,
mtd_ftp_amt_in_lc float,
mtd_ftp_amt_in_tc float,
mtd_acum_sav_incr_amt_depst_cnt int,
mtd_acum_sav_lte_depst_amt_in_lc float,
mtd_acum_sav_lte_depst_amt_in_tc float,
mtd_acum_sav_depst_amt_in_lc float,
mtd_acum_sav_depst_amt_in_tc float,
mtd_ldgrbal_act_opng_tdy_in_lc float,
mtd_ldgrbal_act_opng_tdy_in_tc float,
mtd_ldgr_bal_1m_6mth_in_lc float,
mtd_ldgr_bal_1m_6mth_in_tc float,
mtd_trmday_ostd_bal_amt_in_lc float,
mtd_trmday_new_ostd_bal_amt_in_lc float,
mtd_ldgr_bal_td_act_pmture_tdy_lc float,
mtd_ldgr_bal_td_act_pmture_tdy_tc float,
mtd_pnlty_intrst_amt_in_lc float,
mtd_pnlty_intrst_amt_in_tc float,
qtd_actv_depst_act_flag int,
qtd_ldgr_bal_amt_in_lc float,
qtd_ldgr_bal_amt_in_tc float,
qtd_avg_intrst_amt_in_lc float,
qtd_avg_intrst_amt_in_tc float,
qtd_avg_ldgr_bal_amt_in_lc float,
qtd_avg_ldgr_bal_amt_in_tc float,
qtd_intrst_accr_amt_in_lc float,
qtd_intrst_pd_amt_in_lc float,
qtd_intrst_pd_amt_in_tc float,
qtd_debt_brnch_trns_amt_in_lc float,
qtd_debt_brnch_trns_amt_in_tc float,
qtd_debt_oth_trns_amt_in_lc float,
qtd_debt_oth_trns_amt_in_tc float,
qtd_crdt_brnch_trns_amt_in_lc float,
qtd_crdt_brnch_trns_amt_in_tc float,
qtd_crdt_oth_trns_amt_in_lc float,
qtd_crdt_oth_trns_amt_in_tc float,
qtd_nbr_debt_brnch_trns int,
qtd_nbr_debt_oth_trns int,
qtd_nbr_crdt_brnch_trns int,
qtd_nbr_crdt_oth_trns int,
qtd_nii_amt_in_lc float,
qtd_nii_amt_in_tc float,
qtd_fund_cst_amt_in_lc float,
qtd_fund_cst_amt_in_tc float,
qtd_fee_incm_amt_in_lc float,
qtd_fee_incm_amt_in_tc float,
qtd_depst_statry_rsrv_in_lc float,
qtd_depst_statry_rsrv_in_tc float,
qtd_insrnce_prmum_amt_in_lc float,
qtd_insrnce_prmum_amt_in_tc float,
qtd_od_intrst_accr_amt_in_lc float,
qtd_od_intrst_accr_amt_in_tc float,
qtd_avg_intrst_rte float,
qtd_ftp_amt_in_lc float,
qtd_ftp_amt_in_tc float,
qtd_ldgrbal_act_opng_tdy_in_lc float,
qtd_ldgrbal_act_opng_tdy_in_tc float,
qtd_trmday_ostd_bal_amt_in_lc float,
qtd_trmday_new_ostd_bal_amt_in_lc float,
qtd_pnlty_intrst_amt_in_lc float,
qtd_pnlty_intrst_amt_in_tc float,
ytd_actv_depst_act_flag int,
ytd_ldgr_bal_amt_in_lc float,
ytd_ldgr_bal_amt_in_tc float,
ytd_avg_ldgr_bal_amt_in_lc float,
ytd_avg_ldgr_bal_amt_in_tc float,
ytd_intrst_accr_amt_in_lc float,
ytd_intrst_pd_amt_in_lc float,
ytd_intrst_pd_amt_in_tc float,
ytd_debt_brnch_trns_amt_in_lc float,
ytd_debt_brnch_trns_amt_in_tc float,
ytd_debt_oth_trns_amt_in_lc float,
ytd_debt_oth_trns_amt_in_tc float,
ytd_crdt_brnch_trns_amt_in_lc float,
ytd_crdt_brnch_trns_amt_in_tc float,
ytd_crdt_oth_trns_amt_in_lc float,
ytd_crdt_oth_trns_amt_in_tc float,
ytd_nbr_debt_brnch_trns int,
ytd_nbr_debt_oth_trns int,
ytd_nbr_crdt_brnch_trns int,
ytd_nbr_crdt_oth_trns int,
ytd_nii_amt_in_lc float,
ytd_nii_amt_in_tc float,
ytd_fund_cst_amt_in_lc float,
ytd_fund_cst_amt_in_tc float,
ytd_fee_incm_amt_in_lc float,
ytd_fee_incm_amt_in_tc float,
ytd_depst_statry_rsrv_in_lc float,
ytd_depst_statry_rsrv_in_tc float,
ytd_insrnce_prmum_amt_in_lc float,
ytd_insrnce_prmum_amt_in_tc float,
ytd_avg_intrst_amt_in_lc float,
ytd_avg_intrst_amt_in_tc float,
ytd_avg_intrst_rte float,
ytd_od_intrst_accr_amt_in_lc float,
ytd_od_intrst_accr_amt_in_tc float,
ytd_ftp_amt_in_lc float,
ytd_ftp_amt_in_tc float,
ytd_ldgrbal_act_opng_tdy_in_lc float,
ytd_ldgrbal_act_opng_tdy_in_tc float,
ytd_trmday_ostd_bal_amt_in_lc float,
ytd_trmday_new_ostd_bal_amt_in_lc float,
ytd_pnlty_intrst_amt_in_lc float,
ytd_pnlty_intrst_amt_in_tc float,
tot_11mth_avg_bal_amt_in_lc float,
tot_11mth_avg_bal_amt_in_tc float,
tot_12mth_avg_bal_amt_in_lc float,
tot_12mth_avg_bal_amt_in_tc float,
act_opng_flag int,
act_cls_flag int,
act_drmt_flag int,
act_1m_6mth_flag int,
mtd_act_opng_flag int,
mtd_act_cls_flag int,
mtd_act_1m_6mth_flag int,
pre_mture_tm_depst_clsre_flag int,
mtd_pre_mture_tm_depst_clsre_flag int,
mture_tm_depst_clsre_flag int,
mtd_mture_tm_depst_clsre_flag int,
crdt_rte_flag int,
acum_sav_depst_cyc_day int,
acum_sav_lte_depst_flag int,
mtd_acum_sav_lte_depst_flag int,
ltd_acum_sav_lte_depst_cnt int,
acum_sav_ovr_due_depst_flag int,
mtd_acum_sav_ovr_due_depst_flag int,
ltd_acum_sav_ovr_due_depst_cnt int,
acum_sav_miss_pmt_depst_flag int,
mtd_acum_sav_miss_pmt_depst_flag int,
ltd_acum_sav_miss_pmt_depst_cnt int,
qtd_act_cls_flag int,
ytd_act_cls_flag int,
ltd_intrst_pd_amt_in_lc float,
ltd_intrst_pd_amt_in_tc float,
exchg_rte float,
cob_dt timestamp,
src_sys_cd string,
updtd_by string,
updtd_dtm timestamp,
dly_intrst_accr_amt_in_tc float,
mtd_intrst_accr_amt_in_tc float,
qtd_intrst_accr_amt_in_tc float,
ytd_intrst_accr_amt_in_tc float,
act_cls_rsn_sr_key int,
tm_depst_trm_mth float,
dly_debt_all_trns_amt_in_lc float,
dly_debt_all_trns_amt_in_tc float,
dly_crdt_all_trns_amt_in_lc float,
dly_crdt_all_trns_amt_in_tc float,
dly_nbr_debt_all_trns int,
dly_nbr_crdt_all_trns int,
mtd_debt_all_trns_amt_in_lc float,
mtd_debt_all_trns_amt_in_tc float,
mtd_crdt_all_trns_amt_in_lc float,
mtd_crdt_all_trns_amt_in_tc float,
mtd_nbr_debt_all_trns int,
mtd_nbr_crdt_all_trns int,
qtd_debt_all_trns_amt_in_lc float,
qtd_debt_all_trns_amt_in_tc float,
qtd_crdt_all_trns_amt_in_lc float,
qtd_crdt_all_trns_amt_in_tc float,
qtd_nbr_debt_all_trns int,
qtd_nbr_crdt_all_trns int,
ytd_debt_all_trns_amt_in_lc float,
ytd_debt_all_trns_amt_in_tc float,
ytd_crdt_all_trns_amt_in_lc float,
ytd_crdt_all_trns_amt_in_tc float,
ytd_nbr_debt_all_trns int,
ytd_nbr_crdt_all_trns int,
qtr1_dly_ldgr_bal_amt_in_lc float,
qtr1_dly_ldgr_bal_amt_in_tc float,
mth1_end_ldgr_bal_amt_in_lc float,
mth1_end_ldgr_bal_amt_in_tc float,
mth2_end_ldgr_bal_amt_in_lc float,
mth2_end_ldgr_bal_amt_in_tc float,
mth3_end_ldgr_bal_amt_in_lc float,
mth3_end_ldgr_bal_amt_in_tc float,
prev_day_end_ldgr_bal_amt_in_tc float,
prev_day_end_ldgr_bal_amt_in_lc float,
prev_mth_end_ldgr_bal_amt_in_tc float,
prev_mth_end_ldgr_bal_amt_in_lc float,
prev_qtr_end_ldgr_bal_amt_in_tc float,
prev_qtr_end_ldgr_bal_amt_in_lc float,
prev_yr_end_ldgr_bal_amt_in_tc float,
prev_yr_end_ldgr_bal_amt_in_lc float,
cmtmnt_fee_eff_dt timestamp,
ltd_pnlty_intrst_amt_in_lc float,
ltd_pnlty_intrst_amt_in_tc float,
dly_avl_bal_amt_in_lc float,
dly_avl_bal_amt_in_tc float,
dly_intrst_expse_tc float,
dly_nii_alco_amt_in_tc float,
dly_nii_alco_amt_in_lc float,
mtd_nii_alco_amt_in_tc float,
mtd_nii_alco_amt_in_lc float,
qtd_nii_alco_amt_in_tc float,
qtd_nii_alco_amt_in_lc float,
ytd_nii_alco_amt_in_tc float,
ytd_nii_alco_amt_in_lc float,
dly_intrst_expse_lc float,
mony_src_sr_key int,
acct_purp_sr_key int,
cus_cif_nbr string,
mtd_intrst_expse_tc float,
mtd_intrst_expse_lc float,
depst_act_sr_key_o int,
ltd_debt_all_trns_amt_in_tc float,
ltd_debt_all_trns_amt_in_lc float,
ltd_crdt_all_trns_amt_in_tc float,
ltd_crdt_all_trns_amt_in_lc float,
depst_ftp_lqdy float,
depst_ftp_prc float,
prev_wk_end_ldgr_bal_amt_in_tc float,
prev_wk_end_ldgr_bal_amt_in_lc float,
prev_mth_end_avg_ldgr_bal_amt_in_lc float,
prev_yr_end_avg_bal_amt_in_lc float,
prev_yr_mth_avg_ldgr_bal_amt_in_lc float,
prev_yr_mth_ldgr_bal_amt_in_lc float,
prev_wk_avg_ldgr_bal_amt_in_tc float,
prev_wk_avg_ldgr_bal_amt_in_lc float,
int_ran_rte string,
dept_vcrm_sr_key int,
depst_act_sr_key float )
using iceberg 
tblproperties (
'engine.hive.enabled'='true',
'read.parquet.vectorization.enabled'='true',
'read.parquet.vectorization.batch-size'='10000',
'write.target-file-size-bytes'='536870912',
'write.distribution-mode'='hash',
'write.merge.mode'='merge-on-read',
'write.update.mode'='merge-on-read',
'write.delete.mode'='merge-on-read'
)