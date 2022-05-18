########################################################################
#Script Name : seed_mdm_sftp.sh
#Purpose     : Script to pick the item files from MDM to SEED server
#Create Date : 26 - June - 2019
#Company     : Infosys
########################################################################  

#!/bin/bash
. $SEED_SHARED_PATH/Scripts/FDP/prod.profile

EXIT_STATUS=1

DATETIME=`date +%D" "%T`
echo "Process Started at: $DATETIME"

TABLE_NM="itm_unabbr"
TEMP_TABLE_NM="temp_itm_unabbr"

psql -v schema=$RS_SEED_LNDP -h $RS_SEED_HOST -d $RS_SEED_ENV -U $RS_SEED_USER -p $RS_PORT -v ON_ERROR_STOP=1<< Eof

create temp table $TEMP_TABLE_NM as
select step_id,mtrl_typ,mtrl_nbr,mtrl_desc,mtrl_unabbr_desc,mnftr_prod_cd,pack,size,sizeuom,gtin,xplnt_sts,vndr_strg_cd,tot_shlf_lf,prod_hierchy,ctmn_corp,true_manfr,brnd_nm,src_creat_dttm,src_mod_dttm,insrt_dttm,updt_dttm,gs1gpc,mtrl_unabbr_desc_es,txt_typ,txt_val,len,wdth,hgt,lwhuom,net_wgt,grs_wgt,wgt_unit,sysco_brnd_prod,catch_wgt_ind,fnb_flg,vndr_ti,vndr_hi,itm_regulatory_type_cd,itm_regulatory_desc,haz_ind,haz_cd,chld_typ_cd,parentsupc
from ( select *, row_number() over (partition by mtrl_nbr order by src_creat_dttm desc, src_mod_dttm desc) row_num from $RS_SEED_LNDP.$TABLE_NM)
  where row_num = 1;

truncate table $RS_SEED_LNDP.$TABLE_NM;

insert into $RS_SEED_LNDP.$TABLE_NM (step_id,mtrl_typ,mtrl_nbr,mtrl_desc,mtrl_unabbr_desc,mnftr_prod_cd,pack,size,sizeuom,gtin,xplnt_sts,vndr_strg_cd,tot_shlf_lf,prod_hierchy,ctmn_corp,true_manfr,brnd_nm,src_creat_dttm,src_mod_dttm,insrt_dttm,updt_dttm,gs1gpc,mtrl_unabbr_desc_es,txt_typ,txt_val,len,wdth,hgt,lwhuom,net_wgt,grs_wgt,wgt_unit,sysco_brnd_prod,catch_wgt_ind,fnb_flg,vndr_ti,vndr_hi,itm_regulatory_type_cd,itm_regulatory_desc,haz_ind,haz_cd,chld_typ_cd,parentsupc)
 select * from $TEMP_TABLE_NM;


Eof

DATETIME=`date +%D" "%T`
echo "Process Completed at: $DATETIME"
#*******************************************End**************************************************************************#
