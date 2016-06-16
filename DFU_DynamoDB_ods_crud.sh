#-----------------------------------
#-- Bash DFU_DynamoDB.sh
#-----------------------------------
clear

#-----------------------------------
#--List devices for a user
#-----------------------------------
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_row -Hcust_id   -h32928
#-----------------------------------
#--Register a new device
#-----------------------------------
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Acreate -Tmb_serialno_row -Hcust_id   -h32928      -Rserial_no -rR10015369
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_row -Hcust_id   -h32928
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_hdr -Hserial_no -hR10015369
#-----------------------------------
#--Set MakerCare months
#-----------------------------------
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aupdate -Tmb_serialno_hdr -Hserial_no -hR10015369 -DUpdate_dt -Vmkcr_mths -v24
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_hdr -Hserial_no -hR10015369
#-----------------------------------
#--Un-Register an old device
#-----------------------------------
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_hdr -Hserial_no -hR10015368
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Adelete -Tmb_serialno_row -Hcust_id   -h32928      -Rserial_no -rR10015368
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_row -Hcust_id   -h32928



#-----------------------------------
#-----------------------------------
#-----------------------------------
#-----------------------------------
#-----------------------------------
#--UNDO
#-----------------------------------
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_row -Hcust_id   -h32928
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aupdate -Tmb_serialno_hdr -Hserial_no -hR10015369  -DUpdate_dt -Vmkcr_mths -v0
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Adelete -Tmb_serialno_row -Hcust_id   -h32928      -Rserial_no -rR10015369   -DUpdate_dt
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Acreate -Tmb_serialno_row -Hcust_id   -h32928      -Rserial_no -rR10015368   -DUpdate_dt
python DFU_DynamoDB.py -eRLW_Params.json -Eddb-user01 -Aquery  -Tmb_serialno_row -Hcust_id   -h32928

