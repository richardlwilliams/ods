#==========================================================
# -- purpose: Manipulates DynamoDb objects
# -- author:  Richard L Williams [rlwx01@gmail.com]
# -- date:    April 2016
#==================================================================================
gApp = "DynamoDB Mgt"
gVer =  0            # - 20160420 - initial versions
gVer =  1            # - 20160610 - first for MB ODS
gVer =  2            # - 20160616 - second for MB ODS
gFix =  0            # - 20160420 - 
#==================================================================================
#
#  python DFU_DynamoDB.py -Edwh-user01 -Alist
#  python DFU_DynamoDB.py -Edwh-user01 -Amake -Tmb_event -Hacct_id -Revnt_dt
#  python DFU_DynamoDB.py -Edwh-user01 -Adetails -Tmb_event
#  python DFU_DynamoDB.py -Edwh-user01 -Adestroy -Tmb_event
#
#  python DFU_DynamoDB.py -Edwh-user01 -Aupdate -Tmb_event -d20 -w2
# 
#  python DFU_DynamoDB.py -Edwh-user01 -Acreate -Tmb_event -Hacct_id -Revnt_dt -hABC-123 -r20160424123456 -n20
#  python DFU_DynamoDB.py -Edwh-user01 -Acreate -Tmb_event -Hacct_id -Revnt_dt -hABC-123 -r20160424123456 -n1000 -b25
#
#  python DFU_DynamoDB.py -Edwh-user01 -Aload   -Tmb_Episode  -HUser_id -REpisode_id -n100 -b25 -y15 -Bmelancholy-jaques -fdumps/201605/26 -Fsalesdata_hdr -z
#  python DFU_DynamoDB.py -Edwh-user01 -Aupdate -Tmb_SerialNo -Hsernum_0 -hM50003035 -Rsku_no -rMP05925DISP -VTest -v'once upon a time in a land far, far away'
#  python DFU_DynamoDB.py -Edwh-user01 -Aread   -Tmb_SerialNo -Hsernum_0 -hM50003035 -Rsku_no -rMP05925DISP
#
#  python DFU_DynamoDB.py -Eddb-user01 -Aquery  -Tmb_serialno_hdr -Hserial_no -hR50028176 -x -j
#
#==================================================================================
# http://boto3.readthedocs.org/en/latest/guide/dynamodb.html
# http://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html
#==================================================================================
#
#===Access Standard Libraries
import sys	  		#-- Command-line arguments
import os     		#-- OS functions _local/current path)
import datetime
import time   		#-- All aspects of time
import dateutil
import json   		#-- Preferred means of navigating JSON data
import unicodedata  #-- check for numeric text
import random		#-- Random number generation
from dateutil.relativedelta import relativedelta
import fnmatch

#===Access External Libraries
import boto3  		#-- accessing AWS instances and resources [pip install boto3 -U]
from boto3.dynamodb.conditions import Key, Attr

from boto.s3.connection import S3Connection
#from boto.s3.key import S3Key

#================
enumPrnt_TimePrev =  0	#
enumPrnt_SimpleMsg = 1	#
enumPrnt_TimeTotal = 2	#
enumPrnt_HashLine =  3	#
enumPrnt_BlankLine = 4	#
enumPrnt_AppLine =   5	#
enumPrnt_TimeLine =  6	#
enumPrnt_ErrorMsg =  7	#
enumPrnt_LineUp2Pt = 8	#
enumPrnt_SubLUp2Pt = 9	#
enumPrnt_DebugMsg = 10	#
enumPrnt_Verbatim = 11  #

#================
gSep  = "/"
gBSl  = "\\"
gNL    = "\n"
gDP    = "."
gComma = ","
gTik   = "'"
gSprd  = " -- "
gUd    = "_"
gDash  = "-"
gPs    = "%s"
gN     = "N"
gY     = "Y"
gYy    = "y"
gColon = ":"
gSemi  = ";"
gSp    = " "
gEq    = "="
gPs    = "%s"
gPi    = "%i"
gSBo   = "["
gSBc   = "]"
gNL    = "\n"
gHshL  = "################################"
gLiSh  = "##"
gS3    = 's3://'
gJson  = '.json'
gAstx  = "*"
gArrow = " --> "

#=====General Global parameters===================================
gPath_nm = os.path.dirname(os.path.realpath(__file__))  #-- Current/Local Path
gdStart    = time.time()
gdPrev     = time.time()
gbLog      = False
gsLogRun   = ""
gbDebug    = False
gbScreen   = False

#================================================================
gAKey    = "AKey"
gSKey    = "SKey"
gFile_nm = "DFU_Params.json"
g_sAKey  = ''
g_sSKey  = ''
g_rtYYYY = 'YYYY'
g_rtMM   = 'MM'
g_rtDD   = 'DD'

#==========================================================
#===Command Line Arguments
gArg_EnvF = "-e"  #-- environment/profile file --> default = "DFU_Params.json
gArg_Envt = "-E"  #-- environment/profile via "DFU_Params.json
gArg_Actn = "-A"  #-- Action
gArg_Tbln = "-T"  #-- table name
gArg_HshK = "-H"  #-- Hash key
gArg_HshV = "-h"  #-- Hash value
gArg_RngK = "-R"  #-- Range key
gArg_RngV = "-r"  #-- Range value
gArg_ValK = "-V"  #-- Value key1
gArg_ValV = "-v"  #-- Value value1
gArg_UalK = "-D"  #-- Value key2
gArg_DatN = "-n"  #-- number of data creates
gArg_PolR = "-p"  #-- number of polls
gArg_PolS = "-s"  #-- number of seconds between polls
gArg_Bulk = "-b"  #-- number of bulk load items
gArg_Dply = "-y"  #-- display percent
gArg_ProR = "-d"  #-- provisioned Reads
gArg_ProW = "-w"  #-- provisioned Writes
gArg_CpOp = "-C"  #-- Comparison Operator -- EQ | NE | LE | LT | GE | GT | NOT_NULL | NULL | CONTAINS | NOT_CONTAINS | BEGINS_WITH | IN | BETWEEN
gArg_JPrs = "-j"  #-- Parse the JSON data after query/scan
gArg_Page = "-P"  #-- Paginate through Query/Scan in increments of nn
gArg_DBug = "-z"  #-- switch on debug-mode
gArg_Scrn = "-x"  #-- switch off screen-mode

gArg_Bckt = "-B"  #-- bucket name
gArg_Fldr = "-f"  #-- folder name
gArg_Fltr = "-F"  #-- filter prefix

#===Action Arguments
gActn_tls = "list"

gActn_tmk = "make"
gActn_tdt = "destroy"
gActn_tdl = "details"
gActn_tmf = "modify"

gActn_dtC = "create"
gActn_dRC = "recreate"
gActn_dtG = "deluge"
gActn_dtR = "read"
gActn_dtU = "update"
gActn_dtD = "delete"
gActn_dtQ = "query"
gActn_dtS = "scan"
gActn_dtL = "load"
gActn_dLT = "load_test"
gActn_dRL = "reload"
gActn_dRT = "reload_test"

gActns_all = [] #-- all
gActns_tbl = [] #-- needs a table
gActns_cmt = [] #-- needs a table and key definition
gActns_itm = [] #-- needs a table and key definition & values (ie. data)
gActns_blk = [] #-- needs a table and key definition & bulk values (ie. data)
gActns_pro = [] #-- alter provisioning of table
gActns_qry = [] #-- query/scan data
gActns_fS3 = [] #-- from S3

#================================================================
#==Generate Dummy Data
#================================================================
gEpisodes = {'1': "Order", '2': "Register", '3': "Usage", '4': "Support", '5': "Message"}
gTbl_prefix = "mb_"

#================================================================
#================================================================
#================================================================
def doPrnt_out(strOut):
    if (not gbLog):
        print strOut
    else:
        doWriteLog(gsLogRun, strOut)

#==========================================================
def doWriteLog(strFile, strText):
    with open(strFile, 'a') as f:
        f.write(strText + "\n")
        
#==========================================================
def doPrnt(strMsg, enumPrnt, iErr = 0, strMsg2 = "", strMsg3 = "", iLineUp = 23):
    global gdPrev
    strOut = ""
    sSubLUp = "   "
    
    #==========================================================
    if   (enumPrnt == enumPrnt_TimePrev):
        strOut = doPrnt_sub(strMsg, gdPrev)
        gdPrev = time.time()
        
    elif (enumPrnt == enumPrnt_SimpleMsg):
        strOut = gSprd + strMsg
        
    elif (enumPrnt == enumPrnt_LineUp2Pt):
        strOut = gSprd + str(strMsg + gColon).ljust(iLineUp, ' ') + "[" + strMsg2 + "] " + strMsg3
        
    elif (enumPrnt == enumPrnt_SubLUp2Pt):
        strOut = sSubLUp + gSprd + str(strMsg + gColon).ljust(iLineUp-len(sSubLUp), ' ') + "[" + strMsg2 + "] " + strMsg3
        
    elif (enumPrnt == enumPrnt_TimeTotal):
        strOut = doPrnt_sub(strMsg, gdStart)
        
    elif (enumPrnt == enumPrnt_HashLine):
        strOut = gLiSh + gHshL + gLiSh
        
    elif (enumPrnt == enumPrnt_BlankLine):
        strOut = ""

    elif (enumPrnt == enumPrnt_AppLine):
        strOut = gLiSh + str(gApp + " [v" + ('%02d' % gVer) + "." + ('%02d' % gFix) + "]").center(len(gHshL), ' ') + gLiSh

    elif (enumPrnt == enumPrnt_TimeLine):
        strOut = gLiSh + str(getTimeNow()).center(len(gHshL), ' ') + gLiSh

    elif (enumPrnt == enumPrnt_DebugMsg):
        strOut = "DEBUG [" + str(strMsg) + "]"

    elif (enumPrnt == enumPrnt_ErrorMsg):
        strOut = "ERROR #[" + str(iErr) + "] -- " + strMsg.replace('\n', '')
        
    elif (enumPrnt == enumPrnt_Verbatim):
        strOut = strMsg
        
    doPrnt_out(strOut)

#==========================================================
def getTimeNow():
    #return str(time.strftime("%Y/%m/%d %T"))       #-- Does not work in Windows..!
    return str(time.strftime("%Y-%m-%d %H:%M:%S"))

#==========================================================
def doPrnt_sub(strMsg, dte):
    sRet = getTimeNow() + " [" + time.strftime("%H:%M:%S", time.gmtime(time.time() - dte)) + "] == " + strMsg.center(20, ' ') + " == "
    return sRet

#==========================================================
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
 
    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
 
    return False

#================================================================
#================================================================
#================================================================
def set_Actns():
    global gActns_all
    global gActns_cmt
    global gActns_itm

    #=======================
    gActns_all.append(gActn_tmk)
    gActns_all.append(gActn_tdt)
    gActns_all.append(gActn_tdl)
    gActns_all.append(gActn_tls)
    gActns_all.append(gActn_tmf)
    gActns_all.append(gActn_dtC)
    gActns_all.append(gActn_dRC)
    gActns_all.append(gActn_dtG)
    gActns_all.append(gActn_dtR)
    gActns_all.append(gActn_dtU)
    gActns_all.append(gActn_dtD)
    gActns_all.append(gActn_dtQ)
    gActns_all.append(gActn_dtS)
    gActns_all.append(gActn_dtL)
    gActns_all.append(gActn_dLT)
    gActns_all.append(gActn_dRL)
    gActns_all.append(gActn_dRT)

    #=======================table
    gActns_tbl.append(gActn_tmk)
    gActns_tbl.append(gActn_tmf)
    gActns_tbl.append(gActn_tdt)
    gActns_tbl.append(gActn_tdl)

    #=======================Hash/Range Keys
    gActns_cmt.append(gActn_tmk)
    
    #=======================
    gActns_itm.append(gActn_dtC)
    gActns_itm.append(gActn_dRC)
    gActns_itm.append(gActn_dtG)
    gActns_itm.append(gActn_dtR)
    gActns_itm.append(gActn_dtU)
    gActns_itm.append(gActn_dtD)
    gActns_itm.append(gActn_dtQ)
    gActns_itm.append(gActn_dtS)
    gActns_itm.append(gActn_dtL)
    gActns_itm.append(gActn_dLT)
    gActns_itm.append(gActn_dRL)
    gActns_itm.append(gActn_dRT)
    
    #=======================
    gActns_qry.append(gActn_dtQ)
    gActns_qry.append(gActn_dtS)
    
    #=======================
    gActns_blk.append(gActn_dtC)
    gActns_blk.append(gActn_dRC)
    gActns_blk.append(gActn_dtG)
    gActns_blk.append(gActn_dtD)
    gActns_blk.append(gActn_dtL)
    gActns_blk.append(gActn_dLT)
    gActns_blk.append(gActn_dRL)
    gActns_blk.append(gActn_dRT)
    
    #=======================
    gActns_pro.append(gActn_tmk)
    gActns_pro.append(gActn_tmf)
    
    #=======================
    gActns_fS3.append(gActn_dtL)
    gActns_fS3.append(gActn_dLT)
    gActns_fS3.append(gActn_dRL)
    gActns_fS3.append(gActn_dRT)

#================================================================
#================================================================
#================================================================
def fGet_Args(strTyp):
    blnFind = False
    
    strKey = ""
    for p in sys.argv:
        if (p[:2] == strTyp):
        	strKey = p[2:]
        	blnFind = True
            
    return blnFind, strKey

#================================================================
def get_Arg_Envt():
    bOK = False
    bOK, strEnv = fGet_Args(gArg_Envt)
    if (strEnv == ""):
        doPrnt("No Environment given [ " + gArg_Envt + "<env> ],   eg. [ " + gArg_Envt + "lab ]", enumPrnt_ErrorMsg, 1)
        bOK = False
    else:
        doPrnt("Environment", enumPrnt_LineUp2Pt, 0, strEnv + "]" + gArrow + "[" + gFile_nm)
        
    return bOK, strEnv

#================================================================
def get_Arg_Bool(sArg, sMsg):
    bVal = False
    bVal, strTmp = fGet_Args(sArg)
    if (bVal):
        doPrnt(sMsg, enumPrnt_LineUp2Pt, 0, str(bVal))
            
    return bVal

#================================================================
def get_Arg_Actn():
    bOK = False
    bOK, strAct = fGet_Args(gArg_Actn)
    if (strAct == ""):
        doPrnt("No Action given [ " + gArg_Actn + "<act> ], eg. [ " + gArg_Actn + str(gActns_all) + " ]", enumPrnt_ErrorMsg, 1)
        bOK = False
    else:
        if (strAct in gActns_all):
            doPrnt("Action", enumPrnt_LineUp2Pt, 0, strAct)
        else:
            doPrnt("Unrecognized Action given [ " + gArg_Actn + strAct + " ], eg. [ " + gArg_Actn + str(gActns_all) + " ]", enumPrnt_ErrorMsg, 1)
            bOK = False
        
    return bOK, strAct
    
#================================================================
def get_Arg_Tbln():
    bOK = False
    bOK, strTbl = fGet_Args(gArg_Tbln)
    if (strTbl == ""):
        doPrnt("No table name given [ " + gArg_Tbln + "<tbl> ], eg. [ " + gArg_Tbln + "user ]", enumPrnt_ErrorMsg, 1)
        bOK = False
    else:
        doPrnt("table name", enumPrnt_LineUp2Pt, 0, strTbl)
        
    return bOK, strTbl
    
#================================================================
def get_Arg_CpOp():
    bOK = False
    bOK, strCOp = fGet_Args(gArg_CpOp)
    if (not bOK):
        strCOp='eq'
        bOK = True
    else:
        if (strCOp == ""):
            doPrnt("No Comparison-Operator given [ " + gArg_CpOp + "<cpop> ], eg. [ " + gArg_CpOp + "lt ]", enumPrnt_ErrorMsg, 1)
            bOK = False
        else:
            doPrnt("Comp-Op", enumPrnt_LineUp2Pt, 0, strCOp)
        
    return bOK, strCOp
    
#================================================================
def get_Arg_HshK(bOptional):
    bOK = False
    bOK, strHsh = fGet_Args(gArg_HshK)
    if (strHsh == ""):
        if (bOptional):
            bOK = True
        else:
            doPrnt("No Hash-key given [ " + gArg_HshK + "<key> ], eg. [ " + gArg_HshK + "id ]", enumPrnt_ErrorMsg, 1)
            bOK = False
    else:
        doPrnt("Hash-key", enumPrnt_LineUp2Pt, 0, strHsh)
        
    return bOK, strHsh
    
#================================================================
def get_Arg_HshV(bOptional):
    bOK = False
    bOK, strHsh = fGet_Args(gArg_HshV)
    if (strHsh == ""):
        if (bOptional):
            bOK = True
        else:
            doPrnt("No Hash-value given [ " + gArg_HshV + "<val> ], eg. [ " + gArg_HshV + "janedoe ]", enumPrnt_ErrorMsg, 1)
            bOK = False
    else:
        doPrnt("Hash-value", enumPrnt_LineUp2Pt, 0, strHsh)
        
    return bOK, strHsh
    
#================================================================
def get_Arg_RngK():
    bOK = False
    bOK, strRng = fGet_Args(gArg_RngK)
    if (not bOK):
        bOK = True
    elif (bOK) and (strRng == ""):
        doPrnt("No Range-key given [ " + gArg_RngK + "<key> ], eg. [ " + gArg_RngK + "date ]", enumPrnt_ErrorMsg, 1)
        bOK = False
    else:
        doPrnt("Range-key", enumPrnt_LineUp2Pt, 0, strRng)
        
    return bOK, strRng
    
#================================================================
def get_Arg_RngV():
    bOK = False
    bOK, strRng = fGet_Args(gArg_RngV)
    if (not bOK):
        bOK = True
    elif (bOK) and (strRng == ""):
        doPrnt("No Range-value given [ " + gArg_RngV + "<key> ], eg. [ " + gArg_RngV + "Doe1 ]", enumPrnt_ErrorMsg, 1)
        bOK = False
    else:
        doPrnt("Range-value", enumPrnt_LineUp2Pt, 0, strRng)
        
    return bOK, strRng
    
#================================================================
def get_Arg_intg(sArg, sMsg, iDef):
    bOK = False
    iNum=0
    
    bOK, sNum = fGet_Args(sArg)
    if (bOK):
        if (not is_number(sNum)):
            doPrnt("No " + sMsg + " given [ " + sArg + "<num> ], eg. [ " + sArg + "25 ]", enumPrnt_ErrorMsg, 1)
            bOK = False
        else:
            iNum = int(sNum)
            if (iNum<1):
                doPrnt("Wrong " + sMsg + " given [ " + sArg + sNum + "]", enumPrnt_ErrorMsg, 1)
            else:
                doPrnt(sMsg, enumPrnt_LineUp2Pt, 0, str(sNum))
    else:
        iNum = iDef
        bOK = True
        
    return bOK, iNum
    
#================================================================
def get_Arg_varc(sArg, sMsg, sEg, sLast, bBlank=False):
    bOK = False
    sNme=''
    
    bOK, sNme = fGet_Args(sArg)
    if (bOK):
        if (len(sNme) > 0 or bBlank):
            if (not bBlank):
                if (sNme[-1] <> sLast):
                    sNme = sNme + sLast
            doPrnt(sMsg, enumPrnt_LineUp2Pt, 0, sNme)
        else:
            doPrnt("No " + sMsg + " given [ " + sArg + "<var> ], eg. [ " + sArg + sEg + " ]", enumPrnt_ErrorMsg, 1)
            bOK = False
    else:
        bOK = True
        
    return bOK, sNme

#================================================================
def get_Keys(strEnv, bDebug):
    dctL0 = {}
    rslt = {}
    rle = {}
    
    #=======================
    bOK = False
    with open(gPath_nm + "/" + gFile_nm, 'r') as f:
        bOK = True
        dctL0 = json.load(f)

    if (bOK):
        strAKey = dctL0[strEnv][gAKey]
        strSKey = dctL0[strEnv][gSKey]
            
    if (bDebug):
        doPrnt("Access-Key", enumPrnt_SubLUp2Pt, 0, strAKey, "")
        doPrnt("Secret-Key", enumPrnt_SubLUp2Pt, 0, strSKey, "")
        
    return strAKey, strSKey
    
#================================================================
#================================================================
#================================================================
def do_list(oClnt):
    # List all tables
    obj_Tbles = oClnt.list_tables()
    
    #print tables
    
    i=0
    #====================================
    for tbl in obj_Tbles['TableNames']:
        i=i+1
    
    print "Number of existing tables:", i
    
    j=0
    #====================================
    for tbl in obj_Tbles['TableNames']:
        j=j+1
        print '(' + str(j) + "/" + str(i) + ')', tbl

    
#================================================================
def do_tbl_destroy(oClnt, strTbl):
    #===========================================================================================
    #-- http://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#DynamoDB.Client.delete_table
    #===========================================================================================
    
    print "Destroying table:", strTbl
    bOK = False
    bOK = doFind(oClnt, strTbl)
    
    #====================================
    if (bOK):
        response = oClnt.delete_table(TableName=strTbl)
    else:
        print " -- cannot; does not exist"
   
#================================================================
def do_tbl_make(oClnt, strTbl, strHshKey, strRngKey, iRds, iWts):
    #===========================================================================================
    #-- http://boto3.readthedocs.org/en/latest/guide/dynamodb.html#creating-a-new-table
    #===========================================================================================
    
    print "Creating table:", strTbl
    bOK = False
    bOK = doFind(oClnt, strTbl)
    
    #====================================
    if (not bOK):
        dctH_nm = {'AttributeName': strHshKey, 'KeyType': 'HASH'}
        dctR_nm = {'AttributeName': strRngKey, 'KeyType': 'RANGE'}
        if (strRngKey==''):
            lstKeySchema = [dctH_nm]
        else:
            lstKeySchema = [dctH_nm, dctR_nm]

        dctH_df = {'AttributeName': strHshKey, 'AttributeType': 'S'}
        dctR_df = {'AttributeName': strRngKey, 'AttributeType': 'S'}
        if (strRngKey==''):
            lstAttribDefn = [dctH_df]
        else:
            lstAttribDefn = [dctH_df, dctR_df]

        #====================================
        obj_Tble = oClnt.create_table(
         TableName				= strTbl,
         KeySchema				= lstKeySchema,
         AttributeDefinitions	= lstAttribDefn,
         ProvisionedThroughput	= get_ProvisThru(iRds, iWts)
        )
    
    else:
        print " -- cannot; already exists"

   
#================================================================
def get_ProvisThru(iRds, iWts):
    dct = {}
    
    dct = {'ReadCapacityUnits' : iRds,'WriteCapacityUnits': iWts}

    return dct
   
#================================================================
def do_tbl_update(oClnt, strTbl, iRds, iWts):
    #===========================================================================================
    #-- http://boto3.readthedocs.org/en/latest/guide/dynamodb.html#creating-a-new-table
    #===========================================================================================
    
    print "Updating table:", strTbl
    bOK = False
    bOK = doFind(oClnt, strTbl)
    
    #====================================
    if (bOK):
        #====================================
        obj_Tble = oClnt.update_table(
         TableName				= strTbl,
         ProvisionedThroughput	= get_ProvisThru(iRds, iWts)
        )
    
    else:
        print " -- cannot; already exists"

    
#================================================================
def do_tbl_details(oClnt, oRsrc, strTbl):
    # get details for one table
    
    bOK = doFind(oClnt, strTbl)

    if (bOK):
        obj_Tble = oRsrc.Table(strTbl)
    
        doPrnt("table", enumPrnt_LineUp2Pt, 0, str(obj_Tble), iLineUp = 27)
        doPrnt("attribute_definitions", enumPrnt_LineUp2Pt, 0, str(obj_Tble.attribute_definitions), iLineUp = 27)
        doPrnt("creation_date_time", enumPrnt_LineUp2Pt, 0, str(obj_Tble.creation_date_time), iLineUp = 27)
        doPrnt("global_secondary_indexes", enumPrnt_LineUp2Pt, 0, str(obj_Tble.global_secondary_indexes), iLineUp = 27)
        doPrnt("item_count", enumPrnt_LineUp2Pt, 0, str(obj_Tble.item_count), iLineUp = 27)
        doPrnt("key_schema", enumPrnt_LineUp2Pt, 0, str(obj_Tble.key_schema), iLineUp = 27)

        doPrnt("latest_stream_arn", enumPrnt_LineUp2Pt, 0, str(obj_Tble.latest_stream_arn), iLineUp = 27)
        doPrnt("latest_stream_label", enumPrnt_LineUp2Pt, 0, str(obj_Tble.latest_stream_label), iLineUp = 27)
        doPrnt("local_secondary_indexes", enumPrnt_LineUp2Pt, 0, str(obj_Tble.local_secondary_indexes), iLineUp = 27)
        doPrnt("provisioned_throughput", enumPrnt_LineUp2Pt, 0, str(obj_Tble.provisioned_throughput), iLineUp = 27)

        doPrnt("stream_specification", enumPrnt_LineUp2Pt, 0, str(obj_Tble.stream_specification), iLineUp = 27)
        doPrnt("table_arn", enumPrnt_LineUp2Pt, 0, str(obj_Tble.table_arn), iLineUp = 27)
        doPrnt("table_name", enumPrnt_LineUp2Pt, 0, str(obj_Tble.table_name), iLineUp = 27)
        doPrnt("table_size_bytes", enumPrnt_LineUp2Pt, 0, str(obj_Tble.table_size_bytes), iLineUp = 27)
        doPrnt("table_status", enumPrnt_LineUp2Pt, 0, str(obj_Tble.table_status), iLineUp = 27)
    
#================================================================
def doFind(oClnt, strTbl):
    iRet = 0
    bOK = False
    
    #====================================
    obj_Tbles = oClnt.list_tables()
    for tbl in obj_Tbles['TableNames']:
        if (tbl==strTbl):
            bOK = True
            break
            
    return bOK
    
#================================================================
def doPolling(oClnt, oRsrc, strTbl, sStatus, intPoll, iSecs, bDebug):
    iRet = 0
    
    #====================================
    if (intPoll > 0):
        #==========================================================
        #-- Poll for when Cluster ready
        #==========================================================
        r = 0
        intPoll = intPoll + 1
        
        while (r < intPoll):
            r = r + 1
            bFound=False
            bFound = doFind(oClnt, strTbl)
            #print bFound
            
            if (bFound):
                bOK = False
                obj_Tble = oRsrc.Table(strTbl)
                if (obj_Tble.table_status == sStatus):
                    bOK = True

                #=======================
                if (bOK):
                    doPrnt("Available", enumPrnt_LineUp2Pt, 0, strTbl, "table is [" + obj_Tble.table_status + "].")
                    iRet = 0
                    break
            
                #=======================
                if (r < intPoll):
                    doPrnt("Waiting [" + str(r) + "]", enumPrnt_LineUp2Pt, 0, strTbl, "is still [" + obj_Tble.table_status + "]")
                    time.sleep(iSecs * 1)
                else:
                    doPrnt("Taking too long for [" + strTbl + "] to be [" + sStatus + "].", enumPrnt_ErrorMsg, 1)
                    iRet = 2
        
    return iRet
    
    
#================================================================
def do_dat_create(bRecreate, oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, sDateKey, bDebug):
    # insert record into table
    bFound = False
    tMain = oRsrc.Table(strTbl)
    
    sKey = get_dat_key(strHshKey, strHshVal, strRngKey, strRngVal)
    
    if (bRecreate):
        rspse = tMain.put_item(Item=sKey)
    else:

        #================================
        bFound = do_dat_reader(True, oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, bDebug)
        if (bFound):
            doPrnt("cannot - already exists", enumPrnt_TimeTotal)
        else: 
            cVal1 = ':val1'
            cVal2 = ':val2'
            
            #================================
            dctExp = {}
            dctExp[cVal1] = strHshVal
            dctExp[cVal2] = strRngVal
        
            #================================
            rspse = tMain.put_item(Item=sKey, 
                                   ConditionExpression=strHshKey + ' <> ' + cVal1 + ' AND ' + strRngKey + ' <> ' + cVal2,
                                   ExpressionAttributeValues=dctExp)

            do_dat_update(oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, "", "", sDateKey, bDebug)

            doPrnt("added - now exists", enumPrnt_TimeTotal)
   
#================================================================
def do_dat_deluge(oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly, bDebug):
    # insert record into table
    tMain = oRsrc.Table(strTbl)
    m=0
    
    doPrnt("Add items", enumPrnt_LineUp2Pt, 0, "items {"+str(iNum)+"} per batch {"+str(iBlk)+"}")
    doPrnt("start loading", enumPrnt_TimeTotal)
    #============================
    if (iNum == 1):
        if (iBlk <= 1):
            m=m+1
            
            sKey, iHash, sType, iType, sDate = do_dat_data(strHshKey, strRngKey)
            rspse = tMain.put_item(Item=sKey)
            #print rspse 
        else:
            #============================
            with tMain.batch_writer() as batch:
                for n in range(iBlk):
                    m=m+1
                    
                    sKey, iHash, sType, iType, sDate = do_dat_data(strHshKey, strRngKey)
                    batch.put_item(Item=sKey)
    else:
        #============================
        j=0
        for n in range(1, iNum+1):
            if (iBlk <= 1):
                m=m+1
                j=j+1
        
                sKey, iHash, sType, iType, sDate = do_dat_data(strHshKey, strRngKey)
                rspse = tMain.put_item(Item=sKey)
                
                if (j>=iNum*iDly/100):
                    print m
                    j=0
            else:
                #============================
                with tMain.batch_writer() as batch:
                    for k in range(iBlk):
                        m=m+1
                        j=j+1
                        
                        sKey, iHash, sType, iType, sDate = do_dat_data(strHshKey, strRngKey)
                        batch.put_item(Item=sKey)
                        
                        do_dat_sub(oRsrc, gTbl_prefix, sType, sType + "_id", iHash, iType, sDate)
                        
                        if (j>=(iNum*iBlk)*iDly/100):
                            print m
                            j=0
            
    #============================
    doPrnt("Items added", enumPrnt_LineUp2Pt, 0, str(m))
    doPrnt("finish loading", enumPrnt_TimeTotal)

#================================================================
def do_dat_sub(oRsrc, strTbl_prefix, sType, strHshKey, iAcct, iType_ID, sDate):
    tbl = oRsrc.Table(strTbl_prefix + sType)

    oItm = {}
    oInfo = {}
        
    oItm[strHshKey] = str(iType_ID)
    oItm['user_id'] = iAcct

    #print sDate
    dDate = datetime.datetime.strptime(sDate, '%Y%m%d_%H%M%S') #.datetime()
    oItm['update_dt'] = str(dDate)
    #print strTbl, oItm
    
    if   (sType == gEpisodes['1']):
        #print gEpisodes['1']
        
        oInfo["directors"] = ["Alice Smith","Bob Jones"]
        oInfo["release_date"] = str(dDate)
        oInfo["rating"] = 12.3456
        oInfo["user_id"] = iAcct
        oInfo["genres"] = ["Comedy","Drama"]
        oInfo["image_url"] = "http://ia.media-imdb.com/images/N/O9ERWAU7FS797AJ7LU8HN09AMUP908RLlo5JF90EWR7LJKQ7@@._V1_SX400_.jpg"
        oInfo["plot"] = "A rock band plays their music at high volumes, annoying the neighbors."
        oInfo["rank"] = 11 
        oInfo["running_time_secs"] = getTimeNow()
        oInfo["actors"] = ["David Matthewman","Ann Thomas","Jonathan G. Neff"]
        
        oItm['info'] = json.dumps(oInfo)
        
    #elif (sType == gEpisodes['2']):
    #    print gEpisodes['2']
    #elif (sType == gEpisodes['3']):
    #    print gEpisodes['3']
    #elif (sType == gEpisodes['4']):
    #    print gEpisodes['4']
        
    rspse = tbl.put_item(Item=oItm)
            

#================================================================
#================================================================
#================================================================
def do_dat_data(strHshKey, strRngKey):
    oItm = {}
        
    iHash = random.randrange(10, 99)                      				#-- Hash-id
    sType = gEpisodes[str(random.randrange(1, len(gEpisodes)+1))]		#-- Type
    iType = random.randrange(100000000000, 999999999999)				#-- Type-id
    
    sDate = get_sDate(2013, 2016) + gUd + get_sTime()
    sRnge = sDate + gUd + sType[:1]										#-- Range-id

    oItm = get_dat_key(strHshKey, str(iHash), strRngKey, sRnge)
    oItm['e_typ'] = sType
    oItm['e_id'] = iType

    return oItm, iHash, sType, iType, sDate
    
#================================================================
def get_sTime():
    sTim = ''
    
    h = random.randrange(0, 23+1)
    n = random.randrange(0, 59+1)
    s = random.randrange(0, 59+1)
    
    sTim = ("%02d" % (h)) + ("%02d" % (n)) + ("%02d" % (s))    # hhnnss

    return sTim
    
#================================================================
def get_sDate(iY_min, iY_max):
    sDte = ''
    
    #--https://docs.python.org/2/library/datetime.html#datetime.datetime.strptime
    y = random.randrange(iY_min, iY_max+1)	            # yyyy
    m = random.randrange(1, 12+1)	                    # m

    dDte1 = datetime.datetime.strptime(str(y) + ("%02d" % (m)) + '01', '%Y%m%d').date()
    dDte2 = dDte1 + relativedelta(months=+1)
    dDte3 = dDte2 + relativedelta(days=-1)
    d = random.randrange(1, dDte3.day)
    
    sDte = str(y) + ("%02d" % (m)) + ("%02d" % (d))     # yyyymmdd

    return sDte

#================================================================
def get_dat_key(strHshKey, strHshVal, strRngKey, strRngVal):
    sKey = {}
    
    if (strRngKey ==''):
        sKey={strHshKey: strHshVal}
    else:
        sKey={strHshKey: strHshVal, strRngKey: strRngVal}
    
    return sKey

#================================================================
#================================================================
#================================================================
def do_dat_reader(bSub, oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, bDebug):
    # retrieve record from table
    bFound = False
    
    obj_Tble = oRsrc.Table(strTbl)
    sKey = get_dat_key(strHshKey, strHshVal, strRngKey, strRngVal)
    if (bDebug): print sKey
    
    response = obj_Tble.get_item(Key=sKey)
    if (bDebug): response
    
    if ('Item' in response):
        if (not bSub): print " -- can do; exists"
        if (not bSub): print response['Item']
        bFound = True
    else:
        if (not bSub): print " -- cannot; does not exist"
    print
    
    return bFound
    
#================================================================
def do_dat_update(oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, sValuKey, sValuVal, sDateKey, bDebug):
    # change record in table
    obj_Tble = oRsrc.Table(strTbl)
    
    sKey = get_dat_key(strHshKey, strHshVal, strRngKey, strRngVal)
    #print sKey
    cVal1 = ':val1'
    cVal2 = ':val2'
    dctExp = {}
    dctExp[cVal2] = str(datetime.datetime.now())
    strUpd  = ' SET ' + sDateKey + ' = ' + cVal2
    
    #================================
    if (sValuVal<>''):
        strUpd += ', ' + sValuKey + ' = ' + cVal1
        dctExp[cVal1] = sValuVal
    else:
        if (sValuKey<>''):
            strUpd += ' REMOVE ' + sValuKey
    
    if (bDebug): print strUpd, dctExp
    #================================
    obj_Tble.update_item(
              Key=sKey
            , UpdateExpression=strUpd
            , ExpressionAttributeValues=dctExp)
 
            
#================================================================
def do_dat_delete(oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, bDebug):
    # delete record from table
    bFound = False
    obj_Tble = oRsrc.Table(strTbl)
    
    #================================
    bFound = do_dat_reader(True, oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, bDebug)
    if (not bFound):
        doPrnt("cannot - does not exist", enumPrnt_TimeTotal)
    else: 

        sKey = get_dat_key(strHshKey, strHshVal, strRngKey, strRngVal)
        if (bDebug): print sKey
    
        obj_Tble.delete_item(Key=sKey)

        doPrnt("can do - no longer exists", enumPrnt_TimeTotal)
            
#================================================================
def do_dat_query(bScan, oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, sCompOp, bJParse, iPage, bDebug):
    # query records from table
    #print "[" + strTbl + "].[" + strHshKey + "].[" + strHshVal + "].[" + strRngKey + "].[" + strRngVal + "]"
    m=0
    iCnt=0
    
    if (bScan):
        sMsg = 'scan'
    else:
        sMsg = 'query'
    doPrnt("start " + sMsg, enumPrnt_TimeTotal)
        
    obj_Tble = oRsrc.Table(strTbl)

    #===============================================
    #..ScanIndexForward=False,  #-- Reverse Order
    #-- http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
    #-- http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
    #===============================================
    bContinue = True
    bFirst = True
    esk = None
    p=0
    
    #==============================
    #== Pagination for Queries
    #==============================
    while bContinue:
        if (bScan):
            objAt = Attr(strHshKey).contains(strHshVal)
            if (bDebug): print objAt
        else:
            objFX = Key(strHshKey).eq(strHshVal)
            if (len(strRngKey)>0):
                objFX = objFX & Key(strRngKey).eq(strRngVal)
            if (bDebug): print objFX
            
        #==============================
        if (iPage==0):
            bContinue = False
            if (bScan):
                response = obj_Tble.scan(FilterExpression=objAt)
            else:
                response = obj_Tble.query(KeyConditionExpression=objFX, ScanIndexForward=False)
        else:
            if (bFirst):
                bFirst = False
                if (bScan):
                    response = obj_Tble.scan(FilterExpression=objAt, ScanIndexForward=False, Limit=iPage)
                else:
                    response = obj_Tble.query(KeyConditionExpression=objFX, ScanIndexForward=False, Limit=iPage)
            else:
                if (bScan):
                    response = obj_Tble.scan(FilterExpression=objAt, ScanIndexForward=False, Limit=iPage, ExclusiveStartKey=esk)
                else:
                    response = obj_Tble.query(KeyConditionExpression=objFX, ScanIndexForward=False, Limit=iPage, ExclusiveStartKey=esk)
        
        iCnt = iCnt + response['Count']

        #==============================
        if (iPage<>0):
            p=p+1
            doPrnt("Pagination", enumPrnt_LineUp2Pt, 0, str(p))
            
            if ('LastEvaluatedKey' in response):
                esk = response['LastEvaluatedKey']
                #print "esk", esk
            else:
                bContinue = False

        #==============================
        if (gbScreen):
            for itm in response['Items']:
                m = do_dat_subprint(m, iCnt, itm, bJParse)

    #==============================
    doPrnt("Count", enumPrnt_LineUp2Pt, 0, str(iCnt))
    doPrnt("finish " + sMsg, enumPrnt_TimeTotal)
          
#================================================================
def do_dat_subprint(m, iCnt, itm, bJParse):
    sFmt = "%0" + str(len(str(iCnt))) + "d"
    m=m+1
    
    doPrnt((sFmt % (m)), enumPrnt_LineUp2Pt, 0, str(itm))
    if (bJParse):
        for ky in itm:
            doPrnt(ky, enumPrnt_LineUp2Pt, 0, itm[ky])
            if (ky == 'info'):
                jKy = json.loads(itm[ky])
                for inf in jKy:
                    doPrnt(inf, enumPrnt_SubLUp2Pt, 0, jKy[inf])
    
    return m

#================================================================
def get_FiltExp(sCOp, strKey, strVal):

    sCOp=sCOp.upper()
    #print sCOp

    #==============================
    if   (sCOp=='EQ'):
        objFX=Attr(strKey).eq(strVal)
    elif (sCOp=='NE'):
        objFX=Attr(strKey).ne(strVal)
    elif (sCOp=='GT'):
        objFX=Attr(strKey).gt(strVal)
    elif (sCOp=='LT'):
        objFX=Attr(strKey).lt(strVal)
    elif (sCOp=='CONTAINS'):
        objFX=Attr(strKey).contains(strVal)
    #...more....
    #sRet='EQ | NE | LE | LT | GE | GT | NOT_NULL | NULL | CONTAINS | NOT_CONTAINS | BEGINS_WITH | IN | BETWEEN'

    return objFX

#================================================================
def do_name_S3_raw(sBckt, sFldr, sFltr):
     return gS3 + sBckt + gSep + sFldr + sFltr

#================================================================
def do_name_S3(mybucket, k):
     return gS3 + mybucket.name + gSep + k.key
          
#================================================================
def do_setup_Dir(sDir):
    yyyy = datetime.date.today().strftime("%Y")
    mm = datetime.date.today().strftime("%m")
    dd = datetime.date.today().strftime("%d")
    dir = sDir.replace(g_rtYYYY, yyyy).replace(g_rtMM, mm).replace(g_rtDD, dd)
    
    return dir
         
#==========================================================
def getFolder(key):
	#==========================================================
	# cdw/us/010-land/ptd/hepc/triggers/ 
	# ['cdw', 'us', '010-land', 'ptd', 'hepc', 'triggers', '']
	# 0 cdw
	# 1 us
	# 2 010-land
	# 3 ptd
	# 4 hepc
	# 5 triggers
	# 6
	#==========================================================
	# cdw/us/010-land/ptd/hepc/triggers/BMS_HEPC_DX_201411.ptrg 
	# ['cdw', 'us', '010-land', 'ptd', 'hepc', 'triggers', 'BMS_HEPC_DX_201411.ptrg']
	# 0 cdw
	# 1 us
	# 2 010-land
	# 3 ptd
	# 4 hepc
	# 5 triggers
	# 6 BMS_HEPC_DX_201411.ptrg
	#==========================================================
    bFolder=False
    strFile   = ""
    strFolder = ""

    strPts = str(key.name).split(gSep)
    strFile = strPts[len(strPts)-1]
    
    if (strFile == ""):
        bFolder=True
        strFolder = key.name
    else:
        strFolder = key.name[:len(key.name)-len(strFile)] 
        
    #print "Folder.File:  [" + strFolder + "].[" + strFile + "]"

    return bFolder, strFolder, strFile
    
#================================================================
def do_dat_load(bReload, bTest, oRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly, sBckt, strFldr, strFltr, bDebug):
    # insert record into table
    tMain = oRsrc.Table(strTbl)
    m=0
    j=0
    
    doPrnt("Add items", enumPrnt_LineUp2Pt, 0, "items {"+str(iNum)+"} per batch {"+str(iBlk)+"}")
    doPrnt("start loading", enumPrnt_TimePrev)
    doPrnt('table',  enumPrnt_LineUp2Pt, 0, strTbl)
    
    #==========================================================
    #print do_name_S3_raw(sBckt, sFldr, sFltr)
    
    conn = S3Connection(g_sAKey, g_sSKey)
    mybucket = conn.get_bucket(sBckt)
    
    #================================================================
    i=0
    j=0
    for key in mybucket.list():
        bFolder, strFolder, strFile = getFolder(key)

        if (strFldr == strFolder) and (bFolder == False) and (fnmatch.fnmatch(strFile, strFltr)) or (strFltr == ""):
            if (gbDebug): print bFolder, strFolder, strFile, do_name_S3(mybucket, key)
            i=i+1
            doPrnt('Import File [' + str(i) + ']', enumPrnt_LineUp2Pt, 0, do_name_S3(mybucket, key))
            
            sRet = key.get_contents_as_string()

            sData = []
            sData = json.loads(sRet)
            k=0
            for itm in sData:
                j=j+1
                k=k+1
            
                #================================
                if (bTest):
                    sKey = 'cust_ids'
                    #print itm
                    if sKey in itm:
                        #print itm[sKey]
                        lstCust_ids = itm[sKey].split(gSep)
                        itm[sKey] = lstCust_ids
                        #print itm
                    
                #================================
                if (k==iNum):
                    k=0
                    doPrnt(str(j), enumPrnt_TimePrev)
                #================================
                
                #print strHshKey, itm[strHshKey], itm
                if (bReload):
                    rspse = tMain.put_item(Item=itm)
                else:
                    rspse = tMain.put_item(Item=itm, 
                                           ConditionExpression=strHshKey + '<> :val', 
                                           ExpressionAttributeValues={':val': itm[strHshKey]})

    doPrnt('found',  enumPrnt_LineUp2Pt, 0, str(i) + ' files')
    doPrnt('loaded', enumPrnt_LineUp2Pt, 0, str(j) + ' rows')
    
#================================================================
#================================================================
#================================================================
def main():
    iRet=0
    iBlk=0
    iNum=0
    iDly=0
    global gbDebug
    global gbScreen
    global g_sAKey
    global g_sSKey
    global gFile_nm

    #==========================================================
    doPrnt("", enumPrnt_BlankLine)
    doPrnt("", enumPrnt_HashLine)
    doPrnt("", enumPrnt_AppLine)
    doPrnt("", enumPrnt_HashLine)
    doPrnt("", enumPrnt_TimeLine)
    doPrnt("", enumPrnt_HashLine)

    set_Actns()

    doPrnt("", enumPrnt_BlankLine, 0, "")
    doPrnt("", enumPrnt_HashLine, 0, "")
    #=======================
    #== Set up Arguments === 
    #=======================
    doPrnt("Arguments", enumPrnt_LineUp2Pt, 0, str(sys.argv).replace(" ", "")[1:-1])

    bOK=True
    strHshKey = ''
    strHshVal = ''
    strRngKey = ''
    strRngVal = ''
    strCOp = ''
    
    #================================================================
    #================================================================
    if (bOK):
        gbDebug = get_Arg_Bool(gArg_DBug, "Debug on")
        
    if (bOK):
        gbScreen = get_Arg_Bool(gArg_Scrn, "Screen off")
        gbScreen = not gbScreen

    if (bOK):
        bOK, sTmp = get_Arg_varc(gArg_EnvF, 'Environment File', gFile_nm, '')
        if (sTmp<>''):
            gFile_nm = sTmp
        
    if (bOK):
        bOK, strEnv = get_Arg_Envt()
    
    if (bOK):
        g_sAKey, g_sSKey = get_Keys(strEnv, gbDebug)
        
    if (bOK):
        bOK, strAct = get_Arg_Actn()   

    if (bOK):
        bOK, iPolR = get_Arg_intg(gArg_PolR, 'Poll-Count', 25)
        
    if (bOK):
        bOK, iPolS = get_Arg_intg(gArg_PolS, 'Poll-Seconds', 5)
        
    if (bOK):
        if (strAct in gActns_tbl) or (strAct in gActns_itm):
            bOK, strTbl = get_Arg_Tbln()   
    
    if (bOK):
        if (strAct in gActns_qry) or (strAct in gActns_cmt) or (strAct in gActns_itm):
            bOK, strHshKey = get_Arg_HshK(True)   
    
    if (bOK):
        if (strAct in gActns_qry) or (strAct in gActns_itm):
            bOK, strHshVal = get_Arg_HshV(True)   
    
    if (bOK):
        if (strAct in gActns_cmt) or (strAct in gActns_itm):
            bOK, strRngKey = get_Arg_RngK()   

    if (bOK) and (strRngKey<>''):
        if (strAct in gActns_itm):
            bOK, strRngVal = get_Arg_RngV()   
            
    if (bOK):
        if (strAct in gActns_qry):
            bOK, strCOp = get_Arg_CpOp()   
        
    if (bOK):
        if (strAct in gActns_qry):
            bJParse = get_Arg_Bool(gArg_JPrs, "JSON-Parse")

    if (bOK):
        if (strAct in gActns_qry):
            bOK, iPage = get_Arg_intg(gArg_Page, 'Pagination', 0)
   
    if (bOK):
        if (strAct in gActns_itm):
            bOK, iNum = get_Arg_intg(gArg_DatN, 'Create-Number', 10)
    
    if (bOK):
        if (strAct in gActns_itm):
            bOK, iBlk = get_Arg_intg(gArg_Bulk, 'Bulk-group', 25)
    
    if (bOK):
        if (strAct in gActns_itm):
            bOK, iDly = get_Arg_intg(gArg_Dply, 'Display-%age', 10)
    
    iRds=1
    if (bOK):
        if (strAct in gActns_pro):
            bOK, iRds = get_Arg_intg(gArg_ProR, 'Read-Provs', 1)
    
    iWts=1
    if (bOK):
        if (strAct in gActns_pro):
            bOK, iWts = get_Arg_intg(gArg_ProW, 'Write-Provs', 1)
    
    if (bOK):
        if (strAct in gActns_fS3):
            bOK, sBckt = get_Arg_varc(gArg_Bckt, 'S3-bucket', 'melancholy-jaques', '')
    
    if (bOK):
        if (strAct in gActns_fS3):
            sTmp = 'S3-folder'
            bOK, sFldr1 = get_Arg_varc(gArg_Fldr, sTmp, 'dump/201605/27', gSep)
            sFldr = do_setup_Dir(sFldr1)
            if (sFldr<>sFldr1):
                doPrnt(gArrow,  enumPrnt_LineUp2Pt, 0, sFldr)
    
    if (bOK):
        if (strAct in gActns_fS3):
            bOK, sFltr = get_Arg_varc(gArg_Fltr, 'S3-filter', 'salesdata_hdr', '')

    if (bOK):
        if (strAct==gActn_dtU):
            bOK, sValuKey = get_Arg_varc(gArg_ValK, "Update Value", '<tag>', '')

    if (bOK):
        if (strAct==gActn_dtU):
            bOK, sValuVal = get_Arg_varc(gArg_ValV, gArrow + "new Value", '<abc>', '', True)

    if (bOK):
        if (strAct==gActn_dtU) or (strAct==gActn_dtC):
            bOK, sDateKey = get_Arg_varc(gArg_UalK, "Update Date", '<tag>', '')
   
    #================================================================
    #================================================================
    if (bOK):
        doPrnt("", enumPrnt_HashLine, 0, "")
        #==============================

        objClnt =   boto3.client('dynamodb', region_name='us-east-1', aws_access_key_id=g_sAKey, aws_secret_access_key=g_sSKey)
        
        #-- http://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html#table
        objRsrc = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id=g_sAKey, aws_secret_access_key=g_sSKey)
        
        if (strAct==gActn_tls):
            do_list(objClnt)
            
        elif (strAct==gActn_tdl):
            do_tbl_details(objClnt, objRsrc, strTbl)
        else:
            if (strAct==gActn_tmk):
                do_tbl_make(objClnt, strTbl, strHshKey, strRngKey, iRds, iWts)
            
            iRet = doPolling(objClnt, objRsrc, strTbl, 'ACTIVE', iPolR, iPolS, gbDebug)
            
            if (strAct==gActn_tmf):
                do_tbl_update(objClnt, strTbl, iRds, iWts)
            
            iRet = doPolling(objClnt, objRsrc, strTbl, 'ACTIVE', iPolR, iPolS, gbDebug)

            if (strAct==gActn_tdt):
                do_tbl_destroy(objClnt, strTbl)
            
            if (strAct==gActn_dtC):
                do_dat_create(False, objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, sDateKey, gbDebug)
            
            if (strAct==gActn_dRC):
                do_dat_create(True, objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, gbDebug)
            
            if (strAct==gActn_dtG):
                do_dat_deluge(objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly, gbDebug)
            
            if (strAct==gActn_dtL):
                do_dat_load(False, False, objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly,  sBckt, sFldr, sFltr, gbDebug)
            
            if (strAct==gActn_dLT):
                do_dat_load(False, True,  objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly,  sBckt, sFldr, sFltr, gbDebug)
            
            if (strAct==gActn_dRL):
                do_dat_load(True, False,  objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly,  sBckt, sFldr, sFltr, gbDebug)
            
            if (strAct==gActn_dRT):
                do_dat_load(True, True,   objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, iNum, iBlk, iDly,  sBckt, sFldr, sFltr, gbDebug)
            
            if (strAct==gActn_dtR):
                bFound = do_dat_reader(False, objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, gbDebug)
            
            if (strAct==gActn_dtU):
                do_dat_update(objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, sValuKey, sValuVal, sDateKey, gbDebug)
            
            if (strAct==gActn_dtD):
                do_dat_delete(objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, gbDebug)
            
            if (strAct==gActn_dtQ):
                do_dat_query(False, objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, strCOp, bJParse, iPage, gbDebug)
            
            if (strAct==gActn_dtS):
                do_dat_query(True,  objRsrc, strTbl, strHshKey, strHshVal, strRngKey, strRngVal, strCOp, bJParse, iPage, gbDebug)
            
    #==========================================================
    doPrnt("", enumPrnt_BlankLine)
    doPrnt("", enumPrnt_HashLine)
    doPrnt("", enumPrnt_AppLine)
    doPrnt("", enumPrnt_HashLine)
    doPrnt("", enumPrnt_TimeLine)
    doPrnt("", enumPrnt_HashLine)
    doPrnt("", enumPrnt_BlankLine)

#================================================================
if __name__ == "__main__":
    main()
