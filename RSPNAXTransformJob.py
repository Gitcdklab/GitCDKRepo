
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import monotonically_increasing_id, lit, concat_ws, split, explode, coalesce 
from pyspark.sql.types import IntegerType

#args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
sparkSession = glueContext.spark_session
sqlContext = SQLContext(sparkSession.sparkContext, sparkSession)

job = Job(glueContext)
#job.init(args['JOB_NAME'], args)

#---------------------------------------------------------------------------------------------------------------------
# Files and folders
#---------------------------------------------------------------------------------------------------------------------

# source file code, file name and Athena table name
fileCode               = "RSPNAX"
sourceFileName         = fileCode + '.SAMPLEFILE'
sourceTableName        = 'rspnax_sourcefile'

# S3 bucket and root transformation folder
bucketURI              = 's3://a5-s3-bucket1'
transformPath          = bucketURI + '/transformation/'

# shared folders
lookupPath             = transformPath + 'lookup/'
metadataPath           = transformPath + 'metadata/'
outputPath             = transformPath + 'output/'

# file specific folders
sourcePath             = transformPath + fileCode + '/source/'
targetPath             = transformPath + fileCode + '/target/'
rejectedPath           = transformPath + fileCode + '/rejected/'
exceptionsPath         = transformPath + fileCode + '/exceptions/'
auditPath              = transformPath + fileCode + '/audit/'
errorPath              = transformPath + fileCode + '/error/'

# file names
fileMetaFileName       = fileCode + '-metadata.txt'
exceptionsMetaFileName = 'exceptions-metadata.txt'
auditMetaFileName      = 'audit-metadata.txt'

targetFileName         = sourceFileName + ".OUTPUT.txt"
rejectedFileName       = sourceFileName + ".REJECTED.txt"
exceptionsFileName     = sourceFileName + ".EXCEPTIONS.txt"
auditFileName          = sourceFileName + ".AUDIT.txt"
mapStructErrFileName   = sourceFileName + ".MAP_STRUCT_ERRORS.txt"
mapOutputErrFileName   = sourceFileName + ".MAP_OUTPUT_ERRORS.txt"
logFileName            = sourceFileName + ".LOG.txt"

#---------------------------------------------------------------------------------------------------------------------
# RenameDataLakeFile
#---------------------------------------------------------------------------------------------------------------------

def RenameDataLakeFile(s3BucketURI, sourcePath, targetPath, newFileName):
    
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    
    fs = FileSystem.get(URI(s3BucketURI), sc._jsc.hadoopConfiguration())

    # rename created file
    outputFilePath = fs.globStatus(Path(sourcePath + "part*"))[0].getPath()
    fs.rename(outputFilePath, Path(targetPath + newFileName))

#---------------------------------------------------------------------------------------------------------------------
# Read the rspnax sourcefile into a DynamicFrame from the ascii_samplefile Athena table
#---------------------------------------------------------------------------------------------------------------------

#source_df = sqlContext.sql("SELECT monotonically_increasing_id() as _ROW_ID, value FROM rise_etl.b212_sourcefile")
source_df = sqlContext.sql("SELECT monotonically_increasing_id() as _ROW_ID, value FROM text.`" + sourcePath + sourceFileName + ".txt`")

#source_df.show()

source_dyf = DynamicFrame.fromDF(source_df, glueContext)
source_dyf.show()
#---------------------------------------------------------------------------------------------------------------------
# Read xRef-Exchange lookup table
#---------------------------------------------------------------------------------------------------------------------
#xrefExch_df    = sparkSession.read.option("header", True).option("sep", '|').csv(lookupPath + 'x-ref-exchange.txt')

#xrefExchList = map(lambda row: row.asDict(), xrefExch_df.collect())
#xrefExchLookup = {elem["_Key_Exch"]: elem["_Value_Exch"] for elem in xrefExchList}

#print(xrefExchLookup)
#---------------------------------------------------------------------------------------------------------------------
# Create Map tables
#---------------------------------------------------------------------------------------------------------------------
#CD-01#
mapLiteratureCode = {
           '1': 'Y',
           ' ': 'Y',
           '0': 'N'
}
print(f"01: {mapLiteratureCode}")

#CD-02#
mapPlanType = {
           'S': 'RRSP',
           'I': 'RRIF',
            'E': 'RESP',
            'L': 'LIRA',
            'F': 'RLIF',
            'G': 'GRSP',
            'P': 'PIPP',
            'A': 'LRIF',
            'D': 'DPSP'
}                
print(f"02: {mapPlanType}")

#CD-03#
mapAccountStatus = {
            'A': 'LIVE',
            ' ': 'LIVE',
            'B': 'PEND',
            'C': 'CLSD',
            'D': 'DEAD'
}                
print(f"03: {mapAccountStatus}")

#CD-04#
mapRrifSuccessorCode = {
            'N': '0',
            ' ': '0',
            'Y': '1'
}                
print(f"04: {mapRrifSuccessorCode}")

#CD-05#
mapRspApplicationCode = {
            '0': 'N',
            '1': 'Y'
}                
print(f"05: {mapRspApplicationCode}")

#CD-06#
mapLockInPlanCode = {
            ' ': 'OPEN',
            'L': 'LOKD',
            'P': 'PLGD',
            'R': 'REST',
            'E': 'ESTV',
            'S': 'SUCC'
}                
print(f"06: {mapLockInPlanCode}")

#CD-07#
mapLockInPlanDate = {
            ' ': 'FA',
            'AB': 'AB',
            'BC': 'BC',
            'FD': 'FD',
            'FN': 'NF',
            'MB': 'MB',
            'NA': 'AA',
            'NB': 'NB',
            'NL': 'NL',
            'NO': 'NN',
            'NR': 'ND',
            'NS': 'NS',
            'NT': 'NT',
            'ON': 'ON',
            'PE': 'PE',
            'PQ': 'PQ',
            'SK': 'SK',
            'UK': 'NK',
            'YT': 'YT'
}                
print(f"07: {mapLockInPlanDate}")

#CD-08#
mapCloseReason = {
            'AN': 'ANP',
            'DR': 'DER',
            'ET': 'ETP',
            'HB': 'HBP',
            'LP': 'LLL',
            'MB': 'MBD',
            'OE': 'OIE',
            'RE': 'WOE',
            'RO': 'RTR',
            'RP': 'ETT',
            'TF': 'TFT',
            'T?': 'TFI'
}                
print(f"08: {mapCloseReason}")

#CD-09#
mapFeePaidCode = {
            'D': 'CIP',
            'M': 'MGN',
            'E': 'EXT',
            'B': 'RSP',
            'R': 'RRP'
}                
print(f"09: {mapFeePaidCode}")

#CD-10#
mapSpousalEver = {
            'Y': '1',
            'N': '0'
}                
print(f"10: {mapSpousalEver}")

#CD-11#
mapSpousalAnnuity = {
            'Y': '1',
            'N': '0',
            ' ': '0'
}                
print(f"11: {mapSpousalAnnuity}")

#CD-12#
mapRegistrationCode = {
            'Y': '1',
            ' ': '0'
}                
print(f"12: {mapRegistrationCode}")

#CD-13#
mapPayFrequency = {
            'W': '52',
            'M': '12',
            'B': '24',
            'Q': '4',
            'S': '2',
            'A': '1',
            'K': '60',
            'H': '15',
            '=': '-'
}                
print(f"13: {mapPayFrequency}")

#CD-14#
mapPayMethod = {
            'ABB': '1',
            'AFT': '2',
            'BRH': '3',
            'CHQ': '4',
            'CH3': '5',
            'TFR': '6'
}                
print(f"14: {mapPayMethod}")

#CD-15#
mapGrossNetInd = {
            'G': 'GR',
            ' ': 'GR',
            'N': 'NT'
}                
print(f"15: {mapGrossNetInd}")

#CD-16#
mapWithholdTax = {
            'N': '0',
            'Y': '1'
}                
print(f"16: {mapWithholdTax}")

mapAlternateGrossIndicator = {
    "G": "GR",
    " ": "GR",
    "N": "NT"
}
print(f"17: {mapAlternateGrossIndicator}")

#CD-18#
mapSpousalContributionCode = {
            'Y': '1',
            'N': '0',
            ' ': '0'
}                
print(f"18: {mapSpousalContributionCode}")

#CD-19#
mapAlternateWithholdingTax1Option = {
            'N': '0',
            'Y': '1'
}                
print(f"19: {mapAlternateWithholdingTax1Option}")

#CD-20#
mapInvestmentVariety = {
            'A': 'AIV',
            'M': 'MBV',
            'N': 'FPV'
}                
print(f"20: {mapInvestmentVariety}")

#CD-21#
mapCalcCode = {
            'O': 'OLD',
            'N': 'NEW',
            ' ': 'NEW'
}                
print(f"21: {mapCalcCode}")

#CD-22#
mapFuturePaymentCode = {
            'D': 'DEF',
            'R': 'REC',
            'C': 'CAN',
            'S': 'SUP'
}                
print(f"22: {mapFuturePaymentCode}")

#CD-23#
mapThirdPartyInstruction = {
            'Y': '1',
            'N': '0',
            ' ': '0'
}                
print(f"23: {mapThirdPartyInstruction}")

#CD-24#
mapPayFrequencyBankB = {
            'W': '52',
            'M': '12',
            'B': '24',
            'Q': '4',
            'S': '2',
            'A': '1',
            'K': '60',
            'H': '15',
            '=': '-'
}                
print(f"24: {mapPayFrequencyBankB}")

#CD-25#
mapMethodBankB = {
            'ABB': '1',
            'AFT': '2',
            'BRH': '3',
            'CHQ': '4',
            'CH3': '5',
            'TFR': '6'
}                
print(f"25: {mapMethodBankB}")

#CD-26#
mapSpousalBankA = {
            'Y': '1',
            'N': '0',
            ' ': '0'
}                
print(f"26: {mapSpousalBankA}")

#CD-27#
mapSpousalBankB = {
            'Y': '1',
            'N': '0',
            ' ': '0'
}                
print(f"27: {mapSpousalBankB}")
#---------------------------------------------------------------------------------------------------------------------
# Define structure of each row in the DynamicFrame
#---------------------------------------------------------------------------------------------------------------------
import logging

def log_errors(inner):
    def wrapper(*args, **kwargs):
        try:
            return inner(*args, **kwargs)
        except Exception as e:
            logging.exception('Error in function: {}'.format(inner))
            raise
    return wrapper
print("OK1")

@log_errors
def CreateSourceRowStructure(record):
    
    rowId      = record["_ROW_ID"]
    rowData    = record["value"]
    
    rowStatus  = 0
    exceptions = []

    # Check row length
    rowLength = len(rowData)
    
    if rowLength != 134:
        # Row has incorrect length != 134
        rowStatus = -1
        exceptions.append(f"INVALID_LENGHT#ROW#Invalid row length [{rowLength}]")
        
        record["_ROW_STATUS"]     = rowStatus
        record["_ROW_EXCEPTIONS"] = exceptions

        return record
        
        # Row has correct length = 134
        record["CARRIAGE CONTROL"]     = rowData[1-1:1]
        record["CLIENT−NUMBER"]        = rowData[2-1:4]
        record["TRID (= 00)"]          = rowData[13-1:14]
        record["RECORD-1:ID (=1)"]     = rowData[15-1:15]
        rowType                        = rowData[13-1:15] #(TRID | RECORD-1:ID)
        
        
    if rowType == "000":
        # Row type: HEADER (000)
            rowType = "HEADER"          
            record["ACCOUNT−NUMBER (BBBAAAAA)"] = rowData[5-1:12]
            
            record["HEADER ID DATE="]           = rowData[16-1:20]
            record["HEADER DATE-1:MM"]          = rowData[21-1:22]            
            record["HEADER DATE-1:DD"]          = rowData[23-1:24]            
            record["HEADER DATE-1:YY"]          = rowData[25-1:26]            
            record["HEADER P2 DATA"]            = rowData[27-1:133]

    elif rowType == "001":
        # Row type: REGISTERED PLAN RECORD (001)        
            rowType = "REGISTERED PLAN RECORD"
            record["BRANCH"]                               = rowData[5-7]
            record["ACCOUNT"]                              = rowData[8-12]
            
            record["ACCOUNT−TYPE (SHOULD ALWAYS = 1)"]     = rowData[16-1:16]            
            record["ACCOUNT−CHECK−DIGIT"]                  = rowData[17-1:17]
            branch                                         = rowData[5-7]        
            account                                        = rowData[8-12]
            accountType                                    = rowData[16-1:16]
            accountCheckDigit                              = rowData[17-1:17]
            record["New Field: CONCATENATE (Branch+Account+Type+Check)"] = branch + account + accountType + accountCheckDigit

            #CD-01#
            literatureCode                      = rowData[18-1:18]
            literatureCodeMap                   = mapLiteratureCode.get(literatureCode)
              
            if literatureCodeMap is not None:
                      record["LITERATURE−CODE"] = literatureCodeMap
            else:
                      record["LITERATURE−CODE"] = literatureCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#LITERATURE−CODE#No mapped value for [{literatureCode}]")
                      
            #CD-02#
            planType                            = rowData[19-1:19]
            planTypeMap                         = mapPlanType.get(PlanType)
              
            if planTypeMap is not None:
                      record["PLAN−TYPE"]       = planTypeMap
            else:
                      record["PLAN−TYPE"]       = planType
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#PLAN−TYPE #No mapped value for [{planType}]")

            #CD-03#
            accountStatus                       = rowData[20-1:20]
            accountStatusMap                    = mapAccountStatus.get(accountStatus)
              
            if accountStatuseMap is not None:
                      record["ACCOUNT−STATUS"]  = accountStatuseMap
            else:
                      record["ACCOUNT−STATUS"]  = accountStatus
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#PLAN−TYPE #No mapped value for [{accountStatus}]")                     

            #CD-04#
            rrifSuccessorCode                   = rowData[21-1:21]        
            rrifSuccessorCodeMap                = mapRrifSuccessorCode.get(rrifSuccessorCode)
              
            if rrifSuccessorCodeMap is not None:
                      record["RRIF−SUCCESSOR−CODE"] = rrifSuccessorCodeMap
            else:
                      record["RRIF−SUCCESSOR−CODE"] = rrifSuccessorCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#RRIF−SUCCESSOR−CODE #No mapped value for [{rrifSuccessorCode}]")                         

            #CD-05#
            rspApplicationCode                  = rowData[22-1:22]                
            rspApplicationCodeMap               = mapRspApplicationCode.get(rspApplicationCode)
              
            if rspApplicationCodeMap is not None:
                      record["RSP−APPLICATION−CODE"] = rspApplicationCodeMap
            else:
                      record["RSP−APPLICATION−CODE"] = rspApplicationCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#RSP−APPLICATION−CODE #No mapped value for [{rspApplicationCode}]")

            record["RSP−TYPE (FUTURE USE)"]     = rowData[23-1:24]            
            record["TAX−GEO−CODE"]              = rowData[25-1:27]
                      
            #CD-06#
            lockInPlanCode                      = rowData[28-1:28]        
            lockInPlanCodeMap                   = mapLockInPlanCode.get(lockInPlanCode)
              
            if lockInPlanCodeMap is not None:
                      record["LOCK−IN−PLAN−CODE"] = lockInPlanCodeMap
            else:
                      record["LOCK−IN−PLAN−CODE"] = lockInPlanCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#LOCK−IN−PLAN−CODE #No mapped value for [{lockInPlanCode}]")
            
            record["LOCK−IN−PLAN−DATE−CC"]      = rowData[29-1:30]            
            record["LOCK−IN−PLAN−DATE−YY"]      = rowData[31-1:32]            
            record["LOCK−IN−PLAN−DATE−MM"]      = rowData[33-1:34]

            #CD-07#
            lockInPlanDate                      = rowData[35-1:36]        
            lockInPlanDateMap                   = mapLockInPlanDate.get(lockInPlanDate)
              
            if lockInPlanDateMap is not None:
                      record["LOCK−IN−PLAN−DATE−DD"] = lockInPlanDateMap
            else:
                      record["LOCK−IN−PLAN−DATE−DD"] = lockInPlanDate
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#LOCK−IN−PLAN−DATE−DD #No mapped value for [{lockInPlanDate}]")

            record["’YT’ = YUKON"]              = rowData[37-1:38]

            #CD-08#
            closeReason                         = rowData[39-1:40]        
            closeReasonMap                      = mapCloseReason.get(closeReason)
              
            if closeReasonMap is not None:
                      record["CLOSE−REASON − CLIENT DEFINED"]     = closeReasonMap
            else:
                      record["CLOSE−REASON − CLIENT DEFINED"]     = closeReason
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#CLOSE−REASON − CLIENT DEFINED #No mapped value for [{closeReason}]")
                              
            #CD-09#
            feePaidCode                         = rowData[41-1:41]                
            feePaidCodeMap                      = mapFeePaidCode.get(feePaidCode)
              
            if feePaidCodeMap is not None:
                      record["FEE−PAID−CODE"]   = feePaidCodeMap
            else:
                      record["FEE−PAID−CODE"]   = feePaidCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#FEE−PAID−CODE #No mapped value for [{feePaidCode}]")
              
            record["FILLER"]                    = rowData[42-1:42]            
            record["FEE−TYPE"]                  = rowData[43-1:43]            
            record["SPOUSAL−NAME"]              = rowData[44-1:73]            
            record["SPOUSAL−SIN"]               = rowData[74-1:82]

            #CD-10#
            spousalEver                         = rowData[83-1:83]                
            spousalEverMap                      = mapSpousalEver.get(spousalEver)
              
            if spousalEverMap is not None:
                      record["SPOUSAL−EVER"]    = spousalEverMap
            else:
                      record["SPOUSAL−EVER"]    = spousalEver
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#SPOUSAL−EVER #No mapped value for [{spousalEver}]")
                      
            record["SPOUSAL−BIRTHDATE−CC"]      = rowData[84-1:85]            
            record["SPOUSAL−BIRTHDATE−YY"]      = rowData[86 -87]            
            record["SPOUSAL−BIRTHDATE−MM"]      = rowData[88 -89]            
            record["SPOUSAL−BIRTHDATE−DD"]      = rowData[90 -91]         

            #CD-11#
            spousalAnnuity                      = rowData[92-1:92]                
            spousalAnnuityMap                   = mapSpousalAnnuity.get(spousalAnnuity)
              
            if spousalAnnuityMap is not None:
                      record["SPOUSAL−ANNUITY"] = spousalAnnuityMap
            else:
                      record["SPOUSAL−ANNUITY"] = spousalAnnuity
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#SPOUSAL−ANNUITY #No mapped value for [{spousalAnnuity}]")
                      
            record["BENEFICIARY−1−NAME"]        = rowData[93-1:122]            
            record["BENEFICIARY−1−SIN"]         = rowData[123-1:131]            
            record["FILLER"]                    = rowData[132-1:133]

            
    elif rowType == "002":
        # Row type: PRINCIPAL RECORD 2 (002)
            rowType = "PRINCIPAL RECORD 2"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]

            record["BENEFICIARY−1−RELATION"]    = rowData[16-1:25]            
            record["BENEFICIARY−1−MISC"]        = rowData[26-1:35]            
            record["BENEFICIARY−2−NAME"]        = rowData[36-1:65]            
            record["BENEFICIARY−2−SIN−1"]       = rowData[66-1:74]            
            record["BENEFICIARY−2−RELATION"]    = rowData[75-1:84]            
            record["BENEFICIARY−2−MISC"]        = rowData[85-1:94]            
            record["PREVIOUS−GEO−CODE"]         = rowData[95 -97]            
            record["GEO-DATE-CC"]               = rowData[98-1:99]            
            record["GEO-DATE-YY"]               = rowData[100-1:101]            
            record["GEO-DATE-MM"]               = rowData[102-1:103]            
            record["GEO-DATE-DD"]               = rowData[104-1:105]            
            record["OPEN−DATE−CC"]              = rowData[106-1:107]            
            record["OPEN−DATE−YY"]              = rowData[108-1:109]            
            record["OPEN−DATE−MM"]              = rowData[110-1:111]            
            record["OPEN−DATE−DD"]              = rowData[112-1:113]            
            record["CLOSE−DATE−CC"]             = rowData[114-1:115]            
            record["CLOSE−DATE−YY"]             = rowData[116-1:117]            
            record["CLOSE−DATE−MM"]             = rowData[118-1:119]            
            record["CLOSE−DATE−DD"]             = rowData[120-1:121]            
            record["CHANGE−DATE−CC"]            = rowData[122-1:123]            
            record["CHANGE−DATE−YY"]            = rowData[124-1:125]            
            record["CHANGE−DATE−MM"]            = rowData[126-1:127]            
            record["CHANGE−DATE−DD"]            = rowData[128-1:129]

            #CD-12#
            registrationCode                    = rowData[130-1:130]                
            registrationCodeMap                 = mapRegistrationCode.get(registrationCode)
              
            if registrationCodeMap is not None:
                      record["REGISTRATION−CODE"]     = registrationCodeMap
            else:
                      record["REGISTRATION−CODE"]     = registrationCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#REGISTRATION−CODE #No mapped value for [{registrationCode}]")

            record["SIBLING−NUMBER"]            = rowData[131-1:132]            
            record["FILLER"]                    = rowData[133-1:133]
            
            
    elif rowType == "003":
        # Row type: PRINCIPAL RECORD 3 (003)        
            rowType = "PRINCIPAL RECORD 3"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]
              
            record["GROUP−NO"]                  = rowData[16-1:21]            
            record["Filler"]                    = rowData[22-1:133]
                      
                              
    elif rowType == "101":
        # Row type: RRIF PAY INFO RECORD 1 (101)
            rowType = "RRIF PAY INFO RECORD 1"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]

            record["RIF-YE-VALUE"]              = rowData[16-1:24]            
            record["RR1-TERM"]                  = rowData[25-1:26]            
            record["FILLER"]                    = rowData[27-1:28]               
              
            #CD-13#
            payFrequency                        = rowData[29 -29]                
            payFrequencyMap                     = mapPayFrequency.get(payFrequency)
              
            if payFrequencyMap is not None:
                      record["PAY-FREQUENCY"]   = payFrequencyMap
            else:
                      record["PAY-FREQUENCY"]   = payFrequency
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#PAY-FREQUENCY #No mapped value for [{payFrequency}]")

            #CD-14#
            payMethod                           = rowData[30-1:34]        
            payMethodMap                        = mapPayMethod.get(payMethod)
              
            if payMethodMap is not None:
                      record["PAY-FREQUENCY"]   = payMethodMap
            else:
                      record["PAY-FREQUENCY"]   = payMethod
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#PAY-FREQUENCY #No mapped value for [{payMethod}]")

            record["RR-AGENT"]                  = rowData[35-1:38]            
            record["RR-BANK-TRANSIT"]           = rowData[39 -43]            
            record["RR-BANK-ACCOUNT"]           = rowData[44-1:48]

            #CD-15#
            grossNetInd                         = rowData[59-1:59]                
            grossNetIndMap                      = mapGrossNetInd.get(grossNetInd)
              
            if grossNetIndMap is not None:
                      record["GROSS-NET-IND"]   = grossNetIndMap
            else:
                      record["GROSS-NET-IND"]   = grossNetInd
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#GROSS-NET-IND #No mapped value for [{grossNetInd}]")

            #CD-16#
            withholdTax                         = rowData[60-1:60]                
            withholdTaxMap                      = mapWithholdTax.get(withholdTax)
                      
            if withholdTaxMap is not None:
                      record["WITHHOLD-TAX"]    = withholdTaxMap
            else:
                      record["WITHHOLD-TAX"]    = withholdTax
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#WITHHOLD-TAX #No mapped value for [{withholdTax}]")
                      
            record["FIRST PAYMENT-1:CC"]        = rowData[61-1:62]            
            record["FIRST PAYMENT-1:YY"]        = rowData[63-1:64]            
            record["FIRST PAYMENT-1:MM"]        = rowData[65-1:66]            
            record["FIRST PAYMENT-1:DD"]        = rowData[67-1:68]            
            record["FIRST PAYMENT-1:AMOUNT"]    = rowData[69-1:77]            
            record["ELECTED PAY-1:AMOUNT"]      = rowData[78-1:86]            
            record["MINIMUM PAY-1:AMOUNT"]      = rowData[87-1:95]            
            record["ALTERNATE DATE-1:CC"]       = rowData[96-1:97]            
            record["ALTERNATE DATE-1:YY"]       = rowData[98-1:99]            
            record["ALTERNATE DATE-1:MM"]       = rowData[100-1:101]            
            record["ALTERNATE DATE-1:DD"]       = rowData[102-1:103]

            #CD-17#
            alternateGrossIndicator             = rowData[104-1:104]                
            alternateGrossIndicatorMap          = mapAlternateGrossIndicator.get(alternateGrossIndicator)
                      
            if alternateGrossIndicatorMap is not None:
                      record["ALTERNATE GROSS/NET INDICATOR"]     = alternateGrossIndicatorMap
            else:
                      record["ALTERNATE GROSS/NET INDICATOR"]     = alternateGrossIndicator
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#ALTERNATE GROSS/NET INDICATOR #No mapped value for [{alternateGrossIndicator}]")               
          
            #CD-18#
            spousalContributionCode             = rowData[105-1:105]        
            spousalContributionCodeMap          = mapSpousalContributionCode.get(spousalContributionCode)
                      
            if alternateGrossIndicatorMap is not None:
                      record["SPOUSAL CONTRIBUTION CODE"]     = alternateGrossIndicatorMap
            else:
                      record["SPOUSAL CONTRIBUTION CODE"]     = spousalContributionCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#SPOUSAL CONTRIBUTION CODE #No mapped value for [{spousalContributionCode}]")               

            record["MAX PAY AMOUNT"]                         = rowData[106-1:114]            
            record["ALTERNATE PAY EVER-1:EVERY JAN"]         = rowData[115-1:115]            
            record["ALTERNATE PAY EVER-1:EVERY FEB"]         = rowData[116- 116]            
            record["ALTERNATE PAY EVER-1:EVERY MAR"]         = rowData[117-1:117]            
            record["ALTERNATE PAY EVER-1:EVERY APR"]         = rowData[118-1:118]            
            record["ALTERNATE PAY EVER-1:EVERY MAY"]         = rowData[119-1:119]            
            record["ALTERNATE PAY EVER-1:EVERY JUN"]         = rowData[120-1:120]            
            record["ALTERNATE PAY EVER-1:EVERY JUL"]         = rowData[121-1:121]            
            record["ALTERNATE PAY EVER-1:EVERY AUG"]         = rowData[122-1:122]            
            record["ALTERNATE PAY EVER-1:EVERY SEP"]         = rowData[123-1:123]            
            record["ALTERNATE PAY EVER-1:EVERY OCT"]         = rowData[124-1:124]            
            record["ALTERNATE PAY EVER-1:EVERY NOV"]         = rowData[125-1:125]            
            record["ALTERNATE PAY EVER-1:EVERY DEC"]         = rowData[126-1:126]            
            record["FILLER"]                                 = rowData[127-1:133]                 
                      
                              
    elif rowType == "102":
        # Row type: RRIF PAY INFO RECORD 2 (102)        
            rowType = "RRIF PAY INFO RECORD 2"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]
              
            record["JAN PAY-1:AMOUNT"]          = rowData[16-1:24]            
            record["FEB PAY-1:AMOUNT"]          = rowData[25-1:33]            
            record["MAR PAY-1:AMOUNT"]          = rowData[34-1:42]            
            record["APR PAY-1:AMOUNT"]          = rowData[43-1:51]            
            record["MAY PAY-1:AMOUNT"]          = rowData[52-1:60]            
            record["JUN PAY-1:AMOUNT"]          = rowData[61-1:69]            
            record["JUL PAY-1:AMOUNT"]          = rowData[70-1:78]            
            record["AUG PAY-1:AMOUNT"]          = rowData[79-1:87]            
            record["SEP PAY-1:AMOUNT"]          = rowData[88-1:96]            
            record["OCT PAY-1:AMOUNT"]          = rowData[97-1:105]            
            record["NOV PAY-1:AMOUNT"]          = rowData[106-1:114]            
            record["DEC PAY-1:AMOUNT"]          = rowData[115-1:123]

            #CD-19#
            alternateWithholdingTax1Option      = rowData[124-1:124]        
            alternateWithholdingTax1OptionMap   = mapAlternateWithholdingTax1Option.get(alternateWithholdingTax1Option)
                      
            if alternateWithholdingTax1OptionMap is not None:
                      record["ALTERNATE WITHHOLDING TAX-1:OPTION"]     = alternateWithholdingTax1OptionMap
            else:
                      record["ALTERNATE WITHHOLDING TAX-1:OPTION"]     = alternateWithholdingTax1Option
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#ALTERNATE WITHHOLDING TAX-1:OPTION #No mapped value for [{alternateWithholdingTax1Option}]")               

            record["ALTERNATE FEDERAL TAX RATE (NN.NN)"]         = rowData[125-1:128]            
            record["ALTERNATE PROVINCIAL TAX RATE (NN.NN)"]      = rowData[129-1:132]            
            record["FILLER"]                                     = rowData[133-1:133]


    elif rowType == "103":
        # Row type: RRIF PAY INFO RECORD 3 (103)
            rowType = "RRIF PAY INFO RECORD 3"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]

            #CD-20#
            investmentVariety                   = rowData[16-1:16]                
            investmentVarietyMap                = mapInvestmentVariety.get(investmentVariety)
              
            if investmentVarietyMap is not None:
                      record["INVESTMENT VARIETY"]     = investmentVarietyMap
            else:
                      record["INVESTMENT VARIETY"]     = investmentVariety
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#INVESTMENT VARIETY #No mapped value for [{investmentVariety}]")

            record["FED TAX RATE (NN.NN)"]      = rowData[17-1:20]            
            record["PROV TAX RATE (NN.NN)"]     = rowData[21-1:24]            
            record["FLAT TAX AMOUNT"]           = rowData[25-1:29]            
            record["BRANCH"]                    = rowData[30-32]            
            record["ACCOUNT"]                   = rowData[33-37]            
            record["TYPE"]                      = rowData[38-1:38]            
            record["CHECK DIGIT"]               = rowData[39-1:39]
  
            #CD-21#
            calcCode                            = rowData[40-1:40]        
            calcCodeMap                         = mapCalcCode.get(calcCode)
              
            if calcCodeMap is not None:
                      record["CALC CODE"]       = calcCodeMap
            else:
                      record["CALC CODE"]       = calcCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#CALC CODE #No mapped value for [{calcCode}]")
                      
            #CD-22#
            futurePaymentCode                   = rowData[41-1:41]        
            futurePaymentCodeMap                = mapFuturePaymentCode.get(futurePaymentCode)
              
            if futurePaymentCodeMap is not None:
                      record["FUTURE PAYMENT CODE"]         = futurePaymentCodeMap
            else:
                      record["FUTURE PAYMENT CODE"]         = futurePaymentCode
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#FUTURE PAYMENT CODE #No mapped value for [{futurePaymentCode}]")
                      
                      
            #CD-23#
            thirdPartyInstruction               = rowData[42-1:42]        
            thirdPartyInstructionMap            = mapThirdPartyInstruction.get(thirdPartyInstruction)
              
            if thirdPartyInstructionMap is not None:
                      record["THIRD PARTY INSTRUCTION"]     = thirdPartyInstructionMap
            else:
                      record["THIRD PARTY INSTRUCTION"]     = thirdPartyInstruction
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#THIRD PARTY INSTRUCTION #No mapped value for [{thirdPartyInstruction}]")

            record["LAST PAYMENT DATE-1:CC"]    = rowData[43-1:44]            
            record["LAST PAYMENT DATE-1:YY"]    = rowData[45-1:46]            
            record["LAST PAYMENT DATE-1:MM"]    = rowData[47-1:48]            
            record["LAST PAYMENT DATE-1:DD"]    = rowData[49-1:50]            
            record["FILLER"]                    = rowData[51-1:133]

            
    elif rowType == "711":
        # Row type: DECEASED RECORD 1 (711)
            rowType = "DECEASED RECORD 1"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]

            record["ORIGINATOR NAME-1:ADDRESS-1:LINE 1"] = rowData[16-1:45]            
            record["ORIGINATOR NAME-1:ADDRESS-1:LINE 2"] = rowData[46-1:75]            
            record["ORIGINATOR NAME-1:ADDRESS-1:LINE 3"] = rowData[76-1:105]            
            record["FILLER"]                             = rowData[106-1:133]                    


    elif rowType == "712":
        # Row type: DECEASED RECORD 2 (712)            
            rowType = "DECEASED RECORD 2"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]

            record["ORIGINATOR NAME-1:ADDRESS LINE 4"]     = rowData[16-1:45]            
            record["ORIGINATOR NAME-1:ADDRESS LINE 5"]     = rowData[46-1:75]            
            record["ORIGINATOR NAME-1:ADDRESS LINE 6"]     = rowData[76-1:105]            
            record["SUCCESSOR OPEN DATE-1:CC"]             = rowData[106-1:107]            
            record["SUCCESSOR OPEN DATE-1:YY"]             = rowData[108-1:109]            
            record["SUCCESSOR OPEN DATE-1:MM"]             = rowData[110-1:111]            
            record["SUCCESSOR OPEN DATE-1:DD"]             = rowData[112-1:113]            
            record["SUCESSOR DATE OF DEATH-1:CC"]          = rowData[114-1:115]            
            record["SUCESSOR DATE OF DEATH-1:YY"]          = rowData[116-1:117]            
            record["SUCESSOR DATE OF DEATH-1:MM"]          = rowData[118-1:119]            
            record["SUCESSOR DATE OF DEATH-1:DD"]          = rowData[120-1:121]            
            record["FILLER"]                               = rowData[122-1:133]         


    elif rowType == "713":
        # Row type: RRIF BANK B PAY INFO RECORD (713)
            rowType = "RRIF BANK B PAY INFO RECORD"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]
              
            #CD-24#
            payFrequencyBankB                   = rowData[16-1:16]                        
            payFrequencyBankBMap                = mapPayFrequencyBankB.get(payFrequencyBankB)
              
            if payFrequencyBankBMap is not None:
                      record["PAY FREQUENCY (BANK B)"]     = payFrequencyBankBMap
            else:
                      record["PAY FREQUENCY (BANK B)"]     = payFrequencyBankB
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#PAY FREQUENCY (BANK B) #No mapped value for [{payFrequencyBankB}]")
              
            #CD-25#
            methodBankB                         = rowData[17-1:21]        
            methodBankBMap                      = mapMethodBankB.get(methodBankB)
              
            if methodBankBMap is not None:
                      record["METHOD (BANK B)"] = methodBankBMap
            else:
                      record["METHOD (BANK B)"] = methodBankB
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#PAY-FREQUENCY #No mapped value for [{methodBankB}]")

            record["AGENT (BANK B)"]                        = rowData[22-1:25]            
            record["TRANSIT (BANK B)"]                      = rowData[26-1:30]            
            record["CLIENT ACCOUNT (BANK B)"]               = rowData[31-1:45]            
            record["FIRST PAYMENT DATE-CC (BANK B)"]        = rowData[46-1:47]            
            record["FIRST PAYMENT DATE-YY (BANK B)"]        = rowData[48-1:49]            
            record["FIRST PAYMENT DATE-MM (BANK B)"]        = rowData[50-1:51]            
            record["FIRST PAYMENT DATE-DD (BANK B)"]        = rowData[52-1:53]            
            record["FIRST PAYMENT AMOUNT (BANK B)"]         = rowData[54-1:62]            
            record["ELECTED PAY AMOUNT (BANK B)"]           = rowData[63-1:71]        

            #CD-26#
            spousalBankA                        = rowData[72-1:72]                        
            spousalBankAMap                     = mapSpousalBankA.get(spousalBankA)
              
            if spousalBankAMap is not None:
                      record["SPOUSAL (BANK A)"]     = spousalBankAMap
            else:
                      record["SPOUSAL (BANK A)"]     = spousalBankA
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#SPOUSAL (BANK A) #No mapped value for [{spousalBankA}]")
              
            #CD-27#
            spousalBankB                        = rowData[73-1:73]                 
            spousalBankBMap                     = mapSpousalBankB.get(spousalBankB)
              
            if spousalBankBMap is not None:
                      record["SPOUSAL (BANK B)"]     = spousalBankBMap
            else:
                      record["SPOUSAL (BANK B)"]     = spousalBankB
                      rowStatus = -3
                      exceptions.append(f"INVALID_MAP#SPOUSAL (BANK B) #No mapped value for [{spousalBankB}]")                         

            record["DO NOT USE START DATE CC (BANK A)"] = rowData[74-1:75]            
            record["DO NOT USE START DATE YY (BANK A)"] = rowData[76-1:77]            
            record["DO NOT USE START DATE MM (BANK A)"] = rowData[78-1:79]            
            record["DO NOT USE END DATE CC (BANK A)"]   = rowData[80-1:81]            
            record["DO NOT USE END DATE YY (BANK A)"]   = rowData[82-1:83]            
            record["DO NOT USE END DATE MM (BANK A)"]   = rowData[84-1:85]            
            record["DO NOT USE START DATE CC (BANK B)"] = rowData[86-1:87]            
            record["DO NOT USE START DATE YY (BANK B)"] = rowData[88-1:89]            
            record["DO NOT USE START DATE MM (BANK B)"] = rowData[90-1:91]            
            record["DO NOT USE END DATE CC (BANK B)"]   = rowData[92-1:93]            
            record["DO NOT USE END DATE YY (BANK B)"]   = rowData[94-1:95]            
            record["DO NOT USE END DATE MM (BANK B)"]   = rowData[96-1:97]            
            record["FILLER"]                            = rowData[98-1:133]                         
                      
                      
    elif rowType == "999":
        # Row type: TRAILER (999)
            rowType = "TRAILER"
            record["BRANCH"]                    = rowData[5-7]            
            record["ACCOUNT"]                   = rowData[8-12]

            record["TLR ID CNTR"]               = rowData[16-1:20]            
            record["TLR COUNT"]                 = rowData[21-1:31]            
            record["TRAILER P2 DATA"]           = rowData[32-1:133]                               

                              
    else:
        # Bad structure of the record
            rowType = "<Unrecognized>"
            rowStatus = -2
            exceptions.append(f"INVALID_STRUCTURE#ROW TYPE#Invalid row type")

            
    record["_ROW_TYPE"]                         = rowType
    record["_ROW_STATUS"]                       = rowStatus
    record["_ROW_EXCEPTIONS"]                   = exceptions
    print ("NOK2")      
    return record
  
print ("OK5")                              


#---------------------------------------------------------------------------------------------------------------------
# Define structure of each row in the DynamicFrame
#---------------------------------------------------------------------------------------------------------------------
import logging

def log_errors(inner):
    def wrapper(*args, **kwargs):
        try:
            return inner(*args, **kwargs)
        except Exception as e:
            logging.exception('Error in function: {}'.format(inner))
            raise
    return wrapper

@log_errors
def CreateSourceRowStructure(record):
    
    rowId      = record["_ROW_ID"]
    rowData    = record["value"]
    
    rowStatus  = 0
    exceptions = []

    # Check row length
    rowLength = len(rowData)
    
    if rowLength != 349:
        
        # Row has incorrect length != 349
        rowStatus = -1
        exceptions.append(f"INVALID_LENGHT#ROW#Invalid row length [{rowLength}]")
        
        record["_ROW_STATUS"]     = rowStatus
        record["_ROW_EXCEPTIONS"] = exceptions

        return record
        
    # Row has correct length = 349
    
    c16       = rowData[16-1]
    c22       = rowData[22-1]
    c23       = rowData[23-1]
    c29       = rowData[29-1]

    trid      = rowData[29-1]
    spin      = rowData[30-1]
    rowType   = trid + spin
    
    branchNumber        = rowData[4-1:6]
    accountType         = rowData[15-1]
    accountChekckDigit  = '~'
    
    if rowType == "11":

        if c23 == " ":

            # Customer Position Header
            rowType = "Customer Position Header"
            accountChekckDigit                                 = rowData[300-1]

            record["CLIENT NUMBER"]                            = rowData[1-1:3]
            record["BRANCH NUMBER"]                            = rowData[4-1:6]
            record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
            record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]
            
            # Account Type lookup
            accountType                                        = rowData[15-1]
            accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)
            
            if accountTypeXRef is not None:
                record["ACCOUNT TYPE"]                         = accountTypeXRef
                record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
            else:
                record["ACCOUNT TYPE"]                         = accountType
                record["NEW ACCOUNT TYPE"]                     = ""
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

            record["SECURITY NUMBER"]                          = rowData[16-1:22]
            
            filler1                                            = rowData[23-1:28]
            if filler1 == " " * 6:
                record["FILLER 1"]                             = filler1
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")
            
            record["TRID"]                                     = rowData[29-1]
            record["SPIN"]                                     = rowData[30-1]
            record["SECURITY DESCRIPTION LINE 1"]              = rowData[31-1:60]
            record["DATE LAST ACTIVE"]                         = rowData[61-1:66]
            record["MEMO FLAGS (0000)"]                        = rowData[67-1:70]
            
            # Exchange Code lookup
            exchangeCode                                       = rowData[71-1]
            exchangeCodeXRef                                   = xrefExchLookup.get(exchangeCode)
            
            if exchangeCodeXRef is not None:
                record["EXCHANGE CODE"]                        = exchangeCodeXRef
            else:
                record["EXCHANGE CODE"]                        = exchangeCode
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#EXCHANGE CODE#No lookup value for [{exchangeCode}]")
            
            record["SECURITY SPIN"]                            = rowData[72-1]
            record["TRADE DATE QUANTITY SIGN"]                 = rowData[73-1]
            record["TRADE DATE QUANTITY"]                      = rowData[74-1:90]
            record["SETTLEMENT DATE QUANTITY SIGN"]            = rowData[91-1]
            record["SETTLEMENT DATE QUANTITY"]                 = rowData[92-1:108]
            record["MEMO FIELD 1 SIGN"]                        = rowData[109-1]
            record["MEMO FIELD 1"]                             = rowData[110-1:126]
            record["MEMO FIELD 2 SIGN"]                        = rowData[127-1]
            record["MEMO FIELD 2"]                             = rowData[128-1:144]
            record["MEMO FIELD 3 SIGN"]                        = rowData[145-1]
            record["MEMO FIELD 3"]                             = rowData[146-1:162]
            record["MEMO FIELD 4 SIGN"]                        = rowData[163-1]
            record["MEMO FIELD 4"]                             = rowData[164-1:180]
            record["SHORT VS BOX SIGN"]                        = rowData[181-1]
            record["SHORT VS BOX"]                             = rowData[181-1:198]
            record["NUMBER OF SHORT DAYS SIGN"]                = rowData[199-1]
            record["NUMBER OF SHORT DAYS"]                     = rowData[200-1]
            record["HOUSE REQUIREMENT % SIGN"]                 = rowData[201-1]
            record["HOUSE REQUIREMENT %"]                      = rowData[202-1:204]
            record["RECORD FLAGS"]                             = rowData[205-1:208]
            record["MARKET PRICE"]                             = rowData[209-1:223]
            record["IDA"]                                      = rowData[224-1:227]

            # Security Class Code lookup
            secClassCode                                       = rowData[228-1:234]
            secClassCodeXRef                                   = xrefSecClassCodeLookup.get(secClassCode[1-1])
            
            if secClassCodeXRef is not None:
                record["MSD CLASS CODE"]                       = secClassCodeXRef
            else:
                record["MSD CLASS CODE"]                       = secClassCode
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#MSD CLASS CODE#No lookup value for [{secClassCode}]")

            record["MARKET VALUE SIGN"]                        = rowData[235-1]
            record["MARKET VALUE"]                             = rowData[235-1:252]
            record["MATURITY DATE"]                            = rowData[253-1:258]
            record["HOUSE REQUIREMENT AMOUNT SIGN"]            = rowData[259-1]
            record["HOUSE REQUIREMENT AMOUNT"]                 = rowData[258-1:276]
            record["S/D REQUIREMENT AMOUNT SIGN"]              = rowData[277-1]
            record["S/D REQUIREMENT AMOUNT"]                   = rowData[278-1:294]
            record["PREFERRED SEG SECURITY FLAG"]              = rowData[295-1:296]

            # Account Range Indicator Mapping
            accountRangeIncicator                              = rowData[297-1]
            accountRangeIncicatorMap                           = mapTransSideIncicator.get(accountRangeIncicator)
            
            if accountRangeIncicatorMap is not None:
                record["ACCOUNT RANGE INDICATOR"]              = accountRangeIncicatorMap
            else:
                record["ACCOUNT RANGE INDICATOR"]              = accountRangeIncicator
                rowStatus = -3
                exceptions.append(f"INVALID_MAP#ACCOUNT RANGE INDICATOR#No mapped value for [{accountRangeIncicator}]")

            filler8                                            = rowData[298-1:299]
            if filler8 == " " * 2:
                record["FILLER 8"]                             = filler8
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[298:299] (8)#Filler containing data [{filler8}]")
            
            record["ACCOUNT CHECK DIGIT"]                      = rowData[300-1]

            filler9                                            = rowData[301-1:349]
            if filler9 == " " * 49:
                record["FILLER 9"]                             = filler9
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[301:349] (9)#Filler containing data [{filler9}]")
            
        elif c22 != " ":
            
            # Option Header
            rowType = "Option Header"
            accountChekckDigit                                 = rowData[289-1]

            record["CLIENT NUMBER"]                            = rowData[1-1:3]
            record["BRANCH NUMBER"]                            = rowData[4-1:6]
            record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
            record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]
            
            # Account Type lookup
            accountType                                        = rowData[15-1]
            accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)
            
            if accountTypeXRef is not None:
                record["ACCOUNT TYPE"]                         = accountTypeXRef
                record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
            else:
                record["ACCOUNT TYPE"]                         = accountType
                record["NEW ACCOUNT TYPE"]                     = ""
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

            record["UNDERLYING SECURITY NUMBER"]               = rowData[16-1:21]
            record["OPTION BYTE"]                              = rowData[22-1]

            filler1                                            = rowData[23-1:28]
            if filler1 == " " * 6:
                record["FILLER 1"]                             = filler1
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

            record["TRID"]                                     = rowData[29-1]
            record["SPIN"]                                     = rowData[30-1]
            record["SECURITY DESCRIPTION"]                     = rowData[31-1:60]

            filler2                                            = rowData[61-1:66]
            if filler2 == " " * 6:
                record["FILLER 2"]                             = filler2
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[61:66] (2)#Filler containing data [{filler2}]")

            record["OPTION SECURITY NUMBER"]                   = rowData[67-1:72]
            
            # Expiry Indicator Mapping
            expiryIndicator                                    = rowData[67-1:68]
            expiryIndicatorMap                                 = "T "
            
            if expiryIndicator == "X ":
                record["EXPIRATION CODE"]                      = expiryIndicatorMap
            else:
                record["EXPIRATION CODE"]                      = expiryIndicator
                rowStatus = -3
                exceptions.append(f"INVALID_MAP#EXPIRATION CODE#No mapped value for [{expiryIndicator}]")

            # Option Code Mapping
            optionCode                                         = rowData[69-1:70]
            optionCodeMap                                      = mapOptionCode.get(optionCode)
            
            if optionCodeMap is not None:
                record["OPTION CODE"]                          = optionCodeMap
            else:
                record["OPTION CODE"]                          = optionCode
                rowStatus = -3
                exceptions.append(f"INVALID_MAP#OPTION CODE#No mapped value for [{optionCode}]")

            # Exchange Code lookup
            exchangeCode                                       = rowData[71-1]
            exchangeCodeXRef                                   = xrefExchLookup.get(exchangeCode)
            
            if exchangeCodeXRef is not None:
                record["EXCHANGE CODE"]                        = exchangeCodeXRef
            else:
                record["EXCHANGE CODE"]                        = exchangeCode
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#EXCHANGE CODE#No lookup value for [{exchangeCode}]")

            # Legacy Special Information Mapping
            securitySpin                                       = rowData[72-1]
            securitySpinMap                                    = "O"
            
            if securitySpin == "M":
                record["SECURITY SPIN"]                        = securitySpinMap
            else:
                record["SECURITY SPIN"]                        = securitySpin
                rowStatus = -3
                exceptions.append(f"INVALID_MAP#SECURITY SPIN#No mapped value for [{securitySpin}]")

            record["LONG QUANTITY SIGN"]                       = rowData[73-1]
            record["LONG QUANTITY"]                            = rowData[74-1:90]
            record["SHORT QUANTITY SIGN"]                      = rowData[91-1]
            record["SHORT QUANTITY"]                           = rowData[92-1:106]
            record["STRIKE PRICE"]                             = rowData[210-1:223]
            record["IDA"]                                      = rowData[224-1:227]

            # Security Class Code lookup
            secClassCode                                       = rowData[228-1:234]
            secClassCodeXRef                                   = xrefSecClassCodeLookup.get(secClassCode[1-1])
            
            if secClassCodeXRef is not None:
                record["MSD CLASS CODE"]                       = secClassCodeXRef
            else:
                record["MSD CLASS CODE"]                       = secClassCode
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#MSD CLASS CODE#No lookup value for [{secClassCode}]")

            filler7                                            = rowData[235-1:252]
            if filler7 == " " * 18:
                record["FILLER 7"]                             = filler7
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[235:252] (7)#Filler containing data [{filler7}]")

            record["EXPIRATION DATE"]                          = rowData[253-1:258]

            filler8                                            = rowData[259-1:297]
            if filler8 == " " * 39:
                record["FILLER 8"]                             = filler8
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[259:297] (8)#Filler containing data [{filler8}]")

            record["LAST DIGIT OF UNDERLYING SECURITY NUMBER"] = rowData[297-1]
            record["ACCOUNT CHECK DIGIT"]                      = rowData[298-1]

            filler9                                            = rowData[299-1:349]
            if filler9 == " " * 39:
                record["FILLER 9"]                             = filler9
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[259:297] (9)#Filler containing data [{filler9}]")
            
        else:
            # Bad structure of the record
            rowStatus = -2
            exceptions.append(f"INVALID_STRUCTURE#ROW TYPE[TRID={trid},SPIN={spin},@[22]={c22}]#Invalid row type")
        
    elif rowType == "12":

        # Customer Position Trailer
        rowType = "Customer Position Trailer"
        accountChekckDigit                                 = rowData[86-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]
            
        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["SECURITY DESCRIPTION LINE 2"]              = rowData[31-1:60]
        record["MATURITY OR EXPIRATION DATE"]              = rowData[61-1:69]
        record["BOND INTEREST RATE"]                       = rowData[69-1:79]
        record["FREQUENCY CODE"]                           = rowData[80-1]
        record["CALL CODE"]                                = rowData[81-1]
        record["DATED DATE"]                               = rowData[82-1:85]
        record["ACCOUNT CHECK DIGIT"]                      = rowData[86-1]

        filler9                                            = rowData[87-1:349]
        if filler9 == " " * 263:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[259:297] (9)#Filler containing data [{filler9}]")

    elif rowType == "22":

        # When-Issue Customer Trailer
        rowType = "When-Issue Customer Trailer"
        accountChekckDigit                                 = rowData[86-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN "]                                    = rowData[30-1]
        record["SECURITY DESC"]                            = rowData[31-1:60]
        record["MATURITY OR EXPIRATION DATE"]              = rowData[61-1:69]
        record["BOND INTEREST RATE"]                       = rowData[69-1:79]
        record["FREQUENCY CODE"]                           = rowData[80-1]
        record["CALL CODE"]                                = rowData[81-1]
        record["DATED DATE"]                               = rowData[82-1:85]
        record["ACCOUNT CHECK DIGIT"]                      = rowData[86-1]
        record["FILLER 9"]                                 = rowData[87-1:349]
        
        filler9                                            = rowData[87-1:349]
        if filler9 == " " * 263:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[87:349] (9)#Filler containing data [{filler9}]")

    elif rowType == "21":

        # When-Issue Customer Header
        rowType = "When-Issue Customer Header"
        accountChekckDigit                                 = rowData[267-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["SECURITY DESC"]                            = rowData[31-1:60]
        record["DATE LAST ACTIVE"]                         = rowData[61-1:66]

        filler2                                            = rowData[61-1:68]
        if filler2 == " " * 8:
            record["FILLER 2"]                             = filler2
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[61:68] (2)#Filler containing data [{filler2}]")

        # Exchange Code lookup
        exchangeCode                                       = rowData[69-1]
        exchangeCodeXRef                                   = xrefExchLookup.get(exchangeCode)

        if exchangeCodeXRef is not None:
            record["EXCHANGE CODE"]                        = exchangeCodeXRef
        else:
            record["EXCHANGE CODE"]                        = exchangeCode
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#EXCHANGE CODE#No lookup value for [{exchangeCode}]")

        record["SECURITY SPIN"]                            = rowData[70-1]
        record["QUANTITY SIGN"]                            = rowData[71-1]
        record["QUANTITY"]                                 = rowData[72-1:88]
        record["CONTRACT PRICE (INTEGER)"]                 = rowData[89-1:96]
        record["CONTRACT PRICE (FRACTION)"]                = rowData[97-1:104]
        record["CONTRACT VALUE SIGN"]                      = rowData[105-1]
        record["CONTRACT VALUE"]                           = rowData[106-1:122]
        record["MARKET VALUE SIGN"]                        = rowData[123-1]
        record["MARKET VALUE"]                             = rowData[124-1:140]

        filler7                                            = rowData[141-1:142]
        if filler7 == " " * 2:
            record["FILLER 7"]                             = filler7
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[141:142] (7)#Filler containing data [{filler7}]")

        record["RECORD FLAGS"]                             = rowData[143-1:144]
        record["MARKET PRICE"]                             = rowData[145-1:161]
        record["FILLER 8"]                                 = rowData[162-1:266]

        filler8                                            = rowData[162-1:266]
        if filler8 == " " * 105:
            record["FILLER 8"]                             = filler8
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[162:266] (8)#Filler containing data [{filler8}]")

        record["ACCOUNT CHECK DIGIT"]                      = rowData[267-1]

        filler9                                            = rowData[268-1:349]
        if filler9 == " " * 82:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[268:349] (9)#Filler containing data [{filler9}]")


    elif rowType == "2A":

        # When-Issue Trading Header
        rowType = "When-Issue Trading Header"
        accountChekckDigit                                 = rowData[267-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["SECURITY DESCRIPTION"]                     = rowData[31-1:60]
        record["DATE LAST ACTIVE"]                         = rowData[61-1:66]

        filler2                                            = rowData[61-1:68]
        if filler2 == " " * 8:
            record["FILLER 2"]                             = filler2
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[61:68] (2)#Filler containing data [{filler2}]")

        # Exchange Code lookup
        exchangeCode                                       = rowData[69-1]
        exchangeCodeXRef                                   = xrefExchLookup.get(exchangeCode)

        if exchangeCodeXRef is not None:
            record["EXCHANGE CODE"]                        = exchangeCodeXRef
        else:
            record["EXCHANGE CODE"]                        = exchangeCode
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#EXCHANGE CODE#No lookup value for [{exchangeCode}]")

        record["SECURITY SPIN"]                            = rowData[70-1]
        record["QUANTITY SIGN"]                            = rowData[71-1]
        record["QUANTITY"]                                 = rowData[72-1:88]
        record["CONTRACT PRICE (INTEGER)"]                 = rowData[89-1:96]
        record["CONTRACT PRICE (FRACTION)"]                = rowData[97-1:104]
        record["CONTRACT VALUE SIGN"]                      = rowData[105-1]
        record["CONTRACT VALUE"]                           = rowData[106-1:122]
        record["MARKET VALUE SIGN"]                        = rowData[123-1]
        record["MARKET VALUE"]                             = rowData[124-1:140]

        filler7                                            = rowData[141-1:142]
        if filler7 == " " * 2:
            record["FILLER 7"]                             = filler7
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[141:142] (7)#Filler containing data [{filler7}]")

        record["RECORD FLAGS"]                             = rowData[143-1:144]
        record["MARKET PRICE"]                             = rowData[145-1:161]

        filler8                                            = rowData[162-1:266]
        if filler8 == " " * 105:
            record["FILLER 8"]                             = filler8
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[162:266] (8)#Filler containing data [{filler8}]")

        record["ACCOUNT CHECK DIGIT"]                      = rowData[267-1]

        filler9                                            = rowData[268-1:349]
        if filler9 == " " * 82:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[268:349] (9)#Filler containing data [{filler9}]")

    elif rowType == "1A":

        # Trading Analysis Trade Date
        rowType = "Trading Analysis Trade Date"
        accountChekckDigit                                 = rowData[349-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["SECURITY DESCRIPTION"]                     = rowData[31-1:60]
        record["BOOK DLA"]                                 = rowData[61-1:66]
        record["MARGIN DLA"]                               = rowData[67-1:72]

        filler2                                            = rowData[73-1:76]
        if filler2 == " " * 4:
            record["FILLER 2"]                             = filler2
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[73:76] (2)#Filler containing data [{filler2}]")

        # Exchange Code lookup
        exchangeCode                                       = rowData[71-1]
        exchangeCodeXRef                                   = xrefExchLookup.get(exchangeCode)

        if exchangeCodeXRef is not None:
            record["EXCHANGE CODE"]                        = exchangeCodeXRef
        else:
            record["EXCHANGE CODE"]                        = exchangeCode
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#EXCHANGE CODE#No lookup value for [{exchangeCode}]")

        record["SECURITY SPIN"]                            = rowData[78-1]
        record["T/D QUANTITY"]                             = rowData[79-1:95]
        record["T/D QUANTITY SIGN"]                        = rowData[96-1]
        record["T/D COST"]                                 = rowData[97-1:113]
        record["T/D COST SIGN"]                            = rowData[114-1]
        record["T/D UNREALIZED P/L"]                       = rowData[115-1:131]
        record["T/D UNREALIZED P/L SIGN"]                  = rowData[132-1]
        record["BOND FREQUENCY CODE"]                      = rowData[133-1:134]
        record["DATE LAST CALCULATED"]                     = rowData[135-1:140]
        record["HAIRCUT %"]                                = rowData[141-1:143]
        record["HAIRCUT % SIGN"]                           = rowData[144-1]
        record["CONCESSION"]                               = rowData[145-1:159]
        record["CONCESSION SIGN"]                          = rowData[160-1]
        record["HOUSE PRICE"]                              = rowData[161-1:175]
        record["HOUSE PRICE SIGN"]                         = rowData[176-1]
        record["T/D REALIZED P/L"]                         = rowData[177-1:193]
        record["T/D REALIZED P/L SIGN"]                    = rowData[194-1]
        record["FILLER 8"]                                 = rowData[195-1:202]

        filler8                                            = rowData[195-1:202]
        if filler8 == " " * 8:
            record["FILLER 8"]                             = filler8
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[195:202] (8)#Filler containing data [{filler8}]")

        record["RECORD FLAGS"]                             = rowData[203-1:206]
        record["MARKET PRICE"]                             = rowData[207-1:221]
        record["IDA SIGN"]                                 = rowData[222-1]
        record["IDA"]                                      = rowData[223-1:226]

        # Security Class Code lookup
        secClassCode                                       = rowData[228-1:234]
        secClassCodeXRef                                   = xrefSecClassCodeLookup.get(secClassCode[1-1])

        if secClassCodeXRef is not None:
            record["MSD CLASS CODE"]                       = secClassCodeXRef
        else:
            record["MSD CLASS CODE"]                       = secClassCode
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#MSD CLASS CODE#No lookup value for [{secClassCode}]")

        filler9                                            = rowData[234-1:348]
        if filler9 == " " * 115:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[234:348] (9)#Filler containing data [{filler9}]")

        record["ACCOUNT CHECK DIGIT"]                      = rowData[349-1]
        
    elif rowType == "1B":

        # Trading Analysis Settlement Date
        rowType = "Trading Analysis Settlement Date"
        accountChekckDigit                                 = rowData[267-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["SECURITY DESCRIPTION"]                     = rowData[31-1:60]
        record["OPTION EXPIRATION DATE"]                   = rowData[61-1:66]

        filler2                                            = rowData[67-1:69]
        if filler2 == " " * 3:
            record["FILLER 2"]                             = filler2
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[67:69] (2)#Filler containing data [{filler2}]")

        record["INTEREST FREQUENCY"]                       = rowData[70-1]
        record["S/D QUANTITY"]                             = rowData[71-1:87]
        record["S/D QUANTITY SIGN"]                        = rowData[88-1]
        record["S/D COST"]                                 = rowData[89-1:105]
        record["S/D COST SIGN"]                            = rowData[106-1]
        record["MEMO INTEREST CHARGE"]                     = rowData[107-1:117]
        record["MEMO INTEREST CHARGE SIGN"]                = rowData[118-1]
        record["DIVIDEND/INTEREST"]                        = rowData[119-1:129]
        record["DIVIDEND/INTEREST SIGN"]                   = rowData[130-1]
        record["GROSS CREDIT"]                             = rowData[131-1:141]
        record["GROSS CREDIT SIGN"]                        = rowData[142-1]
        record["EARNED INTEREST"]                          = rowData[143-1:153]
        record["EARNED INTEREST SIGN"]                     = rowData[154-1]
        record["INTEREST RATE"]                            = rowData[155-1:165]
        record["INTEREST RATE SIGN"]                       = rowData[166-1]
        record["S/D REALIZED P/L"]                         = rowData[167-1:183]
        record["S/D REALIZED P/L SIGN"]                    = rowData[184-1]

        filler7                                            = rowData[185-1:203]
        if filler7 == " " * 19:
            record["FILLER 7"]                             = filler7
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[185:203] (7)#Filler containing data [{filler7}]")

        record["IDA"]                                      = rowData[204-1:207]

        filler8                                            = rowData[208-1:266]
        if filler8 == " " * 59:
            record["FILLER 8"]                             = filler8
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[208:266] (8)#Filler containing data [{filler8}]")

        record["ACCOUNT CHECK DIGIT"]                      = rowData[267-1]

        filler9                                            = rowData[268-1:349]
        if filler9 == " " * 82:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[268:349] (9)#Filler containing data [{filler9}]")

    elif rowType == "2B":

        # When-Issue Trading Trailer
        rowType = "When-Issue Trading Trailer"
        accountChekckDigit                                 = rowData[267-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["SECURITY NUMBER"]                          = rowData[16-1:22]

        filler1                                            = rowData[23-1:28]
        if filler1 == " " * 6:
            record["FILLER 1"]                             = filler1
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["SECURITY DESCRIPTION"]                     = rowData[31-1:60]
        record["OPTION EXPIRATION DATE"]                   = rowData[61-1:66]

        filler2                                            = rowData[67-1:69]
        if filler2 == " " * 3:
            record["FILLER 2"]                             = filler2
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[67:69] (2)#Filler containing data [{filler2}]")

        record["INTEREST FREQUENCY"]                       = rowData[70-1]
        record["INTEREST FREQUENCY SIGN"]                  = rowData[71-1]
        record["S/D QUANTITY"]                             = rowData[72-1:88]
        record["S/D QUANTITY SIGN"]                        = rowData[89-1]
        record["S/D COST"]                                 = rowData[90-1:106]
        record["S/D COST SIGN"]                            = rowData[107-1]
        record["MEMO INTEREST CHARGE"]                     = rowData[108-1:116]
        record["MEMO INTEREST CHARGE SIGN"]                = rowData[117-1]
        record["DIVIDEND/INTEREST"]                        = rowData[118-1:126]
        record["DIVIDEND/INTEREST SIGN"]                   = rowData[127-1]
        record["GROSS CREDIT"]                             = rowData[128-1:136]
        record["GROSS CREDIT SIGN"]                        = rowData[137-1]
        record["EARNED INTEREST"]                          = rowData[138-1:146]
        record["EARNED INTEREST SIGN"]                     = rowData[147-1]
        record["INTEREST RATE"]                            = rowData[148-1:159]
        record["INTEREST RATE SIGN"]                       = rowData[160-1]
        record["S/D REALIZED P/L"]                         = rowData[161-1:175]
        record["S/D REALIZED P/L SIGN"]                    = rowData[176-1]

        filler7                                            = rowData[177-1:203]
        if filler7 == " " * 27:
            record["FILLER 7"]                             = filler7
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[177:203] (7)#Filler containing data [{filler7}]")

        record["IDA"]                                      = rowData[204-1]

        filler8                                            = rowData[208-1:266]
        if filler8 == " " * 59:
            record["FILLER 8"]                             = filler8
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[208:266] (8)#Filler containing data [{filler8}]")

        record["ACCOUNT CHECK DIGIT"]                      = rowData[267-1]

        filler9                                            = rowData[268-1:349]
        if filler9 == " " * 82:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[268:349] (9)#Filler containing data [{filler9}]")

    elif rowType == "7 ":

        if c16 != " ":

            # Customer Balance, F/Type
            rowType = "Customer Balance, F/Type"
            accountChekckDigit                                 = rowData[349-1]

            record["CLIENT NUMBER"]                            = rowData[1-1:3]
            record["BRANCH NUMBER"]                            = rowData[4-1:6]
            record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
            record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

            # Account Type lookup
            accountType                                        = rowData[15-1]
            accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

            if accountTypeXRef is not None:
                record["ACCOUNT TYPE"]                         = accountTypeXRef
                record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
            else:
                record["ACCOUNT TYPE"]                         = accountType
                record["NEW ACCOUNT TYPE"]                     = ""
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

            record["HIGH−VALUE"]                               = rowData[16-1]
            record["DATE LAST ACTIVE (BKPG / MRGN)"]           = rowData[17-1:22]

            filler1                                            = rowData[23-1:28]
            if filler1 == " " * 6:
                record["FILLER 1"]                             = filler1
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[23:28] (1)#Filler containing data [{filler1}]")

            record["TRID"]                                     = rowData[29-1]
            record["SPIN"]                                     = rowData[30-1]
            record["CUSTOMER SHORT NAME"]                      = rowData[31-1:50]
            record["CUSTOMER INSTRUCTIONS"]                    = rowData[51-1:52]
            record["ACCOUNT CODE"]                             = rowData[53-1:55]
            record["RR CODE"]                                  = rowData[56-1:58]

            filler2                                            = rowData[59-1]
            if filler2 == " " * 1:
                record["FILLER 2"]                             = filler2
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[59] (2)#Filler containing data [{filler2}]")

            record["S/D BALANCE SIGN"]                         = rowData[60-1]
            record["S/D BALANCE"]                              = rowData[61-1:77]
            record["T/D BALANCE/MARKET VALUE SIGN"]            = rowData[78-1]
            record["T/D BALANCE/MARKET VALUE"]                 = rowData[79-1:95]
            record["OUTSTANDING CALL / DAY−2 CALL SIGN"]       = rowData[96-1]
            record["OUTSTANDING CALL / DAY−2 CALL"]            = rowData[97-1:113]
            record["SMA CALL/DAY−3 CALL SIGN"]                 = rowData[114-1]
            record["SMA CALL/DAY−3 CALL"]                      = rowData[115-1:131]
            record["BUYING POWER/DAY−5 CALL SIGN"]             = rowData[132-1]
            record["BUYING POWER/DAY−5 CALL"]                  = rowData[133-1:149]
            record["MARGIN/CASH DEFICIENCY SIGN"]              = rowData[150-1]
            record["MARGIN/CASH DEFICIENCY"]                   = rowData[151-1:167]
            record["MARGIN INTEREST ACCUMULATION SIGN"]        = rowData[168-1]
            record["MARGIN INTEREST ACCUMULATION"]             = rowData[169-1:185]
            record["HOUSE EXCESS SIGN"]                        = rowData[186-1]
            record["HOUSE EXCESS"]                             = rowData[187-1:203]
            record["ISIP INDEX COST SIGN"]                     = rowData[204-1]
            record["ISIP INDEX COST"]                          = rowData[225-1:221]
            record["TBD SIGN"]                                 = rowData[222-1]
            record["TBD"]                                      = rowData[223-1:240]

            filler9                                            = rowData[241-1:348]
            if filler9 == " " * 108:
                record["FILLER 9"]                             = filler9
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[241:348] (9)#Filler containing data [{filler9}]")

            record["ACCOUNT CHECK DIGIT"]                      = rowData[349-1]
            
        else:

            # Loan Account
            rowType = "Loan Account"
            accountChekckDigit                                 = '~'

            record["CLIENT NUMBER"]                            = rowData[1-1:3]
            record["BRANCH NUMBER"]                            = rowData[4-1:6]
            record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
            record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

            # Account Type lookup
            accountType                                        = rowData[15-1]
            accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

            if accountTypeXRef is not None:
                record["ACCOUNT TYPE"]                         = accountTypeXRef
                record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
            else:
                record["ACCOUNT TYPE"]                         = accountType
                record["NEW ACCOUNT TYPE"]                     = ""
                rowStatus = -3
                exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

            filler1                                            = rowData[16-1]
            if filler1 == " " * 1:
                record["FILLER 1"]                             = filler1
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[16] (1)#Filler containing data [{filler1}]")
            
            record["DATE LAST ACTIVE (BKPG / MRGN)"]           = rowData[17-1:28]
            record["TRID"]                                     = rowData[29-1]
            record["SPIN"]                                     = rowData[30-1]
            record["ACCOUNT NAME"]                             = rowData[31-1:50]
            record["ACCOUNT CODES"]                            = rowData[51-1:53]

            filler2                                            = rowData[54-1:59]
            if filler2 == " " * 6:
                record["FILLER 2"]                             = filler2
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[54:59] (2)#Filler containing data [{filler2}]")

            record["S/D BALANCE SIGN"]                         = rowData[60-1]
            record["S/D BALANCE"]                              = rowData[61-1:77]
            record["MARKET VALUE OF SECURITIES SIGN"]          = rowData[78-1]
            record["MARKET VALUE OF SECURITIES"]               = rowData[79-1:95]
            record["RATIO MV−TO−S/D BALANCE SIGN"]             = rowData[96-1]
            record["RATIO MV−TO−S/D BALANCE"]                  = rowData[97-1:113]

            filler9                                            = rowData[114-1:349]
            if filler9 == " " * 236:
                record["FILLER 9"]                             = filler9
            else:
                rowStatus = -4
                exceptions.append(f"INVALID_FILLER#FILLER@[114:349] (9)#Filler containing data [{filler9}]")

    elif rowType == "71":

        # Customer Balance, Last in Account
        rowType = "Customer Balance, Last in Account"
        accountChekckDigit                                 = rowData[349-1]

        record["CLIENT NUMBER"]                            = rowData[1-1:3]
        record["BRANCH NUMBER"]                            = rowData[4-1:6]
        record["ACCOUNT NUMBER"]                           = rowData[7-1:11]
        record["FOREIGN CURRENCY CODE"]                    = rowData[12-1:14]

        # Account Type lookup
        accountType                                        = rowData[15-1]
        accountTypeXRef                                    = xrefAccTypeLookup.get(accountType)

        if accountTypeXRef is not None:
            record["ACCOUNT TYPE"]                         = accountTypeXRef
            record["NEW ACCOUNT TYPE"]                     = branchNumber + accountTypeXRef + accountChekckDigit
        else:
            record["ACCOUNT TYPE"]                         = accountType
            record["NEW ACCOUNT TYPE"]                     = ""
            rowStatus = -3
            exceptions.append(f"INVALID_XREF#ACCOUNT TYPE#No lookup value for [{accountType}]")

        record["HIGH−VALUE (BPS Internal use only)"]       = rowData[16-1]
        record["DATE LAST ACTIVE"]                         = rowData[17-1:22]
        record["DATE"]                                     = rowData[23-1:28]
        record["TRID"]                                     = rowData[29-1]
        record["SPIN"]                                     = rowData[30-1]
        record["CUSTOMER SHORT NAME"]                      = rowData[31-1:50]
        record["CUSTOMER INSTRUCTIONS"]                    = rowData[51-1:52]
        record["ACCOUNT CODE"]                             = rowData[53-1:55]
        record["RR CODE"]                                  = rowData[56-1:58]

        filler2                                            = rowData[59-1]
        if filler2 == " " * 1:
            record["FILLER 2"]                             = filler2
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[59] (2)#Filler containing data [{filler2}]")

        record["S/D BALANCE SIGN"]                         = rowData[60-1]
        record["S/D BALANCE"]                              = rowData[61-1:77]
        record["T/D BALANCE/MARKET VALUE SIGN"]            = rowData[78-1]
        record["T/D BALANCE/MARKET VALUE"]                 = rowData[79-1:95]
        record["TODAYS CALL/DAY−1 CALL SIGN"]              = rowData[96-1]
        record["TODAYS CALL/DAY−1 CALL"]                   = rowData[97-1:113]
        record["OUTSTANDING CALL / DAY−2 CALL SIGN"]       = rowData[114-1]
        record["OUTSTANDING CALL / DAY−2 CALL"]            = rowData[115-1:131]
        record["SMA CALL/DAY−3 CALL SIGN"]                 = rowData[132-1]
        record["SMA CALL/DAY−3 CALL"]                      = rowData[133-1:149]
        record["HOUSE CALL/DAY−4 CALL SIGN"]               = rowData[150-1]
        record["HOUSE CALL/DAY−4 CALL"]                    = rowData[151-1:167]
        record["BUYING POWER/DAY−5 CALL SIGN"]             = rowData[168-1]
        record["BUYING POWER/DAY−5 CALL"]                  = rowData[169-1:185]
        record["CASH AVAILABLE OR EQUITY PERCENT SIGN"]    = rowData[186-1]
        record["CASH AVAILABLE OR EQUITY PERCENT"]         = rowData[187-1:203]
        record["MARGIN/CASH DEFICIENCY SIGN"]              = rowData[204-1]
        record["MARGIN/CASH DEFICIENCY"]                   = rowData[203-1:221]
        record["TYPE 5 BALANCE IN TYPE 2 A/C SIGN"]        = rowData[222-1]
        record["TYPE 5 BALANCE IN TYPE 2 A/C"]             = rowData[223-1:239]

        filler7                                            = rowData[240-1:249]
        if filler7 == " " * 10:
            record["FILLER 7"]                             = filler7
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[240:249] (7)#Filler containing data [{filler7}]")

        record["MARGIN INTEREST ACCUMULATION SIGN"]        = rowData[250-1]
        record["MARGIN INTEREST ACCUMULATION"]             = rowData[251-1:261]
        record["HOUSE EXCESS SIGN"]                        = rowData[262-1]
        record["HOUSE EXCESS"]                             = rowData[262-1:279]
        record["ISIP INDEX COST SIGN"]                     = rowData[280-1]
        record["ISIP INDEX COST"]                          = rowData[281-1:297]
        record["FILLER 8"]                                 = rowData[297-1:303]

        filler8                                            = rowData[297-1:303]
        if filler8 == " " * 7:
            record["FILLER 8"]                             = filler8
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[297:303] (8)#Filler containing data [{filler8}]")

        record["LANGUAGE"]                                 = rowData[304-1]

        filler9                                            = rowData[305-1:320]
        if filler9 == " " * 16:
            record["FILLER 9"]                             = filler9
        else:
            rowStatus = -4
            exceptions.append(f"INVALID_FILLER#FILLER@[305:320] (9)#Filler containing data [{filler9}]")

        # Account Range Indicator Mapping
        accountRangeIncicator                              = rowData[297-1]
        accountRangeIncicatorMap                           = mapTransSideIncicator.get(accountRangeIncicator)

        if accountRangeIncicatorMap is not None:
            record["ACCOUNT RANGE INDICATOR"]              = accountRangeIncicatorMap
        else:
            record["ACCOUNT RANGE INDICATOR"]              = accountRangeIncicator
            rowStatus = -3

        record["TELEPHONE NUMBER"]                         = rowData[322-1:330]
        record["ISIP NET P/S SIGN"]                        = rowData[331-1]
        record["ISIP NET P/S"]                             = rowData[332-1:348]
        record["ACCOUNT CHECK DIGIT"]                      = rowData[349-1]

    elif rowType == "91":
    
        # Client Totals Header
        rowType = "Client Totals Header"

    elif rowType == "92":
    
        # Client Totals Trailer
        rowType = "Client Totals Trailer"

    elif rowType == "00":
    
        # Date Record
        rowType = "Date Record"

    else:
        # Bad structure of the record
        rowType = "<Unrecognized>"
        rowStatus = -2
        exceptions.append(f"INVALID_STRUCTURE#ROW TYPE[TRID={trid},SPIN={spin}]#Invalid row type")

    record["_ROW_TYPE"]                                    = rowType
    record["_ROW_STATUS"]                                  = rowStatus
    record["_ROW_EXCEPTIONS"]                              = exceptions
    
    return record

#---------------------------------------------------------------------------------------------------------------------
# Apply the structure transformation to the DynamicFrame
#---------------------------------------------------------------------------------------------------------------------

mapped_source_dyf = Map.apply(frame = source_dyf, f = CreateSourceRowStructure)

mapStructErrCount = mapped_source_dyf.stageErrorsCount()
if mapStructErrCount > 0:
    mapped_source_err_df = mapped_source_dyf.errorsAsDynamicFrame().repartition(1).toDF()
    mapped_source_err_df.write.mode("append").json(errorPath)
    
    RenameDataLakeFile(bucketURI, errorPath, errorPath, mapStructErrFileName)
    #raise Exception(f"Errors [{mappingErrCount}] during record structure mapping.")

print(mapStructErrCount)
#errors_source_dyf = mapped_source_dyf.errorsAsDynamicFrame()
#errors_source_dyf.show()
#---------------------------------------------------------------------------------------------------------------------
# Split DynamicFrame to the default and the rejected outputs
#---------------------------------------------------------------------------------------------------------------------
from awsglue.dynamicframe import DynamicFrameCollection
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: 
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

# Conditional Router
conditionalRouter = threadedRoute(glueContext,
  source_DyF = mapped_source_dyf,
  group_filters = [GroupFilter(name = "output_transformed", filters = lambda row: (bool(int(row["_ROW_STATUS"]) == 0))), GroupFilter(name = "output_rejected", filters = lambda row: (not(bool(int(row["_ROW_STATUS"]) == 0))))])

# Rows successfuly transformed -> transformed_dyf
transformed_dyf   = SelectFromCollection.apply(dfc=conditionalRouter, key="output_transformed", transformation_ctx="output_transformed")

# Rows rejected -> rejected_dyf
rejected_dyf = SelectFromCollection.apply(dfc=conditionalRouter, key="output_rejected", transformation_ctx="output_rejected")

#---------------------------------------------------------------------------------------------------------------------
# Form each output row
#---------------------------------------------------------------------------------------------------------------------
@log_errors
def CreateOutputRowStructure(record):

    rowData   = record["value"]
    rowType   = record["_ROW_TYPE"]
    
    if rowType == "Customer Position Header":

        # Customer Position Header 
        # Fields:     48 in/49 out
        # Transforms: 5

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],

            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION LINE 1"],
            record["DATE LAST ACTIVE"],
            record["MEMO FLAGS (0000)"],
            record["EXCHANGE CODE"],
            record["SECURITY SPIN"],
            record["TRADE DATE QUANTITY SIGN"],
            record["TRADE DATE QUANTITY"],
            record["SETTLEMENT DATE QUANTITY SIGN"],
            record["SETTLEMENT DATE QUANTITY"],
            record["MEMO FIELD 1 SIGN"],
            record["MEMO FIELD 1"],
            record["MEMO FIELD 2 SIGN"],
            record["MEMO FIELD 2"],
            record["MEMO FIELD 3 SIGN"],
            record["MEMO FIELD 3"],
            record["MEMO FIELD 4 SIGN"],
            record["MEMO FIELD 4"],
            record["SHORT VS BOX SIGN"],
            record["SHORT VS BOX"],
            record["NUMBER OF SHORT DAYS SIGN"],
            record["NUMBER OF SHORT DAYS"],
            record["HOUSE REQUIREMENT % SIGN"],
            record["HOUSE REQUIREMENT %"],
            record["RECORD FLAGS"],
            record["MARKET PRICE"],
            record["IDA"],
            record["MSD CLASS CODE"],
            record["MARKET VALUE SIGN"],
            record["MARKET VALUE"],
            record["MATURITY DATE"],
            record["HOUSE REQUIREMENT AMOUNT SIGN"],
            record["HOUSE REQUIREMENT AMOUNT"],
            record["S/D REQUIREMENT AMOUNT SIGN"],
            record["S/D REQUIREMENT AMOUNT"],
            record["PREFERRED SEG SECURITY FLAG"],
            record["ACCOUNT RANGE INDICATOR"],
            record["FILLER 8"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])

    elif rowType == "Option Header":

        # Option Header
        # Fields:     30 in/31 out
        # Transforms: 7

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],

            record["UNDERLYING SECURITY NUMBER"],
            record["OPTION BYTE"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION"],
            record["FILLER 2"],
            record["OPTION SECURITY NUMBER"],
            record["EXPIRATION CODE"],
            record["OPTION CODE"],
            record["EXCHANGE CODE"],
            record["SECURITY SPIN"],
            record["LONG QUANTITY SIGN"],
            record["LONG QUANTITY"],
            record["SHORT QUANTITY SIGN"],
            record["SHORT QUANTITY"],
            record["STRIKE PRICE"],
            record["IDA"],
            record["MSD CLASS CODE"],
            record["FILLER 7"],
            record["EXPIRATION DATE"],
            record["FILLER 8"],
            record["LAST DIGIT OF UNDERLYING SECURITY NUMBER"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])

    elif rowType == "Customer Position Trailer":

        # Customer Position Trailer
        # Fields:     17 in/18 out
        # Transforms: 2

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION LINE 2"],
            record["MATURITY OR EXPIRATION DATE"],
            record["BOND INTEREST RATE"],
            record["FREQUENCY CODE"],
            record["CALL CODE"],
            record["DATED DATE"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])

    elif rowType == "When-Issue Customer Trailer":

        # When-Issue Customer Trailer
        # Fields:     17 in/18 out
        # Transforms: 2

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN "],
            record["SECURITY DESC"],
            record["MATURITY OR EXPIRATION DATE"],
            record["BOND INTEREST RATE"],
            record["FREQUENCY CODE"],
            record["CALL CODE"],
            record["DATED DATE"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])

    elif rowType == "When-Issue Customer Header":

        # When-Issue Customer Header
        # Fields:     28 in/29 out
        # Transforms: 3

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESC"],
            record["DATE LAST ACTIVE"],
            record["FILLER 2"],
            record["EXCHANGE CODE"],
            record["SECURITY SPIN"],
            record["QUANTITY SIGN"],
            record["QUANTITY"],
            record["CONTRACT PRICE (INTEGER)"],
            record["CONTRACT PRICE (FRACTION)"],
            record["CONTRACT VALUE SIGN"],
            record["CONTRACT VALUE"],
            record["MARKET VALUE SIGN"],
            record["MARKET VALUE"],
            record["FILLER 7"],
            record["RECORD FLAGS"],
            record["MARKET PRICE"],
            record["FILLER 8"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])
        
    elif rowType == "When-Issue Trading Header":

        # When-Issue Trading Header
        # Fields:     18 in/19 out
        # Transforms: 3

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            record["SECURITY NUMBER"],
            
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION"],
            record["DATE LAST ACTIVE"],
            record["FILLER 2"],
            record["EXCHANGE CODE"],
            record["SECURITY SPIN"],
            record["QUANTITY SIGN"],
            record["QUANTITY"],
            record["CONTRACT PRICE (INTEGER)"],
            record["CONTRACT PRICE (FRACTION)"],
            record["CONTRACT VALUE SIGN"],
            record["CONTRACT VALUE"],
            record["MARKET VALUE SIGN"],
            record["MARKET VALUE"],
            record["FILLER 7"],
            record["RECORD FLAGS"],
            record["MARKET PRICE"],
            record["FILLER 8"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])
        
    elif rowType == "Trading Analysis Trade Date":

        # Trading Analysis Trade Date
        # Fields:     39 in/40 out
        # Transforms: 4

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION"],
            record["BOOK DLA"],
            record["MARGIN DLA"],
            record["FILLER 2"],
            record["EXCHANGE CODE"],
            record["SECURITY SPIN"],
            record["T/D QUANTITY"],
            record["T/D QUANTITY SIGN"],
            record["T/D COST"],
            record["T/D COST SIGN"],
            record["T/D UNREALIZED P/L"],
            record["T/D UNREALIZED P/L SIGN"],
            record["BOND FREQUENCY CODE"],
            record["DATE LAST CALCULATED"],
            record["HAIRCUT %"],
            record["HAIRCUT % SIGN"],
            record["CONCESSION"],
            record["CONCESSION SIGN"],
            record["HOUSE PRICE"],
            record["HOUSE PRICE SIGN"],
            record["T/D REALIZED P/L"],
            record["T/D REALIZED P/L SIGN"],
            record["FILLER 8"],
            record["RECORD FLAGS"],
            record["MARKET PRICE"],
            record["IDA SIGN"],
            record["IDA"],
            record["MSD CLASS CODE"],
            record["FILLER 9"],
            record["ACCOUNT CHECK DIGIT"]
        ])
        
    elif rowType == "Trading Analysis Settlement Date":

        # Trading Analysis Settlement Date
        # Fields:     34 in/35 out
        # Transforms: 2

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION"],
            record["OPTION EXPIRATION DATE"],
            record["FILLER 2"],
            record["INTEREST FREQUENCY"],
            record["S/D QUANTITY"],
            record["S/D QUANTITY SIGN"],
            record["S/D COST"],
            record["S/D COST SIGN"],
            record["MEMO INTEREST CHARGE"],
            record["MEMO INTEREST CHARGE SIGN"],
            record["DIVIDEND/INTEREST"],
            record["DIVIDEND/INTEREST SIGN"],
            record["GROSS CREDIT"],
            record["GROSS CREDIT SIGN"],
            record["EARNED INTEREST"],
            record["EARNED INTEREST SIGN"],
            record["INTEREST RATE"],
            record["INTEREST RATE SIGN"],
            record["S/D REALIZED P/L"],
            record["S/D REALIZED P/L SIGN"],
            record["FILLER 7"],
            record["IDA"],
            record["FILLER 8"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])

    elif rowType == "When-Issue Trading Trailer":

        # When-Issue Trading Trailer
        # Fields:     35 in/36 out
        # Transforms: 2

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["SECURITY NUMBER"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["SECURITY DESCRIPTION"],
            record["OPTION EXPIRATION DATE"],
            record["FILLER 2"],
            record["INTEREST FREQUENCY"],
            record["INTEREST FREQUENCY SIGN"],
            record["S/D QUANTITY"],
            record["S/D QUANTITY SIGN"],
            record["S/D COST"],
            record["S/D COST SIGN"],
            record["MEMO INTEREST CHARGE"],
            record["MEMO INTEREST CHARGE SIGN"],
            record["DIVIDEND/INTEREST"],
            record["DIVIDEND/INTEREST SIGN"],
            record["GROSS CREDIT"],
            record["GROSS CREDIT SIGN"],
            record["EARNED INTEREST"],
            record["EARNED INTEREST SIGN"],
            record["INTEREST RATE"],
            record["INTEREST RATE SIGN"],
            record["S/D REALIZED P/L"],
            record["S/D REALIZED P/L SIGN"],
            record["FILLER 7"],
            record["IDA"],
            record["FILLER 8"],
            record["ACCOUNT CHECK DIGIT"],
            record["FILLER 9"]
        ])
        
    elif rowType == "Customer Balance, F/Type":

        # Customer Balance, F/Type
        # Fields:     37 in/38 out
        # Transforms: 2

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],

            record["HIGH−VALUE"],
            record["DATE LAST ACTIVE (BKPG / MRGN)"],
            record["FILLER 1"],
            record["TRID"],
            record["SPIN"],
            record["CUSTOMER SHORT NAME"],
            record["CUSTOMER INSTRUCTIONS"],
            record["ACCOUNT CODE"],
            record["RR CODE"],
            record["FILLER 2"],
            record["S/D BALANCE SIGN"],
            record["S/D BALANCE"],
            record["T/D BALANCE/MARKET VALUE SIGN"],
            record["T/D BALANCE/MARKET VALUE"],
            record["OUTSTANDING CALL / DAY−2 CALL SIGN"],
            record["OUTSTANDING CALL / DAY−2 CALL"],
            record["SMA CALL/DAY−3 CALL SIGN"],
            record["SMA CALL/DAY−3 CALL"],
            record["BUYING POWER/DAY−5 CALL SIGN"],
            record["BUYING POWER/DAY−5 CALL"],
            record["MARGIN/CASH DEFICIENCY SIGN"],
            record["MARGIN/CASH DEFICIENCY"],
            record["MARGIN INTEREST ACCUMULATION SIGN"],
            record["MARGIN INTEREST ACCUMULATION"],
            record["HOUSE EXCESS SIGN"],
            record["HOUSE EXCESS"],
            record["ISIP INDEX COST SIGN"],
            record["ISIP INDEX COST"],
            record["TBD SIGN"],
            record["TBD"],
            record["FILLER 9"],
            record["ACCOUNT CHECK DIGIT"]
        ])
            
    elif rowType == "Loan Account":

        # Loan Account
        # Fields:     19 in/20 out
        # Transforms: 2

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],

            record["FILLER 1"],
            record["DATE LAST ACTIVE (BKPG / MRGN)"],
            record["TRID"],
            record["SPIN"],
            record["ACCOUNT NAME"],
            record["ACCOUNT CODES"],
            record["FILLER 2"],
            record["S/D BALANCE SIGN"],
            record["S/D BALANCE"],
            record["MARKET VALUE OF SECURITIES SIGN"],
            record["MARKET VALUE OF SECURITIES"],
            record["RATIO MV−TO−S/D BALANCE SIGN"],
            record["RATIO MV−TO−S/D BALANCE"],
            record["FILLER 9"]
        ])

    elif rowType == "Customer Balance, Last in Account":

        # Customer Balance, Last in Account
        # Fields:     50 in/51 out
        # Transforms: 3

        record["_ROW_DATA"] = "|".join([
            record["CLIENT NUMBER"],
            record["BRANCH NUMBER"],
            record["ACCOUNT NUMBER"],
            record["FOREIGN CURRENCY CODE"],
            record["ACCOUNT TYPE"],
            record["NEW ACCOUNT TYPE"],
            
            record["HIGH−VALUE (BPS Internal use only)"],
            record["DATE LAST ACTIVE"],
            record["DATE"],
            record["TRID"],
            record["SPIN"],
            record["CUSTOMER SHORT NAME"],
            record["CUSTOMER INSTRUCTIONS"],
            record["ACCOUNT CODE"],
            record["RR CODE"],
            record["FILLER 2"],
            record["S/D BALANCE SIGN"],
            record["S/D BALANCE"],
            record["T/D BALANCE/MARKET VALUE SIGN"],
            record["T/D BALANCE/MARKET VALUE"],
            record["TODAYS CALL/DAY−1 CALL SIGN"],
            record["TODAYS CALL/DAY−1 CALL"],
            record["OUTSTANDING CALL / DAY−2 CALL SIGN"],
            record["OUTSTANDING CALL / DAY−2 CALL"],
            record["SMA CALL/DAY−3 CALL SIGN"],
            record["SMA CALL/DAY−3 CALL"],
            record["HOUSE CALL/DAY−4 CALL SIGN"],
            record["HOUSE CALL/DAY−4 CALL"],
            record["BUYING POWER/DAY−5 CALL SIGN"],
            record["BUYING POWER/DAY−5 CALL"],
            record["CASH AVAILABLE OR EQUITY PERCENT SIGN"],
            record["CASH AVAILABLE OR EQUITY PERCENT"],
            record["MARGIN/CASH DEFICIENCY SIGN"],
            record["MARGIN/CASH DEFICIENCY"],
            record["TYPE 5 BALANCE IN TYPE 2 A/C SIGN"],
            record["TYPE 5 BALANCE IN TYPE 2 A/C"],
            record["FILLER 7"],
            record["MARGIN INTEREST ACCUMULATION SIGN"],
            record["MARGIN INTEREST ACCUMULATION"],
            record["HOUSE EXCESS SIGN"],
            record["HOUSE EXCESS"],
            record["ISIP INDEX COST SIGN"],
            record["ISIP INDEX COST"],
            record["FILLER 8"],
            record["LANGUAGE"],
            record["FILLER 9"],
            record["ACCOUNT RANGE INDICATOR"],
            record["TELEPHONE NUMBER"],
            record["ISIP NET P/S SIGN"],
            record["ISIP NET P/S"],
            record["ACCOUNT CHECK DIGIT"],
        ])
                
    elif rowType == "Client Totals Header":
    
        # Client Totals Header
        record["_ROW_DATA"] = record["value"]

    elif rowType == "Client Totals Trailer":
    
        # Client Totals Trailer
        record["_ROW_DATA"] = record["value"]

    elif rowType == "Date Record":
    
        # Date Record
        record["_ROW_DATA"] = record["value"]

    else:
        # Bad structure of the record
        record["_ROW_DATA"] = record["value"]
    
    return record

#---------------------------------------------------------------------------------------------------------------------
# Apply the output row structure to the DynamicFrame
#---------------------------------------------------------------------------------------------------------------------

mapped_target_dyf = Map.apply(frame = transformed_dyf, f = CreateOutputRowStructure)

mapOutputErrCount = mapped_target_dyf.stageErrorsCount()
if mapOutputErrCount > 0:
    mapped_target_err_df = mapped_target_dyf.errorsAsDynamicFrame().repartition(1).toDF()
    mapped_target_err_df.write.mode("append").json(errorPath)
    
    RenameDataLakeFile(bucketURI, errorPath, errorPath, mapOutputErrFileName)
    #raise Exception(f"Errors [{outputMappingErrCount}] during output record mapping.")

print(mapOutputErrCount)
#error_frame = target_dyf.errorsAsDynamicFrame()
#error_frame.show()

#---------------------------------------------------------------------------------------------------------------------
# Output the target file
#---------------------------------------------------------------------------------------------------------------------

# Merge the partitions of the DataFrame
merged_target_df = mapped_target_dyf.select_fields(["_ROW_ID", "_ROW_TYPE", "_ROW_DATA"]).repartition(1).toDF()

output_target_df = merged_target_df.select(["_ROW_TYPE", "_ROW_DATA"]).orderBy(merged_target_df["_ROW_ID"].cast(IntegerType()))
output_target_df.show()

output_target_df.select("_ROW_DATA").write.mode("append").option("lineSep","\r\n").text(targetPath)

RenameDataLakeFile(bucketURI, targetPath, outputPath, targetFileName)

#---------------------------------------------------------------------------------------------------------------------
# Process and output the rejected file
#---------------------------------------------------------------------------------------------------------------------

# Merge the partitions of the DataFrame
merged_rejected_df = rejected_dyf.select_fields(["_ROW_ID", "_ROW_TYPE", "_ROW_STATUS", "_ROW_EXCEPTIONS", "value"]).repartition(1).toDF()

# Transform the exceptions column
rejected_df = merged_rejected_df.select([
                "_ROW_ID", 
                "_ROW_TYPE",
                "_ROW_STATUS", 
                (concat_ws(";", merged_rejected_df["_ROW_EXCEPTIONS"])).alias("_ROW_EXCEPTIONS"), 
                merged_rejected_df["value"].alias("_ROW_DATA")
              ]).orderBy(merged_rejected_df["_ROW_ID"].cast(IntegerType()))

#rejected_df.show()

# Output the rejected file
output_rejected_df = rejected_df.select(["_ROW_ID", "_ROW_TYPE", "_ROW_STATUS", "_ROW_DATA"])
output_rejected_df.write.option("header","true").option("lineSep","\r\n").option("sep","|").mode("append").csv(rejectedPath)

RenameDataLakeFile(bucketURI, rejectedPath, outputPath, rejectedFileName)

#---------------------------------------------------------------------------------------------------------------------
# Process and output the exceptions file
#---------------------------------------------------------------------------------------------------------------------

# Extract exceptions
exceptions_raw_df   = merged_rejected_df.select(["_ROW_ID", "_ROW_TYPE", explode("_ROW_EXCEPTIONS").alias("_ROW_EXCEPTION")])
exceptions_split_df = exceptions_raw_df.withColumn("_ROW_EXCEPTION_DATA", split(exceptions_raw_df["_ROW_EXCEPTION"], "#"))

# Extract exceptions details
exceptions_df = exceptions_split_df.select(
        "_ROW_ID", 
        "_ROW_TYPE",
        (exceptions_split_df["_ROW_EXCEPTION_DATA"][0]).alias("_EXCEPTION_TYPE"),
        (exceptions_split_df["_ROW_EXCEPTION_DATA"][1]).alias("_EXCEPTION_SOURCE"),
        (exceptions_split_df["_ROW_EXCEPTION_DATA"][2]).alias("_EXCEPTION_DESC")
    ).repartition(1).orderBy(exceptions_split_df["_ROW_ID"].cast(IntegerType()))

#exceptions_df.show()

# Merge the partitions of the DataFrame and output the exceptions file
exceptions_df.write.option("header","true").option("lineSep","\r\n").option("sep","|").mode("append").csv(exceptionsPath)

RenameDataLakeFile(bucketURI, exceptionsPath, outputPath, exceptionsFileName)

#---------------------------------------------------------------------------------------------------------------------
# Collect Audit data for source rows
#---------------------------------------------------------------------------------------------------------------------

# Source rows results per file
source_rows_df = source_df.groupBy().count()

source_audit_df = source_rows_df.select([
        (lit("ROWS_IN")).alias("_REPORT_SECTION"), 
        (lit("FILE")).alias("_REPORT_LEVEL"), 
        (lit("<file>")).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (source_rows_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

source_audit_df.show()
#---------------------------------------------------------------------------------------------------------------------
# Collect Audit data for transformed rows
#---------------------------------------------------------------------------------------------------------------------

# Merge the partitions of the DataFrame
transformed_df = transformed_dyf.toDF()

# Reading file specific metadata file
file_metadata_df = sparkSession.read.option("header", True).option("sep", '|').csv(metadataPath + fileMetaFileName)

# Transformed rows results per row type
transformed_type_rows_df = transformed_df.select("_ROW_TYPE").groupBy("_ROW_TYPE").count()

# Joining with file specific metadata
transformed_meta_type_df = file_metadata_df.join(transformed_type_rows_df, file_metadata_df["ROW_TYPE"] == transformed_type_rows_df["_ROW_TYPE"], "inner")

transformed_meta_type_df.show()

transformed_audit_type_df = \
    transformed_meta_type_df.select([
        (lit("ROWS_TRANSFORMED")).alias("_REPORT_SECTION"), 
        (lit("ROW_TYPE")).alias("_REPORT_LEVEL"), 
        (transformed_meta_type_df["ROW_TYPE"]).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (transformed_meta_type_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

transformed_audit_type_df.show()
# Transformed rows results per file
transformed_rows_df = transformed_type_rows_df.groupBy().sum("count").withColumnRenamed("sum(count)","count")

transformed_audit_df = transformed_rows_df.select([
        (lit("ROWS_TRANSFORMED")).alias("_REPORT_SECTION"), 
        (lit("FILE")).alias("_REPORT_LEVEL"),
        (lit("<file>")).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (transformed_rows_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

transformed_audit_df.show()
#---------------------------------------------------------------------------------------------------------------------
# Collect Audit data for output rows
#---------------------------------------------------------------------------------------------------------------------

# Output rows results per row type
output_type_rows_df = output_target_df.select("_ROW_TYPE").groupBy("_ROW_TYPE").count()

# Joining with file specific metadata
output_meta_type_df = file_metadata_df.join(output_type_rows_df, file_metadata_df["ROW_TYPE"] == output_type_rows_df["_ROW_TYPE"], "inner")

output_meta_type_df.show()
output_audit_type_df = \
    output_meta_type_df.select([
        (lit("ROW_TYPE")).alias("_REPORT_LEVEL"), 
        (output_meta_type_df["ROW_TYPE"]).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (output_meta_type_df["count"]).alias("ROWS_OUT"),
        (output_meta_type_df["count"] * output_meta_type_df["FIELDS_IN"]).alias("FIELDS_IN"), 
        (output_meta_type_df["count"] * output_meta_type_df["FIELDS_OUT"]).alias("FIELDS_OUT"), 
        (output_meta_type_df["count"] * output_meta_type_df["TRANSFORMS"]).alias("TRANSFORMS")
    ]) \
    .unpivot(
        ["_REPORT_LEVEL", "_ROW_TYPE", "_REPORT_LINE_LABEL"], 
        ["ROWS_OUT", "FIELDS_IN", "FIELDS_OUT", "TRANSFORMS"], 
        "_REPORT_SECTION", 
        "_REPORT_RESULT_VALUE"
    ) \
    .select(["_REPORT_SECTION", "_REPORT_LEVEL", "_ROW_TYPE", "_REPORT_LINE_LABEL", "_REPORT_RESULT_VALUE"])

output_audit_type_df.show()

# Output rows results per file
output_type_df = output_type_rows_df.groupBy().sum("count").withColumnRenamed("sum(count)","count")

output_audit_df = output_type_df.select([
        (lit("ROWS_OUT")).alias("_REPORT_SECTION"), 
        (lit("FILE")).alias("_REPORT_LEVEL"), 
        (lit("<file>")).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (output_type_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

output_audit_df.show()
#---------------------------------------------------------------------------------------------------------------------
# Collect Audit data for rejected rows
#---------------------------------------------------------------------------------------------------------------------

# Rejected rows results per row type
rejected_rows_type_df = rejected_df.select("_ROW_TYPE").groupBy("_ROW_TYPE").count()
#rejected_rows_type_df.show()

rejected_audit_type_df = rejected_rows_type_df.select([
        (lit("ROWS_REJECTED")).alias("_REPORT_SECTION"), 
        (lit("ROW_TYPE")).alias("_REPORT_LEVEL"), 
        (rejected_rows_type_df["_ROW_TYPE"]).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (rejected_rows_type_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

rejected_audit_type_df.show()

# Rejected rows results per file
rejected_rows_df = rejected_rows_type_df.groupBy().sum("count").withColumnRenamed("sum(count)","count")

rejected_audit_df = rejected_rows_df.select([
        (lit("ROWS_REJECTED")).alias("_REPORT_SECTION"), 
        (lit("FILE")).alias("_REPORT_LEVEL"), 
        (lit("<file>")).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"), 
        (rejected_rows_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

rejected_audit_df.show()
#---------------------------------------------------------------------------------------------------------------------
# Collect Audit data for exceptions
#---------------------------------------------------------------------------------------------------------------------

# Reading exceptions metadata file
ex_metadataFilePath = metadataPath + exceptionsMetaFileName
ex_metadata_df = sparkSession.read.option("header", True).option("sep", '|').csv(ex_metadataFilePath)

# Joining with exceptions metadata
exceptions_full_df = ex_metadata_df.join(exceptions_df, ex_metadata_df["EXCEPTION_TYPE"] == exceptions_df["_EXCEPTION_TYPE"], "inner")

# Exeption rows results per row type and exception category
exception_rows_type_df = exceptions_full_df.select(["_ROW_TYPE", "EXCEPTION_CATEGORY"]).groupBy(["_ROW_TYPE", "EXCEPTION_CATEGORY"]).count()

exception_rows_type_df.show()

exception_audit_type_df = exception_rows_type_df.select([
        (exception_rows_type_df["EXCEPTION_CATEGORY"]).alias("_REPORT_SECTION"), 
        (lit("ROW_TYPE")).alias("_REPORT_LEVEL"), 
        (exception_rows_type_df["_ROW_TYPE"]).alias("_ROW_TYPE"), 
        (lit(None)).alias("_REPORT_LINE_LABEL"),
        (exception_rows_type_df["count"]).alias("_REPORT_RESULT_VALUE")
    ])

exception_audit_type_df.show()
#---------------------------------------------------------------------------------------------------------------------
# Collect all Audit data
#---------------------------------------------------------------------------------------------------------------------

# Audit information
all_audit_df = source_audit_df \
    .union(output_audit_df) \
    .union(rejected_audit_df) \
    .union(transformed_audit_df) \
    .union(transformed_audit_type_df) \
    .union(output_audit_type_df) \
    .union(rejected_audit_type_df) \
    .union(exception_audit_type_df)

all_audit_df.show()

#all_audit_df.select(["_REPORT_SECTION", "_REPORT_LEVEL", "_ROW_TYPE", "_REPORT_RESULT_VALUE"]).write.option("header","true").option("lineSep","\r\n").option("sep","\t").option("quote", "").mode("append").csv(auditPath)

#RenameDataLakeFile(bucketURI, auditPath, outputPath, "ALL." + auditFileName)

# Reading audit metadata file
audit_metadata_df = sparkSession.read.option("header", True).option("sep", '|').csv(metadataPath + auditMetaFileName)
audit_metadata_df.show()

file_audit_metadata_df = file_metadata_df.withColumn("_ROW_LEVEL", lit("ROW_TYPE"))
file_audit_metadata_df.show()

audit_metadata_join_df = audit_metadata_df.join(
        file_audit_metadata_df, 
        audit_metadata_df["REPORT_LEVEL"] == file_audit_metadata_df["_ROW_LEVEL"],
        "left"
    )

audit_metadata_join_df.show()

audit_metadata_type_df = audit_metadata_join_df.select([
        "REPORT_LINE_NO",
        "REPORT_SECTION_NO",
        "REPORT_SECTION",
        "REPORT_LEVEL",
        (coalesce(audit_metadata_join_df["ROW_TYPE"], lit("<file>"))).alias("ROW_TYPE"),
        "REPORT_LINE_LABEL", 
        "REPORT_RESULT_LABEL",
        (coalesce(audit_metadata_join_df["ROW_TYPE_NO"], lit(0))).alias("ROW_TYPE_NO")
    ])

audit_metadata_type_df.show()

audit_merged_df = audit_metadata_type_df.join(all_audit_df, 
                                              (audit_metadata_type_df["REPORT_SECTION"] == all_audit_df["_REPORT_SECTION"]) & 
                                              (audit_metadata_type_df["REPORT_LEVEL"]   == all_audit_df["_REPORT_LEVEL"]) & 
                                              (audit_metadata_type_df["ROW_TYPE"]       == all_audit_df["_ROW_TYPE"]), 
                                              "left")

audit_df = audit_merged_df.select([
        (coalesce(audit_merged_df["REPORT_LINE_NO"], lit(0))).alias("REPORT_LINE_NO"),
        "REPORT_SECTION_NO",
        "REPORT_SECTION",
        "REPORT_LEVEL",
        (lit(sourceFileName)).alias("FILE_NAME"),
        "ROW_TYPE_NO",
        "ROW_TYPE",
        "REPORT_LINE_LABEL",
        "REPORT_RESULT_LABEL",
        (coalesce(audit_merged_df["_REPORT_RESULT_VALUE"], lit(0))).alias("REPORT_RESULT_VALUE"),
    ]).repartition(1)

audit_df.show()

audit_report_df = audit_df \
    .orderBy([audit_merged_df["ROW_TYPE_NO"].cast(IntegerType()), audit_merged_df["REPORT_SECTION_NO"].cast(IntegerType()), audit_merged_df["REPORT_LINE_NO"].cast(IntegerType())]) \
    .select([(monotonically_increasing_id()).alias("REPORT_LINE_NO"), "FILE_NAME", "ROW_TYPE", "REPORT_LINE_LABEL", "REPORT_RESULT_LABEL", "REPORT_RESULT_VALUE"])

#audit_report_df.show()
#---------------------------------------------------------------------------------------------------------------------
# Output the audit file
#---------------------------------------------------------------------------------------------------------------------

# Merge the partitions of the DataFrame
audit_report_pivot_df = audit_df.groupBy(["FILE_NAME", "ROW_TYPE_NO", "ROW_TYPE"]).pivot("REPORT_RESULT_LABEL").sum("REPORT_RESULT_VALUE") \
    .repartition(1).orderBy([audit_df["ROW_TYPE_NO"].cast(IntegerType())])

audit_report_pivot_df.show()

audit_report_tab_df = audit_report_pivot_df \
    .select([
        (audit_report_pivot_df["ROW_TYPE_NO"].cast(IntegerType()) + 1).alias("REPORT_LINE_NO"), 
        "FILE_NAME",
        "ROW_TYPE",
        "Rows In",
        "Rows Out",
        "Rows Rejected",
        "Rows Transformed",
        "Rows Not Transformed",
        "Rows Not Output",
        "Fields In",
        "Fields Out",
        "Transformations",
        "Invalid structure",
        "Invalid transformation",
        "Invalid data",
        "Missing data"
    ])

audit_report_tab_df.write.option("header","true").option("lineSep","\r\n").option("sep","|").option("quote", "").mode("append").csv(auditPath)

RenameDataLakeFile(bucketURI, auditPath, outputPath, "TAB." + auditFileName)
#---------------------------------------------------------------------------------------------------------------------
# End of Job
#---------------------------------------------------------------------------------------------------------------------
job.commit()
