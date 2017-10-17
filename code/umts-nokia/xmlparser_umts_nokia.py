import xml.etree.cElementTree as ET
import ConfigParser
import sys
import os
import gzip
import json
import operator
from ConfigParser import NoOptionError
from pickle import FALSE

class nokiaXmlParser():
    inputXmlText = ""
    inputIniFile = ""
    inputFileName = ""
    smeasurementType = ""
    sLogError = list()
    neighbor = dict()

      
    def __init__(self, inXmlString, inIniFile,  sFileName):
        self.inputXmlText = inXmlString
        self.inputIniFile = inIniFile
        self.inputFileName = sFileName
        self.smeasurementType = ""
        self.sLogError = list()
        self.neighbor = dict()
        

    def WriteElementsNothingUse(self, parr,vcoll,tabname):
        
        strFullPathOutput = tabname+'__ElementNAMES__NothingUse.txt'
        
        if not os.path.isfile(strFullPathOutput):
            f = open(strFullPathOutput,'w')
            f.write(tabname+'\n')
            for p in vcoll:
                f.write(p+'\n')               
            f.close()
        else:
            if os.stat(strFullPathOutput).st_size == 0:
                f = open(strFullPathOutput,'w')
                f.write(tabname+'\n')
                for p in vcoll:
                    f.write(p+'\n')                
                f.close()
    


    
    def WriteElementsNotUse_NotFound2(self, parr,vcoll,tabname ):        
        sItem = ''    
        
        strFullPathOutput = tabname+'__ElementsNotFound.txt'
        if os.path.isfile(strFullPathOutput):
            return                            
        for p in parr:        
            if not p in vcoll:
                if len(sItem) > 0: 
                    sItem = sItem + ',' +p
                else:
                    sItem = p       
        
        if len(sItem) > 0:
            f = open(strFullPathOutput,'w')
            f.write(sItem+'\n')
            f.close()
        sItem = ''
        
        strFullPathOutput = tabname+'__ElementsNotUsed.txt'
        if os.path.isfile(strFullPathOutput):
            return   
        for p in vcoll:        
            if not p in parr: 
                if len(sItem) > 0: 
                    sItem = sItem + ',' +p
                else:
                    sItem = p        
                
        if len(sItem) > 0:
            f = open(strFullPathOutput,'w')
            f.write(sItem+'\n')
            f.close()
        
    
        
    def ElementsNotUse_NotFound(self, parr, vcoll ):
        
        sJustFileName = self.inputFileName
        if (sJustFileName.rfind('/') >= 0):
            sJustFileName = sJustFileName[sJustFileName.rfind('/')+1:]
        if (sJustFileName.rfind('\\') >= 0):
            sJustFileName = sJustFileName[sJustFileName.rfind('\\')+1:]
        
        sItem = ''        
        for p in parr:        
            if not p in vcoll:
                if len(sItem) > 0: 
                    sItem = sItem + ',' +p
                else:
                    sItem = p       
        
        if len(sItem) > 0:            
            self.sLogError.append(sJustFileName+': ElementsNotInXML='+sItem)           
    




    @staticmethod
    def ReadConfigIni(inputIniFile):
        sLogError = list()
        conf=ConfigParser.ConfigParser()
        conf.read(inputIniFile)
        ret=dict()
        for sect in conf.sections():
            if sect == 'Table':
                ret.update({'Table':dict()})
                for item,val in conf.items(sect):
                    tabdef = val.split(',')
                    coll = []
                    for pair in tabdef:
                        coll.append(pair.split(':'))
                    ret['Table'].update({item:coll})
            if sect == 'General':
                ret.update({'General':dict()})
                try:
                    mkt = conf.get(sect,'Market','')
                except NoOptionError:                                     
                    sLogError.append('config.ini=Ini file Market option error.')
                    mkt = ''
                    
                try:
                    region = conf.get(sect,'Region', '')
                except NoOptionError:
                    sLogError.append('config.ini=Ini file Region option error.')
                    region = '' 
                       
                try:            
                    header = conf.get(sect,'CommonHeader').split(',')
                except NoOptionError:
                    sLogError.append('config.ini=Ini file CommonHeader option error.')
                    header = ''           
                
                ret['General'].update({'Market':mkt})
                ret['General'].update({'Region':region})            
                ret['General'].update({'CommonHeader':header})
               
        return ret, sLogError

    '''
    def WritePairArrayToFile(self, parr,vcoll, cellItem, smeasurementType):        
                 
        updateThisValue=''
        nItems = 0
        arr = list()
        for p in parr:
            bexist = True
            if not p in vcoll:
                bexist = False        
            if bexist:
                arr.append(vcoll[p])
            else:
                arr.append("")
                            
        return arr
    '''


    def WritePairArrayToFile(self, parr,vcoll, cellItem, smeasurementType):        
        
        strItemSum =''        
        arr = list()
        pair_list = list()
        for p in parr:
            bexist = True
            if not p in vcoll:
                bexist = False        
            if bexist:                                                                                                         
                arr.append(vcoll[p])                
            else:
                arr.append("")                                       

        '''
        if (smeasurementType == 'rcpm_olpc_wcel'):                            
            parsingElements['TR_CLASS'] = sTR_CLASS
            parsingElements['TR_SUBCLASS'] = sTR_SUBCLASS
            parsingElements['CHTYPE'] = sChType
        M1024C4:UL_CRC_OKS_W,M1024C5:UL_CRC_NOKS_W
        UL_CRC_NOKS_W_CS
        UL_CRC_OKS_W_CS
        UL_CRC_NOKS_W_PS
        UL_CRC_OKS_W_PS
        UL_CRC_NOKS_W_HSUPA
        UL_CRC_OKS_W_HSUPA                
        if (smeasurementType == 'rcpm_rlc_wcel'):                            
            parsingElements['TR_CLASS'] = sTR_CLASS
            parsingElements['TR_SUBCLASS'] = sTR_SUBCLASS
            parsingElements['CHTYPE'] = sChType
        M1026C31:RLC_AM_SDU_DL_PS_VOL_W
        M1026C29:RLC_AM_DL_MEAS_TIME_W
        M1026C30:RLC_AM_SDU_UL_PS_VOL_W
        M1026C33:RLC_AM_UL_MEAS_TIME_W
        PS_NRT_data or PS_RT_data    DCH    RLC_AM_SDU_DL_PS_VOL_W
        PS_NRT_data or PS_RT_data    <>DCH    RLC_AM_SDU_DL_PS_VOL_W
        PS_NRT_data or PS_RT_data    DCH    RLC_AM_DL_MEAS_TIME_W
        PS_NRT_data or PS_RT_data    <>DCH    RLC_AM_DL_MEAS_TIME_W
        PS_NRT_data or PS_RT_data    <>HSUPA    RLC_AM_SDU_UL_PS_VOL_W
                                     HSUPA    RLC_AM_SDU_UL_PS_VOL_W
        PS_NRT_data or PS_RT_data    <>HSUPA    RLC_AM_UL_MEAS_TIME_W
                                     HSUPA    RLC_AM_UL_MEAS_TIME_W

        '''
                   
        if (smeasurementType == 'rcpm_rlc_wcel'):            
            if self.neighbor.has_key(cellItem):                        
                pair_list = self.neighbor[cellItem].split('|')                        
            else:
                strItemSum ='0|0|0|0|0|0|0|0'
                pair_list = strItemSum.split('|')
                
            strItemSum =''
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and  vcoll['CHTYPE'] == 'DCH'):
                strItemSum = str( int(pair_list[0]) + int(vcoll['m1026c31']) )+'|'               
                
            else:
                strItemSum = pair_list[0] + '|'               
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and  vcoll['CHTYPE'] != 'DCH'):
                strItemSum = strItemSum + str( int(pair_list[1]) + int(vcoll['m1026c31']) )+'|'                
            else:
                strItemSum = strItemSum+ pair_list[1] + '|'
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and  vcoll['CHTYPE'] == 'DCH'):                                
                strItemSum = strItemSum + str( int(pair_list[2]) + int(vcoll['m1026c29']) )+'|'
            else:                
                strItemSum = strItemSum + pair_list[2] + '|'
                
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and  vcoll['CHTYPE'] != 'DCH'):
                strItemSum = strItemSum + str( int(pair_list[3]) + int(vcoll['m1026c29']) )+'|'                
            else:
                strItemSum = strItemSum+ pair_list[3] + '|'
            
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and  vcoll['CHTYPE'] != 'HSUPA'):
                strItemSum = strItemSum + str( int(pair_list[4]) + int(vcoll['m1026c30']) )+'|'                
            else:
                strItemSum = strItemSum+ pair_list[4] + '|'
            if ( vcoll['CHTYPE'] == 'HSUPA'):
                strItemSum = strItemSum + str( int(pair_list[5]) + int(vcoll['m1026c30']) )+'|'                
            else:
                strItemSum = strItemSum+ pair_list[5] + '|'
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and  vcoll['CHTYPE'] != 'HSUPA'):
                strItemSum = strItemSum + str( int(pair_list[6]) + int(vcoll['m1026c33']) )+'|'                
            else:
                strItemSum = strItemSum+ pair_list[6] + '|'
            if ( vcoll['CHTYPE'] == 'HSUPA'):
                strItemSum = strItemSum + str( int(pair_list[7]) + int(vcoll['m1026c33']) )                
            else:
                strItemSum = strItemSum+ pair_list[7]                                 

            self.neighbor[cellItem] = strItemSum

        if (smeasurementType == 'rcpm_olpc_wcel'):            
            if self.neighbor.has_key(cellItem):                        
                pair_list = self.neighbor[cellItem].split('|')                        
            else:
                strItemSum ='0|0|0|0|0|0'
                pair_list = strItemSum.split('|')
                
            strItemSum =''
            if ( vcoll['TR_SUBCLASS'] == 'CS_voice'):
                strItemSum = str( int(pair_list[0]) + int(vcoll['m1024c5']) )+'|'
                strItemSum = strItemSum + str(int(pair_list[1]) + int(vcoll['m1024c4']) )+'|'
            else:
                strItemSum = pair_list[0] + '|'+ pair_list[1] + '|'
            
            if ( vcoll['TR_SUBCLASS'] == 'PS_NRT_data' and ( vcoll['TR_CLASS'] == 'background' or vcoll['TR_CLASS'] == 'interactive' )):               
                strItemSum = strItemSum + str(int(pair_list[2]) + int(vcoll['m1024c5']) )+'|'
                strItemSum = strItemSum + str(int(pair_list[3]) + int(vcoll['m1024c4']) )+'|'
            else:
                strItemSum = strItemSum + pair_list[2] + '|' + pair_list[3] + '|'
            
            
            if ( (vcoll['TR_SUBCLASS'] == 'PS_NRT_data' or vcoll['TR_SUBCLASS'] == 'PS_RT_data') and ( vcoll['CHTYPE'] == 'HSUPA' )):
                strItemSum = strItemSum + str( int(pair_list[4]) + int(vcoll['m1024c5']) )+'|'
                strItemSum = strItemSum + str( int(pair_list[5]) + int(vcoll['m1024c4']) )
            else:
                strItemSum = strItemSum + pair_list[4] + '|' + pair_list[5]                        

            self.neighbor[cellItem] = strItemSum
                    
        return arr
    
    @staticmethod
    def GetHeaderIniSUM(inputIniFile,  sTableName):
        
        sLogError = list()
        sResultHeader = ''
        conf, sLogError = nokiaXmlParser.ReadConfigIni(inputIniFile)
        if len(conf.keys()) == 0:
            sLogError.append('config.ini=Cannot read Config.ini file.')
            return sResultHeader       
        commonHeader = [col for col in conf['General']['CommonHeader']]
        if (sTableName == 'rcpm_olpc_wcel' or sTableName == 'rcpm_rlc_wcel'):
            sTableName = sTableName + 'sum'
        for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
            if (sTableName == tabname):                         
                arr = []                              
                arr = arr + commonHeader
                for p in colls:
                    try:                            
                        arr.append(p[1])               
                    except IndexError:
                        print ''
                
                sResultHeader= '|'.join( arr)
                break
        return sResultHeader, sLogError
    
        

    @staticmethod
    def GetHeaderIni(inputIniFile,  sTableName):
        
        sLogError = list()
        sResultHeader = ''
        conf, sLogError = nokiaXmlParser.ReadConfigIni(inputIniFile)
        if len(conf.keys()) == 0:
            sLogError.append('config.ini=Cannot read Config.ini file.')
            return sResultHeader       
        commonHeader = [col for col in conf['General']['CommonHeader']]
        
        for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
            if (sTableName == tabname):                         
                arr = []                              
                arr = arr + commonHeader
                for p in colls:
                    try:                            
                        arr.append(p[1])               
                    except IndexError:
                        print ''
                
                sResultHeader= '|'.join( arr)
                break
        return sResultHeader, sLogError

        
    def ParseXmlString(self):                 
        inputXmlString= self.inputXmlText
        inputIniFile=self.inputIniFile
        sFileName =self.inputFileName
        listOfList =[]
        listNB = list()  
        ResultData = list()    
        arrElements = []
        parsingElements = dict()
        
        PMTarget = False
        bTabnameFound = False 
        conf, self.sLogError = nokiaXmlParser.ReadConfigIni(inputIniFile)
        if len(conf.keys()) == 0:                      
            self.sLogError.append('config.ini=Cannot read Config.ini file.')
            exit()
        commonHeader = [col for col in conf['General']['CommonHeader']]
       
        strFileOSSItem = ''
        sJustFileName = sFileName
        if (sJustFileName.rfind('/') >= 0):
            sJustFileName = sJustFileName[sJustFileName.rfind('/')+1:]
        if (sJustFileName.rfind('\\') >= 0):
            sJustFileName = sJustFileName[sJustFileName.rfind('\\')+1:]
        if (len(sJustFileName) == 0):
            sJustFileName = sFileName
            
        strFileOSSItem = sJustFileName.split('_')[3]
        strFileOSSItem = strFileOSSItem.split('.')[0]

        commonStr = conf['General']['Region'] + '|' + conf['General']['Market'] + '|'+strFileOSSItem+ '|'

        tree = ET.ElementTree(ET.fromstring(inputXmlString))
            
        sDateTimeInterval = ''
        sDateTime = ''
        smeasurementType = ''
        nPMMOResult = 0               
        nMO=0
        cellid =''
        cellid2 = ''
        
        mCCMnc =''
        sTR_CLASS =''
        sTR_SUBCLASS =''
        sChType =''
        bTabnameFound = False

        for node in tree.iter():            
            strItem = node.tag
            if (strItem.find('}') >= 0):                
                    strItem = strItem.split('}', 1)[1]
            PMTarget = False
            #print strItem, node.text, "attrib: ", node.attrib
            if (strItem.find('PMSetup') >= 0):
                sDateTimeInterval = node.attrib.get('startTime')
                sDateTimeInterval = sDateTimeInterval[:16]
                sDateTime = sDateTimeInterval
                sDateTime = sDateTime.replace('-', '').replace('T', '_').replace(':', '')
                sDateTimeInterval = sDateTimeInterval.replace('T', ' ')+ "|"+ node.attrib.get('interval')
                commonStr = commonStr+sDateTimeInterval.replace('T', ' ')                
                PMTarget = False
            
            elif (strItem.find('PMMOResult') >= 0):
                if (nPMMOResult > 0 and bTabnameFound == True):
                    for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
                        if (smeasurementType == tabname):
                            #cellid2 = mCCMnc+'-'+ cellid.split ('/')[2].split('-')[1]+'-'+cellid.split ('/')[3].split('-')[1]              
                            strItem = commonStr +  '|' #+ cellid + '|'+cellid2 +'|'                            
                            ResultData.append(strItem+'|'.join( self.WritePairArrayToFile(arrElements,parsingElements, parsingElements['localMoid'] + '|' +parsingElements['cell_id_1'] + '|' +parsingElements['cell_id_2'], smeasurementType) ))
                    
                nMO=0
                cellid =''
                cellid2 = ''
                
                parsingElements = dict()
                nPMMOResult += 1
                PMTarget = False                
                
            elif (strItem.find('_Nokia') >= 0):                
                PMTarget = True                
                smeasurementType= node.attrib.get('measurementType') 
                smeasurementType = smeasurementType.lower()
                if (smeasurementType == 'rcpm_olpc_wcel' or smeasurementType == 'rcpm_rlc_wcel'):                            
                    parsingElements['TR_CLASS'] = sTR_CLASS
                    parsingElements['TR_SUBCLASS'] = sTR_SUBCLASS
                    parsingElements['CHTYPE'] = sChType
                    sTR_CLASS =''
                    sTR_SUBCLASS =''
                    sChType =''
                            
                    
                if (bTabnameFound == False):             
                    for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
                        if (smeasurementType == tabname):                         
                            arr = [] 
                            arr = arr + commonHeader
                            for p in colls:
                                try:                            
                                    arr.append(p[1])
                                    arrElements.append(p[0])
                                except IndexError:
                                    print ''
                            
                            #ResultData.append('|'.join( arr))                                  
                            bTabnameFound = True
                            break
                            
        
                if (bTabnameFound == False and nPMMOResult > 10):                                        
                    break
                
            elif (strItem.find('DN') >= 0):                
                mCCMnc = node.text
                if (node.text.find('TR_CLASS') >= 0):
                    sTR_CLASS = node.text.split('/')[1]
                    sTR_CLASS = sTR_CLASS[sTR_CLASS.find('-')+1:]
                    sTR_SUBCLASS = node.text.split('/')[2]
                    sTR_SUBCLASS = sTR_SUBCLASS[sTR_SUBCLASS.find('-')+1:]
                    
                if (node.text.find('ChType-') >= 0):
                    sChType = node.text.split('/')[1]
                    sChType = sChType[sChType.find('-')+1:]
                
            if (strItem == 'localMoid'):
                parsingElements[strItem] = node.text[node.text.find(':')+1:]
                try:
                    rnc = parsingElements['baseId'][parsingElements['baseId'].rfind('-')+1:]
                    bts_cel = parsingElements['localMoid'].split('/')
                except Exception as e:
                    parsingElements['cell_id_1'] =''
                try:                    
                    parsingElements['cell_id_2'] = rnc +"-"+bts_cel[0][bts_cel[0].rfind('-')+1:]+"-"+bts_cel[1][bts_cel[1].rfind('-')+1:]                
                except Exception as e:
                    parsingElements['cell_id_2'] = parsingElements['localMoid']
                try:                    
                    parsingElements['cell_id_1'] = "PLMN-PLMN/" +parsingElements['baseId'][parsingElements['baseId'].find('-')+1:]+'/'+parsingElements['localMoid'][parsingElements['localMoid'].find('-')+1:]
                except Exception as e:
                    parsingElements['cell_id_1'] =''
            else:
                if strItem[0] == 'M':
                    parsingElements[strItem.lower()] = node.text
                else:
                    parsingElements[strItem] = node.text                                
                  
        if (nPMMOResult > 0 and bTabnameFound):
            for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
                if (smeasurementType == tabname):                
                    strItem = commonStr +  '|' + cellid + '|'
                    #cellid2 = mCCMnc+'-'+ cellid.split ('/')[2].split('-')[1]+'-'+cellid.split ('/')[3].split('-')[1]              
                    strItem = commonStr +  '|' #+ cellid + '|'+cellid2 +'|'
                                            
                    ResultData.append(strItem+'|'.join( self.WritePairArrayToFile(arrElements,parsingElements,parsingElements['localMoid']+  '|' +parsingElements['cell_id_1']+  '|' +parsingElements['cell_id_2'],smeasurementType) ))

            #self.WriteElementsNotUse_NotFound2(arrElements,parsingElements.keys(),smeasurementType)
            self.ElementsNotUse_NotFound(arrElements,parsingElements.keys())


        if bTabnameFound == False:
            self.WriteElementsNothingUse(arrElements,parsingElements,smeasurementType)                    
        if (smeasurementType == 'rcpm_olpc_wcel' or smeasurementType == 'rcpm_rlc_wcel'):
            strItem=''
            for key, value in self.neighbor.items():                
                strItem = commonStr +  '|' +parsingElements['baseId'] + '|' + key+'|'+value                
                listNB.append(strItem)

        listOfList.append(ResultData)
        listOfList.append(listNB)        
        
        return listOfList, smeasurementType

    def Main(self):
        
        self.smeasurementType=''      
        
        dataArray, self.smeasurementType = self.ParseXmlString( )
                                        
        return dataArray, self.smeasurementType, self.sLogError
