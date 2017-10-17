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
    sElementsNothingUse = list()
    neighbor = dict()

      
    def __init__(self, inXmlString, inIniFile,  sFileName):
        try:
            self.inputXmlText = inXmlString
            self.inputIniFile = inIniFile
            self.inputFileName = sFileName
            self.smeasurementType = ""
            self.sLogError = list()
            self.sElementsNothingUse = list()
            self.neighbor = dict()
        except Exception as e:
            print "Exception: %s" % (e)
        

    @staticmethod
    def ReadConfigIni(inputIniFile):
        try:
            sLogError = list()
            conf=ConfigParser.ConfigParser()
            conf.read(inputIniFile)
            ret=dict()
            sDebugNothingUseInElement=''
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
                        sDebugNothingUseInElement = conf.get(sect,'DebugNothingUseInElement','')
                    except NoOptionError:                                     
                        sLogError.append('config.ini=Ini file DebugNothingUseInElement option error.')
                        sDebugNothingUseInElement = ''
                        
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

                    ret['General'].update({'DebugNothingUseInElement':sDebugNothingUseInElement})
                    ret['General'].update({'Market':mkt})
                    ret['General'].update({'Region':region})            
                    ret['General'].update({'CommonHeader':header})
                   
            return ret, sLogError
        except Exception as e:
            print "Exception: %s" % (e)

    def WriteElementsNothingUse(self, parr,vcoll,tabname):
        try:        
            sJustFileName = self.inputFileName
            if (sJustFileName.rfind('/') >= 0):
                sJustFileName = sJustFileName[sJustFileName.rfind('/')+1:]
            if (sJustFileName.rfind('\\') >= 0):
                sJustFileName = sJustFileName[sJustFileName.rfind('\\')+1:]

            sJustFileName = sJustFileName + '=' +tabname
            sItem = ''                       
            for p in vcoll:
                if len(sItem) > 0: 
                    sItem = sItem + ',' +p
                else:
                    sItem = p

            #print 'WriteElementsNothingUse: '+sItem
            if len(sItem) > 0:            
                self.sElementsNothingUse.append(sJustFileName+': ElementsNothingUse='+sItem)
        except Exception as e:
            print "Exception: %s" % (e)
        

    def WritePairArrayToFile(self, parr,vcoll, cellItem, smeasurementType):        
        try:                 
            updateThisValue=''
            nItems = 0
            arr = list()
            for p in parr:
                bexist = True
                if not p in vcoll:
                    bexist = False        
                if bexist:
                    if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):  
                        nItems = nItems + 1                  
                        if self.neighbor.has_key(cellItem):                        
                            pair_list = self.neighbor[cellItem].split('|')
                            for pair in pair_list:
                                x,y = pair.split("=")
                                if (p == x):                                                                        
                                    updateThisValue = self.neighbor[cellItem]
                                    nValItem = int(y) + int(vcoll[p])
                                    if (nValItem > 0):
                                        nValItem =nValItem
                                    updateThisValue = updateThisValue.replace(x+'='+y, x+'='+str(nValItem))
                                    self.neighbor[cellItem] = updateThisValue
                        else:
                            if nItems > 1:
                                updateThisValue = updateThisValue +"|"+ p+'='+vcoll[p] 
                            else:
                                updateThisValue = updateThisValue +p+'='+vcoll[p]
                                                                      

                    arr.append(vcoll[p])
                else:
                    arr.append("")
                    if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):
                        nItems = nItems + 1
                        if nItems > 1:
                            self.neighbor[cellItem] = self.neighbor[cellItem] +"|"+p+"="
                        else:
                            self.neighbor[cellItem] = self.neighbor[cellItem] +p+"="
            
            if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):            
                self.neighbor[cellItem] = updateThisValue                    
            return arr
        except Exception as e:
            print "Exception: %s" % (e)
    

    @staticmethod
    def GetHeaderIni(inputIniFile,  sTableName):
        try:        
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
                    
                    if ('lte_isys_ho_utran_nb' == sTableName or 'lte_isys_ho_gsm_nb' == sTableName):
                        commonHeader.remove('MO_DN')                               
                        arr = arr + commonHeader
                        arr.append( 'MO_DN_Source' )
                        arr.append( 'MO_DN_Target' )            
                    else:
                        arr = arr + commonHeader
                    for p in colls:
                        try:                            
                            arr.append(p[1])               
                        except IndexError:
                            print ''
                    
                    sResultHeader= '|'.join( arr)
                    break
            return sResultHeader, sLogError
        except Exception as e:
            print "Exception: %s" % (e)

    @staticmethod
    def GetHeaderIniNB( inputIniFile,  sTableName):
        try:        
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
                    
                    if ('lte_isys_ho_utran_nb' == sTableName or 'lte_isys_ho_gsm_nb' == sTableName):
                        commonHeader.remove('MO_DN')                               
                        arr = arr + commonHeader
                        arr.append( 'MO_DN_Source' )                
                        for p in colls:
                            try:                            
                                arr.append(p[1])               
                            except IndexError:
                                print ''
                    
                        sResultHeader= '|'.join( arr)
                        break
            return sResultHeader, sLogError
        except Exception as e:
            print "Exception: %s" % (e)
    
  
        
    def ElementsNotUse_NotFound(self, parr, vcoll ):
        try:        
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
        except Exception as e:
            print "Exception: %s" % (e)
        


    def ParseXmlString(self):
        try:
            inputXmlString= self.inputXmlText
            inputIniFile=self.inputIniFile
            sFileName =self.inputFileName
            listOfList =[]
            listNB = list()  
            ResultData = list()    
            arrElements = []
            parsingElements = dict()
            lte_cell_throughput = False
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
                
            strFileOSSItem = sJustFileName.split('_')[0]

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
            bTabnameFound = False
            mneighb_cell_ho_ECI = ''

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
                    lte_cell_throughput = False
                    PMTarget = False
                
                elif (strItem.find('PMMOResult') >= 0):
                    if (nPMMOResult > 0 and bTabnameFound == True):
                        for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
                            if (smeasurementType == tabname):
                                cellid2 = mCCMnc+'-'+ cellid.split ('/')[2].split('-')[1]+'-'+cellid.split ('/')[3].split('-')[1]              
                                strItem = commonStr +  '|' + cellid + '|'+cellid2 +'|'
                                if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):
                                    strItem = commonStr +  '|' + cellid2 + '|'+cellid +'|'
                                    strItem = strItem + mneighb_cell_ho_ECI + '|'
                                
                                ResultData.append(strItem+'|'.join( self.WritePairArrayToFile(arrElements,parsingElements, cellid, smeasurementType) ))
                        
                    nMO=0
                    cellid =''
                    cellid2 = ''
                    
                    parsingElements = dict()
                    nPMMOResult += 1
                    lte_cell_throughput = False
                    PMTarget = False
                elif (strItem.find('PMTarget') >= 0 or strItem.find('NE-LNBTS_') >= 0):                
                    PMTarget = True
                    smeasurementType= node.attrib.get('measurementType') 
                    smeasurementType = smeasurementType.lower()
                    if smeasurementType == 'lte_cell_throughput':
                        lte_cell_throughput = True
                    else:
                        lte_cell_throughput = False
                        
                    if (bTabnameFound == False):             
                        for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
                            if (smeasurementType == tabname):                         
                                arr = []
                                arrElements = []
                                
                                if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):
                                    commonHeader.remove('MO_DN')                               
                                    arr = arr + commonHeader
                                    arr.append( 'MO_DN_Source' )
                                    arr.append( 'MO_DN_Target' )
                                    commonHeader.append('MO_DN')
                                else:
                                    arr = arr + commonHeader
                                for p in colls:
                                    try:                            
                                        arr.append(p[1])
                                        arrElements.append(p[0])
                                    except IndexError:
                                        print ''
                                
                                #ResultData.append('|'.join( arr))                                  
                                bTabnameFound = True
            
                        if (bTabnameFound == False and nPMMOResult > 10):                    
                            break
                    
                elif (len(strItem) == 2 and strItem.find('MO') >= 0):                
                    nMO = nMO + 1
                elif (nMO == 1 and len(strItem) == 2 and strItem.find('DN') >= 0):                
                    cellid = node.text
                elif (nMO == 1 and len(strItem) == 6 and strItem.find('baseId') >= 0):                
                    if node.text.find('-') and len(node.text.split('-')) > 1:
                        cellid ='PLMN-PLMN/'+node.text.split('-')[1]+'-'+node.text.split('-')[2]   
                elif (nMO == 1 and len(strItem) == 9 and strItem.find('localMoid') >= 0):                
                    if node.text.find('-') and len(node.text.split('-')) > 2:
                        cellid =cellid+'/'+node.text.split('-')[1]+'-'+node.text.split('-')[2]+'-'+node.text.split('-')[3]                
                elif (nMO == 2 and len(strItem) == 2 and strItem.find('DN') >= 0):                
                    mCCMnc = node.text
                    mCCMnc = mCCMnc.split('/')[1].split('-')[1]+mCCMnc.split('/')[2].split('-')[1]
                elif (nMO == 3 and len(strItem) == 2 and strItem.find('DN') >= 0):                
                    mneighb_cell_ho_ECI = node.text
                
                if (len(strItem) > 3 and nMO > 0 and lte_cell_throughput == False and PMTarget == False):                
                    parsingElements[strItem] = node.text
                if (len(strItem) > 3 and nMO == 2 and lte_cell_throughput == True and PMTarget == False):
                    parsingElements[strItem] = node.text
                if (len(strItem) > 3 and nMO == 3 and PMTarget == False):   
                    mneighb_cell_ho_ECI = node.text                        
                if (bTabnameFound == False):
                    arrElements.append(strItem)
                
            if (nPMMOResult > 0 and bTabnameFound):
                for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
                    if (smeasurementType == tabname):                
                        strItem = commonStr +  '|' + cellid + '|'
                        cellid2 = mCCMnc+'-'+ cellid.split ('/')[2].split('-')[1]+'-'+cellid.split ('/')[3].split('-')[1]              
                        strItem = commonStr +  '|' + cellid + '|'+cellid2 +'|'
                        if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):
                            strItem = commonStr +  '|' + cellid2 + '|'+cellid +'|'
                            strItem = strItem + mneighb_cell_ho_ECI + '|'
                            
                        ResultData.append(strItem+'|'.join( self.WritePairArrayToFile(arrElements,parsingElements,cellid,smeasurementType) ))

                self.ElementsNotUse_NotFound(arrElements,parsingElements.keys())

            if (bTabnameFound == False and (conf['General']['DebugNothingUseInElement'] == 'true' or conf['General']['DebugNothingUseInElement'] == '1')):                
                self.WriteElementsNothingUse(arrElements,arrElements,smeasurementType)

            if ('lte_isys_ho_utran_nb' == smeasurementType or 'lte_isys_ho_gsm_nb' == smeasurementType):
                strItem=''
                for key, value in self.neighbor.items():
                    cellid2 = mCCMnc+'-'+ key.split ('/')[2].split('-')[1]+'-'+key.split ('/')[3].split('-')[1]
                    strItem = commonStr +  '|' +cellid2 + '|' + key
                    pair_list = value.split('|')
                    for pair in pair_list:
                        x,y = pair.split("=")
                        strItem = strItem +'|'+y
                    listNB.append(strItem)

            listOfList.append(ResultData)
            listOfList.append(listNB)        
            
            return listOfList, smeasurementType
        except Exception as e:
            print "Exception: %s" % (e)

    def Main(self):
        try:
            self.smeasurementType=''
            self.sLogError= list()
            dataArray= list()
            listOfError = list()
            dataArray, self.smeasurementType = self.ParseXmlString( )
            
            listOfError.append(self.sLogError)
            listOfError.append(self.sElementsNothingUse)        
            return dataArray, self.smeasurementType, listOfError
        except Exception as e:
            print "Exception: %s" % (e)
