#!/usr/bin/python
import xml.etree.cElementTree as ET
import ConfigParser
import sys
import os
import gzip
import zlib
import json
import operator
import copy
import uuid
from datetime import datetime,timedelta
#from cassandra.cluster import Cluster

class XMLParser():
	#cluster = Cluster(['127.0.0.1']) 
	delim = ','
	"""
	paramaters
	fn: input filename 
	input_format: 
		'file': for gzip file input
		'bytes': for unzip sequence file  
		'string': for unzip string
		'gzipstream': for gzip squence file
	output_format: 'file' or 'db' (cassandra)
	str_in: this is for 'string' input format o/w it is empty by default
	"""
	def __init__(self,fn,input_format,output_format,str_in='',config_ini='config.ini'):
		self.fn = fn
		if input_format == 'file':
			self.filename = fn
		elif input_format == 'string':
			self.str_in = str_in
		elif input_format == 'bytes':
			self.str_in = str(str_in)
		elif input_format == 'gstream':
			self.str_in = zlib.decompress(str(str_in), zlib.MAX_WBITS|32)
		else:
			self.str_in = ""
		self.type_in = input_format
		self.type_out = output_format
		self.config_ini = config_ini
		self.ParseFilename(fn)
		self.rcoll = dict()
		self.err = dict()
		self.tabfile = dict()
		#self.cluster = Cluster(['127.0.0.1'])
		#self.dbtable = 'kpi_tmo_15' 
		#self.dl = ','
		self.conf = XMLParser.ReadConfigIni(self.config_ini)
		if self.conf['General']['Technology'] == 'UMTS':
			self.cell_keyword = 'UtranCell'
		else:
			self.cell_keyword = 'EUtranCellFDD' 
		#self.session = self.cluster.connect('tts_eric')

	@staticmethod
	def myquote(str):
		#return "'%s'" % str
		return str
	
	def TimeZoneAdjust(self,mytime,timezone,sign):       
		mydatetime=datetime.strptime(mytime, "%Y-%m-%d %H:%M")
		if sign == '-':
			newtime= mydatetime - timedelta(hours=timezone)
		else:
			newtime = mydatetime + timedelta(hours=timezone)
		return newtime.strftime('%Y-%m-%d %H:%M')

	def ParseFilename(self,filepath):
		#OSS1_-0400_A20170807.1200-0400-1215-0400_SubNetwork=ONRM_ROOT_MO,SubNetwork=Manhattan,MeContext=LBK04025A_statsfile.xml
		info = dict()
		[mydir,myname]=os.path.split(filepath)
		coll=myname.split(',')
		pos=coll[0].find('.')
		str1 = coll[0][pos+1:]
		str0 = coll[0][0:pos]
		
		pos1 = str0.find('_')
		if pos1 > 0:
			ossname=str0[0:pos1]
		else:
			ossname='Unknown'
		info.update({'ossname':ossname})
		
		sign=str0[pos1+1:pos1+2]	
		timezone=int(str0[pos1+2:pos1+4])
		
		d = str0[-8:]
		year=d[0:4]
		mon=d[4:6]
		day=d[6:8]
		
		str2 = str1.split('_')
		#example: 1200-0400-1215-0400 or 1200+0400-1215+0400
		timestr=str2[0][0:4]
		zonestr=str2[0][5:9]
		
		ts = year + '-' + mon + '-' +  day + ' ' + timestr[0:2] + ':' + timestr[2:4]
		hlts = year + '-' + mon + '-' +  day + ' ' + timestr[0:2] + ':00'
		
		'''
		if zonestr == '0000': #UTC time, need to apply time zone to get local time
			hlts = self.TimeZoneAdjust(hlts,timezone,sign)
			ts = self.TimeZoneAdjust(ts,timezone,sign)
		'''
		#first apply zone string
		timezone0=int(zonestr[0:2])
		sign0=str2[0][4:5]
		if sign0 == '-':
			sign0='+'
		else:
			sign0='-'
		hlts = self.TimeZoneAdjust(hlts,timezone0,sign0)
		ts = self.TimeZoneAdjust(ts,timezone0,sign0)
		
		#now apply timezone
		hlts = self.TimeZoneAdjust(hlts,timezone,sign)
		ts = self.TimeZoneAdjust(ts,timezone,sign)
		
		info.update({'ts':ts})
		info.update({'hlts':hlts})
		info.update({'SubNetwork_2':coll[1][11:]})
		a2 = coll[2][10:].split('_')
		info.update({'MeContext':a2[0]})
		self.finfo = info

	@staticmethod
	def ReadConfigIni(ini_config):
		conf=ConfigParser.ConfigParser()
		conf.read(ini_config)
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
				mkt = conf.get(sect,'Market')
				region = conf.get(sect,'Region')
				hlmkt = conf.get(sect,'HL_MARKET')
				header = conf.get(sect,'CommonHeader').split(',')
				ret['General'].update({'Market':mkt})
				ret['General'].update({'HL_MARKET':hlmkt})
				ret['General'].update({'Region':region})
				ret['General'].update({'CommonHeader':header})
				if conf.has_option(sect,'LTECellHeader'):
					lteheader = conf.get(sect,'LTECellHeader').split(',')
					ret['General'].update({'LTECellHeader':lteheader})
				if conf.has_option(sect,'UMTSCellHeader'):
					umtsheader = conf.get(sect,'UMTSCellHeader').split(',')
					ret['General'].update({'UMTSCellHeader':umtsheader})
				if conf.has_option(sect,'GSMCellHeader'):
					gsmheader = conf.get(sect,'GSMCellHeader').split(',')
					ret['General'].update({'GSMCellHeader':gsmheader})
				if conf.has_option(sect,'Technology'):
					tech = conf.get(sect,'Technology')
				else: #default to LTE
					tech = 'LTE'
				ret['General'].update({'Technology':tech})
					
		return ret
	
	@staticmethod
	def GetHeader(config_ini):
		#commonHeader = [col for col in conf['General']['CommonHeader']]
		conf=XMLParser.ReadConfigIni(config_ini)
		myheader = list()
		myheader.extend(conf['General']['CommonHeader'])
		for tabname,colls in sorted(conf['Table'].items(), key=operator.itemgetter(0)):
			arr = []
			for p in colls:
				[kind,dim]=XMLParser.ReadCounterType(p[1])
				if ( dim > 1 ):
					for i in range(0,dim):
						arr.append(p[0]+'_'+str(i))
				else:
					arr.append(p[0])
			myheader.extend(arr)
		return XMLParser.delim.join(myheader)
	
	@staticmethod
	def GetRelationHeader():
		ret = dict()
		
		conf=XMLParser.ReadConfigIni('config.ini')
		commonheader = conf['General']['CommonHeader']
		lteheader = copy.deepcopy(commonheader)
		umtsheader = copy.deepcopy(commonheader)
		gsmheader = copy.deepcopy(commonheader)
		for tabname,colls in conf['Table'].items():
			arr = []
			for p in colls:
				[kind,dim]=XMLParser.ReadCounterType(p[1])
				if ( dim > 1 ):
					for i in range(0,dim):
						arr.append(p[0]+'_'+str(i))
				else:
					arr.append(p[0])
			if tabname == 'ltecell':
			
				lteheader.extend(conf['General']['LTECellHeader'])
				lteheader.extend(arr)
			elif tabname == 'umtscell':
				umtsheader.extend(conf['General']['UMTSCellHeader'])
				umtsheader.extend(arr)
			elif tabname == 'gsmcell':
				gsmheader.extend(conf['General']['GSMCellHeader'])
				gsmheader.extend(arr)
				
		ret.update({'ltecell':XMLParser.delim.join(lteheader)})
		ret.update({'umtscell':XMLParser.delim.join(umtsheader)})
		ret.update({'gsmcell':XMLParser.delim.join(gsmheader)})
		return ret
	
	@staticmethod
	def ReadCounterType(type):
		dim = 0
		kind = 0
		if ( type == 'Single' ):
			dim = 1
			kind = 1
		elif ( type.find('PDF') != -1 ):
			kind = 2
			p1 = type.find('[')
			p2 = type.find(']')
			dim = int(type[p1+1:p2])
		elif ( type.find('COMP') != -1 ):
			kind = 3
			p1 = type.find('[')
			p2 = type.find(']')
			dim = int(type[p1+1:p2])
		elif ( type.find('SPECIAL') != -1):
			kind = 4
			dim = 10
		return [kind,dim]

	"""
	For Multiple files
	"""
	@staticmethod
	def ReadInput(input):
		fcoll = list()
		if os.path.isdir(input):
			for fn in os.listdir(input):
				if fn.endswith('.gz'):
					fcoll.append(input+"/"+fn)
		elif os.path.exists(input):
			if input.endswith('.gz'):
				fcoll.append(input)
		return fcoll

	@staticmethod
	def GetStrValue(strin,key,offset):
		pos=strin.find(',',offset)
		nlen=len(key)+1 #plus equal sign
		strVal=""
		if (pos == -1):
			pos=strin.find(' ',pos+1)
		if ( pos != -1 ):
			strVal=strin[offset+nlen:pos]
		else:
			strVal=strin[offset+nlen:]
		return strVal

	@staticmethod
	def GetCellRelation(strMoid,offset,strType):
		strFreqRel=""
		strCellRel=""
		found=False
		if strType == 'ltecell':
			pos=strMoid.find('EUtranFreqRelation',offset)
			if pos != -1:
				strFreqRel=XMLParser.GetStrValue(strMoid,'EUtranFreqRelation',pos)
				pos=strMoid.find('EUtranCellRelation',pos)
				if pos != -1:
					strCellRel=XMLParser.GetStrValue(strMoid,'EUtranCellRelation',pos)
					found = True
		elif strType == 'umtscell':
			pos=strMoid.find('UtranFreqRelation',offset)
			if pos != -1:
				strFreqRel=XMLParser.GetStrValue(strMoid,'UtranFreqRelation',pos)
				pos=strMoid.find('UtranCellRelation',pos)
				if pos != -1:
					strCellRel=XMLParser.GetStrValue(strMoid,'UtranCellRelation',pos)
					found = True
		elif strType == 'gsmcell':
			pos=strMoid.find('GeranFreqGroupRelation',offset)
			if pos != -1:
				strFreqRel=XMLParser.GetStrValue(strMoid,'GeranFreqGroupRelation',pos)
				pos=strMoid.find('GeranCellRelation',pos)
				if pos != -1:
					strCellRel=XMLParser.GetStrValue(strMoid,'GeranCellRelation',pos)
					found = True
		#print(strType,strFreqRel,strCellRel)
		if found:
			return [strFreqRel,strCellRel,strType]
		else:
			return []
		
	#@staticmethod
	def GetCell(self,strMoid):
		ret = []
		strCell="";
		pos=strMoid.find(self.cell_keyword)
		#pos=strMoid.find('UtranCell')
		if pos != -1:
			strCell=XMLParser.GetStrValue(strMoid,self.cell_keyword,pos)
			#strCell=XMLParser.GetStrValue(strMoid,'UtranCell',pos)
		else:
			return strCell
		#cell relation
		ret.append(strCell)
		aRel=[]	
		sType=""
		pos2=strMoid.find('EUtranFreqRelation',pos)
		if pos2 != -1: #LTECell
			aRel=XMLParser.GetCellRelation(strMoid,pos,'ltecell')
		else:
			pos2 = strMoid.find('UtranFreqRelation',pos)
			if pos2 != -1: #UMTS Cell
				aRel=XMLParser.GetCellRelation(strMoid,pos,'umtscell')
			else:
				pos2 = strMoid.find('GeranFreqGroupRelation',pos)
				if pos2 != -1: #GSM Cell
					aRel=XMLParser.GetCellRelation(strMoid,pos,'gsmcell')
		if len(aRel) > 0:
			ret.extend(aRel)
		return ':'.join(ret)

	def CollectMeasureResult(self,mv,vidx,mtmap):
		#aret=[];
		for moid in mv.iter('moid'):
			#cellid=XMLParser.GetCell(moid.text)
			cellid=self.GetCell(moid.text)
			break
		if cellid == "":
			return;
		#print(aret)	
		#cellid=':'.join(aret)
		if not cellid in self.rcoll: 
			self.rcoll.update({cellid:dict()})
		i = 0
		j = 0
		for elm in mv.iter('r'):
			if i == vidx[j]: 
				if elm.text is None:
					self.rcoll[cellid][mtmap[i]] = "0"
				else:
					self.rcoll[cellid][mtmap[i]] = elm.text
				#print(cellid,mtmap[i],'=',elm.text)
				j += 1
				if  j >= len(vidx):
					break
				i += 1	

	def ErrMsg(self,ie,msg):
		if ie not in self.err:
			self.err.update({ie:msg})
		
	def WritePairArrayToArray(self,parr,vcoll):
		arr = list()
		for p in parr:
			[kind,dim] = XMLParser.ReadCounterType(p[1])
			bexist = True
			if p[0] in vcoll:
				val = vcoll[p[0]].split(',')
			else:
				val = ["0"]*dim
				msg = "IE:{0} not found from xml input; set 0 instead".format(p[0])
				self.ErrMsg(p[0],msg)
			if ( kind == 1 ): #Single valuse
				arr.extend(val)
			elif ( kind == 2 ): # fixed array
				n = len(val)
				if n > dim:
					val = val[0:dim]
					msg = "IE:{0} has more elemments than configured".format(p[0])
					self.ErrMsg(p[0],msg)
				elif n < dim:	
					vadd = ["0"]*(dim-n)
					val.extend(vadd)
					msg = "IE:{0} has less elemments than configured".format(p[0])
					self.ErrMsg(p[0],msg)
				for v in val:
					if v == "":
						v = "0"
					arr.append(v)
			elif ( kind == 3 ):
				arr_tmp = ["0"]*dim
				if ( val[0] != "0" ):
					total=int(val[0])
					for i in range(total):
						idx = int(val[2*i+1])
						if idx <= (dim - 1):
							arr_tmp[idx]=val[2*i+2]
				arr.extend(arr_tmp)
			elif ( kind == 4 ):
				arr_tmp = ["0"]*dim
				for i in range(dim):
					arr_tmp[i]=val[i]
				arr.extend(arr_tmp)
		return arr
	
	def GetRelationResult(self):
		commonStr = self.GetCommonStr()
		rcoll = self.rcoll
		dl = XMLParser.delim
		ret = ""
		reta = dict()
		agg = dict()
		for cellid in rcoll:
			myid = cellid.split(':')
			if len(myid) != 4:
				continue
			ret = commonStr + dl + myid[0] + dl + myid[1] + dl + myid[2]
			for tabname, v in self.conf['Table'].items():
				myarr = [];
				#myid = cellid.split(':')
				mykey = tabname + '-' + myid[0]
				if myid[3] == tabname:
					myarr=self.WritePairArrayToArray(v,rcoll[cellid])
				if len(myarr) < 1:
					continue
				#print(cellid,tabname,myarr)
				myval=agg.get(mykey,[])
				myval.append(map(int,myarr))
				agg.update({mykey:myval})	
				if tabname not in reta:
					reta.update({tabname:list()})
				#reta.update({mykey:map(int,myarr)})
				reta[tabname].append(ret+ dl + dl.join(myarr))
				
		#for k,v in agg.items():
		#	print(k,map(sum,zip(*v)))
		return reta
		
	def GetResult(self):	
		commonStr = self.GetCommonStr()
		rcoll = self.rcoll
		dl = XMLParser.delim
		ret = ""
		for cellid in rcoll:
			myid = cellid.split(':')
			if len(myid) != 1:
				continue
			myarr = []
			ret += commonStr + dl + XMLParser.myquote(cellid) + dl
			for tabname, v in sorted(self.conf['Table'].items(), key=operator.itemgetter(0)):
				if tabname != 'site':
					continue
				#print(tabname,cellid)
				myarr.extend(self.WritePairArrayToArray(v,rcoll[cellid]))
			ret += dl.join(myarr)
			ret += '\n'
			#ret.insert(dl.join(myarr))
		if len(self.err) != 0:
			ret += '|' + self.GetErrMsg()
		return ret
	"""
	def InsertToCassandra(self):
		commonStr = self.GetHeader4Cassandra()
		dl = self.dl
		for cellid in self.rcoll:
			myarr = []
			str = commonStr + dl + XMLParser.myquote(cellid) + dl
			for tabname, v in sorted(self.conf['Table'].items(), key=operator.itemgetter(0)):
				myarr.extend(self.WritePairArrayToArray(v,rcoll[cellid]))
        		str += dl.join(myarr)
			#print('INSERT INTO ' + dbtable + ' (' + tableColumns + ') VALUES ('+valueString+');');
			self.session.execute('INSERT INTO ' + self.dbtable + ' (' + myheader + ') VALUES ('+str+');');
			print(commonStr,cellid)
	"""

	def GetCommonStr(self):
		dl = XMLParser.delim
		finfo = self.finfo
		conf = self.conf
		commonStr = XMLParser.myquote(finfo['hlts'])+ dl + XMLParser.myquote(conf['General']['HL_MARKET']) + dl + '1' + dl + XMLParser.myquote(conf['General']['Region']) + dl + XMLParser.myquote(conf['General']['Market'])+ dl + XMLParser.myquote(finfo['SubNetwork_2']) + dl + XMLParser.myquote(finfo['ts']) + dl + '15' + dl + XMLParser.myquote(finfo['MeContext'])+ dl +XMLParser.myquote(finfo['ossname'])
		return commonStr

	def GetErrMsg(self):
		retmsg = ""
		for k,v in self.err.iteritems():
			retmsg += self.fn + "#" + v + "\n"
		return retmsg
	
	@staticmethod
	def GetReport(inputstr,mycache):
		rpt_err = ""
		rpt_rslt = ""
		for record in inputstr.split("@"):
			coll = record.split("|")
			if len(coll) > 1:
				for errstr in coll[1].split("\n"):
					if len(errstr) > 1:
						tmp = errstr.split("#")
						if tmp[1] not in mycache:
							mycache.append(tmp[1])
							rpt_err += errstr + '\n'
			'''
			for row in coll[0].split("\n"):
				if len(row) > 1:
					rpt_rslt += row+'\n'
			'''
			if "\n" in coll[0]:
				rpt_rslt += coll[0]
		return [rpt_rslt,rpt_err]
		
	def ProcessXML(self):
		if self.type_in == 'file':
			f=gzip.open(self.fn,'rb')
			tree = ET.parse(f)
			root = tree.getroot()
		elif self.type_in == 'bytes' or self.type_in == 'gstream':
			s = self.str_in
			root = ET.fromstring(s)
		elif self.type_in == 'string':
			s = self.str_in
			root = ET.fromstring(s)
		else:
			return
		#root = tree.getroot()
		"""
		counterColl = list()
		for tabname, v in self.conf['Table'].items():
			if tabname != 'site':
				continue
			for row in v:
				counterColl.append(row[0])
		"""		
		for md in root.iter('md'):
			mtmap = dict()
			vidx = []
			idx = 0
			for mt in md.iter('mt'):
				mtmap.update({idx : mt.text})
				vidx.append(idx)
				idx += 1
			"""
			for mt in md.iter('mt'):
				if mt.text in counterColl:
					mtmap.update({idx : mt.text})
					vidx.append(idx)
				idx += 1
			"""
			if len(mtmap) > 0:
				for mv in md.iter('mv'):
					self.CollectMeasureResult(mv,vidx,mtmap)
		if self.type_out == 'file':
			"""
			result = self.GetRelationResult()
			headers = XMLParser.GetRelationHeader()
			for k,v in result.items():
				print(headers[k])
				for row in v:
					print(row)
			"""
			
			return self.GetResult()+'@'
		else:
			"""
			self.InsertToCassandra()
			"""
			return

"""
class LTE(XMLParser):
	def GetCell(strMoid):
		ret = []
		strCell="";
		pos=strMoid.find('EUtranCellFDD')
		if pos != -1:
			strCell=XMLParser.GetStrValue(strMoid,'EUtranCellFDD',pos)
		else:
			return strCell
		#cell relation
		ret.append(strCell)
		aRel=[]	
		sType=""
		pos2=strMoid.find('EUtranFreqRelation',pos)
		if pos2 != -1: #LTECell
			aRel=XMLParser.GetCellRelation(strMoid,pos,'ltecell')
		else:
			pos2 = strMoid.find('UtranFreqRelation',pos)
			if pos2 != -1: #UMTS Cell
				aRel=XMLParser.GetCellRelation(strMoid,pos,'umtscell')
			else:
				pos2 = strMoid.find('GeranFreqGroupRelation',pos)
				if pos2 != -1: #GSM Cell
					aRel=XMLParser.GetCellRelation(strMoid,pos,'gsmcell')
		if len(aRel) > 0:
			ret.extend(aRel)
		return ':'.join(ret)
		
class UMTS(XMLParser):
	def GetCell(self,strMoid):
		ret = []
		strCell="";
		pos=strMoid.find('UtranCell')
		if pos != -1:
			strCell=XMLParser.GetStrValue(strMoid,'UtranCell',pos)
		else:
			return strCell
		#cell relation
		ret.append(strCell)
		aRel=[]	
		sType=""
		pos2=strMoid.find('EUtranFreqRelation',pos)
		if pos2 != -1: #LTECell
			aRel=XMLParser.GetCellRelation(strMoid,pos,'ltecell')
		else:
			pos2 = strMoid.find('UtranFreqRelation',pos)
			if pos2 != -1: #UMTS Cell
				aRel=XMLParser.GetCellRelation(strMoid,pos,'umtscell')
			else:
				pos2 = strMoid.find('GeranFreqGroupRelation',pos)
				if pos2 != -1: #GSM Cell
					aRel=XMLParser.GetCellRelation(strMoid,pos,'gsmcell')
		if len(aRel) > 0:
			ret.extend(aRel)
		return ':'.join(ret)
"""
