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
		#self.cluster = Cluster(['127.0.0.1'])
		#self.dbtable = 'kpi_tmo_15' 
		#self.dl = ','
		self.conf = XMLParser.ReadConfigIni(self.config_ini)
		#self.session = self.cluster.connect('tts_eric')

	@staticmethod
	def myquote(str):
		#return "'%s'" % str
		return str

	def ParseFilename(self,filepath):
		info = dict()
		[mydir,myname]=os.path.split(filepath)
		coll=myname.split(',')
		pos=coll[0].find('.')
		str1 = coll[0][pos+1:]
		d = coll[0][0:pos]
		year=d[1:5]
		mon=d[5:7]
		day=d[7:9]
		str2 = str1.split('_')
		a = str2[0].split('-')
		ts = year + '-' + mon + '-' +  day + ' ' + a[0][0:2] + ':' + a[0][2:4]
		hlts = year + '-' + mon + '-' +  day + ' ' + a[0][0:2] + ':00'
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
	def GetCell(strMoid):
		strCell="";
		pos=strMoid.find('EUtranCellFDD')
		if pos != -1:
			pos2=strMoid.find(',',pos+1)
			if (pos2 == -1):
				pos2=strMoid.find(' ',pos+1)
			if ( pos2 != -1 ):	
				strCell=strMoid[pos+14:pos2]
			else:
				strCell=strMoid[pos+14:]
		return strCell

	def CollectMeasureResult(self,mv,vidx,mtmap):
		cellid="";
		for moid in mv.iter('moid'):
			cellid=XMLParser.GetCell(moid.text)
			break
		if cellid == "":
			return;
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
				#print(i,j,mtmap[i],'=',elm.text)
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

	def GetResult(self):	
        	commonStr = self.GetCommonStr()
		rcoll = self.rcoll
		dl = XMLParser.delim
		ret = ""
		for cellid in rcoll:
			myarr = []
			ret += commonStr + dl + XMLParser.myquote(cellid) + dl
			for tabname, v in sorted(self.conf['Table'].items(), key=operator.itemgetter(0)):
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
		commonStr = XMLParser.myquote(finfo['hlts'])+ dl + XMLParser.myquote(conf['General']['HL_MARKET']) + dl + '1' + dl + XMLParser.myquote(conf['General']['Region']) + dl + XMLParser.myquote(conf['General']['Market']) + dl + XMLParser.myquote(finfo['SubNetwork_2']) + dl + XMLParser.myquote(finfo['ts']) + dl + '15' + dl + XMLParser.myquote(finfo['MeContext']) 
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
			for row in coll[0].split("\n"):
				if len(row) > 1:
					rpt_rslt += row+'\n'
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
		for md in root.iter('md'):
			mtmap = dict()
			vidx = []
			idx = 0
			for mt in md.iter('mt'):
				mtmap.update({idx : mt.text})
				vidx.append(idx)
				idx += 1
			if len(mtmap) > 0:
				for mv in md.iter('mv'):
					self.CollectMeasureResult(mv,vidx,mtmap)
		if self.type_out == 'file':
			return self.GetResult()+'@'
		else:
			"""
			self.InsertToCassandra()
			"""
			return
