#!/usr/local/bin/python3.4

# ------------------------------------------------
#Copyright 2014 AT&T Intellectual Property
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# -------------------------------------------

# Implementation of GSHUB REST service
# for announcement and discovery of gs instances, sources and sinks

from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading
import getopt
import sys
import re
import cgi
import socket
import json

# lis of URLS for all the REST calls we will serve
DISCOVER_INSTANCE_URL = '/v1/discover-instance'
DISCOVER_INITINSTANCE_URL = '/v1/discover-initialized-instance'
DISCOVER_SOURCE_URL =  '/v1/discover-source'
DISCOVER_SINK_URL =  '/v1/discover-sink'
DISCOVER_STARTPROCESSING_URL = '/v1/discover-start-processing'
ANNOUNCE_INSTANCE_URL =  '/v1/announce-instance'
ANNOUNCE_INITINSTANCE_URL =  '/v1/announce-initialized-instance'
ANNOUNCE_SOURCE_URL =  '/v1/announce-source'
ANNOUNCE_SINK_URL =  '/v1/announce-sink'
ANNOUNCE_STARTPROCESSING_URL = '/v1/announce-start-processing'
ANNOUNCE_STREAM_SUBSCRIPTION = '/v1/announce-stream-subscription'
ANNOUNCE_FTA_INSTANCE = '/v1/announce-fta-instance'
ANNOUNCE_METRICS = '/v1/log-metrics'

# gs instance endpoints
gs_instances = {}

# initialized gs instances
gs_init_instances = {}

# instances for which processing started
gs_startprocessing_instances = {}

# source endpoints
sources = {}

# sink endpoints
sinks = {}


# exctract endpoint information from json data
def extract_endpoint(data) :
	name = ''
	ip = ''
	port = 0
		
	try :
		doc = json.loads(str(data, 'utf-8'))	
	except :
		print ('Invalid json message ' + str(data, 'utf-8'))
		return []
	
	for key in doc.keys() :
		if key == 'name' :
			name = doc[key]
		elif key == 'ip' :
			ip = doc[key]
			# validate ip address		
			try :
				socket.inet_pton(socket.AF_INET, ip)
			except :
				print ('Invalid IPV4 address ' + ip)
				ip = ''
		elif key == 'port' :
			# validate port number
			try :
				port = int(doc[key])
			except :
				print ('Invalid port number ' + doc[key])
				port = 0
	
	if name == '' or ip == '' or port == 0 :
		print ('Name, ip or port is missing from json message ' + str(doc))
		return []
	

	return [name, ip, port]


# extract instance name from json data
def extract_instance_name(data) :
	name = ''
	
	try :
		doc = json.loads(str(data, 'utf-8'))	
	except :
		print ('Invalid json message ' + str(data, "utf-8"))
		return ''
	
	for key in doc.keys() :
		if key == 'name' :
			name = doc[key]
	
	if name == '' :
		print ('Name field is missing in json message ' + str(doc))
	elif (name in gs_instances) == False:
		print ('Attempt to announce the initialization or start of processing for unknown instance ' + name)
		name = ''
		
	return name
	
# handler for HTTP requests. We will override do_PORT and do_GET of BaseHTTPRequestHandler
class HTTPRequestHandler(BaseHTTPRequestHandler):

	def do_POST(self):
		if re.search(ANNOUNCE_INSTANCE_URL, self.path) != None:		
			if self.headers.get_content_type() == 'application/json' :
				# Find content length
				content_len = 0
				for i in range(len(self.headers.keys())):
					if self.headers.keys()[i] == 'Content-Length' :
						content_len = int (self.headers.values()[i])
						break
				if content_len != 0 :
					# extract endpoint information
					endpoint = extract_endpoint(self.rfile.read(content_len))
					if endpoint == [] :
						self.send_response(400)
					else :
						self.send_response(200)
						gs_instances[endpoint[0]] = [endpoint[1], endpoint[2]]
				else :
					self.send_response(400)

			else:
				self.send_response(400)
			self.end_headers()		
			
		elif re.search(ANNOUNCE_INITINSTANCE_URL, self.path) != None:
			if self.headers.get_content_type() == 'application/json' :
				# Find content length
				content_len = 0
				for i in range(len(self.headers.keys())):
					if self.headers.keys()[i] == 'Content-Length' :
						content_len = int (self.headers.values()[i])
						break
				if content_len != 0 :
					# extract name of initialized gs instance				
					name = extract_instance_name(self.rfile.read(content_len))
					if name == '' :
						self.send_response(400)
					else :
						self.send_response(200)
						gs_init_instances[name] = 1
				else :
					self.send_response(400)

			else:
				self.send_response(400)
			self.end_headers()	
			
		elif re.search(ANNOUNCE_SOURCE_URL, self.path) != None:
			if self.headers.get_content_type() == 'application/json' :
				# Find content length
				content_len = 0
				for i in range(len(self.headers.keys())):
					if self.headers.keys()[i] == 'Content-Length' :
						content_len = int (self.headers.values()[i])
						break
				if content_len != 0 :
					# extract endpoint information				
					endpoint = extract_endpoint(self.rfile.read(content_len))
					if endpoint == [] :
						self.send_response(400)
					else :
						self.send_response(200)
						sources[endpoint[0]] = [endpoint[1], endpoint[2]]
				else :
					self.send_response(400)

			else:
				self.send_response(400)
			self.end_headers()
			
		elif re.search(ANNOUNCE_SINK_URL, self.path) != None:
			if self.headers.get_content_type() == 'application/json' :
				# Find content length
				content_len = 0
				for i in range(len(self.headers.keys())):
					if self.headers.keys()[i] == 'Content-Length' :
						content_len = int (self.headers.values()[i])
						break
				if content_len != 0 :
					# extract endpoint information				
					endpoint = extract_endpoint(self.rfile.read(content_len))
					if endpoint == [] :
						self.send_response(400)
					else :
						self.send_response(200)
						sinks[endpoint[0]] = [endpoint[1], endpoint[2]]
				else :
					self.send_response(400)

			else:
				self.send_response(400)
			self.end_headers()	
			
		elif re.search(ANNOUNCE_STARTPROCESSING_URL, self.path) != None:
			if self.headers.get_content_type() == 'application/json' :
				# Find content length
				content_len = 0
				for i in range(len(self.headers.keys())):
					if self.headers.keys()[i] == 'Content-Length' :
						content_len = int (self.headers.values()[i])
						break
				if content_len != 0 :
					# extract name of initialized gs instance				
					name = extract_instance_name(self.rfile.read(content_len))
					if name == '' :
						self.send_response(400)
					else :
						self.send_response(200)
						gs_startprocessing_instances[name] = 1
				else :
					self.send_response(400)

			else:
				self.send_response(400)
			self.end_headers()
			
		# we do not do any processing for ANNOUNCE_STREAM_SUBSCRIPTION, ANNOUNCE_FTA_INSTANCE and ANNOUNCE_METRICS in gshub simulator		
		elif (re.search(ANNOUNCE_STREAM_SUBSCRIPTION, self.path) != None) or (re.search(ANNOUNCE_FTA_INSTANCE, self.path) != None) or (re.search(ANNOUNCE_METRICS, self.path) != None):
			if self.headers.get_content_type() == 'application/json' :
				# Find content length
				content_len = 0
				for i in range(len(self.headers.keys())):
					if self.headers.keys()[i] == 'Content-Length' :
						content_len = int (self.headers.values()[i])
						break
				if content_len != 0 :				
					self.send_response(200)
				else :
					self.send_response(400)

			else:
				self.send_response(400)
			self.end_headers()	
			
		else:
			self.send_response(404)
			self.end_headers()
		return

	def do_GET(self):
		if re.search(DISCOVER_INSTANCE_URL + '/*', self.path) != None:
			instance = self.path.split('/')[-1]
			# check if this instance is registered
			if instance in gs_instances :
				self.send_response(200)
				self.send_header('Content-Type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes("{\"ip\" : \"" + gs_instances[instance][0] + "\", \"port\": " + str(gs_instances[instance][1]) + "}", "utf-8"))
			else:
				self.send_response(400)
				self.end_headers()


		elif re.search(DISCOVER_INITINSTANCE_URL + '/*', self.path) != None:
			instance = self.path.split('/')[-1]
			# check if this instance is initialized
			if instance in gs_init_instances :
				self.send_response(200)
				self.send_header('Content-Type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes("{\"ip\" : \"" + gs_instances[instance][0] + "\", \"port\": " + str(gs_instances[instance][1]) + "}", "utf-8"))
			else:
				self.send_response(400)
				self.end_headers()

		elif re.search(DISCOVER_SOURCE_URL + '/*', self.path) != None:
			source = self.path.split('/')[-1]
			# check if it is a registered source
			if source in sources :
				self.send_response(200)
				self.send_header('Content-Type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes("{\"ip\" : \"" + sources[source][0] + "\", \"port\": " + str(sources[source][1]) + "}", "utf-8"))
			else:
				self.send_response(400)
				self.end_headers()
				
		elif re.search(DISCOVER_SINK_URL + '/*', self.path) != None:
			sink = self.path.split('/')[-1]
			# check if it is a registered sink
			if sink in sinks :
				self.send_response(200)
				self.send_header('Content-Type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes("{\"ip\" : \"" + sinks[sink][0] + "\", \"port\": " + str(sinks[sink][1]) + "}", "utf-8"))
			else:
				self.send_response(400)
				self.end_headers()
				
		elif re.search(DISCOVER_STARTPROCESSING_URL + '/*', self.path) != None:
			instance = self.path.split('/')[-1]
			# check if this instance is initialized
			if instance in gs_startprocessing_instances :
				self.send_response(200)
				self.send_header('Content-Type', 'application/json')
				self.end_headers()
				self.wfile.write(bytes("{}", "utf-8"))
			else:
				self.send_response(400)
				self.end_headers()				
		else:
			self.send_response(404)
			self.end_headers()
		return


# we will use standard python threaded HTTP server
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
	allow_reuse_address = True

	def shutdown(self):
		self.socket.close()
		HTTPServer.shutdown(self)

class SimpleHttpServer:
	def __init__(self, ip, port):
		self.server = ThreadedHTTPServer((ip,port), HTTPRequestHandler)

	def start(self):
		self.server_thread = threading.Thread(target=self.server.serve_forever)
		self.server_thread.daemon = True
		self.server_thread.start()
		
	def waitForThread(self):
		self.server_thread.join()

	def stop(self):
		self.server.shutdown()
		self.waitForThread()


# print usage instructions
def usage():
	print ('./gshub.py [-p port]')
	
	

def main():
	# process command-line arguments
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hp:v", ["help", "port="])
	except getopt.GetoptError as err:
		# print help information and exit:
		print(str(err))	
		usage()
		sys.exit(2)

	port = 0
	for o, a in opts:
		if o in ("-h", "--help"):
			usage()
			sys.exit(0)
		elif o in ("-p", "--port"):
			port = int(a)
		else:
			print ('Unknown command-line option ' + o)

	# start HTTP server to serve REST calls
	server = SimpleHttpServer('127.0.0.1', port)

	# record HTTP server address in gshub.log
	f = open('gshub.log', 'w')
	f.write('127.0.0.1:' + str(server.server.server_port) + '\n')
	f.close()
		
	print ('GSHUB Running on port ' + str(server.server.server_port) + ' ...')
	server.start()
	server.waitForThread()


if __name__ == "__main__":
	main()

