#!/usr/bin/env python





"------------------------------------------------------------------------------"
#############
## Imports ##
#############

## Standard library imports

from datetime import date


## Third party imports

from sodapy import Socrata


## Local application imports





"------------------------------------------------------------------------------"
################
## Parameters ##
################

token = "5HfsId12lhMMzSlYANoAq451w"





"------------------------------------------------------------------------------"
###############
## Functions ##
###############


##
def get_client(token):
	return Socrata("data.cityofchicago.org", token)



##
def ingesta_inicial(client, limit=300000):
	return client.get("4ijn-s7e5", limit=limit)



##
def ingesta_consecutiva(client, limit=1000):
	today = date.today()
	today = today.strftime('%Y-%m-%d')
	return client.get("4ijn-s7e5", limit=limit)



##
def get_s3_resource():
	pass



##
def guardar_ingesta(name):
	pass





"------------------------------------------------------------------------------"
#################
## END OF FILE ##
#################
"------------------------------------------------------------------------------"
