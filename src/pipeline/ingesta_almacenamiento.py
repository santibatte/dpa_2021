#!/usr/bin/env python

from datetime import date
from sodapy import Socrata

token = "5HfsId12lhMMzSlYANoAq451w"

def get_client(token):
	return Socrata("data.cityofchicago.org", token)


def ingesta_inicial(client, limit=300000):
	return client.get("4ijn-s7e5", limit=limit)


def ingesta_consecutiva(client, limit=1000):
	today = date.today()
	today = today.strftime('%Y-%m-%d')
	return client.get("4ijn-s7e5", limit=limit)


def get_s3_resource():
	pass

def guardar_ingesta(name):
	pass