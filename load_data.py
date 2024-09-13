import pandas as pd
import pymongo
import os
import sys

conn = open('connection.txt', 'r').read()
client = pymongo.MongoClient(conn)
db = client.graph
if len(sys.argv) != 2:
    print("Usage: python3 load_data.py <FOLDER_NAME>")
    sys.exit(1)

folder = sys.argv[1]

if not os.path.exists(folder):
    print("Error: folder doesn't exist")
    sys.exit(1)

if not all(elem in os.listdir(folder) for elem in ['asset.csv', 'company.csv', 'site.csv', 'employee.csv']):
    print("Target folder is missing CSVs")
    sys.exit(1)

coll = db[folder]

coll.drop()

coll.create_index([('member_of', 1)])
coll.create_index([('id', 1)])

assets = pd.read_csv(os.path.join(folder, 'asset.csv'))
companies = pd.read_csv(os.path.join(folder, 'company.csv'))
sites = pd.read_csv(os.path.join(folder, 'site.csv'))
employees = pd.read_csv(os.path.join(folder, 'employee.csv'))

assets['type'] = 'Asset'
assets = assets.rename(columns={'Asset ID': 'id', 'Parent Site ID': 'member_of'})

companies['type'] = 'Company'
companies = companies.rename(columns={'Company ID': 'id'})
#  companies['member_of'] = companies['_id']

sites['type'] = 'Site'
sites = sites.rename(columns={'Site ID': 'id', 'Parent Company ID': 'member_of'})

employees['type'] = 'Employee'
employees = employees.rename(columns={'Employee ID': 'id', 'Member of Company ID': 'member_of'})

coll.insert_many(assets.to_dict('records'))
coll.insert_many(companies.to_dict('records'))
coll.insert_many(sites.to_dict('records'))
coll.insert_many(employees.to_dict('records'))
