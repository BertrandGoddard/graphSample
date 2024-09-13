from datetime import datetime, timedelta
import pymongo
import random
import os
import sys

conn = open('connection.txt', 'r').read()
client = pymongo.MongoClient(conn)
db = client.graph
if len(sys.argv) != 3:
    print("Usage: python3 query.py <FOLDER_NAME> listAssets | checkAsset")
    sys.exit(1)

folder = sys.argv[1]

if folder not in db.list_collection_names():
    print("Error: collection doesn't exist")
    sys.exit(1)

if sys.argv[2] not in ['listAssets', 'checkAsset']:
    print("command not found %s" % sys.argv[2])
    sys.exit(1)

coll = db[folder]

employeeCompanyLookup = {
    '$graphLookup': {
        'from': folder,
        'startWith': "$member_of",
        'connectFromField': "member_of",
        'connectToField': "id",
        'as': "company",
        'depthField': "depth"
    }
}

companyUnwind = {
    '$unwind': {
        'path': '$company'
    }
}

companyAssetLookup = {
    '$graphLookup': {
        'from': folder,
        'startWith': "$company.id",
        'connectFromField': "id",
        'connectToField': "member_of",
        'as': "assets",
        'depthField': "depth"
    }
}

assetFilter = {
    '$set': {
        'assets': {
            '$filter': {
                'input': "$assets",
                'as': "asset",
                'cond': {
                    '$eq': ["$$asset.type", "Asset"]
                }
            }
        }
    }
}

assetCompanyLookup = {
    '$graphLookup': {
        'from': folder,
        'startWith': '$id',
        'connectFromField': 'member_of',
        'connectToField': 'id',
        'as': 'company'
    }
}

companyFilter = {
    '$set': {
        'company': {
            '$filter': {
                'input': "$company",
                'as': "company",
                'cond': {
                    '$eq': ["$$company.type", "Company"]
                }
            }
        }
    }
}

setCompany = {
    '$set': {
        'company': {
            '$arrayElemAt': [
                '$company', 0
            ]
        }
    }
}

totalExecutionTime = 0
numExecutions = 10

if sys.argv[2] == 'listAssets':
    for i in range(numExecutions):
        print('%d of %d' % (i+1, numExecutions))

        if folder == 'sample_100k_3_4_withP' or folder =='sample_100k_3_4_withP_flattened':
            eid = random.randint(0, 9)
            employeeMatch = {
                '$match': {
                    'id': 'PID' + str(eid)
                }
            }
        else:
            eid = random.randint(0, 9999)
            employeeMatch = {
                '$match': {
                    'id': 'EID' + str(eid) + '_0'
                }
            }

        pipeline = [
            employeeMatch,
            employeeCompanyLookup,
            companyUnwind,
            companyAssetLookup,
            assetFilter
        ]

        t1 = datetime.now()
        res = list(coll.aggregate(pipeline))
        t2 = datetime.now()

        totalExecutionTime += (t2-t1).total_seconds()
else:
    for i in range(numExecutions):
        print('%d of %d' % (i+1, numExecutions))
        assetId = random.randint(0, 9999)
        eid = random.randint(0, 9999)
        pipeline = [{
            '$match': {
                'id': 'AID' + str(assetId) + '_0_0'
            }
        }, assetCompanyLookup, companyFilter, setCompany]

        t1 = datetime.now()
        res = list(coll.aggregate(pipeline))
        employee = coll.find_one({'id': 'EID' + str(eid) + '_0'})
        t2 = datetime.now()

        totalExecutionTime += (t2-t1).total_seconds()

print("Time per query: %f ms" % (totalExecutionTime / numExecutions * 1000))
