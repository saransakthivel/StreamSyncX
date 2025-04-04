from pymongo import MongoClient
from datetime import datetime, timezone
try:
    client = MongoClient("mongodb+srv://smartgrid:yQPJi5bLVrWLsd6s@psgsynccluster.yuuy7.mongodb.net/")
    db = client["test_db"]
    collection = db["test_collection"]

    data = {"name": "test Entry", "timestamp": datetime.now(timezone.utc)}
    collection.insert_one(data)

    print("Data Inserted: ", data)

except Exception as e:
    print("Error Occured: ", str(e))