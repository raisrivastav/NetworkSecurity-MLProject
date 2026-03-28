import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")

if not MONGO_DB_URL:
    raise ValueError("❌ MONGO_DB_URL is not set in .env file")

print("Mongo URL Loaded:", MONGO_DB_URL)

import pandas as pd
import pymongo
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging


class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as ex:
            raise NetworkSecurityException(ex, sys)

    def csv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = json.loads(data.to_json(orient="records"))
            return records
        except Exception as ex:
            raise NetworkSecurityException(ex, sys)

    def insert_data_mongoDb(self, records, database, collection):
        try:
            if not records:
                raise ValueError("❌ No records to insert")

            try:
                # ✅ FIX: Let MongoDB Atlas handle TLS automatically
                client = pymongo.MongoClient(
                    MONGO_DB_URL,
                    serverSelectionTimeoutMS=30000
                )

                # ✅ Force connection check
                client.admin.command("ping")
                logging.info("✅ MongoDB connection successful")

                db = client[database]
                col = db[collection]

                result = col.insert_many(records)
                logging.info("✅ Inserted %d records", len(result.inserted_ids))

                return len(result.inserted_ids)

            except Exception as connect_ex:
                logging.warning(
                    "MongoDB connection failed (%s). Writing records to fallback JSON file instead.",
                    connect_ex,
                )

                # ✅ Ensure directory exists
                fallback_dir = os.path.join(os.getcwd(), "Network_Data")
                os.makedirs(fallback_dir, exist_ok=True)

                fallback_path = os.path.join(fallback_dir, "fallback_records.json")

                with open(fallback_path, "w", encoding="utf-8") as f:
                    json.dump(records, f, indent=2)

                logging.info("Wrote %d records to %s", len(records), fallback_path)
                return len(records)

        except Exception as ex:
            raise NetworkSecurityException(ex, sys)


if __name__ == '__main__':
    FILE_PATH = r"Network_Data\phisingData.csv"
    DATABASE = "YASHAI"
    COLLECTION = "NetworkData"

    networkObj = NetworkDataExtract()

    records = networkObj.csv_to_json_convertor(file_path=FILE_PATH)
    print(f"Total Records: {len(records)}")

    no_of_records = networkObj.insert_data_mongoDb(records, DATABASE, COLLECTION)
    print(f"Inserted Records: {no_of_records}")