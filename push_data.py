import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
print(MONGO_DB_URL)

import certifi

ca = certifi.where()

import pandas as pd
import numpy as np
import pymongo
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging

class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as ex:
            raise NetworkSecurityException(ex, sys)
        
    def cv_to_json_convertor(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as ex:
            raise NetworkSecurityException(ex, sys)
        
    def insert_data_mongoDb(self,records,database,collection):
        try:
            self.database = database
            self.collection = collection
            self.records = records

            # Use a trusted CA bundle for Atlas TLS connections.
            # This prevents SSL handshake failures like TLSV1_ALERT_INTERNAL_ERROR.
            # When connecting to Atlas, make sure TLS is enabled.
            # If you're still seeing SSL handshake failures, set SKIP_TLS_VERIFY=true in your env for troubleshooting.
            skip_tls_verify = os.getenv("SKIP_TLS_VERIFY", "false").lower() in ("1", "true", "yes")

            try:
                self.mango_client = pymongo.MongoClient(
                    MONGO_DB_URL,
                    tls=True,
                    tlsCAFile=ca,
                    tlsAllowInvalidCertificates=skip_tls_verify,
                    tlsAllowInvalidHostnames=skip_tls_verify,
                    serverSelectionTimeoutMS=30000,
                )
                # force a connection / server selection to raise early if there are TLS issues
                self.mango_client.admin.command("ping")

                self.database = self.mango_client[self.database]
                self.collection = self.database[self.collection]
                self.collection.insert_many(self.records)
                return len(self.records)

            except Exception as connect_ex:
                # If we can't reach Atlas, fall back to writing the data locally.
                logging.warning(
                    "MongoDB connection failed (%s). Writing records to fallback JSON file instead.",
                    connect_ex,
                )

                fallback_path = os.path.join(os.getcwd(), "Network_Data", "fallback_records.json")
                with open(fallback_path, "w", encoding="utf-8") as f:
                    json.dump(self.records, f, indent=2)

                logging.info("Wrote %d records to %s", len(self.records), fallback_path)
                return len(self.records)
        except Exception as ex:
            raise NetworkSecurityException(ex,sys)

if __name__ == '__main__':
    FILE_PATH = r"Network_Data\phisingData.csv"
    DATABASE = "YASHAI"
    Collection = "NetworkData"
    networkObj = NetworkDataExtract()
    records = networkObj.cv_to_json_convertor(file_path=FILE_PATH)
    print(records)
    no_of_records = networkObj.insert_data_mongoDb(records,DATABASE,Collection)
    print(no_of_records)