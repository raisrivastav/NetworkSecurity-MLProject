import sys

from NetworkSecurity.components.data_ingestion import DataIngestion
from NetworkSecurity.components.data_validation import DataValidation
from NetworkSecurity.exception.exception import NetworkSecurityException
from NetworkSecurity.logging.logger import logging
from NetworkSecurity.entity.config_entity import DataIngestionConfig, DataValidationConfig
from NetworkSecurity.entity.config_entity import TrainingPipelineConfig

if __name__ == '__main__':
    try:
        trainingpipelineconfig = TrainingPipelineConfig()
        dataingestionconfig = DataIngestionConfig(trainingpipelineconfig)
        dataingestion = DataIngestion(dataingestionconfig)
        logging.info("Initiate the data ingestion")

        dataingestionartifact = dataingestion.initiate_data_ingestion()
        logging.info("Data Initiation Completed")
        print(dataingestionartifact)

        data_validation_config = DataValidationConfig(trainingpipelineconfig)
        data_validation = DataValidation(dataingestionartifact,data_validation_config)
        logging.info("Initiate the data validation")
        data_validation.initiate_data_validation()
        logging.info("Data validation completed")
        
    except Exception as ex:
        raise NetworkSecurityException(ex, sys)