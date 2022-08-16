import os
import asyncio
import _scproxy
import pymssql
from azure.cosmos import CosmosClient, PartitionKey

from sql_to_cosmos.src.sql_to_cosmos import SqlToCosmosReplicator


SQL_URI = os.getenv("SQL_URI")
SQL_USER = os.getenv("SQL_USER")
SQL_PWD = os.getenv("SQL_PWD")
SQL_DB = os.getenv("SQL_DB")
COSMOS_URI = os.getenv("COSMOS_URI")
COSMOS_KEY = os.getenv("COSMOS_KEY")

sql_client = pymssql.connect(SQL_URI, SQL_USER, SQL_PWD, SQL_DB)
cosmos_client = CosmosClient(COSMOS_URI, COSMOS_KEY)
replicator = SqlToCosmosReplicator(
    sql_client, 
    cosmos_client, 
    "FTA", "Person", PartitionKey("/id"),
    start_procedure="Cosmos.spStart",
    end_procedure="Cosmos.spEnd",
)

asyncio.run(replicator.run())