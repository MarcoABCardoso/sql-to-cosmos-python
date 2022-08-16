import json
import asyncio
from azure.cosmos.aio import CosmosClient, DatabaseProxy, ContainerProxy


class SqlToCosmosReplicator:
    def __init__(
        self,
        sql_client,
        cosmos_client: CosmosClient,
        db_name,
        container_name,
        partition_key,
        start_procedure,
        end_procedure,
    ) -> None:
        self.sql_client = sql_client
        self.cosmos_client = cosmos_client
        self.db_name = db_name
        self.container_name = container_name
        self.partition_key = partition_key

        self.start_procedure = start_procedure
        self.end_procedure = end_procedure

        self.db: DatabaseProxy = None
        self.container: ContainerProxy = None
    

    async def run(self):
        self.setup_cosmos()
        start_time, end_time, read_procedure = self.run_start_procedure()
        num_records = await self.run_replication(start_time, end_time, read_procedure)
        self.run_end_procedure(num_records)


    def setup_cosmos(self):
        print("Setting up Cosmos DB and Container...")
        self.db = self.cosmos_client.create_database_if_not_exists(self.db_name)
        self.container = self.db.create_container_if_not_exists(self.container_name, self.partition_key, offer_throughput=4000)
        print("Done setting up Cosmos DB and Container.")


    def run_start_procedure(self):
        cursor = self.sql_client.cursor()  
        cursor.callproc(self.start_procedure, (self.container_name,))
        row = None
        for r in cursor:
            row = r
        if not row:
            raise Exception("Start procedure should return row")
        self.sql_client.commit()
        return row


    async def run_replication(self, start_time, end_time, read_procedure):
        cursor = self.sql_client.cursor(as_dict=True)
        cursor.callproc(read_procedure, (start_time, end_time))
        total_calls = 0
        calls = []
        for row in cursor:
            call = asyncio.create_task(self.upsert_item(row))
            calls.append(call)
            total_calls += 1
            if total_calls % 150 == 0:
                await asyncio.gather(*calls)
                print(f"Done with {total_calls} calls...")
                calls = []
        await asyncio.gather(*calls)
        return total_calls


    async def upsert_item(self, row):
        return self.container.upsert_item(json.loads(row['JSON']))


    def run_end_procedure(self, document_count):
        cursor = self.sql_client.cursor()  
        cursor.callproc(self.end_procedure, (self.container_name, "ok", 1, document_count))
        self.sql_client.commit()
