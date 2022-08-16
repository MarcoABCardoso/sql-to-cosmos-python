import pytest
from azure.cosmos import PartitionKey

from src.sql_to_cosmos import SqlToCosmosReplicator
from tests.mock_cosmos import MockCosmosClient
from tests.mock_sql import MockSqlClient, mock_rows


@pytest.mark.asyncio
async def test_replication():
    sql_client = MockSqlClient()
    cosmos_client = MockCosmosClient()
    partition_key = PartitionKey("/id")
    replicator = SqlToCosmosReplicator(
        sql_client, 
        cosmos_client, 
        "FTA", "PersonCache", partition_key,
        start_procedure="Cosmos.spStart",
        read_procedure="Cosmos.spGetPersonsToRefreshV2",
        end_procedure="Cosmos.spEnd",
    )
    await replicator.run()
    # Creates database and container
    cosmos_client.create_database_if_not_exists.assert_called_with("FTA")
    replicator.db.create_container_if_not_exists.assert_called_with("PersonCache", partition_key, offer_throughput=4000)
    # Calls the start procedure
    replicator.sql_client.persistent_cursor.callproc.assert_any_call("Cosmos.spStart", ("PersonCache",))
    # Upserts each item
    for row in mock_rows:
        replicator.container.upsert_item.assert_any_call(row)
    replicator.sql_client.commit.assert_called()
    # Calls the end procedure
    replicator.sql_client.persistent_cursor.callproc.assert_called_with("Cosmos.spEnd", ('PersonCache', 'ok', 1, 302))

@pytest.mark.asyncio
async def test_replication_procedure_error():
    sql_client = MockSqlClient()
    cosmos_client = MockCosmosClient()
    partition_key = PartitionKey("/id")
    replicator = SqlToCosmosReplicator(
        sql_client, 
        cosmos_client, 
        "FTA", "PersonCache", partition_key,
        start_procedure="Cosmos.spStartError",
        read_procedure="Cosmos.spGetPersonsToRefreshV2",
        end_procedure="Cosmos.spEnd",
    )
    try:
        await replicator.run()
    except Exception as err:
        assert str(err) == "Start procedure should return row"