from unittest.mock import AsyncMock, MagicMock


class MockCosmosClient:
    def __init__(self):
        self.create_database_if_not_exists = MagicMock(return_value=MockDatabaseProxy())
class MockDatabaseProxy:
    def __init__(self):
        self.create_container_if_not_exists = MagicMock(return_value=MockContainerProxy())
class MockContainerProxy:
    def __init__(self):
        self.upsert_item = AsyncMock(return_value={})