import json
from datetime import datetime
from unittest.mock import MagicMock


mock_rows = [ {"id": id} for id in range(302) ]
class MockSqlClient:
    def __init__(self):
        self.persistent_cursor = MockCursor()
        self.cursor = MagicMock(return_value=self.persistent_cursor)
        self.commit = MagicMock()
class MockCursor:
    def __init__(self):
        self.rows = []
        self.callproc = MagicMock(side_effect=self.procedure_side_effect)
    def procedure_side_effect(self, *args):
        if (args[0] == "Cosmos.spStart"):
            self.rows = [(datetime(1970, 1, 1), datetime(2022, 1, 1))]
        if (args[0] == "Cosmos.spGetPersonsToRefreshV2"):
            self.rows = [{'JSON': json.dumps(row)} for row in mock_rows]
    def __iter__(self):
        return CursorIterator(self.rows)
class CursorIterator:
    def __init__(self, rows):
        self.index = -1
        self.rows = rows
    def __next__(self):
        self.index += 1
        if self.index >= len(self.rows):
            raise StopIteration
        return self.rows[self.index]
