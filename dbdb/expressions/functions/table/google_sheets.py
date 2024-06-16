from dbdb.expressions.functions.base import TableFunction
from dbdb.tuples.rows import Rows

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import itertools
import json
import os
import asyncio

API_KEY = os.getenv("DBDB_GSHEETS_API_KEY")


class GoogleSheetsTableFunction(TableFunction):
    NAMES = ["GOOGLE_SHEET"]

    def __init__(self, args):
        if len(args) not in [1, 2]:
            raise RuntimeError("GOOGLE_SHEETS function expects 1 or 2 args")

        self.sheet_id = args[0]
        self.tab_id = args[1] if len(args) == 2 else None

        self.check_api_key()
        service = build("sheets", "v4", developerKey=API_KEY)
        self.sheet = service.spreadsheets()

    def details(self):
        return {
            "sheet_tab_id": self.tab_id,
            "columns": [],
        }

    def check_api_key(self):
        if not API_KEY:
            raise RuntimeError(
                "dbdb was not initialized with a gsheets API key & can't query google sheets :/"
            )

    def col_to_letter(self, col):
        """Gets the letter of a column number"""
        r = ""
        while col > 0:
            v = (col - 1) % 26
            r = chr(v + 65) + r
            col = (col - v - 1) // 26
        return r

    def list_fields(self):
        tab = self.tab_id
        if tab:
            tab_range = f"{tab}!A1:ZZ1"
        else:
            tab_range = "A1:ZZ1"

        row_data = (
            self.sheet.values()
            .get(spreadsheetId=self.sheet_id, range=tab_range)
            .execute()
        )

        # get first row (headers)
        rows = row_data.get("values", [[]])
        return rows[0]

    async def fields(self):
        try:
            return self.list_fields()

        except HttpError as e:
            err_data = json.loads(e.content).get("error", {})
            err_code = err_data.get("code", "Unknown")
            err_status = err_data.get("status", "Unknown")
            err_msg = err_data.get("message", "Unknown")
            raise RuntimeError(f"{err_status} ({err_code}): {err_msg}")

    async def generate(self):
        tab = self.tab_id
        fields = await self.fields()

        start = self.col_to_letter(1)
        end = self.col_to_letter(len(fields))

        prefix = f"{tab}!" if tab else ""
        tab_range = f"{prefix}{start}2:{end}"

        result = (
            self.sheet.values()
            .get(spreadsheetId=self.sheet_id, range=tab_range)
            .execute()
        )

        for row in result.get("values", []):
            yield row
