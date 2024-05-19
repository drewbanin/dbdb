from dbdb.operators.base import Operator, OperatorConfig, pipeline
from dbdb.tuples.rows import Rows
from dbdb.const import ROOT_DIR

import itertools
import json
import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


API_KEY = os.getenv('DBDB_GSHEETS_API_KEY')


def check_api_key():
    if not API_KEY:
        raise RuntimeError("dbdb was not initialized with a gsheets API key & can't query google sheets :/")


class GoogleSheetsConfig(OperatorConfig):
    def __init__(
        self,
        table,
        function_args,
    ):
        self.table = table

        if len(function_args) not in [1, 2]:
            raise RuntimeError("GOOGLE_SHEETS function expects 1 or 2 args")

        self.sheet_id = function_args[0]
        self.tab_id = function_args[1] if len(function_args) == 2 else None

        check_api_key()
        self.service = build("sheets", "v4", developerKey=API_KEY)


class GoogleSheetsOperator(Operator):
    Config = GoogleSheetsConfig

    def name(self):
        return "Google Sheet"

    @classmethod
    def function_name(cls):
        return "GOOGLE_SHEET"

    def details(self):
        return {
            "table": self.config.tab_id,
            "columns": [],
        }

    def col_to_letter(self, col):
        '''Gets the letter of a column number'''
        r = ''
        while col > 0:
            v = (col - 1) % 26
            r = chr(v + 65) + r
            col = (col - v - 1) // 26
        return r

    async def make_iterator(self, sheet, columns):
        tab = self.config.tab_id
        num_columns = len(columns)

        start = self.col_to_letter(1)
        end = self.col_to_letter(len(columns))

        prefix = f"{tab}!" if tab else ""
        tab_range = f"{prefix}{start}2:{end}"

        result = (
            sheet.values()
            .get(spreadsheetId=self.config.sheet_id, range=tab_range)
            .execute()
        )

        values = result.get('values', [])

        for record in values:
            self.stats.update_row_processed(record)

            yield record
            self.stats.update_row_emitted(record)

        self.stats.update_done_running()

    def get_columns(self, sheet):
        tab = self.config.tab_id
        if tab:
            tab_range = f"{tab}!A1:ZZ1"
        else:
            tab_range = "A1:ZZ1"

        header_row = (
            sheet.values()
            .get(spreadsheetId=self.config.sheet_id, range=tab_range)
            .execute()
        )

        cols = header_row.get("values", [[]])

        # first row
        return [self.config.table.field(col) for col in cols[0]]

    async def run(self):
        check_api_key()

        self.stats.update_start_running()

        service = self.config.service
        sheet = service.spreadsheets()

        try:
            fields = self.get_columns(sheet)
        except HttpError as e:
            err_data = json.loads(e.content).get('error', {})
            err_code = err_data.get('code', 'Unknown')
            err_status = err_data.get('status', 'Unknown')
            err_msg = err_data.get('message', 'Unknown')
            raise RuntimeError(f"{err_status} ({err_code}): {err_msg}")

        iterator = self.make_iterator(sheet, fields)

        return Rows(
            self.config.table,
            fields,
            iterator,
        )
