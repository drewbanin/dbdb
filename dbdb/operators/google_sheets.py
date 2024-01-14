from dbdb.operators.base import Operator, OperatorConfig, pipeline
from dbdb.tuples.rows import Rows
from dbdb.const import ROOT_DIR

import itertools
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

class GoogleSheetsConfig(OperatorConfig):
    def __init__(
        self,
        table,
        sheet_id,
        tab_id=None,
    ):
        self.table = table

        self.sheet_id = sheet_id
        self.tab_id = tab_id

        creds = self._get_creds()
        self.service = build("sheets", "v4", credentials=creds)

    def _get_creds(self):
        token_path = os.path.join(ROOT_DIR, "token.json")
        creds_path = os.path.join(ROOT_DIR, "credentials.json")

        print("GSHEETS - Getting creds")
        creds = None
        if os.path.exists(token_path):
            print("GSHEETS - Getting creds from file")
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                print("GSHEETS - Getting creds - running oauth flow")
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_path, "w") as token:
                token.write(creds.to_json())

        return creds


class GoogleSheetsOperator(Operator):
    Config = GoogleSheetsConfig

    def name(self):
        return "Google Sheet"

    def details(self):
        return {
            "table": "idk?",
            "columns": "idk?"
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
        self.stats.update_start_running()

        # self.reader = FileReader(self.config.table_ref)
        service = self.config.service
        sheet = service.spreadsheets()

        fields = self.get_columns(sheet)
        iterator = self.make_iterator(sheet, fields)

        return Rows(
            self.config.table,
            fields,
            iterator,
        )
