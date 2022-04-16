
from dbdb.files.file_format import read_pages

from dbdb.operators.file_operator import (
    TableScan,
    TableScanConfig
)

config = TableScanConfig(
    table_ref='my_table.dumb',
    columns=['my_number', 'my_time'],
    limit=5

)
operator = TableScan(config)

for row in operator.run():
    print(row)

