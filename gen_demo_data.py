#!/usr/bin/env python

import datetime
import time
import hexdump
import random
import math


from dbdb.io.file_format import (
    Table,
    Column,
    ColumnInfo,
    ColumnData
)
from dbdb.io.types import (
    DataType,
    DataEncoding,
    DataCompression,
    DataSorting
)

from dbdb.lang import lang

from faker import Faker
from faker.providers import person
from faker.providers import date_time

fake = Faker()
fake.add_provider(person)


def make_date(dt):
    return int(time.mktime(dt.timetuple()))


NUM_PEOPLE = 1_000
NUM_FRIENDS = 100

people = []
for i in range(NUM_PEOPLE):
    people.append({
        "user_id": i,
        "created_at": make_date(fake.date_this_decade()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
    })

friends = []
for i in range(NUM_FRIENDS):
    friend_1 = random.choice(people)
    friend_2 = random.choice(people)

    friends.append({
        "friend_1": friend_1['user_id'],
        "friend_2": friend_2['user_id'],
    })


user_id = Column.new(
    column_name='user_id',
    column_type=DataType.INT32,
    encoding=DataEncoding.RAW,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.UNSORTED,
    data=[i['user_id'] for i in people]
)

created_at = Column.new(
    column_name='created_at',
    column_type=DataType.DATE,
    encoding=DataEncoding.RAW,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.SORTED,
    data=[i['created_at'] for i in people]
)

first_name = Column.new(
    column_name='first_name',
    column_type=DataType.STR,
    encoding=DataEncoding.RAW,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.UNSORTED,
    column_width=32,
    data=[i['first_name'] for i in people]
)

last_name = Column.new(
    column_name='last_name',
    column_type=DataType.STR,
    encoding=DataEncoding.RAW,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.UNSORTED,
    column_width=32,
    data=[i['last_name'] for i in people]
)


people_table = Table(
    columns=[user_id, created_at, first_name, last_name]
)


people_table.describe()
byte_array = people_table.serialize()

with open('people.dumb', 'wb') as fh:
    fh.write(byte_array)

# --------- Second table

friend_1 = Column.new(
    column_name='from_friend',
    column_type=DataType.INT32,
    encoding=DataEncoding.RAW,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.UNSORTED,
    data=[i['friend_1'] for i in friends]
)

friend_2 = Column.new(
    column_name='to_friend',
    column_type=DataType.INT32,
    encoding=DataEncoding.RAW,
    compression=DataCompression.ZLIB,
    sorting=DataSorting.UNSORTED,
    data=[i['friend_2'] for i in friends]
)

friends_table = Table(
    columns=[friend_1, friend_2]
)


friends_table.describe()
byte_array = friends_table.serialize()

with open('friends.dumb', 'wb') as fh:
    fh.write(byte_array)
