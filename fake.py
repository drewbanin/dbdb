
import datetime
import time
import random

from faker import Faker
from faker.providers import person
from faker.providers import date_time

fake = Faker()
fake.add_provider(person)


def make_date(dt):
    return int(time.mktime(dt.timetuple()))


people = []
for i in range(100):
    people.append({
        "id": i,
        "created_at": make_date(fake.date_this_decade()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
    })

friends = []
for i in range(1000):
    friend_1 = random.choice(people)
    friend_2 = random.choice(people)

    friends.append({
        "friend_1": friend_1['id'],
        "friend_2": friend_2['id'],
    })


print(people)
print(friends)
