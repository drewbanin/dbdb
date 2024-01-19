
import asyncio
import collections

class RowIter:
    def __init__(self, iterator):
        self.iterator = iterator
        self.consumers = []

        self.seen = []

    def new_consumer(self):
        new_deque = collections.deque()
        for val in self.seen:
            new_deque.append(val)
        self.consumers.append(new_deque)

        def gen(mydeque):
            while True:
                if not mydeque:
                    try:
                        newval = next(self.iterator)
                        self.seen.append(newval)
                    except StopIteration:
                        return

                    for consumer in self.consumers:
                        consumer.append(newval)

                yield mydeque.popleft()

        return gen(new_deque)


iterator = iter(range(10))
row_iter = RowIter(iterator)

consumer_1 = row_iter.new_consumer()

print("c1", next(consumer_1))
print("c1", next(consumer_1))
print("c1", next(consumer_1))

consumer_2 = row_iter.new_consumer()
print("c2", next(consumer_2))
print("c2", next(consumer_2))
print("c2", next(consumer_2))
print("c2", next(consumer_2))

for v in consumer_2:
    print("c2", v)

for v in consumer_1:
    print("c1", v)
