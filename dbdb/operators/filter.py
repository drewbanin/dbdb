from dbdb.operators.base import Operator, OperatorConfig
import itertools


"""
To do this for real, we'd need/want to be able to push
these predicates down into the file decoder. That would
let us skip pages based on metadata... we can't really
do that up at this level of abstraction. Best approach
is probably to make some reusable logic that can either
1) be pushed down into the encoder/decoder level OR
2) be applied to a stream of decoded data for predicates
   which cannot be applied at the decoder

Also: it's important that we're able to preserve information
about these predicates. An example predicate might look like:

    WHERE my_date > '2022-01-01' AND color = 'red'

If we have a sort key on my_date, then we can push this down
to the data pages for filtering. We'd want to push down _that_
predicate, but not the color predicate. If the logic is instead

    WHERE my_date > '2022-01-01' OR color = 'red'

Then we're kind of out of luck... need to read all of the pages
out of the my_date column and process them up here. I just don't
see any way around doing that....

It's fine for now, but let's circle back here...
"""


class FilterConfig(OperatorConfig):
    def __init__(
        self,
        predicates,
    ):
        self.predicates = predicates


class FilterOperator(Operator):
    Config = FilterConfig

    def run(self, tuples):
        predicates = self.config.predicates
        for row in tuples:
            if all([p(row) for p in predicates]):
                yield row
