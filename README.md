
### dbdb - drew banin's database

dbdb is a (not very sophisticated) columnar database that I made to learn more
about how columnar databases work. dbdb implements:
- a custom binary file format for storing table data
- a simple (but pretty compliant!) sql parser
- an execution engine for processing queries
- a web ui for viewing query plans, running queries, and visualizing (or hearing) results

On that last point -- dbdb can also be a musical instrument! Check out the
examples at [drewbanin.com](http://drewbanin.com) if you want to make your own
music with SQL :)

I learned so much while building dbdb! Someday, I'd like to write more about
what I've learned. Until then, check out some high-level details about dbdb
below.

### Running dbdb

**Set up your env variables**

Some env vars are required for the google sheets and openai integrations.
If you don't plan on using those features, you can skip this step.

```bash
export DBDB_GSHEETS_API_KEY='...'
export DBDB_OPENAI_API_KEY='...'
```


**Running the frontend**
```bash
cd web/dbdb
npm install
npm start
```


**Running the server**
```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
uvicorn server:app --reload
```

### Query syntax

dbdb supports:
 - basic `SELECT` statements
 - common table expressions & subqueries
 - `LEFT OUTER` and `INNER` joins
 - grouping & aggregations (see below)
 - window functions (with support for ordering, partitioning, and frame clauses)
 - all the other stuff you'd expect, like case-when exprs, type casting, etc
 - creating and dropping tables

### Function reference

<details>
<summary>Scalar functions</summary>

| Function | Args | Description |
|----------|------|-------------|
| SIN(x) | x: input angle in radians | Returns the sine of x |
| COS(x) | x: input angle in radians | Returns the cosine of x |
| POW(x, y) | x: base number<br>y: exponent | Returns x raised to the power of y |
| IFF(x, y, z) | x: condition<br>y: value if true<br>z: value if false | Returns y if x is true, z if x is false |
</details>
<details>
<summary>Aggregate functions</summary>

| Function | Args | Description |
|----------|------|-------------|
| MIN | expression | Returns the minimum value encountered in the expression across all rows |
| MAX | expression | Returns the maximum value encountered in the expression across all rows |
| SUM | expression | Returns the sum of all values in the expression across all rows |
| AVG | expression | Returns the arithmetic mean (average) of all values in the expression across all rows |
| COUNT | expression | Counts the number of rows. Supports DISTINCT modifier to count only unique values |
| LIST_AGG/LISTAGG | expression [, delimiter] | Concatenates values from multiple rows into a single string, separated by delimiter (defaults to comma). Supports DISTINCT modifier to include only unique values. The delimiter must be a string literal |
</details>

<details>
<summary>Table Functions</summary>

| Function | Args | Description |
|----------|------|-------------|
| GENERATE_SERIES | count [, delay] | Generates a table of numbers from 0 to count-1, with an optional delay between each row |
| GOOGLE_SHEET | sheet_id [, tab_id] | Queries a google sheet and returns the result as a table. If a tab_id is provided, only that tab is queried. Otherwise, only the first tab in the sheet is returned |
| ASK_GPT | prompt | Queries gpt-4o with the given prompt and returns the result as a table |
</details>


<details>
<summary>Window Functions</summary>

| Function | Args | Description |
|----------|------|-------------|
| COUNT | None | Returns the total number of rows in the current window partition |
| ROW_NUMBER | None | Returns the sequential row number (starting from 1) of the current row within its window partition |
| SUM | expr | Calculates the sum of the specified expression across all rows in the window partition |
| MIN | expr | Returns the minimum value of the specified expression across all rows in the window partition |
| MAX | expr | Returns the maximum value of the specified expression across all rows in the window partition |
| AVG/MEAN | expr | Calculates the arithmetic mean (average) of the specified expression across all rows in the window partition. Returns `None` if there are no rows |
| LAG | expr, [offset=1] | Returns the value of the specified expression from the row that is `offset` rows before the current row. Returns `None` if the offset goes beyond the window bounds |
| LEAD | expr, [offset=1] | Returns the value of the specified expression from the row that is `offset` rows after the current row. Returns `None` if the offset goes beyond the window bounds |

</details>


### File format (DUMB)

**Overview**

dbdb uses a custom binary file format (called `DUMB`) for storing table data. DUMB is short for "Drew's Unified Message Buffer". dbdb's files are stored with the `.dumb` extension and (by default) reside in the `data/` directory in the project root. Tables can be organized into "database" and "schema" hierarchies -- these translate into the subfolders inside of the `data/` directory. So, a table named `public.example.data` would be stored in `data/public/example/data.dumb`.

**File structure**

DUMB files are stored in two parts: a header section and a data section. The header section contains metadata about the table like the column names, types, compression settings, and a pointer to the location of the column data in the data section. The data section contains the (optionally encoded) data for each column.

This setup makes it possible for the query planner to read the (small) header section up-front, determine which columns are needed for the query, and skip over the data for any columns that are not needed. dbdb does not do this today, but it could in the future, so that's cool.

Column data is stored in one or more pages. By default, each page is 8KB of data. Each page contains:
- a `min` and `max` value for the column (this can be used for pushing down filters to the file format level in the future)
- a bitfield indicating which values in the page are null
- an encoding-specific page payload containing the actual column data


**More details**
<details>
<summary>Supported Types</summary>

- BOOL [1 byte]
- INT8 [1 byte]
- INT32 [4 bytes]
- STR [variable]
- DATE [4 bytes]
- FLOAT64 [8 bytes]
</details>

<details>
<summary>Supported encodings</summary>

- RAW - not encoded
- RUN_LENGTH - stores consecutive values as a tuple of values: [value, run_length]
- DELTA - stores deltas between consecutive values
- DICTIONARY - stores string values once in a dictionary, then uses dictionary indices as pointers to the values
</details>

<details>
<summary>Column compression</summary>

- RAW - not compressed
- ZLIB - uses zlib to compress the data (after encoding)
</details>


### Contributing

I probably won't be accepting PRs to this repo, though I do encourage you to
make a fork and experiment with your own ideas! You probably shouldn't run
dbdb in anything approximating a production environment, but if you did, that would
be extremely funny and you have to tell me about it.


**Running tests**

dbdb actually has pretty good sql tests! Check them out in the
`tests/sql` directory. Make sure you source your `.env` file first.

```bash
# install dev requirements
pip install -r dev-requirements.txt
pytest
```


### Future work
- [ ] support existing / modern file and table formats (Parquet, Iceberg)
- [ ] build a query optimizer
- [ ] read the book "crafting interpreters" and then rebuild dbdb :p