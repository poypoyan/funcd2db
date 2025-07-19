# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

import funcsv2db

# the four functions below just append (unsafe) insert queries
# in a "do" block. this is PostgreSQL PL/SQL.
queries = ['']


# runs before all queries for each entry
# main - main table
# pkey - primary key field of main
# pkey_var - variable name of primary key of main
def init(main: str, pkey: str, pkey_var: str) -> None:
    queries[0] += f'''do $$
declare
{ pkey_var } { main }.{ pkey }%type;
begin\n
'''


# runs after all queries for each entry
def end() -> None:
    queries[0] += 'end $$'


# query for entries
# main - main table
# entry - the entry data
# main_fields - fields in the main to be inserted
# sel_fields - other fields for "select" section in "insert select"
# sel_from - "from" section for "select" section in "insert select"
# sel_where - "where" section for "select" section in "insert select"
# pkey - primary key field of main
# pkey_var - variable name of primary key of main
def main_query(main: str, entry: str, main_fields: str,
    sel_fields: str, sel_from: str, sel_where: str, pkey: str, pkey_var: str) -> None:
    queries[0] += f'''----
insert into { main } ({ main_fields })
select \'{ entry }\', { sel_fields } from { sel_from } where { sel_where }
returning { pkey } into { pkey_var };\n
'''


# query for headers
# junc - junction table
# header - the header data
# main_forkey - foreign key for main table
# other_forkey - foreign key for other table
# other_table - "from" table for "select" section in "insert select"
# other_field - "where" field for "select" section in "insert select"
# pkey - primary key field of other (funcsv2db assumes to be same as in main)
# pkey_var - variable name of primary key of main
def junc_query(junc: str, header: str, main_forkey: str, other_forkey: str,
    other_table: str, other_field: str, pkey: str, pkey_var: str) -> None:
    queries[0] += f'''insert into { junc } ({ main_forkey }, { other_forkey })
select { pkey_var }, { pkey } from { other_table } where { other_field } = \'{ header }\';\n
'''


# preprocess entries
def capitalize(s: str) -> str:
    return s.capitalize()


if __name__ == "__main__":
    csv_config = 'sample-data/config.csv'
    conf = funcsv2db.get_config(csv_config)

    csv_extract = 'sample-data/pangasinan-personal-pronouns.csv'
    in_vars = {'lang': 'Pangasinan', 'ref': 'pang-ref-grammar-benton'}
    # you can add limit=n to limit the output
    funcsv2db.convert(csv_extract, init, end, main_query, junc_query, conf, capitalize, in_vars)
    print(queries[0])
