# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

import json, funcsv2db


# the four functions below just append (unsafe) insert queries
# in a "do" block. this is PL/pgSQL (Postgres).
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
# entry - the entry data
# main - main dictionary from config json
# pkey - primary key field of main
# pkey_var - variable name of primary key of main
def main_query(entry: str, main: dict, pkey: str, pkey_var: str) -> None:
    joined_fields = ', '.join(main['all_fields'])
    joined_vals = ', '.join(main['other_values'])
    joined_where = ' and '.join([f'{i[0]} = \'{ i[1] }\'' for i in main['where']])
    queries[0] += f'''----
insert into { main['table'] } ({ joined_fields })
select \'{ entry }\', { joined_vals } from { main['from'] } where { joined_where }
returning { pkey } into { pkey_var };\n
'''


# query for headers
# header - the header data
# junc_fields - junc_fields dictionary from config json
# junc_where - an element from junc_where list from config json
# pkey - primary key field of main
# pkey_var - variable name of primary key of main
def junc_query(header: str, junc_fields: dict, junc_where: list, pkey: str, pkey_var: str) -> None:
    joined_fields = ', '.join(junc_fields[junc_where[2]])
    queries[0] += f'''insert into { junc_where[2] } ({ joined_fields })
select { pkey_var }, { pkey } from { junc_where[3] } where { junc_where[4] } = \'{ header }\';\n
'''


# preprocess entries
def capitalize(s: str) -> str:
    return s.capitalize()


if __name__ == "__main__":
    csv_config = 'sample-data/config.json'

    with open(csv_config, 'rb') as conf_file:
        conf = json.loads(conf_file.read())

    csv_extract = 'sample-data/pangasinan-personal-pronouns.csv'
    in_vars = {'lang': 'Pangasinan', 'ref': 'pang-ref-grammar-benton'}
    # you can add limit=n to limit the entries to be processed
    funcsv2db.convert(csv_extract, init, end, main_query, junc_query, conf, capitalize, in_vars)
    print(queries[0])
