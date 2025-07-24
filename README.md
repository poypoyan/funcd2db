# funcsv2db
A CSV processor to insert to database for use in Funcwords project.

Just run `sample.py` for example output and read the code for usage. The output to stdout is just a "do" block with (unsafe) insert queries inside.

## Use Case

Since this is used for a specific project, the way CSVs are processed is specific, too. Consider the following CSV of a grammar table:

![Pangasinan Personal Pronouns](doc/personal-pronoun-table.png)

Let's call the cells inside the blue, red, and green boxes *entries*, *headers in column* (`h_in_c` in code), and *headers in row* (`h_in_r` in code), respectively. This program can be used iff the headers correspond to prior data (i.e., prior records from other table/s), thus aside from the insertion of entries in a specific table (let's call it the *main* table), there will also be insertion to junction table/s connecting the other table/s to the main.

## Assumptions
- Primary key fields for all tables have the same name (`id` by default). It is possible to override this in your custom `main_query` and `junc_query` functions.
- The empty cells in headers means *ditto* (i.e., same as the previous non-empty cell).
- *Same Value in Same Row is Same Data* (SVSRSD). For example, the "siák" entry below "Nominative" and the one below "Possessive" will be one inserted data to main table.