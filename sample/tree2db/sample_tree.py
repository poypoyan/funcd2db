# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

from funcd2db import tree2db

# this prints a series of (unsafe) insert queries
def query(line: list, prev: str) -> None:
    entry = line[0]
    mode = int(line[1])
    if 0 <= mode <= 2:
        if len(line) > 2:
            gl_field = ', Glottolog'
            gl_entry = f', \'{ line[2] }\''
        else:
            gl_field = ''
            gl_entry = ''

        print(f'''insert into Language_Node (Name, NodeType, ParentNode{ gl_field })
select \'{ entry }\', { mode }, Id{ gl_entry } from Language_Node where Name = \'{ prev }\';\n''')
    elif mode == 3:
        print(f'''insert into Language_Othername (Name, Language)
select \'{ entry }\', Id from Language_Node where Name = \'{ prev }\';\n''')


if __name__ == '__main__':
    tree2db.convert('tree.txt', query)
