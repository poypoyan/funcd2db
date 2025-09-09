# treedb, an indent tree text processor
# Component of funcd2db project by poypoyan
# 
# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

import re


def convert(extract_file: str, query_cb, indent: int=4) -> None:
    if indent == 0:
        detect_indent = '^(\t*)(.*)'
    elif indent > 0:
        detect_indent = '^( *)(.*)'
    else:
        raise ValueError('Value of indent parameter must be non-negative.')

    store_depth = []
    prev_depth = -1
    with open(extract_file) as tree:
        for line in tree:
            res = re.search(detect_indent, line)
            if res:
                if indent:
                    depth = -(-len(res.group(1)) // indent)   # ceiling division
                else:
                    depth = len(res.group(1))
                line_main = _clean_list(res.group(2).split(','))

                if line_main[0]:
                    if len(store_depth) == depth and depth - prev_depth == 1:
                        store_depth.append(line_main[0])
                    elif len(store_depth) > depth and depth - prev_depth <= 1:
                        store_depth[depth] = line_main[0]
                    else:
                        raise ValueError(f'Invalid indent of line "{ res.group(2) }" with respect to previous line.')

                    prev_depth = depth
                    if depth > 0:
                        query_cb(line_main, store_depth[depth - 1])


def _clean_list(l: list) -> list:
    out = []
    for i in l:
        out.append(i.strip())
    return out
