# Funcsv2db - A CSV processor to insert to database for use in Funcwords project.
# By poypoyan
# 
# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

from collections import OrderedDict
from copy import deepcopy
import csv, re


def convert(extract_file: str, init_cb, end_cb, main_cb, junc_cb,
    conf: dict, pre_process, in_vars: dict, limit: int = None, svsrsd: bool = True) -> None:
    if not 'pkey' in in_vars:
        in_vars['pkey'] = 'id'

    main_pkey = conf['main']['table'].lower() + '_' + in_vars['pkey'].lower()
    h_in_r = []
    saved_h_in_c = {}
    limit_ctr = 0
    main_copy = None

    # check cols and apply ditto
    check_col = set()
    for i in conf['junc_wheres']:
        if i['row_col'] == 'c':
            if i['nth'] in check_col:
                raise ValueError(f'Detected duplicate "junc_where" entry for column { i['nth'] }.')
            check_col.add(i['nth'])

            if i['ditto']:
                saved_h_in_c[i['nth']] = ''

    with open(extract_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        for _ in range(conf['header_rows']):
            h_in_r.append(_clean_list(next(csv_reader)[conf['header_cols']:]))

        # check rows and apply ditto
        max_r = max([len(i) for i in h_in_r])
        check_row = set()
        for i in conf['junc_wheres']:
            if i['row_col'] == 'r':
                if i['nth'] in check_row:
                    raise ValueError(f'Detected duplicate "junc_where" entry for row { i['nth'] }.')
                check_row.add(i['nth'])
                h_in_r[i['nth']] += [''] * (max_r - len(h_in_r[i['nth']]))

                if i['ditto']:
                    temp_r = ''
                    for j0, j1 in enumerate(h_in_r[i['nth']]):
                        h_in_r[i['nth']][j0], temp_r = _fill_curr(j1, temp_r)

        init_cb(conf['main'], in_vars['pkey'], main_pkey)

        # evaluate values in main dict
        main_copy = deepcopy(conf['main'])
        main_copy['other_values'] = [_in_vars_eval(i, in_vars) for i in main_copy['other_values']]
        for i in main_copy['where']:
            i[1] = _in_vars_eval(i[1], in_vars)

        for row in csv_reader:
            row_cl = _clean_list(row)

            # conversion main part
            # assumes "Same Value in Same Row is Same Data" (SVSRSD) principle:
            # if a value is found more than once in a row, all of that is one main data in database.
            skip_svsrsd = []

            for i, j in enumerate(row_cl[conf['header_cols']:]):
                if i in skip_svsrsd:
                    continue

                main_cb(pre_process(j), main_copy, in_vars['pkey'], main_pkey)

                rows_svsrsd = []
                if svsrsd:
                    for k0, k1 in enumerate(row_cl[conf['header_cols'] + i + 1:]):
                        if k1 == j:
                            skip_idx = i + k0 + 1
                            skip_svsrsd.append(skip_idx)
                            rows_svsrsd.append(skip_idx)

                for k in conf['junc_wheres']:
                    iv_evaled = _in_vars_eval(k['value'], in_vars)
                    if k['row_col'] == '':
                        final_eval = iv_evaled
                    else:
                        final_eval = _table_eval(iv_evaled, i, k, row_cl, h_in_r, saved_h_in_c)

                    junc_cb(final_eval, conf['junc_fields'], k, in_vars['pkey'], main_pkey)

                    if k['row_col'] == 'r':
                        for l in rows_svsrsd:
                            junc_cb(_table_eval(iv_evaled, l, k, row_cl, h_in_r, saved_h_in_c),
                                conf['junc_fields'], k, in_vars['pkey'], main_pkey)
                limit_ctr += 1
                if limit != None and limit_ctr >= limit:
                    break
            if limit != None and limit_ctr >= limit:
                break
    end_cb()


def _clean_list(l: list) -> list:
    last_non_trail_idx = 0
    out = []
    for i, j in enumerate(l):
        j_cl = j.strip()
        out.append(j_cl)
        if j_cl != '':
            last_non_trail_idx = i

    return out[:last_non_trail_idx + 1]


def _fill_curr(new: str, curr: str) -> tuple:
    if new != '':
        return new, new
    else:
        return curr, curr


def _in_vars_eval(in_str: str, in_vars: dict) -> str:
    eval_str = ''
    last_end = 0
    for i in re.finditer('{.*?}', in_str):
        catched_var = i.group()[1:-1]
        if catched_var == '' or catched_var.isnumeric():
            append_str = i.group()   # give way to _table_eval
        elif not catched_var in in_vars:
            raise ValueError(f'Catched variable \'{ catched_var }\' not found in in_vars.')
        else:
            append_str = in_vars[catched_var]
        eval_str += in_str[last_end:i.start()] + append_str
        last_end = i.end()
    eval_str += in_str[last_end:]
    return eval_str


def _table_eval(in_str: str, idx: int, jw: dict,
    row_cl: list, h_in_r: list, saved_h_in_c: list) -> str:
    eval_str = ''
    last_end = 0
    for i in re.finditer('{.*?}', in_str):
        jump = int(0 if i.group()[1:-1] == '' else i.group()[1:-1])
        append_str = ''

        if jw['row_col'] == 'r':
            if jump == 0:
                append_str = h_in_r[jw['nth']][idx]
            elif idx < len(h_in_r[jw['nth'] + jump]):
                append_str = h_in_r[jw['nth'] + jump][idx]
        elif jw['row_col'] == 'c':
            append_str = row_cl[jw['nth'] + jump]
            if jump == 0 and jw['nth'] in saved_h_in_c:
                saved_h_in_c[jw['nth']], append_str = _fill_curr(append_str, saved_h_in_c[jw['nth']])

        eval_str += in_str[last_end:i.start()] + append_str
        last_end = i.end()
    eval_str += in_str[last_end:]
    return re.sub(' +', ' ', eval_str.strip())
