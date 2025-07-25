# Funcsv2db - A CSV processor to insert to database for use in Funcwords project.
# By poypoyan
# 
# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

from collections import OrderedDict
from copy import deepcopy
import csv, re


def convert(extract_file: str, init_cb, end_cb, main_cb, junc_cb,
    conf: dict, pre_process, in_vars: dict, limit: int = None,
    svsrsd: bool = True, ditto_row: bool = True, ditto_col: bool = True) -> None:
    if not 'pkey' in in_vars:
        in_vars['pkey'] = 'id'

    main_pkey = conf['main']['table'].lower() + '_' + in_vars['pkey'].lower()
    h_in_r = []
    saved_h_in_c = {}
    limit_ctr = 0
    main_copy = None

    if ditto_col:
        for i in conf['junc_wheres']:
            if i[0] == 'c':
                saved_h_in_c[i[1]] = ''

    with open(extract_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        for _ in range(conf['header_rows']):
            h_in_r.append(_clean_list(next(csv_reader)[conf['header_cols']:]))
        if ditto_row:
            _refill_empty_r(h_in_r, conf['junc_wheres'])

        init_cb(conf['main']['table'], in_vars['pkey'], main_pkey)

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
                    iv_evaled = _in_vars_eval(k[5], in_vars)
                    if k[0] == '':
                        final_eval = iv_evaled
                    else:
                        final_eval = _table_eval(iv_evaled, i, k, row_cl, h_in_r, saved_h_in_c)

                    junc_cb(final_eval, conf['junc_fields'], k, in_vars['pkey'], main_pkey)

                    if k[0] == 'r':
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


def _refill_empty_r(h_in_r: list, junc_where: dict) -> None:
    max_r = max([len(i) for i in h_in_r])
    for i in junc_where:
        if i[0] == 'r':
            h_in_r[i[1]] += [''] * (max_r - len(h_in_r[i[1]]))

            temp_r = ''
            for j0, j1 in enumerate(h_in_r[i[1]]):
                h_in_r[i[1]][j0], temp_r = _fill_curr(j1, temp_r)


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
    return eval_str.strip()


def _table_eval(in_str: str, idx: int, jw: list,
    row_cl: list, h_in_r: list, saved_h_in_c: list) -> str:
    eval_str = ''
    last_end = 0
    for i in re.finditer('{.*?}', in_str):
        jump = int(0 if i.group()[1:-1] == '' else i.group()[1:-1])
        ext_str = ''
            
        if jw[0] == 'r':
            if jump == 0:
                ext_str = h_in_r[jw[1]][idx]
            elif idx < len(h_in_r[jw[1] + jump]):
                ext_str = h_in_r[jw[1] + jump][idx]
        elif jw[0] == 'c':
            ext_str = row_cl[jw[1] + jump]
            if jump == 0 and jw[1] in saved_h_in_c:
                saved_h_in_c[jw[1]], ext_str = _fill_curr(saved_h_in_c[jw[1]], ext_str)

        eval_str += in_str[last_end:i.start()] + ext_str
        last_end = i.end()
    eval_str += in_str[last_end:]
    return eval_str.strip()
