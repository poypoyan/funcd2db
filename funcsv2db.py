# Funcsv2db - A CSV processor to insert to database for use in Funcwords project.
# By poypoyan
# 
# Distributed under the MIT software license. See the accompanying
# file LICENSE or https://opensource.org/license/mit/.

from collections import OrderedDict
import csv, re


class Config:
    # main: list - 0 is the table, 1 is the "main" field (e.g., Name), rest are other fields.
    # main_where: str - manually continue the query
    # junc: dict - key is a table, value is a list like main, but for junction.
    # junc_where: OrderedDict of list - each element is of form: 0 is a table, 1 is a field, 2 is value.
    # h_in_r/c: int - headers in row/column
    def __init__(self):
        self.main = []
        self.main_where = ''
        self.junc = {}
        self.junc_where = OrderedDict()
        self.num_h_in_r = 0
        self.num_h_in_c = 0


def get_config(conf_file: str) -> Config:
    conf = Config()
    row_state = ''
    curr_junc = ''
    non_table_ctr = 0

    with open(conf_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if row[0] == '~m':
                conf.main = clean_list(row[1:])
                row_state = 'm'
            elif row[0] == '~j':
                row_cl = clean_list(row[1:4])
                curr_junc = row_cl[0]
                conf.junc[curr_junc] = row_cl[1:3]
                row_state = 'j'
            elif row[0] == '~t':
                conf.num_h_in_r = int(row[1])
                conf.num_h_in_c = int(row[2])
            elif row[0][:2] in ['~r', '~c']:
                header_info = (row[0][:2], int(row[0][2:]), curr_junc)
                conf.junc_where[header_info] = clean_list(row[1:4])
            elif row[0] == '':
                if row_state == 'm' and row[1] != '':
                    conf.main_where = clean_list(row[1:4])
                elif row_state == 'j' and row[1] != '':
                    header_info = ('', non_table_ctr, curr_junc)
                    conf.junc_where[header_info] = clean_list(row[1:4])
                    non_table_ctr += 1
    return conf


def clean_list(l: list) -> list:
    last_non_trail_idx = 0
    out = []
    for i, j in enumerate(l):
        j_cl = j.strip()
        out.append(j_cl)
        if j_cl != '':
            last_non_trail_idx = i

    return out[:last_non_trail_idx + 1]


def convert(extract_file: str, init_cb, end_cb, main_cb, junc_cb,
    conf: Config, pre_process, in_vars: dict, limit: int = None) -> None:
    if not 'pkey' in in_vars:
        in_vars['pkey'] = 'id'

    main_pkey = conf.main[0].lower() + '_' + in_vars['pkey'].lower()
    h_in_r = []
    saved_h_in_c = {}
    limit_ctr = 0

    for i in conf.junc_where:
        if i[0] == '~c':
            saved_h_in_c[i[1]] = ''

    with open(extract_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        for _ in range(conf.num_h_in_r):
            h_in_r.append(clean_list(next(csv_reader)[conf.num_h_in_c:]))
        _refill_empty_r(h_in_r, conf.junc_where)

        init_cb(conf.main[0], in_vars['pkey'], main_pkey)

        for row in csv_reader:
            row_cl = clean_list(row)

            # conversion main part
            # assumes "Same Value in Same Row is Same Data" (SVSRSD) principle:
            # if a value is found more than once in a row, all of that is one main data in database.
            skip_svsrsd = []

            for i, j in enumerate(row_cl[conf.num_h_in_c:]):
                if i in skip_svsrsd:
                    continue

                main_cb(conf.main[0], pre_process(j), ", ".join(conf.main[1:]),
                    _in_vars_eval(conf.main_where[0], in_vars), conf.main_where[1],
                    _in_vars_eval(conf.main_where[2], in_vars), in_vars['pkey'], main_pkey)

                rows_svsrsd = []
                for k0, k1 in enumerate(row_cl[conf.num_h_in_c + i + 1:]):
                    if k1 == j:
                        skip_idx = i + k0 + 1
                        skip_svsrsd.append(skip_idx)
                        rows_svsrsd.append(skip_idx)

                for k0, k1 in conf.junc_where.items():
                    iv_evaled = _in_vars_eval(k1[2], in_vars)
                    if k0[0] == '':
                        junc_cb(k0[2], iv_evaled, conf.junc[k0[2]][0], conf.junc[k0[2]][1], k1[0], k1[1],
                            in_vars['pkey'], main_pkey)
                        continue

                    junc_cb(k0[2], _table_eval(iv_evaled, i, k0, row_cl, h_in_r, saved_h_in_c),
                        conf.junc[k0[2]][0], conf.junc[k0[2]][1], k1[0], k1[1], in_vars['pkey'], main_pkey)
                    if k0[0] == '~r':
                        for l in rows_svsrsd:
                            junc_cb(k0[2], _table_eval(iv_evaled, l, k0, row_cl, h_in_r, saved_h_in_c),
                                conf.junc[k0[2]][0], conf.junc[k0[2]][1], k1[0], k1[1], in_vars['pkey'], main_pkey)
                limit_ctr += 1
                if limit != None and limit_ctr >= limit:
                    break
            if limit != None and limit_ctr >= limit:
                break
    end_cb()


def _fill_curr(new: str, curr: str) -> tuple:
    if new != '':
        return new, new
    else:
        return curr, curr


def _refill_empty_r(h_in_r: list, junc_where: dict) -> None:
    max_r = max([len(i) for i in h_in_r])
    for i in junc_where:
        if i[0] == '~r':
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


def _table_eval(in_str: str, idx: int, jw_key: tuple,
    row_cl: list, h_in_r: list, saved_h_in_c: list) -> str:
    eval_str = ''
    last_end = 0
    for i in re.finditer('{.*?}', in_str):
        jump = int(0 if i.group()[1:-1] == '' else i.group()[1:-1])
        ext_str = ''
            
        if jw_key[0] == '~r':
            if jump == 0:
                ext_str = h_in_r[jw_key[1]][idx]
            elif idx < len(h_in_r[jw_key[1] + jump]):
                ext_str = h_in_r[jw_key[1] + jump][idx]
        elif jw_key[0] == '~c':
            ext_str = row_cl[jw_key[1] + jump]
            if jump == 0:
                saved_h_in_c[jw_key[1]], ext_str = _fill_curr(saved_h_in_c[jw_key[1]], ext_str)

        eval_str += in_str[last_end:i.start()] + ext_str
        last_end = i.end()
    eval_str += in_str[last_end:]
    return eval_str.strip()
