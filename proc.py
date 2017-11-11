import os
import json
import argparse

import tablib


def proc_files(source_dir, output_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir_path, output_name), 'a') as o_f:
        for root, dirs, files in os.walk(os.path.join(dir_path, source_dir)):
            for name in files:
                if not name.endswith('.csv'):
                    continue
                with open(os.path.join(root, name)) as f:
                    try:
                        data = tablib.Dataset().load(f.read())
                    except:
                        print('Unable to load file', os.path.join(root, name))
                        continue
                    for row in data.dict:
                        group = {}
                        for k, v in row.items():
                            if k != 'timestamp':
                                k = k.strip()
                                v = v.strip()
                                try:
                                    v = float(v)
                                except:
                                    v = 0
                                parts = k.split('.')
                                SQT_ID = parts[0] + '.' + parts[1]
                                group[SQT_ID] = group.get(SQT_ID, {})
                                if parts[-1].startswith('SQT_'):
                                    group[SQT_ID]['SQT_ID'] = SQT_ID
                                    group[SQT_ID][parts[-1]] = v
                                else:
                                    parts = parts[-1].split('$')
                                    group[SQT_ID]['GX_ID'] = parts[0]
                                    group[SQT_ID][parts[-1]] = v
                        for _, v in group.items():
                            r = v.copy()
                            r['timestamp'] = row['timestamp'].strip()
                            o_f.write(json.dumps(r) + '\n')


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-s', '--source', help='Source directory that contains CSV files')
    arg_parser.add_argument(
        '-o', '--output', help='Output file name')
    args = arg_parser.parse_args()
    proc_files(source_dir=args.source, output_name=args.output)
