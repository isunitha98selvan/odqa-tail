import json
import pickle
import random
from pprint import pprint

import numpy as np
import pandas as pd
from tqdm import tqdm


random.seed(123)
np.random.seed(123)


def load_property_dict():
    fpath = './properties.txt'
    property_dict = {}
    with open(fpath) as fin:
        for line in fin:
            pid, pname = line.strip().split(' ', maxsplit=1)
            property_dict[pname] = pid

    return property_dict


def load_valid_property_counts():
    d = pickle.load(open('./property_s2_count/all_filtered.pickle', 'rb'))
    s2_cnt_threshold = 3
    valid_unique_s2_threshold = 10

    valid_property_s2 = {}
    for property, data in d.items():
        valid_s2_cnts = [(s2, cnt) for s2, cnt in data if cnt >= s2_cnt_threshold]
        if len(valid_s2_cnts) < valid_unique_s2_threshold:
            continue
        valid_property_s2[property] = valid_s2_cnts

    return  valid_property_s2


def load_triplets(data_type):
    property_s1_s2 = {}
    df = pd.read_table(f'./data/{data_type}_triplets.tsv')
    for _, s1_id, property_id, s2_id, _, _, _ in df.values:
        property_s1_s2.setdefault(property_id, {}).setdefault(s1_id, set()).add(s2_id)

    return property_s1_s2


def load_p_s2_cnt():
    p_s2_cnt = pickle.load(open('./data/property_s2.pickle', 'rb'))
    new_p_s2_cnt = {}
    for p, s2_cnt in p_s2_cnt.items():
        new_s2_cnt = {}
        for s2, cnt in s2_cnt.items():
            new_s2_cnt[s2] = new_s2_cnt.get(s2, 0) + cnt
        new_p_s2_cnt[p] = sorted(new_s2_cnt.items(), key=lambda x: x[1], reverse=True)
    return new_p_s2_cnt


def load_id2name():
    id2name = pickle.load(open('./data/id2name.pickle', 'rb'))
    return id2name


def run():
    id2name = load_id2name()

    head_p_s1_s2 = load_triplets('head')
    tail_p_s1_s2 = load_triplets('tail')
    triplets = {'head': head_p_s1_s2,
                'tail': tail_p_s1_s2}
    print(f'head # distinct properties: {len(head_p_s1_s2)}')
    print(f'tail # distinct properties: {len(tail_p_s1_s2)}')

    # count number of triplets per property that can be extracted from both head and tail datasets
    max_per_property = 500
    extractable_property_count = {}
    for property in triplets['head']:
        if property not in triplets['tail']:
            continue
        if 'DEPRECATED' in property:
            continue
        extractable_property_count[property] = min(
            min(len(triplets['head'][property]), len(triplets['tail'][property])),
            max_per_property
        )

    print('# selected properties:', len(extractable_property_count))
    print('# selected triplets:', sum(extractable_property_count.values()))

    pid_template = json.load(open('./data/pid_template.json'))
    # generate triplets
    for triplet_type in ['head', 'tail']:
        triplets_df = pd.DataFrame(columns=['s1', 'property', 's2', 's1_ID', 'property_ID', 's2_ID', 'question'])
        for property, extract_cnt in tqdm(extractable_property_count.items()):
            # extract triplets for each property
            s1_s2s = triplets[triplet_type][property]
            s1s = s1_s2s.keys()
            if extract_cnt != len(s1_s2s):
                s1s = random.sample(s1_s2s.keys(), extract_cnt)

            # generate candidates
            for s1 in s1s:
                s2s = list(s1_s2s[s1])
                triplets_df.loc[len(triplets_df.index)] = [
                    id2name[s1],
                    id2name[property],
                    [id2name[s2] for s2 in s2s],
                    s1,
                    property,
                    [s2 for s2 in s2s],
                    pid_template[property].format(id2name[s1])
                ]

        # dump to tsv
        triplets_df.to_csv(f'./data/{triplet_type}_triplets_30k.tsv', sep='\t')
        print(f'generated {triplet_type} dataset')


if __name__ == '__main__':
    run()
