import os
import sys
import pickle
from pprint import pprint
from collections import defaultdict

import sling
from tqdm import tqdm


# parse args
DATA_TYPE = sys.argv[1]
REC_IDX = sys.argv[2]

record_fpath = f'/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/wikidata-items-0000{REC_IDX}-of-00010.rec'
triplet_output_dir = f'./data_{DATA_TYPE}'

os.makedirs(triplet_output_dir, exist_ok=True)


def load_property_dict():
    fpath = './properties.txt'
    property_dict = {}
    with open(fpath) as fin:
        for line in fin:
            pid, pname = line.strip().split(' ', maxsplit=1)
            property_dict[pid] = pname

    return property_dict


def dump_triplets(triplets, chunk_idx):
    fpath = os.path.join(triplet_output_dir, f'triplets_{REC_IDX}_{chunk_idx}')
    with open(fpath, 'w') as fout:
        for triplet in triplets:
            fout.write('\t'.join(triplet))
            fout.write('\n')


if __name__ == '__main__':
    restart_chunk = 3
    restart_id = 589644 + 1
    restart_step = 2

    # load records    
    commons = sling.Store()
    commons.freeze()
    store = sling.Store(commons)
    recin = sling.RecordReader(record_fpath)

    # load wikipedia mappings
    qid2wikipedia = pickle.load(open('./data/wikipedia_mappings.pickle', 'rb'))

    # load property dictionary
    pid2pname = load_property_dict()

    property_count = defaultdict(int)
    n_selected_entities = 0
    chunk_idx = 0
    triplets = []
    pbar = tqdm(enumerate(recin))
    for row_id, (qid, rec) in pbar:
        if row_id < restart_id:
            continue

        fields = store.parse(rec)

        # check language
        lang = fields['lang']
        is_english = lang is not None and lang.id == '/lang/en'
        if not is_english:
            continue

        s1_qid = fields.id
        s1_name = fields.name

        # lookup wikipedia article mapping
        if s1_qid not in qid2wikipedia:
            continue

        if qid2wikipedia[s1_qid][1] != '/w/item/kind/article':
            continue

        s1_triplets = []
        for property, s2 in fields:
            property_id = property.id
            if property_id.startswith('P') and isinstance(s2, sling.Frame):
                s2_qid = s2.id
                s2_name = s2.name
                if s2_qid is not None and s2_qid.startswith('Q'):
                    s1_triplets.append([s1_qid, property_id, s2_qid])
            else:
                continue

        n_triplets = len(s1_triplets)
        if DATA_TYPE == 'head' and n_triplets > 15:
            triplets += s1_triplets
            n_selected_entities += 1
        elif DATA_TYPE == 'tail' and (n_triplets < 3 and n_triplets > 0):
            triplets += s1_triplets
            n_selected_entities += 1

        pbar.set_description(f'{n_selected_entities}')

        if len(triplets) > 500000:
            dump_triplets(triplets, chunk_idx)
            chunk_idx += 1
            triplets = []

    if len(triplets) > 0:
        dump_triplets(triplets, chunk_idx)

    print(f'# total {DATA_TYPE} entities: {n_selected_entities}')

    recin.close()

