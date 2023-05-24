import sys
import pickle
from collections import defaultdict

import sling
from tqdm import tqdm


def load_property_dict():
    fpath = './properties.txt'
    property_dict = {}
    with open(fpath) as fin:
        for line in fin:
            pid, pname = line.strip().split(' ', maxsplit=1)
            property_dict[pid] = pname

    return property_dict


def load_used_properties():
    properties = set()
    for fpath in ['./data/head_triplets_id.tsv', './data/tail_triplets_id.tsv']:
        with open(fpath) as fin:
            for line in fin:
                s1, property, s2 = line.strip().split('\t')
                properties.add(property)

    return properties


if __name__ == '__main__':
    REC_IDX = sys.argv[1]

    restart_chunk = 3
    restart_id = 589644 + 1
    restart_step = 2

    used_properties = load_used_properties()

    # load records
    commons = sling.Store()
    commons.freeze()
    store = sling.Store(commons)
    record_fpath = f'/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/wikidata-items-0000{REC_IDX}-of-00010.rec'
    recin = sling.RecordReader(record_fpath)

    # load property dictionary
    pid2pname = load_property_dict()

    property2s2 = dict()
    property_count = defaultdict(int)
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

        for property, s2 in fields:
            property_id = property.id
            if (property_id.startswith('P') and 
                property_id in used_properties and
                isinstance(s2, sling.Frame)):
                s2_qid = s2.id
                if s2_qid is not None and s2_qid.startswith('Q'):
                    _s2_cnts = property2s2.setdefault(property_id, dict())
                    _s2_cnts[s2_qid] = _s2_cnts.get(s2_qid, 0) + 1
            else:
                continue

    recin.close()

    with open(f'./data_property_s2/property_s2_{REC_IDX}.pickle', 'wb') as fout:
        pickle.dump(property2s2, fout)
