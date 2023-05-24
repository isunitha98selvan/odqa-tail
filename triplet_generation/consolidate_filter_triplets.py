import os
import sys
import pickle

import sling
import pandas as pd


DATA_TYPE = sys.argv[1]
assert DATA_TYPE in {'head', 'tail'}


def load_property_dict():
    fpath = './properties.txt'
    property_dict = {}
    with open(fpath) as fin:
        for line in fin:
            pid, pname = line.strip().split(' ', maxsplit=1)
            property_dict[pid] = pname

    return property_dict


def load_triplets(rec_idx):
    all_triplets = []
    all_n = 0
    for f_idx in range(16):
        fname = f'./data_{DATA_TYPE}/triplets_{rec_idx}_{f_idx}'
        if not os.path.exists(fname):
            break
        with open(fname) as fin:
            for line in fin:
                all_n += 1
                s1, property_id, s2 = line.strip().split('\t')
                all_triplets.append((s1, property_id, s2))
    return all_triplets


def filter_triplets(kb, all_triplets):
    blacklist_properties = get_blacklist()
    wikipedia_mappings = load_wikipedia_mappings()
    pid2pname = load_property_dict()
    name_table = sling.PhraseTable(
        kb,
        '/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/en/phrase-table.repo'
    )

    ret = []

    for s1_id, property_id, s2_id in all_triplets:
        if property_id in blacklist_properties:
            continue
        if s1_id == s2_id:
            continue
        if wikipedia_mappings[s1_id][1] != '/w/item/kind/article':
            continue

        property_name = pid2pname[property_id]
        s1_frame = kb[s1_id]
        s2_frame = kb[s2_id]

        if s1_frame is None or s2_frame is None or s1_frame.name is None or s2_frame.name is None:
            continue
        try:
            s1_name = str(s1_frame.name)
            s2_name = str(s2_frame.name)
        except:  # name format is not recognized
            continue

        # there are multiple entities with the same surface form as s1
        entity_ids = set()
        for entity in name_table.lookup(s1_name):
            entity_ids.add(entity.id)
        if len(entity_ids) > 1:
            continue

        ret.append((s1_id, property_id, s2_id, s1_name, property_name, s2_name))

    return ret


def get_blacklist():
    return {'P1012', 'P105', 'P106', 'P111', 'P115', 'P1151', 'P1204', 'P122', 'P1299', 'P1313', 'P1343', 'P135', 'P1365', 'P138', 'P1424', 'P1435', 'P1441', 'P1455', 'P1456', 'P1464', 'P1478', 'P1479', 'P1533', 'P1535', 'P1536', 'P1537', 'P155', 'P1552', 'P1557', 'P156', 'P1629', 'P163', 'P1672', 'P171', 'P1754', 'P1811', 'P1877', 'P1889', 'P194', 'P195', 'P2033', 'P2094', 'P2184', 'P2354', 'P237', 'P2384', 'P2388', 'P248', 'P2517', 'P2541', 'P2579', 'P2596', 'P2670', 'P2695', 'P2868', 'P2959', 'P301', 'P3095', 'P3096', 'P31', 'P3113', 'P3342', 'P3403', 'P3450', 'P358', 'P360', 'P361', 'P366', 'P3719', 'P3831', 'P3858', 'P3876', 'P4151', 'P4195', 'P4312', 'P4425', 'P457', 'P460', 'P461', 'P5008', 'P5125', 'P5869', 'P598', 'P5996', 'P6104', 'P6186', 'P6365', 'P642', 'P6477', 'P703', 'P7084', 'P7153', 'P734', 'P735', 'P751', 'P7561', 'P7782', 'P7867', 'P793', 'P7937', 'P805', 'P8225', 'P8402', 'P8464', 'P8596', 'P8646', 'P8933', 'P8989', 'P910', 'P912', 'P9215', 'P971'}


def load_wikipedia_mappings():
    fname = './data/wikipedia_mappings.pickle'
    with open(fname, 'rb') as fin:
        mappings = pickle.load(fin)
    return mappings


def load_kb():
    # load KB
    kb = sling.Store()
    kb.load('/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/kb.sling')
    kb.freeze()
    return kb


def main():
    kb = load_kb()

    data = []
    for rec_idx in range(0, 10):
        triplets = load_triplets(rec_idx)
        triplets = filter_triplets(kb, triplets)
        data.extend(triplets)

    df = pd.DataFrame(
        columns=['s1_ID', 'property_ID', 's2_ID', 's1_name', 'property_name', 's2_name'],
        data=data
    )

    df.to_csv(f'./data/{DATA_TYPE}_triplets.tsv', sep='\t')


if __name__ == '__main__':
    main()
