import pickle
from tqdm import tqdm

import sling
import pandas as pd


def load_kb():
    # load KB
    kb = sling.Store()
    kb.load('/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/kb.sling')
    kb.freeze()
    return kb


def get_1hop_subgraph(kb, entity_qid, exclude_qid_pid=None):
    subgraph = {}
    for neighbor_p, neighbor in kb[entity_qid]:
        neighbor_pid = neighbor_p.id

        if neighbor_pid is None or neighbor_pid[0] != 'P':
            continue
        if (exclude_qid_pid and
            entity_qid == exclude_qid_pid[0] and
            neighbor_pid == exclude_qid_pid[1]):  # exclude the (s1, p) pair in question
            continue
        if not isinstance(neighbor, sling.Frame):
            continue
        neighbor_qid = neighbor.id
        if neighbor_qid is None or neighbor_qid[0] != 'Q':
            continue
        if neighbor_qid == entity_qid:  # exclude 1hop loops
            continue

        subgraph.setdefault(neighbor_pid, []).append(neighbor_qid)

    return subgraph


def get_khop_subgraph(kb, s1_qid, p_pid, k):
    khop_neighbors = {}
    exclude_qid_pid = (s1_qid, p_pid)

    subgraph_1hop = get_1hop_subgraph(kb, s1_qid, exclude_qid_pid=exclude_qid_pid)
    khop_neighbors[1] = {s1_qid: subgraph_1hop}

    for i in range(2, k + 1):
        subgraph_khop = {}
        for parent_qid, property_child in khop_neighbors[i - 1].items():
            for property, children in property_child.items():
                for child_qid in children:
                    subgraph_khop[child_qid] = get_1hop_subgraph(kb, child_qid, exclude_qid_pid=exclude_qid_pid)
        khop_neighbors[i] = subgraph_khop

    return khop_neighbors


def load_dataset(dtype):
    fpath = f'/home/selvansr/capstone/final_data/{dtype}_1k.csv'
    df = pd.read_csv(fpath)
    return df


def main():
    kb = load_kb()

    for dtype in ['head', 'tail']:
        print(dtype)

        triplet_subgraphs = {}
        dataset = load_dataset(dtype)

        for s1, p, s2 in tqdm(dataset[['s1_ID', 'property_ID', 's2_ID']].values):
            triplet_key = f'{s1}_{p}_{s2}'
            triplet_subgraphs[triplet_key] = get_khop_subgraph(kb, s1, p, 2)

        with open(f'./data/{dtype}_triplet_1k_subgraphs.pickle', 'wb') as fout:
            pickle.dump(triplet_subgraphs, fout)


if __name__ == '__main__':
    main()