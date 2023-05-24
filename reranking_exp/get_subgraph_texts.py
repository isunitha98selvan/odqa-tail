import pickle
from tqdm import tqdm

import sling
import pandas as pd


def load_dataset(dtype):
    df = pd.read_csv(f'/home/selvansr/capstone/final_data/{dtype}_1k.csv')
    return df


def load_subgraphs(dtype):
    with open(f'./data/{dtype}_triplet_1k_subgraphs.pickle', 'rb') as fin:
        subgraphs = pickle.load(fin)
    return subgraphs


def load_kb():
    # load KB
    kb = sling.Store()
    kb.load('/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/kb.sling')
    kb.freeze()
    return kb


def get_text(subgraph, kb):
    triplets = []
    for s1, p1_o1s in subgraph[1].items():
        for p1, o1s in p1_o1s.items():
            for s2 in o1s:
                triplets.extend([s1, p1, s2])
                for p2, o2s in subgraph[2][s2].items():
                    for s3 in o2s:
                        triplets.extend([s2, p2, s3])
    
    triplets = [kb[v].name for v in triplets]

    return triplets


def main():
    kb = load_kb()

    for dtype in ['head', 'tail']:
        print(dtype)

        df = load_dataset(dtype)
        subgraphs = load_subgraphs(dtype)
        
        subgraph_texts = []
        
        for s1_id, p_id, s2_id in df[['s1_ID', 'property_ID', 's2_ID']].values:
            triplet_key = f'{s1_id}_{p_id}_{s2_id}'
            subgraph_text = get_text(subgraphs[triplet_key], kb)
            subgraph_texts.append(subgraph_text)
            
        df['subgraph_text'] = subgraph_texts

        df.to_csv(f'./data/{dtype}_1k_triplet_subgraph.csv')
        print()


if __name__ == '__main__':
    main()