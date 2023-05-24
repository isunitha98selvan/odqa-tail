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


def find_paths(subgraph, triplet_key):
    s1, p, s2 = triplet_key.split('_')
    correct_answers = set(eval(s2))

    paths = []

    for s1, p1_o1s in subgraph[1].items():
        for p1, o1s in p1_o1s.items():
            for s2 in o1s:
                if s2 in correct_answers:
                    paths.append([s1, p1, s2])
                for p2, o2s in subgraph[2][s2].items():
                    for s3 in o2s:
                        if s3 in correct_answers:
                            paths.append([s1, p1, s2, p2, s3])
                        for p3, o3s in subgraph[3][s3].items():
                            for s4 in o3s:
                                if s4 in correct_answers:
                                    paths.append([s1, p1, s2, p2, s3, p3, s4])
                                for p4, o4s in subgraph[4][s4].items():
                                    for s5 in o4s:
                                        if s5 in correct_answers:
                                            paths.append([s1, p1, s2, p2, s3, p3, s4, p4, s5])
    
    return paths


def main():
    kb = load_kb()

    for dtype in ['head', 'tail']:
        print(dtype)

        df = load_dataset(dtype)
        subgraphs = load_subgraphs(dtype)
        
        answer_found = {1: [], 2: [], 3: [], 4: []}
        triplet_paths = {}
        for s1, p, s2 in tqdm(df[['s1_ID', 'property_ID', 's2_ID']].values):
            triplet_key = f'{s1}_{p}_{s2}'
            subgraph = subgraphs[triplet_key]

            s2 = eval(s2)
            for k in [1, 2, 3, 4]:
                found = False
                khop_subgraph = subgraph[k]
                for parent, children_graph in khop_subgraph.items():
                    if found:
                        break
                    for property, children in children_graph.items():
                        if set(s2) & set(children):
                            answer_found[k].append(triplet_key)
                            found = True
                            break

        for k in [1, 2, 3, 4]:
            print(f'{k}: {len(answer_found[k])} / {len(df)}')

        triplets = set(answer_found[1] + answer_found[2] + answer_found[3] + answer_found[4])
        for triplet in triplets:
            paths = find_paths(subgraphs[triplet], triplet)
            triplet_paths[triplet] = paths

        with open(f'./data/{dtype}_1k_triplet_paths.pickle', 'wb') as fout:
            pickle.dump(triplet_paths, fout)

        data = []
        for s1_id, p_id, s2_id, s1, p, s2, question in df[['s1_ID', 'property_ID', 's2_ID', 's1', 'property', 's2', 'GPT-3 question']].values:
            triplet_key = f'{s1_id}_{p_id}_{s2_id}'
            if triplet_key in triplet_paths:
                for path_id in triplet_paths[triplet_key]:
                    path_name = [kb[_id].name for _id in path_id]
                    data.append([s1_id, p_id, s2_id, s1, p, s2, path_id, path_name, question])

        paths_df = pd.DataFrame(columns=['s1_ID', 'property_ID', 's2_ID', 's1', 'property', 's2', 'path_ID', 'path', 'GPT-3 question'], data=data)
        print('# of unique questions:', len(set(paths_df['GPT-3 question'].values)))
        paths_df.to_csv(f'./data/{dtype}_1k_triplet_paths.csv')
        print()


if __name__ == '__main__':
    main()