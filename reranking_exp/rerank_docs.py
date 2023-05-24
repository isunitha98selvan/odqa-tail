import json
import pickle
from tqdm import tqdm

import torch
import pandas as pd

from sentence_transformers import SentenceTransformer


def dump_docs(docs, dtype):
    json.dump(docs, open(f'./data/DPR_reranked_{dtype}_1k.json', 'w'))


def load_dataset(dtype):
    df = pd.read_csv(f'./data/{dtype}_1k_triplet_subgraph.csv')
    return df


def main():
    DEVICE = 'cuda'

    for dtype in ['tail', 'head']:
        print(dtype)

        # load paths
        df = load_dataset(dtype)

        # load model
        model = SentenceTransformer('multi-qa-mpnet-base-dot-v1')
        model = model.eval()
        model = model.to(DEVICE)

        # compute similarity between path and doc
        all_docs = []
        for docs, subgraph_text in tqdm(df[['dpr', 'subgraph_text']].values):
            docs = eval(docs)
            
            subgraph_text = ' '.join(subgraph_text)
            subgraph_enc = model.encode(
                subgraph_text,
                convert_to_tensor=True,
                device=DEVICE,
                normalize_embeddings=True
            )

            texts = [doc['text'] for doc in docs]
            texts_enc = model.encode(
                texts,
                convert_to_tensor=True,
                device=DEVICE,
                normalize_embeddings=True
            )

            cos_sim_scores = torch.matmul(texts_enc, subgraph_enc)
            
            for doc, sim_score in zip(docs, cos_sim_scores):
                doc['subgraph_sim_score'] = float(sim_score)

            all_docs.append(docs)

        # dump results
        dump_docs(all_docs, dtype)


if __name__ == '__main__':
    main()
