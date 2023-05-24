## DPR Reranking experiment

1. `extract_subgraph.py`: extract all 4-hop subgraphs for triplets
2. `find_paths.py`: find paths from s1 to s2 that is not the triplet itself
3. `rerank_docs.py`: rerank documents retrieved by DPR using SBERT to compute text similarity between s1 &rarr; s2 path and document
4. `evaluate_rankings.py`: evaluate document ranking