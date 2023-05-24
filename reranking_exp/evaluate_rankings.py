import json
from pprint import pprint


def load_dpr(dtype):
    with open(f'./data/DPR_reranked_{dtype}_1k.json') as fin:
        d = json.load(fin)
    return d


def evaluate_docs(docs):
    sorted_docs = sorted(docs, key=lambda x: x['subgraph_sim_score'], reverse=True)
    ret_reranked = {}
    for k in [1, 10, 20, 50, 100]:
        ret_reranked[k] = False
        for doc in sorted_docs[:k]:
            if doc['has_answer']:
                ret_reranked[k] = True
                break

    sorted_docs = sorted(docs, key=lambda x: x['score'], reverse=True)
    ret_orig = {}
    for k in [1, 10, 20, 50, 100]:
        ret_orig[k] = False
        for doc in sorted_docs[:k]:
            if doc['has_answer']:
                ret_orig[k] = True
                break

    return ret_reranked, ret_orig


def main():
    for dtype in ['tail', 'head']:
        print(f'<{dtype}>')
        doc_sets = load_dpr(dtype)
        
        count_path, count_orig = {}, {}
        for docs in doc_sets:
            ret_reranked, ret_orig = evaluate_docs(docs)
            for k in [1, 10, 20, 50, 100]:
                if ret_reranked[k]:
                    count_path[k] = count_path.get(k, 0) + 1
                if ret_orig[k]:
                    count_orig[k] = count_orig.get(k, 0) + 1

        print('reranked with subgraph')
        for k in [1, 10, 20, 50, 100]:
            print(f'{k}: {count_path[k]} / {len(doc_sets)} ({100. * count_path[k]/len(doc_sets):.2f})')
        print()
        print('original')
        for k in [1, 10, 20, 50, 100]:
            print(f'{k}: {count_orig[k]} / {len(doc_sets)} ({100. * count_orig[k]/len(doc_sets):.2f})')
        print()

if __name__ == '__main__':
    main()