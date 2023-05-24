import sling
import pickle
from tqdm import tqdm

mapping = sling.Store()
mapping.load('/projects/rsalakhugroup/haitians/tools/sling/local/data/e/wiki/en/mapping.sling')


all_mappings = {}


for x in tqdm(mapping):
    try:
        if x.id.startswith('/wp'):
            wikipedia = x.id
            qid = x['/w/item/qid'].id
            kind = x['/w/item/kind'].id

            all_mappings[qid] = (wikipedia, kind)
    except UnicodeDecodeError:
        continue


pickle.dump(all_mappings, open('./mappings.pickle', 'wb'))