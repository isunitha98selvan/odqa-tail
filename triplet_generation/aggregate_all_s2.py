import pickle


aggregated_dict = {}

for idx in range(10):
    with open(f'./data_property_s2/property_s2_{idx}.pickle', 'rb') as fin:
        d = pickle.load(fin)
        for property, s2_cnt in d.items():
            _dict = aggregated_dict.setdefault(property, {})
            for s2, cnt in s2_cnt.items():
                s2_cleaned = s2.split()[0]
                _dict[s2_cleaned] = _dict.get(s2_cleaned, 0) + cnt


with open('./data/property_s2.pickle', 'wb') as fout:
    pickle.dump(aggregated_dict, fout)
