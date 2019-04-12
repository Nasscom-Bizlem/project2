import json
import os

from project_2 import p2_process


with open('p2materials/test_data.json') as f:
    test_data = json.load(f)


files = test_data.keys()
print('Total', len(files), files)

for i, f in enumerate(files):
    print('Processing file ', f, '(%r/%r)' % (i + 1, len(files)))
    output = p2_process(os.path.join('p2materials', f), verbose=False)['SOITEMS']
    sample = test_data[f]['SOITEMS']
    diff = 0

    if len(output) != len(sample):
        print('\t', 'Not same length', len(output), len(sample))
        continue
        
    for i, item in enumerate(output):
        for key in item.keys():
            if key not in sample[i]:
                diff += 1
                print('\t', key, 'not found in test data index', i)
            elif item[key] != sample[i][key]:
                diff += 1
                print('\t', i, key, item[key], sample[i][key])
        
    if diff == 0:
        print('\tGood content')

print('Done!')

