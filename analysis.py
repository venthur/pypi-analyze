import main

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

results = main.get_results()
backends = main.get_backends()

backends = pd.DataFrame(backends.items(), columns=['hash', 'backend'])

results = pd.merge(results, backends, on='hash')
results = results.drop(columns=['path', 'repository', 'hash'])

#results = results.dropna()
results.backend = results.backend.astype('string')
results.backend = results.backend.map(lambda x: x.split('.')[0])
results.backend = results.backend.map(lambda x: x.split('_')[0])

counts = results['backend'].value_counts()
n = len(results)
print(n)
print(counts)
to_remove = counts[4:].index
results['backend'] = results['backend'].replace(to_remove, 'other')

#results = results[~(results['uploaded_on'] > '2024')]
results = results[~(results['uploaded_on'] < '2018')]

order = results.backend.value_counts().index

#bins = pd.date_range(start=results['uploaded_on'].min(), end=results['uploaded_on'].max(), freq='M')
#results['uploaded_on'] = pd.cut(results['uploaded_on'], bins=bins, labels=bins[:-1])


sns.set_theme(palette='colorblind')

BIN_WIDTH = 7 * 4

g = sns.displot(results, x='uploaded_on', hue='backend', element='step',
    binwidth=BIN_WIDTH,
    multiple='fill', stat='percent', hue_order=order, facet_kws={'legend_out': False})
g.figure.set_size_inches(12, 6)
g.set(title=f'Relative distribution of build backends over time. (bin width={BIN_WIDTH} days, {n=:.1e} uploads)')
g.set_axis_labels('Upload date', 'Uploads')
g.tight_layout()
g.figure.savefig('relative.png')

g = sns.displot(results, x='uploaded_on', hue='backend', element='step',
    col='backend', hue_order=order, col_order=order,
    legend=False,
    binwidth=BIN_WIDTH,
)
g.figure.set_size_inches(19, 6)
g.figure.suptitle(f'Absolute distribution of build backends over time. (bin width={BIN_WIDTH} days, {n=:.1e} uploads)')
g.set_axis_labels('Upload date', 'Uploads')
g.tight_layout()
g.figure.savefig('absolute.png')

#sns.displot(results, x='uploaded_on', hue='backend', element='step',
#    multiple='layer', fill=False, hue_order=order, 
#    binwidth=BIN_WIDTH,
#)

#sns.displot(results, x='uploaded_on', hue='backend', element='step',
#    multiple='stack', hue_order=order,
#    binwidth=BIN_WIDTH,
#)

plt.show()
