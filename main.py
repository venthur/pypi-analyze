import argparse
import tomllib
import pickle
import os.path
import logging

import urllib3
import duckdb
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# │ project_name    │ VARCHAR     │ YES     │
# │ project_version │ VARCHAR     │ YES     │
# │ project_release │ VARCHAR     │ YES     │
# │ uploaded_on     │ TIMESTAMP   │ YES     │
# │ path            │ VARCHAR     │ YES     │
# │ archive_path    │ VARCHAR     │ YES     │
# │ size            │ UBIGINT     │ YES     │
# │ hash            │ BLOB        │ YES     │
# │ skip_reason     │ VARCHAR     │ YES     │
# │ lines           │ UBIGINT     │ YES     │
# │ repository      │ UINTEGER    │ YES     │
QUERY = """
    select
        path,
        hash,
        uploaded_on,
        repository
    from 'data/*.parquet'
    where
        lower(string_split(path, '/')[-1]) == 'pyproject.toml' and
        len(string_split(path, '/')) == 5
    order by uploaded_on desc
"""

RESULTS = 'results.csv.gz'


def get_results():
    if os.path.isfile(RESULTS):
        logger.info('Loading results from csv')
        results = pd.read_csv(RESULTS, parse_dates=['uploaded_on'])
    else:
        logger.info('Querying data from parquet files')
        results = duckdb.query(QUERY)
        results = results.df()
        results.hash = results.hash.apply(bytearray.hex)
        results.to_csv(RESULTS, index=False)
    return results


def get_backends():
    logger.info('Loading saved backends')
    try:
        backends = pickle.load(open('backends.pickle', 'rb'))
    except:
        backends = {}
    return backends


def save_backends(backends):
    pickle.dump(backends, open('backends.pickle', 'wb'))


def fetch_data():
    results = get_results()
    backends = get_backends()

    backends = {k: v for k, v in backends.items() if v is not None}

    # total number of rows
    rows = results.shape[0]
    processed = results.hash.isin(backends)
    skipped = processed.sum()
    logger.info(f"Skipping {skipped} rows")
    results = results[~processed]

    for i, row in enumerate(results.iterrows()):
        path, hash_, uploaded_on, repository = row[1]
        url = f"https://raw.githubusercontent.com/pypi-data/pypi-mirror-{repository}/code/{path}"

        if i % 500 == 0:
            logger.info(f"{i+skipped}/{rows} ({(i+skipped)/rows*100:.2f}%) [{uploaded_on}]")
            save_backends(backends)

        if hash_ in backends:
            continue

        try:
            response = urllib3.request('GET', url)
            data = response.data.decode()
        except:
            continue

        try:
            data = tomllib.loads(data)
        except:
            backend = 'PARSING_ERROR'
        try:
            backend = data['build-system']['build-backend']
        except:
            # fallback to setuptools as per:
            # https://pip.pypa.io/en/stable/reference/build-system/pyproject-toml/#fallback-behaviour
            backend = 'setuptools.build_meta:__legacy__'
        backends[hash_] = backend

    save_backends(backends)


def analyze():
    results = get_results()
    backends = get_backends()

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


def main(arguments=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-f',
        '--fetch-data',
        action='store_true',
        help='Fetch data',
    )
    parser.add_argument(
        '-a',
        '--analyze',
        action='store_true',
        help='Analyze data',
    )
    args = parser.parse_args(arguments)

    if args.fetch_data:
        fetch_data()
    if args.analyze:
        analyze()


if __name__ == '__main__':
    main()
