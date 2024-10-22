import argparse
import tomllib
import pickle
import os.path
import logging

import urllib3
import duckdb
import polars as pl
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
    order by uploaded_on asc
"""

RESULTS = 'results.parquet'


def get_results():
    if os.path.isfile(RESULTS):
        logger.info('Loading results from parquet file')
        results = pl.read_parquet(RESULTS)
    else:
        logger.info('Querying data from parquet files')
        results = duckdb.query(QUERY)
        results = results.pl()
        results = results.with_columns(
            pl.col('hash').bin.encode(encoding='hex'),
            pl.col('uploaded_on').dt.date()
        )
        results.write_parquet(RESULTS)
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
    results = results.filter(~pl.col('hash').is_in(backends.keys()))
    skipped = rows - len(results)
    logger.info(f"Skipping {skipped} rows")

    for i, row in enumerate(results.iter_rows()):
        path, hash_, uploaded_on, repository = row
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

    backends = pl.DataFrame({
        'hash': backends.keys(),
        'backend': backends.values(),
    }, strict=False)

    results = results.join(backends, on='hash', how='inner')
    results = results.drop(['path', 'repository', 'hash'])

    results = results.with_columns(
        pl.col('backend')
        .str.split('.').list.first()
        .str.split('_').list.first()
        .str.split('-').list.first()
        .str.split(':').list.first()
    )

    top = (
        results.group_by('backend').len().sort('len', descending=True)
        .select('backend').head(4).to_series()
    )

    results = results.with_columns(
        pl.when(pl.col('backend').is_in(top))
        .then(pl.col('backend'))
        .otherwise(pl.lit('other'))
    )

    counts = results['backend'].value_counts()
    n = len(results)
    #print(n)
    #print(counts)

    results = results.filter(
        pl.col('uploaded_on') >= pl.date(2018, 1, 1),
    )

    order = (
        results.group_by('backend').len().sort('len', descending=True)
        .select('backend').to_series()
    )

    grouped = (
        results.sort('uploaded_on')
        .group_by_dynamic('uploaded_on', group_by='backend', every='1mo')
        .agg(pl.len().alias('count'))
    )
    #print(grouped)

    normalized = (
        grouped.with_columns([
        (
            pl.col('count') / pl.col('count').sum()
        ).over('uploaded_on')
    ]))
    #print(normalized)

    #print(results)

    sns.set_theme(palette='colorblind')

    g = sns.relplot(
        normalized, x='uploaded_on', y='count', hue='backend',
        hue_order=order,
        kind='line',
        facet_kws={'legend_out': False},
    )
    g.figure.set_size_inches(12, 6)
    g.set(title='Relative distribution of build backends over time.')
    g.set(ylim=(0, 1))
    g.set_axis_labels('Upload date', 'Uploads')
    g.tight_layout()
    g.figure.savefig('relative_single.png')

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

    parser.add_argument(
        '-t',
        '--trim-dataset',
        nargs=1,
        help='Trim dataset',
    )

    args = parser.parse_args(arguments)

    if args.fetch_data:
        fetch_data()
    if args.analyze:
        analyze()
    if args.trim_dataset:
        dataset = args.trim_dataset[0]
        trim_dataset(dataset, 'data/')


def trim_dataset(dsfile, dsdir):
    dsfiles = set()
    with open(dsfile) as fh:
        for line in fh:
            filename = line.strip().split('/')[-1]
            dsfiles.add(filename)

    dsdirfiles = set()
    with os.scandir(dsdir) as it:
        for entry in it:
            if entry.is_file():
                dsdirfiles.add(entry.name)

    for file in dsdirfiles - dsfiles:
        print(f"Deleting {file}")
        # delete the file
        os.remove(os.path.join(dsdir, file))


if __name__ == '__main__':
    main()
