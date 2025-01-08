import argparse
import tomllib
import pickle
import os.path
import logging
import gzip

import urllib3
import duckdb
import polars as pl
from matplotlib import pyplot as plt
import matplotlib as mpl

plt.style.use('tableau-colorblind10')

mpl.rcParams['figure.figsize'] = [12.0, 6.0]
# mpl.rcParams['figure.dpi'] = 100
mpl.rcParams['figure.constrained_layout.use'] = True
mpl.rcParams['axes.grid'] = True
mpl.rcParams['grid.alpha'] = 0.5
mpl.rcParams['lines.linewidth'] = 2

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
        with gzip.open('backends.pickle.gz', 'rb') as fh:
            backends = pickle.load(fh)
    except:
        backends = {}
    return backends


def save_backends(backends):
    with gzip.open('backends.pickle.gz', 'wb') as fh:
        pickle.dump(backends, fh)


def fetch_data():
    results = get_results()
    backends = get_backends()

    unique_hashes = results.select(
        pl.col('hash').n_unique(),
    ).item(0, 0)

    backends = {k: v for k, v in backends.items() if v is not None}

    results = results.filter(~pl.col('hash').is_in(backends.keys()))

    for i, row in enumerate(results.iter_rows()):
        path, hash_, uploaded_on, repository = row
        url = f"https://raw.githubusercontent.com/pypi-data/pypi-mirror-{repository}/code/{path}"

        if i % 500 == 0:
            logger.info(f"{len(backends)}/{unique_hashes} ({len(backends)/unique_hashes*100:.2f}%) [{uploaded_on}]")
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
            backend = 'DEFAULT'
        backends[hash_] = backend

    save_backends(backends)


def analyze():
    results = get_results()
    backends = get_backends()

    unique_hashes = results.select(
        pl.col('hash').n_unique(),
    ).item(0, 0)
    logger.info(f"{len(backends)}/{unique_hashes} ({len(backends)/unique_hashes*100:.2f}%)")

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

    # rename 'DEFAULT' to 'setuptools'
    results = results.with_columns(
        pl.col('backend').replace('DEFAULT', 'setuptools')
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

    n = len(results)
    #print(n)

    results = results.filter(
        pl.col('uploaded_on') >= pl.date(2018, 1, 1),
        #pl.col('uploaded_on') >= pl.date(2019, 1, 1),
        #pl.col('uploaded_on') < pl.date(2025, 1, 1),
    )

    order = (
        results.group_by('backend').len().sort('len', descending=True)
        .select('backend').to_series()
    )

    # move 'other' to the end
    order = order.to_list()
    if 'other' in order:
        order.remove('other')
        order.append('other')

    results = results.with_columns(
        pl.col('uploaded_on')
        .dt.truncate('3mo')
    )
    results = results.with_columns(
        pl.col('uploaded_on').dt.year().cast(str) + '-Q' + pl.col('uploaded_on').dt.quarter().cast(str)
    )

    grouped = (
        results.group_by(
            ['uploaded_on', 'backend'],
        )
        .agg(pl.count('backend').alias('count'))
        .sort('uploaded_on')
    )

    normalized = (
        grouped.with_columns([
        (
            pl.col('count') / pl.col('count').sum() * 100
        ).over('uploaded_on')
    ]))

    #print(results)

    xmin, xmax = results['uploaded_on'].min(), results['uploaded_on'].max()
    xticks = [s for s in results['uploaded_on'].sort().unique().to_list() if s.endswith('Q4')]

    fig, ax = plt.subplots()
    for backend in order:
       ax.plot(normalized.filter(pl.col('backend') == backend)['uploaded_on'],
               normalized.filter(pl.col('backend') == backend)['count'],
               label=backend)
    ax.set(title='Relative distribution of build backends by quarter.')
    ax.set_xlabel('Date')
    ax.set_ylabel('Percentage')
    ax.set_xticks(xticks)
    ax.xaxis.set_minor_locator(mpl.ticker.AutoMinorLocator('auto'))
    ax.set_ylim(0)
    ax.set_xlim(xmin, xmax)
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position('right')
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.legend()

    plt.savefig('relative.png')


    fig, axes = plt.subplots(1, len(order), sharex=True, sharey=True)
    for i, backend in enumerate(order):
        color = plt.rcParams['axes.prop_cycle'].by_key()['color'][i]
        axes[i].plot(
            grouped.filter(pl.col('backend') == backend)['uploaded_on'],
            grouped.filter(pl.col('backend') == backend)['count'] / 1000,
            label=backend,
            color=color,
        )

        axes[i].fill_between(
            grouped.filter(pl.col('backend') == backend)['uploaded_on'],
            0,
            grouped.filter(pl.col('backend') == backend)['count'] / 1000,
            label=backend,
            color=color,
            alpha=0.7,
        )
        axes[i].set(title=backend)
        axes[i].set_xticks(xticks)
        axes[i].xaxis.set_minor_locator(mpl.ticker.AutoMinorLocator('auto'))
        axes[i].set_ylim(0)
        axes[i].set_xlim((xmin, None))
        axes[i].spines['right'].set_visible(False)
        axes[i].spines['top'].set_visible(False)

    fig.suptitle('Absolute distribution of build backends by quarter.')
    fig.autofmt_xdate(rotation=90, ha='center')
    fig.supxlabel('Date')
    fig.supylabel('Uploads (in thousands)')

    plt.savefig('absolute.png')

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
