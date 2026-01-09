import argparse
import gzip
import logging
import os.path
import pickle
import sys
from datetime import datetime

import duckdb
import matplotlib as mpl
import polars as pl
import tomllib
import urllib3
from matplotlib import pyplot as plt
from rich.logging import RichHandler
from rich.progress import track

plt.style.use('tableau-colorblind10')

mpl.rcParams['figure.figsize'] = [12.0, 6.0]
mpl.rcParams['figure.constrained_layout.use'] = True
mpl.rcParams['axes.grid'] = True
mpl.rcParams['grid.alpha'] = 0.5
mpl.rcParams['lines.linewidth'] = 2

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
#    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
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

# top n backends to display, the others are merged into "other"
TOP = 4

def get_results(cachefile):
    """Get query results.

    This method loads directly from the `cachefile` file if it exists.
    Otherwise it queries the parquet files, stores the results in `cachefile`
    and returns the results.

    """
    if os.path.isfile(cachefile):
        logger.info('Loading results from parquet file')
        results = pl.read_parquet(cachefile)
    else:
        logger.info('Querying data from parquet files')
        results = duckdb.query(QUERY)
        results = results.pl()
        results = results.with_columns(
            pl.col('hash').bin.encode(encoding='hex'),
            pl.col('uploaded_on').dt.date()
        )
        results.write_parquet(cachefile)
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
    for k, v in backends.items():
        assert isinstance(k, str) and isinstance(v, str)
    with gzip.open('backends.pickle.gz', 'wb') as fh:
        pickle.dump(backends, fh)


def parse_backend(data):
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
    # sometimes the build backend is not a string (e.g. a list of string) which
    # is not allowed as per PIP-517
    if not isinstance(backend, str):
        backend = 'INVALID_ERROR'
    return backend


def fetch_data():
    results = get_results(RESULTS)
    backends = get_backends()

    # sometimes we have more hashes in `backends` than in `results`. i.e. when
    # upstream lost or removed some. we need to remove the ones that don't
    # exist upstream anymore
    valid_hashes = set(results['hash'].to_list())
    old_n = len(backends)
    backends = {k: v for k, v in backends.items() if k in valid_hashes}
    deleted = old_n - len(backends)
    if deleted != 0:
        logger.info(f'Deleted {deleted} items that have been removed upstream.')

    unique_hashes = results.select(
        pl.col('hash').n_unique(),
    ).item(0, 0)

    results = results.filter(~pl.col('hash').is_in(backends.keys()))

    for i, row in enumerate(track(results.iter_rows(), total=len(results))):
        path, hash_, uploaded_on, repository = row
        url = f"https://raw.githubusercontent.com/pypi-data/pypi-mirror-{repository}/code/{path}"

        if i % 500 == 0:
            logger.info(f"{len(backends)/unique_hashes*100:.2f}% done, {len(results)-i} tasks left. [{uploaded_on}]")
            save_backends(backends)

        if hash_ in backends:
            continue

        try:
            response = urllib3.request('GET', url)
            data = response.data.decode()
        except:
            continue

        backend = parse_backend(data)
        backends[hash_] = backend

    logger.info(f"Finished with {len(backends)/unique_hashes*100:.2f}% done, {unique_hashes - len(backends)} items left. [{uploaded_on}]")

    save_backends(backends)


def quarter_formatter(x, pos=None):
    date = mpl.dates.num2date(x)
    month = date.month
    if month in (1, 2, 3):
        return 'Q1'
    elif month in (4, 5, 6):
        return 'Q2'
    elif month in (7, 8, 9):
        return 'Q3'
    elif month in (10, 11, 12):
        return 'Q4'
    else:
        return ''

def analyze():
    logger.info('Analyzing data')
    logger.info('Loading results')
    results = get_results(RESULTS)
    backends = get_backends()

    backends = pl.DataFrame({
        'hash': backends.keys(),
        'backend': backends.values(),
    }, strict=False)

    results = results.join(backends, on='hash', how='inner')
    results = results.drop(['path', 'repository', 'hash'])

    logger.info('Cleaning data')
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

    # absolute
    top = (
        results.group_by('backend').len().sort('len', descending=True)
        .select('backend').head(TOP).to_series()
    ).to_list()

    # last n days
    cutoff = results['uploaded_on'].max() - pl.duration(days=90)
    top_3mo = (
        results
        .filter(pl.col('uploaded_on') >= cutoff)
        .group_by('backend').len().sort('len', descending=True)
        .select('backend')
        .head(TOP)
        .to_series()
    ).to_list()

    top = list(set(top) | set(top_3mo))

    # print TOP in absolute values
    print('Top backends in absolute numbers',
        results.group_by('backend').len().sort('len', descending=True)
        .head(10)
     )

    results = results.with_columns(
        pl.when(pl.col('backend').is_in(top))
        .then(pl.col('backend'))
        .otherwise(pl.lit('other'))
    )

    results = results.filter(
        #pl.col('uploaded_on') >= pl.date(2018, 1, 1).dt.offset_by('-3mo'),
        pl.col('uploaded_on') >= pl.date(2018, 1, 1),
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

    # quarterly
    results_quarterly = results.with_columns(
        pl.col('uploaded_on')
        .dt.truncate('3mo')
        .dt.offset_by('1mo2w')
    )
    # weekly
    results_weekly = results.with_columns(
        pl.col('uploaded_on')
        .dt.truncate('1w')
        .dt.offset_by('3d')
    )

    grouped_quarterly = (
        results_quarterly.group_by(
            ['uploaded_on', 'backend'],
        )
        .agg(pl.count('backend').alias('count'))
        .sort('uploaded_on')
    )
    grouped_weekly = (
        results_weekly.group_by(
            ['uploaded_on', 'backend'],
        )
        .agg(pl.count('backend').alias('count'))
        .sort('uploaded_on')
    )

    normalized_quarterly = (
        grouped_quarterly.with_columns([
        (
            pl.col('count') / pl.col('count').sum() * 100
        ).over('uploaded_on')
    ]))
    normalized_weekly = (
        grouped_weekly.with_columns([
        (
            pl.col('count') / pl.col('count').sum() * 100
        ).over('uploaded_on')
    ]))
    print('Top relative of last quarter',
        normalized_quarterly[-len(top)-1:].sort('count', descending=True)
    )
    print('Top relative of last week',
        normalized_weekly[-len(top)-1:].sort('count', descending=True)
    )

    #print(results)

    logger.info('Plotting data')

    xmin, xmax = normalized_quarterly['uploaded_on'].min(), normalized_quarterly['uploaded_on'].max()

    fig = plt.figure()

    ax, ax2 = fig.subplots(nrows=2, sharex=True, height_ratios=[5,1],
                           gridspec_kw={'hspace': 0})
    for backend in order:
       p = ax.plot(
               normalized_quarterly.filter(pl.col('backend') == backend)['uploaded_on'][:-1],
               normalized_quarterly.filter(pl.col('backend') == backend)['count'][:-1],
               '-',
               # '.-',
               label=backend)
       ax.plot(normalized_weekly.filter(pl.col('backend') == backend)['uploaded_on'],
               normalized_weekly.filter(pl.col('backend') == backend)['count'],
               '.',
               # '.-',
               color=p[-1].get_color(),
               alpha=0.3,
        )

    EVENTS = (
        ("2024-08-20", "uv 0.3.0: hatchling"),
        ("2025-07-18", "uv 0.8.0: uv_build"),
    )

    for date, text in EVENTS:
        ax.vlines(
            datetime.strptime(date, "%Y-%m-%d"),
            0, 100,
            colors='grey',
            linewidth=1,
            zorder=-1,
        )
        ax.annotate(
            text,
            xy=(datetime.strptime(date, "%Y-%m-%d"), 0),
            xytext=(-1.1, 1.1),
            textcoords="offset fontsize",
            rotation=90,
        )

    ax.set_title('Relative distribution of build backends', y=1.0, pad=-15,
                 backgroundcolor='white')
    #ax.set_xlabel('Date')
    ax.yaxis.set_major_locator(mpl.ticker.MaxNLocator())
    ax.set_ylabel('Distribution (%)')
    ax.xaxis.set_minor_locator(mpl.dates.MonthLocator(bymonth=[2, 5, 8, 11], bymonthday=15))
    ax.xaxis.set_minor_formatter(mpl.ticker.FuncFormatter(quarter_formatter))
    ax.xaxis.set_major_locator(mpl.dates.YearLocator())
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter("\n%Y"))

    ax.set_ylim(0, 100)
    ax.set_xlim((xmin, xmax))
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position('right')
    ax.xaxis.set_ticks_position('none')
    ax.legend(loc='upper left')

    uploads_quarterly = results_quarterly['uploaded_on'].value_counts().sort('uploaded_on')
    ax2.bar(
        uploads_quarterly['uploaded_on'][:-1],
        uploads_quarterly['count'][:-1],
        width=30,
        color='k',
    )
    ax2.bar(
        uploads_quarterly['uploaded_on'][-1],
        uploads_quarterly['count'][-1],
        width=30,
        color='k',
        alpha=0.5,
    )
    ax2.set_title('Quarterly uploads', y=1.0, pad=-15,
                  backgroundcolor='white')
    ax2.set_ylim((0, None))
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Uploads (#)')
    ax2.yaxis.tick_right()
    ax2.yaxis.set_label_position('right')
    ax2.yaxis.set_major_formatter(mpl.ticker.EngFormatter())

    fig.set_layout_engine('tight')
    plt.savefig('relative.png')

    grouped = grouped_quarterly

    fig, axes = plt.subplots(1, len(order), sharex=True, sharey=True)
    for i, backend in enumerate(order):
        color = plt.rcParams['axes.prop_cycle'].by_key()['color'][i]
        axes[i].plot(
            grouped.filter(pl.col('backend') == backend)['uploaded_on'][:-1],
            grouped.filter(pl.col('backend') == backend)['count'][:-1],
            '-',
            # '.-',
            label=backend,
            color=color,
        )
        axes[i].plot(
            grouped.filter(pl.col('backend') == backend)['uploaded_on'][-1],
            grouped.filter(pl.col('backend') == backend)['count'][-1],
            '.',
            # '.-',
            label=backend,
            color=color,
        )


        axes[i].fill_between(
            grouped.filter(pl.col('backend') == backend)['uploaded_on'][:-1],
            0,
            grouped.filter(pl.col('backend') == backend)['count'][:-1],
            label=backend,
            color=color,
            alpha=0.7,
        )
        # axes[i].fill_between(
        #     grouped.filter(pl.col('backend') == backend)['uploaded_on'][-2:],
        #     0,
        #     grouped.filter(pl.col('backend') == backend)['count'][-2:],
        #     label=backend,
        #     color=color,
        #     alpha=0.3,
        # )
        axes[i].set(title=backend)
        axes[i].xaxis.set_minor_locator(mpl.dates.MonthLocator(bymonth=[1,4,7,10]))
        axes[i].xaxis.set_major_locator(mpl.dates.YearLocator())
        axes[i].xaxis.set_major_formatter(mpl.dates.DateFormatter("%Y"))

        axes[i].yaxis.set_major_formatter(mpl.ticker.EngFormatter())
 
        axes[i].set_ylim(0)
        axes[i].set_xlim((xmin, xmax))

    fig.suptitle('Absolute distribution of build backends by quarter')
    fig.autofmt_xdate(rotation=90, ha='center')
    fig.supxlabel('Date')
    fig.supylabel('Uploads (#)')

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
        if trim_dataset(dataset, 'data/'):
            sys.exit(1)


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


    # if dsfiles != dsdirfiles:
    #     return True
    # return False


if __name__ == '__main__':
    main()
