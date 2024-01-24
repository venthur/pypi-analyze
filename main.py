import tomllib
import pickle
import os.path
import logging

import urllib3
import duckdb
import pandas as pd

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
        skip_reason == '' and
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


def main():
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


if __name__ == '__main__':
    main()
