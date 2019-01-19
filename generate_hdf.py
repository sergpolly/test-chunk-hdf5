import multiprocess as mp
import pandas as pd
import numpy as np



# replace score_tile with some mock random data generator ...
# keep only essential columns , as in
# https://github.com/mirnylab/cooltools/issues/51#issuecomment-441396611


## typical # of columns with datatypes :
# bin1_id                      int64
# bin2_id                      int64
# obs.raw                      int32
# la_exp.donut.value         float64
# la_exp.vertical.value      float64
# la_exp.horizontal.value    float64
# la_exp.lowleft.value       float64



# a "typical" size of an output hdf is ~10GB+
# it is somewhat proportional to the size of the
# corresponding cooler, 
# the differences being due to the fact that
# cooler stores an entire matrix, while
# this hdf stores only ~2-XX megabases worth of
# data around the diagonal AND a bunch of float
# columns to store locally-adjusted expected calculations
# aka "donuts" and such ...


def generate_hdf(nitems, item_size, output, nproc):
    """
    This would just go over a list of nitems and
    for each one - generate a little
    nitems - list of indexes/random seeds to generate a small
             chunk of DataFrame for each one
    output - path to the output hdf5
    nproc  - number of cores to use for data generation 
    """

    job = # function (partial item_size) that generates chunks of pandas DataFrames 
    # to be stored as hdf ...

    if nproc > 1:
        pool = mp.Pool(nproc)
        map_ = pool.imap
        print("creating a Pool of {} workers to tackle {} items".format(
                    nproc, nitems))
    else:
        map_ = map
        print("fallback to serial implementation.")
    try:
        # consider using
        # https://github.com/mirnylab/cooler/blob/9e72ee202b0ac6f9d93fd2444d6f94c524962769/cooler/tools.py#L59
        # here:
        chunks = map_(job, range(nitems))
        append = False
        for chunk in chunks:
            chunk.to_hdf(output,
                         key='results',
                         format='table',
                         append=append)
            append = True
    finally:
        if nproc > 1:
            pool.close()


if __name__ == '__main__':
    # whatever - tune it later to generate
    # 1gb output or 10gb output for testing ...
    generate_hdf(nitems=100,
                item_size=1000,
                output="myfavourite.hdf",
                nproc=4)
