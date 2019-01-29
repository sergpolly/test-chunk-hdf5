import multiprocess as mp
import pandas as pd
import numpy as np
from functools import partial



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



# as I've checked with the real call_dots run
# actual chunks are ~500k in size and there are
# ~>600 of them for a typical run (10Mb, 10kb resolution) ...



# a "typical" size of an output hdf is ~10GB+
# it is somewhat proportional to the size of the
# corresponding cooler, 
# the differences being due to the fact that
# cooler stores an entire matrix, while
# this hdf stores only ~2-XX megabases worth of
# data around the diagonal AND a bunch of float
# columns to store locally-adjusted expected calculations
# aka "donuts" and such ...
def generate_chunk(dummy_index, chunk_size):

    #don't really need that dumy_index ...

    dtypes = {"b1": np.int64,
              "b2": np.int64,
              "obs": np.int64,
              "don": np.float64,
              "hor": np.float64,
              "ver": np.float64,
              "ll": np.float64 }

    # b1/b2:


    low = 0
    high = int(2e+9/1e4)
    size = chunk_size
    dtype = dtypes['b1']

    b1 = np.random.randint(low,high,size,dtype)
    b2 = np.random.randint(low,high,size,dtype)

    # obs:
    low = 0
    high = 4000
    size = chunk_size
    dtype = dtypes['obs']

    obs = np.random.randint(low,high,size,dtype)

    low = 0.0
    high = 40.0
    dtype = dtypes['don']

    # la-exp: don,hor,ver,ll:
    don = ((high - low)*np.random.random(chunk_size) + low).astype(dtype)
    hor = ((high - low)*np.random.random(chunk_size) + low).astype(dtype)
    ver = ((high - low)*np.random.random(chunk_size) + low).astype(dtype)
    ll  = ((high - low)*np.random.random(chunk_size) + low).astype(dtype)

    return pd.DataFrame({"b1": b1, "b2": b2,
                        "obs": obs, "don": don,
                        "hor": hor, "ver": ver,
                        "ll": ll })



def generate_hdf(nitems, item_size, output, nproc):
    """
    This would just go over a list of nitems and
    for each one - generate a little
    nitems - list of indexes/random seeds to generate a small
             chunk of DataFrame for each one
    output - path to the output hdf5
    nproc  - number of cores to use for data generation 
    """

    job = partial(generate_chunk, chunk_size=item_size)
    # function (partial item_size) that generates chunks of pandas DataFrames 
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
                         complevel=9,
                         complib="blosc:snappy",
                         append=append)
            append = True
    finally:
        if nproc > 1:
            pool.close()

#
# some copy-paste from an original call-dots script ...
#

# chunk.to_hdf(output_path,
#              key='results',
#              format='table',
#              complevel=9,
#              complib="blosc:snappy",
#              append=append)




if __name__ == '__main__':
    # whatever - tune it later to generate
    # 1gb output or 10gb output for testing ...
    generate_hdf(nitems=100,
                item_size=500000,
                output="myfavourite.hdf",
                nproc=4)
