import multiprocess as mp
import pandas as pd
import numpy as np

# what we are trying to achieve with reading is
# very simple :
# read an entire hdf , chunk by chunk and do something
# "light" with it...

# one can test stuff using mean()
# in practice we are going to build some
# histogramms using that stuff ...

aa = pd.read_hdf(f,key='results',iterator=True,chunksize=100000)

## time the following thing with %time or whatever ...
gg = [ii['la_exp.donut.value'].mean() for ii in aa]

# CPU times: user 23min 44s, sys: 1.01 s, total: 23min 45s
# Wall time: 23min 45s
