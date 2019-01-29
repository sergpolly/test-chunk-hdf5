
import pandas as pd
import numpy as np
from time import time



output = 'xxx.h5'
chunk_size = 500000
num_chunks = 100
num_elements = num_chunks*chunk_size


def job(_):
    a = np.random.random(chunk_size).astype(np.float64)
    return pd.DataFrame({"a":a})

write_start = time()
#####################
chunks = map(job, range(num_chunks))
append = False
for chunk in chunks:
    chunk.to_hdf(output,
                 key='results',
                 format='table',
                 complevel=9,
                 complib="blosc:snappy",
                 append=append)
    append = True
######################
write_end = time()

# keep sizes big enough , such that writing time >> ~1 second
time_to_write = write_end - write_start

print("time took to write {} elements \
 in {} chunks to {}: {}".format(num_elements,
                                num_chunks,
                                output,
                                time_to_write))



chunksize_read = 1000000

read_start = time()
########################
res = pd.read_hdf(output,key='results',iterator=True,chunksize=chunksize_read)
## time the following thing with %time or whatever ...
compute_res = [ _['a'].mean() for _ in res ]
#########################
read_end = time()

# keep sizes big enough , such that writing time >> ~1 second
time_to_read = read_end - read_start

print("time took to read {} elements \
 in chunks of {} and to do a mock \
 computation per chunk: {}".format(num_elements,
                                num_chunks,
                                time_to_read))


print("some results:")
print(compute_res[:10])

