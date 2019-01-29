# test-chunk-hdf5
test different scenarios on writing/reading a big hdf5 using pandas

i.e. trying to solve https://github.com/mirnylab/cooltools/issues/51 , find a better solution , create a minimal working example to show to the world, or "prove" that it is impossible, whatever ...


Just run this:
```
python super_minimal_test.py
```
it's self-consistent and measures timing ...

Initially I though that the problem is due to multiple columns of different types (float and int), but the in the minimal example it occurs even with a single `float64` column of data !

### todo:
add timing for reading the whole thing in memory
