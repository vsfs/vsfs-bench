# Test Hive Performance

## Steps

1. Download traces `fab download_traces`
1. Run `fab parse_tritonsort_log:amplify=N,seconds=S` to scale up the dataset
for `N` times, and each output file contains `S` seconds of logs.
 * `N=300` is generate about 200 GB Raw Data.

To run the tests

1. `fab start`
1. `fab import_hive_data:csvdir=csv-x300-10s` to import the pre-processed data
for amplify `300` times and `10` seconds per file.
1. Repeatly run `fab test_query_hive:1000000` and collect data, of coz..

For example:

```
for t in 1000000 1500000 2000000; do
  fab test_query_hive:$t | tee hive_${t}_results.txt
done
```
1. `fab stop` to destroy the cluster and reclaim spaces.
