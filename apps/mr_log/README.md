# Test Hive Performance

## Steps

1. Download traces `fab download_traces`
1. Run `fab parse_tritonsort_log:N` to scale up the dataset for `N` times.

To run the tests

1. `fab start`
1. `fab import_hive_data`
1. Repeatly run `fab test_query_hive` and collect data, of coz..
1. `fab stop` to destroy the cluster and reclaim spaces.
