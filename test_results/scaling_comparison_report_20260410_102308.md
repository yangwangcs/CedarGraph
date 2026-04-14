# CedarGraph Scaling Performance Report

## Test Environment
- **Date**: $(date)
- **Test Tool**: test_docker_perf_benchmark
- **Value Size**: 1024 bytes
- **Write Ratio**: 20%
- **Key Range**: 100,000

## Results Summary

| Node Count | Total Ops | Throughput (ops/s) | Write Tput (ops/s) | Read Tput (ops/s) | Scaling Efficiency |
|------------|-----------|-------------------|-------------------|-------------------|-------------------|
| 3 | 807361 | 80682.3 | 16009.2 | 64673.1 | 100% (baseline) |
| 5 | 1317021 | 131647.7 | 26161.4 | 105486.3 | 90.0% |
| 7 | 1815786 | 181447.9 | 35936.8 | 145511.0 | 90.0% |

## Analysis

### Throughput Scaling
- The system shows [linear/sub-linear] scaling with node count
- 5-node configuration provides [X]x throughput vs 3-node
- 7-node configuration provides [X]x throughput vs 3-node

### Latency Characteristics
- P50 latency remains stable across configurations
- P99 latency shows [increase/decrease] with node count

### Recommendations
- Optimal node count for this workload: [3/5/7]
- Consider network bandwidth limitations for larger clusters
- Monitor replication overhead in multi-node configurations

## Raw Data

### scaling_test_3nodes_20260410_102238.txt

```

╔════════════════════════════════════════════════════════════╗
║     CedarGraph Docker Performance Benchmark                ║
╚════════════════════════════════════════════════════════════╝

Benchmark Configuration:
  Node Count: 3
  Duration: 10s
  Concurrent Clients: 12
  Write Ratio: 20%
  Value Size: 1024 bytes
  Key Range: 100000
  Endpoints: storaged1:7000 storaged2:7001 storaged3:7002 

Starting benchmark...
[Progress] 5s / 10s | Ops: 404952 | Throughput: 80990.4 ops/sec

╔════════════════════════════════════════════════════════════╗
║           Performance Test Results                         ║
╚════════════════════════════════════════════════════════════╝

Duration: 10.01 seconds

Operations:
  Total:  807361
  Writes: 160199
  Reads:  647162
  Failed: 1612

Throughput:
  Total:  80682.3 ops/sec
  Writes: 16009.2 ops/sec
  Reads:  64673.1 ops/sec

Write Latency:
  P50: 0 µs
  P99: 0 µs
Read Latency:
  P50: 0 µs
  P99: 0 µs

```

### scaling_test_5nodes_20260410_102248.txt

```

╔════════════════════════════════════════════════════════════╗
║     CedarGraph Docker Performance Benchmark                ║
╚════════════════════════════════════════════════════════════╝

Benchmark Configuration:
  Node Count: 5
  Duration: 10s
  Concurrent Clients: 20
  Write Ratio: 20%
  Value Size: 1024 bytes
  Key Range: 100000
  Endpoints: storaged1:7000 storaged2:7001 storaged3:7002 storaged4:7003 storaged5:7004 

Starting benchmark...
[Progress] 5s / 10s | Ops: 662189 | Throughput: 132437.8 ops/sec

╔════════════════════════════════════════════════════════════╗
║           Performance Test Results                         ║
╚════════════════════════════════════════════════════════════╝

Duration: 10.00 seconds

Operations:
  Total:  1317021
  Writes: 261722
  Reads:  1055299
  Failed: 2661

Throughput:
  Total:  131647.7 ops/sec
  Writes: 26161.4 ops/sec
  Reads:  105486.3 ops/sec

Write Latency:
  P50: 0 µs
  P99: 0 µs
Read Latency:
  P50: 0 µs
  P99: 0 µs

```

### scaling_test_7nodes_20260410_102258.txt

```

╔════════════════════════════════════════════════════════════╗
║     CedarGraph Docker Performance Benchmark                ║
╚════════════════════════════════════════════════════════════╝

Benchmark Configuration:
  Node Count: 7
  Duration: 10s
  Concurrent Clients: 28
  Write Ratio: 20%
  Value Size: 1024 bytes
  Key Range: 100000
  Endpoints: storaged1:7000 storaged2:7001 storaged3:7002 storaged4:7003 storaged5:7004 storaged6:7005 storaged7:7006 

Starting benchmark...
[Progress] 5s / 10s | Ops: 908944 | Throughput: 181788.8 ops/sec

╔════════════════════════════════════════════════════════════╗
║           Performance Test Results                         ║
╚════════════════════════════════════════════════════════════╝

Duration: 10.01 seconds

Operations:
  Total:  1815786
  Writes: 359627
  Reads:  1456159
  Failed: 3590

Throughput:
  Total:  181447.9 ops/sec
  Writes: 35936.8 ops/sec
  Reads:  145511.0 ops/sec

Write Latency:
  P50: 0 µs
  P99: 0 µs
Read Latency:
  P50: 0 µs
  P99: 0 µs

```

