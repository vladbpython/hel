# HEL — Hybrid Event Link

<strong>HEL</strong> is a high-performance channel that unifies
blocking and async synchronization within a single architecture.

I was inspired by:

 - Crossbeam

 - Flume

But it implements its own approach:

A single intrusive wait-list shared between threads and async tasks.

## Why HEL?

Most channel implementations are:

 - either blocking (std::sync, Crossbeam)

 - or async (tokio, async-channel)

 - or maintain separate implementations for each mode

<strong>HEL</strong> bridges both worlds.

## Ideas

 - Intrusive doubly-linked wait list

 - A single SyncList used for:

   - Waker

   - Thread

- Minimal allocations

- Hot path without unnecessary synchronization

- Support for:

  - SPSC

  - MPMC

  - Blocking

  - Async


## Benchmarks

Benchmarks were tested on a MacBook Pro M2 with 32 GB of RAM.

```matlab

=== Latency distribution (100k samples) ===
hel  mpmc hot_path                         p50=   42ns  p99=   42ns  p999=   166ns  max=   9792ns
hel  spsc hot_path                         p50=   41ns  p99=   42ns  p999=   125ns  max=  11083ns
flume     hot_path                         p50=   42ns  p99=   83ns  p999=   125ns  max=   1750ns

hot_path_throughput/hel_mpmc
                        time:   [13.093 ms 13.114 ms 13.137 ms]
                        thrpt:  [76.122 Melem/s 76.254 Melem/s 76.379 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high severe
hot_path_throughput/hel_spsc
                        time:   [5.1774 ms 5.1835 ms 5.1899 ms]
                        thrpt:  [192.68 Melem/s 192.92 Melem/s 193.15 Melem/s]
Found 4 outliers among 100 measurements (4.00%)
  4 (4.00%) high mild
hot_path_throughput/flume
                        time:   [19.745 ms 19.775 ms 19.808 ms]
                        thrpt:  [50.484 Melem/s 50.568 Melem/s 50.645 Melem/s]
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) low mild
  2 (2.00%) high mild
  2 (2.00%) high severe

blocking/hel_mpmc_4p4c  time:   [219.93 ms 223.15 ms 226.66 ms]
                        thrpt:  [4.4119 Melem/s 4.4812 Melem/s 4.5470 Melem/s]
Found 1 outliers among 60 measurements (1.67%)
  1 (1.67%) high mild
blocking/hel_spsc_1p1c  time:   [62.104 ms 63.506 ms 65.100 ms]
                        thrpt:  [15.361 Melem/s 15.747 Melem/s 16.102 Melem/s]
Found 9 outliers among 60 measurements (15.00%)
  1 (1.67%) low mild
  5 (8.33%) high mild
  3 (5.00%) high severe

blocking/flume_4p4c     time:   [463.36 ms 466.01 ms 469.05 ms]
                        thrpt:  [2.1320 Melem/s 2.1459 Melem/s 2.1581 Melem/s]
Found 5 outliers among 60 measurements (8.33%)
  2 (3.33%) high mild
  3 (5.00%) high severe

async/hel_mpmc_4p4c     time:   [126.62 ms 127.54 ms 128.45 ms]
                        thrpt:  [7.7851 Melem/s 7.8408 Melem/s 7.8974 Melem/s]
async/hel_spsc_1p1c     time:   [17.811 ms 18.233 ms 18.686 ms]
                        thrpt:  [53.515 Melem/s 54.846 Melem/s 56.144 Melem/s]
Found 8 outliers among 100 measurements (8.00%)
  5 (5.00%) high mild
  3 (3.00%) high severe
  
async/flume_4p4c        time:   [157.48 ms 159.32 ms 160.71 ms]
                        thrpt:  [6.2223 Melem/s 6.2765 Melem/s 6.3499 Melem/s]
Found 7 outliers among 100 measurements (7.00%)
  3 (3.00%) low severe
  4 (4.00%) low mild

batch_recv/batch_64     time:   [17.063 ms 17.240 ms 17.422 ms]
                        thrpt:  [57.398 Melem/s 58.004 Melem/s 58.607 Melem/s]
Found 1 outliers among 100 measurements (1.00%)
  1 (1.00%) high mild

```