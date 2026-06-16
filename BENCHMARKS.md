# Benchmarks

Goblin v0.11.0 — 2026-06-16

Run with `make benchmarks` (Goblin only) or `make benchmarks CUBDB=1` to include
the CubDB comparison. The `get`/`get_multi` tables report both read hits and
misses; `mixed` reports a mixed read/write workload at varying ratios.

The benchmarks are:

- **put** / **put_multi** — single and batch writes across value sizes (1 B–100 MB) and batch sizes (10–100k).
- **get** / **get_multi** — single and batch reads across dataset sizes (1 kB–1 GB), reporting both read **hits** and **misses** (the bloom-filter reject path).
- **scan** — bounded range queries across dataset sizes.
- **mixed** — a mixed read/write workload at varying ratios (95% / 50% reads) against a warm dataset.

## put

Benchmark

Benchmark run from 2026-06-16 18:10:47.522133Z UTC

## System

Benchmark suite executing on the following system:

<table style="width: 1%">
  <tr>
    <th style="width: 1%; white-space: nowrap">Operating System</th>
    <td>macOS</td>
  </tr><tr>
    <th style="white-space: nowrap">CPU Information</th>
    <td style="white-space: nowrap">Apple M1 Pro</td>
  </tr><tr>
    <th style="white-space: nowrap">Number of Available Cores</th>
    <td style="white-space: nowrap">10</td>
  </tr><tr>
    <th style="white-space: nowrap">Available Memory</th>
    <td style="white-space: nowrap">16 GB</td>
  </tr><tr>
    <th style="white-space: nowrap">Elixir Version</th>
    <td style="white-space: nowrap">1.20.1</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">29.0.2</td>
  </tr>
</table>

## Configuration

Benchmark suite executing with the following configuration:

<table style="width: 1%">
  <tr>
    <th style="width: 1%">:time</th>
    <td style="white-space: nowrap">5 s</td>
  </tr><tr>
    <th>:parallel</th>
    <td style="white-space: nowrap">1</td>
  </tr><tr>
    <th>:warmup</th>
    <td style="white-space: nowrap">2 s</td>
  </tr>
</table>

## Statistics



__Input: 100MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">20.56</td>
    <td style="white-space: nowrap; text-align: right">48.63 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;81.39%</td>
    <td style="white-space: nowrap; text-align: right">37.67 ms</td>
    <td style="white-space: nowrap; text-align: right">247.28 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">10.95</td>
    <td style="white-space: nowrap; text-align: right">91.33 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;13.21%</td>
    <td style="white-space: nowrap; text-align: right">91.09 ms</td>
    <td style="white-space: nowrap; text-align: right">121.66 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap;text-align: right">20.56</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">10.95</td>
    <td style="white-space: nowrap; text-align: right">1.88x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">204.58</td>
    <td style="white-space: nowrap; text-align: right">4.89 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;173.10%</td>
    <td style="white-space: nowrap; text-align: right">4.05 ms</td>
    <td style="white-space: nowrap; text-align: right">17.79 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">114.76</td>
    <td style="white-space: nowrap; text-align: right">8.71 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.81%</td>
    <td style="white-space: nowrap; text-align: right">8.62 ms</td>
    <td style="white-space: nowrap; text-align: right">12.35 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap;text-align: right">204.58</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">114.76</td>
    <td style="white-space: nowrap; text-align: right">1.78x</td>
  </tr>

</table>




__Input: 1B__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.97 K</td>
    <td style="white-space: nowrap; text-align: right">252.15 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;96.48%</td>
    <td style="white-space: nowrap; text-align: right">218 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">853.05 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.52 K</td>
    <td style="white-space: nowrap; text-align: right">283.82 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;36.67%</td>
    <td style="white-space: nowrap; text-align: right">263.54 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">821.66 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap;text-align: right">3.97 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.52 K</td>
    <td style="white-space: nowrap; text-align: right">1.13x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.05 K</td>
    <td style="white-space: nowrap; text-align: right">0.95 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;72.59%</td>
    <td style="white-space: nowrap; text-align: right">0.86 ms</td>
    <td style="white-space: nowrap; text-align: right">1.64 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">0.58 K</td>
    <td style="white-space: nowrap; text-align: right">1.71 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;13.76%</td>
    <td style="white-space: nowrap; text-align: right">1.68 ms</td>
    <td style="white-space: nowrap; text-align: right">2.34 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap;text-align: right">1.05 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">0.58 K</td>
    <td style="white-space: nowrap; text-align: right">1.81x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.27 K</td>
    <td style="white-space: nowrap; text-align: right">305.74 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;40.66%</td>
    <td style="white-space: nowrap; text-align: right">278.13 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">848.14 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">2.94 K</td>
    <td style="white-space: nowrap; text-align: right">340.03 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;102.97%</td>
    <td style="white-space: nowrap; text-align: right">252.54 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1394.53 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap;text-align: right">3.27 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">2.94 K</td>
    <td style="white-space: nowrap; text-align: right">1.11x</td>
  </tr>

</table>

## put_multi

Benchmark

Benchmark run from 2026-06-16 18:12:21.426924Z UTC

## System

Benchmark suite executing on the following system:

<table style="width: 1%">
  <tr>
    <th style="width: 1%; white-space: nowrap">Operating System</th>
    <td>macOS</td>
  </tr><tr>
    <th style="white-space: nowrap">CPU Information</th>
    <td style="white-space: nowrap">Apple M1 Pro</td>
  </tr><tr>
    <th style="white-space: nowrap">Number of Available Cores</th>
    <td style="white-space: nowrap">10</td>
  </tr><tr>
    <th style="white-space: nowrap">Available Memory</th>
    <td style="white-space: nowrap">16 GB</td>
  </tr><tr>
    <th style="white-space: nowrap">Elixir Version</th>
    <td style="white-space: nowrap">1.20.1</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">29.0.2</td>
  </tr>
</table>

## Configuration

Benchmark suite executing with the following configuration:

<table style="width: 1%">
  <tr>
    <th style="width: 1%">:time</th>
    <td style="white-space: nowrap">5 s</td>
  </tr><tr>
    <th>:parallel</th>
    <td style="white-space: nowrap">1</td>
  </tr><tr>
    <th>:warmup</th>
    <td style="white-space: nowrap">2 s</td>
  </tr>
</table>

## Statistics



__Input: 10__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">3.70 K</td>
    <td style="white-space: nowrap; text-align: right">0.27 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;47.48%</td>
    <td style="white-space: nowrap; text-align: right">0.25 ms</td>
    <td style="white-space: nowrap; text-align: right">0.72 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.67 K</td>
    <td style="white-space: nowrap; text-align: right">1.50 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;52.99%</td>
    <td style="white-space: nowrap; text-align: right">1.42 ms</td>
    <td style="white-space: nowrap; text-align: right">2.64 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap;text-align: right">3.70 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.67 K</td>
    <td style="white-space: nowrap; text-align: right">5.55x</td>
  </tr>

</table>




__Input: 100__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">668.71</td>
    <td style="white-space: nowrap; text-align: right">1.50 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;13.79%</td>
    <td style="white-space: nowrap; text-align: right">1.50 ms</td>
    <td style="white-space: nowrap; text-align: right">2.22 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">77.71</td>
    <td style="white-space: nowrap; text-align: right">12.87 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.47%</td>
    <td style="white-space: nowrap; text-align: right">11.33 ms</td>
    <td style="white-space: nowrap; text-align: right">25.98 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap;text-align: right">668.71</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">77.71</td>
    <td style="white-space: nowrap; text-align: right">8.6x</td>
  </tr>

</table>




__Input: 100_000__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">3.87</td>
    <td style="white-space: nowrap; text-align: right">0.26 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.19%</td>
    <td style="white-space: nowrap; text-align: right">0.26 s</td>
    <td style="white-space: nowrap; text-align: right">0.32 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0754</td>
    <td style="white-space: nowrap; text-align: right">13.26 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">13.26 s</td>
    <td style="white-space: nowrap; text-align: right">13.26 s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap;text-align: right">3.87</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0754</td>
    <td style="white-space: nowrap; text-align: right">51.28x</td>
  </tr>

</table>




__Input: 10_000__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">35.00</td>
    <td style="white-space: nowrap; text-align: right">0.0286 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.49%</td>
    <td style="white-space: nowrap; text-align: right">0.0283 s</td>
    <td style="white-space: nowrap; text-align: right">0.0613 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.00</td>
    <td style="white-space: nowrap; text-align: right">1.00 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.18%</td>
    <td style="white-space: nowrap; text-align: right">1.03 s</td>
    <td style="white-space: nowrap; text-align: right">1.11 s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap;text-align: right">35.00</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.00</td>
    <td style="white-space: nowrap; text-align: right">35.07x</td>
  </tr>

</table>




__Input: 1_000__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">285.11</td>
    <td style="white-space: nowrap; text-align: right">3.51 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;15.54%</td>
    <td style="white-space: nowrap; text-align: right">3.49 ms</td>
    <td style="white-space: nowrap; text-align: right">4.38 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.28</td>
    <td style="white-space: nowrap; text-align: right">107.73 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.98%</td>
    <td style="white-space: nowrap; text-align: right">108.41 ms</td>
    <td style="white-space: nowrap; text-align: right">121.66 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap;text-align: right">285.11</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.28</td>
    <td style="white-space: nowrap; text-align: right">30.72x</td>
  </tr>

</table>

## get

Benchmark

Benchmark run from 2026-06-16 18:14:44.762229Z UTC

## System

Benchmark suite executing on the following system:

<table style="width: 1%">
  <tr>
    <th style="width: 1%; white-space: nowrap">Operating System</th>
    <td>macOS</td>
  </tr><tr>
    <th style="white-space: nowrap">CPU Information</th>
    <td style="white-space: nowrap">Apple M1 Pro</td>
  </tr><tr>
    <th style="white-space: nowrap">Number of Available Cores</th>
    <td style="white-space: nowrap">10</td>
  </tr><tr>
    <th style="white-space: nowrap">Available Memory</th>
    <td style="white-space: nowrap">16 GB</td>
  </tr><tr>
    <th style="white-space: nowrap">Elixir Version</th>
    <td style="white-space: nowrap">1.20.1</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">29.0.2</td>
  </tr>
</table>

## Configuration

Benchmark suite executing with the following configuration:

<table style="width: 1%">
  <tr>
    <th style="width: 1%">:time</th>
    <td style="white-space: nowrap">5 s</td>
  </tr><tr>
    <th>:parallel</th>
    <td style="white-space: nowrap">1</td>
  </tr><tr>
    <th>:warmup</th>
    <td style="white-space: nowrap">2 s</td>
  </tr>
</table>

## Statistics



__Input: 100MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">215.66 K</td>
    <td style="white-space: nowrap; text-align: right">4.64 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;245.68%</td>
    <td style="white-space: nowrap; text-align: right">2.71 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">36.13 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">166.80 K</td>
    <td style="white-space: nowrap; text-align: right">6.00 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;188.74%</td>
    <td style="white-space: nowrap; text-align: right">3.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">34.21 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">32.76 K</td>
    <td style="white-space: nowrap; text-align: right">30.52 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;23.86%</td>
    <td style="white-space: nowrap; text-align: right">31.08 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">57.96 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">25.33 K</td>
    <td style="white-space: nowrap; text-align: right">39.48 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;58.50%</td>
    <td style="white-space: nowrap; text-align: right">35.21 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">179.52 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">215.66 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">166.80 K</td>
    <td style="white-space: nowrap; text-align: right">1.29x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">32.76 K</td>
    <td style="white-space: nowrap; text-align: right">6.58x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">25.33 K</td>
    <td style="white-space: nowrap; text-align: right">8.51x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">208.67 K</td>
    <td style="white-space: nowrap; text-align: right">4.79 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;243.65%</td>
    <td style="white-space: nowrap; text-align: right">2.75 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">40.42 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">171.65 K</td>
    <td style="white-space: nowrap; text-align: right">5.83 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;573.03%</td>
    <td style="white-space: nowrap; text-align: right">3.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">34.13 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">33.53 K</td>
    <td style="white-space: nowrap; text-align: right">29.83 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;28.33%</td>
    <td style="white-space: nowrap; text-align: right">29.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">54.82 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">26.43 K</td>
    <td style="white-space: nowrap; text-align: right">37.83 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.21%</td>
    <td style="white-space: nowrap; text-align: right">36 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">74.46 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">208.67 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">171.65 K</td>
    <td style="white-space: nowrap; text-align: right">1.22x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">33.53 K</td>
    <td style="white-space: nowrap; text-align: right">6.22x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">26.43 K</td>
    <td style="white-space: nowrap; text-align: right">7.89x</td>
  </tr>

</table>




__Input: 1GB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">23.79 K</td>
    <td style="white-space: nowrap; text-align: right">0.0420 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;27.18%</td>
    <td style="white-space: nowrap; text-align: right">0.0403 ms</td>
    <td style="white-space: nowrap; text-align: right">0.0786 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">4.18 K</td>
    <td style="white-space: nowrap; text-align: right">0.24 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;45.01%</td>
    <td style="white-space: nowrap; text-align: right">0.27 ms</td>
    <td style="white-space: nowrap; text-align: right">0.50 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">0.108 K</td>
    <td style="white-space: nowrap; text-align: right">9.27 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;65.06%</td>
    <td style="white-space: nowrap; text-align: right">10.17 ms</td>
    <td style="white-space: nowrap; text-align: right">20.19 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">0.0948 K</td>
    <td style="white-space: nowrap; text-align: right">10.55 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.01%</td>
    <td style="white-space: nowrap; text-align: right">11.12 ms</td>
    <td style="white-space: nowrap; text-align: right">12.03 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">23.79 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">4.18 K</td>
    <td style="white-space: nowrap; text-align: right">5.7x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">0.108 K</td>
    <td style="white-space: nowrap; text-align: right">220.56x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">0.0948 K</td>
    <td style="white-space: nowrap; text-align: right">251.02x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">213.48 K</td>
    <td style="white-space: nowrap; text-align: right">4.68 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;419.46%</td>
    <td style="white-space: nowrap; text-align: right">2.67 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">37 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">182.26 K</td>
    <td style="white-space: nowrap; text-align: right">5.49 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;217.39%</td>
    <td style="white-space: nowrap; text-align: right">3.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">31.87 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">45.63 K</td>
    <td style="white-space: nowrap; text-align: right">21.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;31.74%</td>
    <td style="white-space: nowrap; text-align: right">18.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">41.42 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">34.07 K</td>
    <td style="white-space: nowrap; text-align: right">29.35 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;34.05%</td>
    <td style="white-space: nowrap; text-align: right">25.17 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">61.92 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">213.48 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">182.26 K</td>
    <td style="white-space: nowrap; text-align: right">1.17x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">45.63 K</td>
    <td style="white-space: nowrap; text-align: right">4.68x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">34.07 K</td>
    <td style="white-space: nowrap; text-align: right">6.27x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">381.22 K</td>
    <td style="white-space: nowrap; text-align: right">2.62 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;372.65%</td>
    <td style="white-space: nowrap; text-align: right">2.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">3.50 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">217.66 K</td>
    <td style="white-space: nowrap; text-align: right">4.59 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;431.62%</td>
    <td style="white-space: nowrap; text-align: right">2.58 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">35.75 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">191.39 K</td>
    <td style="white-space: nowrap; text-align: right">5.22 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;253.42%</td>
    <td style="white-space: nowrap; text-align: right">3.04 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">34.12 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">94.10 K</td>
    <td style="white-space: nowrap; text-align: right">10.63 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;54.41%</td>
    <td style="white-space: nowrap; text-align: right">8.71 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">25.75 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">381.22 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">217.66 K</td>
    <td style="white-space: nowrap; text-align: right">1.75x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">191.39 K</td>
    <td style="white-space: nowrap; text-align: right">1.99x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">94.10 K</td>
    <td style="white-space: nowrap; text-align: right">4.05x</td>
  </tr>

</table>

## get_multi

Benchmark

Benchmark run from 2026-06-16 18:17:07.433256Z UTC

## System

Benchmark suite executing on the following system:

<table style="width: 1%">
  <tr>
    <th style="width: 1%; white-space: nowrap">Operating System</th>
    <td>macOS</td>
  </tr><tr>
    <th style="white-space: nowrap">CPU Information</th>
    <td style="white-space: nowrap">Apple M1 Pro</td>
  </tr><tr>
    <th style="white-space: nowrap">Number of Available Cores</th>
    <td style="white-space: nowrap">10</td>
  </tr><tr>
    <th style="white-space: nowrap">Available Memory</th>
    <td style="white-space: nowrap">16 GB</td>
  </tr><tr>
    <th style="white-space: nowrap">Elixir Version</th>
    <td style="white-space: nowrap">1.20.1</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">29.0.2</td>
  </tr>
</table>

## Configuration

Benchmark suite executing with the following configuration:

<table style="width: 1%">
  <tr>
    <th style="width: 1%">:time</th>
    <td style="white-space: nowrap">5 s</td>
  </tr><tr>
    <th>:parallel</th>
    <td style="white-space: nowrap">1</td>
  </tr><tr>
    <th>:warmup</th>
    <td style="white-space: nowrap">2 s</td>
  </tr>
</table>

## Statistics



__Input: 100MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">1520.20</td>
    <td style="white-space: nowrap; text-align: right">0.66 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.74%</td>
    <td style="white-space: nowrap; text-align: right">0.65 ms</td>
    <td style="white-space: nowrap; text-align: right">0.74 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">548.00</td>
    <td style="white-space: nowrap; text-align: right">1.82 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.81%</td>
    <td style="white-space: nowrap; text-align: right">1.82 ms</td>
    <td style="white-space: nowrap; text-align: right">2.16 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">38.70</td>
    <td style="white-space: nowrap; text-align: right">25.84 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;2.88%</td>
    <td style="white-space: nowrap; text-align: right">25.57 ms</td>
    <td style="white-space: nowrap; text-align: right">29.60 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">26.20</td>
    <td style="white-space: nowrap; text-align: right">38.17 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.80%</td>
    <td style="white-space: nowrap; text-align: right">36.94 ms</td>
    <td style="white-space: nowrap; text-align: right">48.29 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">1520.20</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">548.00</td>
    <td style="white-space: nowrap; text-align: right">2.77x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">38.70</td>
    <td style="white-space: nowrap; text-align: right">39.28x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">26.20</td>
    <td style="white-space: nowrap; text-align: right">58.02x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">1628.87</td>
    <td style="white-space: nowrap; text-align: right">0.61 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;4.55%</td>
    <td style="white-space: nowrap; text-align: right">0.60 ms</td>
    <td style="white-space: nowrap; text-align: right">0.70 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">675.56</td>
    <td style="white-space: nowrap; text-align: right">1.48 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;15.08%</td>
    <td style="white-space: nowrap; text-align: right">1.45 ms</td>
    <td style="white-space: nowrap; text-align: right">1.92 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">38.22</td>
    <td style="white-space: nowrap; text-align: right">26.17 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;7.28%</td>
    <td style="white-space: nowrap; text-align: right">25.30 ms</td>
    <td style="white-space: nowrap; text-align: right">33.78 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">27.70</td>
    <td style="white-space: nowrap; text-align: right">36.10 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;2.32%</td>
    <td style="white-space: nowrap; text-align: right">35.89 ms</td>
    <td style="white-space: nowrap; text-align: right">38.64 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">1628.87</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">675.56</td>
    <td style="white-space: nowrap; text-align: right">2.41x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">38.22</td>
    <td style="white-space: nowrap; text-align: right">42.62x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">27.70</td>
    <td style="white-space: nowrap; text-align: right">58.8x</td>
  </tr>

</table>




__Input: 1GB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">118.82</td>
    <td style="white-space: nowrap; text-align: right">8.42 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;18.93%</td>
    <td style="white-space: nowrap; text-align: right">7.17 ms</td>
    <td style="white-space: nowrap; text-align: right">10.19 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">24.90</td>
    <td style="white-space: nowrap; text-align: right">40.16 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.75%</td>
    <td style="white-space: nowrap; text-align: right">40.39 ms</td>
    <td style="white-space: nowrap; text-align: right">49.50 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">7.08</td>
    <td style="white-space: nowrap; text-align: right">141.30 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.38%</td>
    <td style="white-space: nowrap; text-align: right">140.01 ms</td>
    <td style="white-space: nowrap; text-align: right">173.60 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">3.85</td>
    <td style="white-space: nowrap; text-align: right">259.78 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.01%</td>
    <td style="white-space: nowrap; text-align: right">260.71 ms</td>
    <td style="white-space: nowrap; text-align: right">326.41 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">118.82</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">24.90</td>
    <td style="white-space: nowrap; text-align: right">4.77x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">7.08</td>
    <td style="white-space: nowrap; text-align: right">16.79x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">3.85</td>
    <td style="white-space: nowrap; text-align: right">30.87x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">2.08 K</td>
    <td style="white-space: nowrap; text-align: right">0.48 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;4.73%</td>
    <td style="white-space: nowrap; text-align: right">0.47 ms</td>
    <td style="white-space: nowrap; text-align: right">0.57 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">1.12 K</td>
    <td style="white-space: nowrap; text-align: right">0.89 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.11%</td>
    <td style="white-space: nowrap; text-align: right">0.89 ms</td>
    <td style="white-space: nowrap; text-align: right">1.13 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">0.0527 K</td>
    <td style="white-space: nowrap; text-align: right">18.99 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.29%</td>
    <td style="white-space: nowrap; text-align: right">18.80 ms</td>
    <td style="white-space: nowrap; text-align: right">24.74 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">0.0370 K</td>
    <td style="white-space: nowrap; text-align: right">27.04 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.94%</td>
    <td style="white-space: nowrap; text-align: right">26.69 ms</td>
    <td style="white-space: nowrap; text-align: right">32.42 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">2.08 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">1.12 K</td>
    <td style="white-space: nowrap; text-align: right">1.86x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">0.0527 K</td>
    <td style="white-space: nowrap; text-align: right">39.57x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">0.0370 K</td>
    <td style="white-space: nowrap; text-align: right">56.35x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">36.14 K</td>
    <td style="white-space: nowrap; text-align: right">27.67 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;12.82%</td>
    <td style="white-space: nowrap; text-align: right">27.04 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">40.30 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">24.67 K</td>
    <td style="white-space: nowrap; text-align: right">40.54 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;16.94%</td>
    <td style="white-space: nowrap; text-align: right">37.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">60.89 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">8.81 K</td>
    <td style="white-space: nowrap; text-align: right">113.53 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;5.96%</td>
    <td style="white-space: nowrap; text-align: right">111.67 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">147.95 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">0.131 K</td>
    <td style="white-space: nowrap; text-align: right">7605.82 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.04%</td>
    <td style="white-space: nowrap; text-align: right">7377.13 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">10591.06 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (miss)</td>
    <td style="white-space: nowrap;text-align: right">36.14 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">24.67 K</td>
    <td style="white-space: nowrap; text-align: right">1.47x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2 (miss)</td>
    <td style="white-space: nowrap; text-align: right">8.81 K</td>
    <td style="white-space: nowrap; text-align: right">4.1x</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2 (hit)</td>
    <td style="white-space: nowrap; text-align: right">0.131 K</td>
    <td style="white-space: nowrap; text-align: right">274.85x</td>
  </tr>

</table>

## scan

Benchmark

Benchmark run from 2026-06-16 18:18:19.324678Z UTC

## System

Benchmark suite executing on the following system:

<table style="width: 1%">
  <tr>
    <th style="width: 1%; white-space: nowrap">Operating System</th>
    <td>macOS</td>
  </tr><tr>
    <th style="white-space: nowrap">CPU Information</th>
    <td style="white-space: nowrap">Apple M1 Pro</td>
  </tr><tr>
    <th style="white-space: nowrap">Number of Available Cores</th>
    <td style="white-space: nowrap">10</td>
  </tr><tr>
    <th style="white-space: nowrap">Available Memory</th>
    <td style="white-space: nowrap">16 GB</td>
  </tr><tr>
    <th style="white-space: nowrap">Elixir Version</th>
    <td style="white-space: nowrap">1.20.1</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">29.0.2</td>
  </tr>
</table>

## Configuration

Benchmark suite executing with the following configuration:

<table style="width: 1%">
  <tr>
    <th style="width: 1%">:time</th>
    <td style="white-space: nowrap">5 s</td>
  </tr><tr>
    <th>:parallel</th>
    <td style="white-space: nowrap">1</td>
  </tr><tr>
    <th>:warmup</th>
    <td style="white-space: nowrap">2 s</td>
  </tr>
</table>

## Statistics



__Input: 100MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">811.73</td>
    <td style="white-space: nowrap; text-align: right">1.23 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;56.42%</td>
    <td style="white-space: nowrap; text-align: right">0.90 ms</td>
    <td style="white-space: nowrap; text-align: right">4.11 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">71.16</td>
    <td style="white-space: nowrap; text-align: right">14.05 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;63.35%</td>
    <td style="white-space: nowrap; text-align: right">13.66 ms</td>
    <td style="white-space: nowrap; text-align: right">33.30 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap;text-align: right">811.73</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">71.16</td>
    <td style="white-space: nowrap; text-align: right">11.41x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.03 K</td>
    <td style="white-space: nowrap; text-align: right">0.97 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;24.88%</td>
    <td style="white-space: nowrap; text-align: right">0.87 ms</td>
    <td style="white-space: nowrap; text-align: right">1.74 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.75 K</td>
    <td style="white-space: nowrap; text-align: right">1.34 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;54.71%</td>
    <td style="white-space: nowrap; text-align: right">1.34 ms</td>
    <td style="white-space: nowrap; text-align: right">2.60 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap;text-align: right">1.03 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.75 K</td>
    <td style="white-space: nowrap; text-align: right">1.37x</td>
  </tr>

</table>




__Input: 1GB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">702.90</td>
    <td style="white-space: nowrap; text-align: right">1.42 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;39.18%</td>
    <td style="white-space: nowrap; text-align: right">1.36 ms</td>
    <td style="white-space: nowrap; text-align: right">2.95 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">66.23</td>
    <td style="white-space: nowrap; text-align: right">15.10 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;72.40%</td>
    <td style="white-space: nowrap; text-align: right">14.74 ms</td>
    <td style="white-space: nowrap; text-align: right">57.65 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap;text-align: right">702.90</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">66.23</td>
    <td style="white-space: nowrap; text-align: right">10.61x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">6.26 K</td>
    <td style="white-space: nowrap; text-align: right">0.160 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;42.70%</td>
    <td style="white-space: nowrap; text-align: right">0.160 ms</td>
    <td style="white-space: nowrap; text-align: right">0.28 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">0.99 K</td>
    <td style="white-space: nowrap; text-align: right">1.01 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;29.88%</td>
    <td style="white-space: nowrap; text-align: right">0.88 ms</td>
    <td style="white-space: nowrap; text-align: right">1.80 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap;text-align: right">6.26 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">0.99 K</td>
    <td style="white-space: nowrap; text-align: right">6.35x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">196.39 K</td>
    <td style="white-space: nowrap; text-align: right">5.09 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;243.59%</td>
    <td style="white-space: nowrap; text-align: right">2.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">34.50 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">90.09 K</td>
    <td style="white-space: nowrap; text-align: right">11.10 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;53.63%</td>
    <td style="white-space: nowrap; text-align: right">9.08 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">27.25 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap;text-align: right">196.39 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">90.09 K</td>
    <td style="white-space: nowrap; text-align: right">2.18x</td>
  </tr>

</table>

## mixed

Benchmark

Benchmark run from 2026-06-16 18:19:20.191237Z UTC

## System

Benchmark suite executing on the following system:

<table style="width: 1%">
  <tr>
    <th style="width: 1%; white-space: nowrap">Operating System</th>
    <td>macOS</td>
  </tr><tr>
    <th style="white-space: nowrap">CPU Information</th>
    <td style="white-space: nowrap">Apple M1 Pro</td>
  </tr><tr>
    <th style="white-space: nowrap">Number of Available Cores</th>
    <td style="white-space: nowrap">10</td>
  </tr><tr>
    <th style="white-space: nowrap">Available Memory</th>
    <td style="white-space: nowrap">16 GB</td>
  </tr><tr>
    <th style="white-space: nowrap">Elixir Version</th>
    <td style="white-space: nowrap">1.20.1</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">29.0.2</td>
  </tr>
</table>

## Configuration

Benchmark suite executing with the following configuration:

<table style="width: 1%">
  <tr>
    <th style="width: 1%">:time</th>
    <td style="white-space: nowrap">5 s</td>
  </tr><tr>
    <th>:parallel</th>
    <td style="white-space: nowrap">1</td>
  </tr><tr>
    <th>:warmup</th>
    <td style="white-space: nowrap">2 s</td>
  </tr>
</table>

## Statistics



__Input: 50% read__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB mixed</td>
    <td style="white-space: nowrap; text-align: right">2.87 K</td>
    <td style="white-space: nowrap; text-align: right">348.81 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;82.72%</td>
    <td style="white-space: nowrap; text-align: right">157.71 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">950.99 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin mixed</td>
    <td style="white-space: nowrap; text-align: right">2.81 K</td>
    <td style="white-space: nowrap; text-align: right">356.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;142.26%</td>
    <td style="white-space: nowrap; text-align: right">236.17 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1756.66 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB mixed</td>
    <td style="white-space: nowrap;text-align: right">2.87 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin mixed</td>
    <td style="white-space: nowrap; text-align: right">2.81 K</td>
    <td style="white-space: nowrap; text-align: right">1.02x</td>
  </tr>

</table>




__Input: 95% read__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Deviation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin mixed</td>
    <td style="white-space: nowrap; text-align: right">22.30 K</td>
    <td style="white-space: nowrap; text-align: right">44.85 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;337.20%</td>
    <td style="white-space: nowrap; text-align: right">4.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">659.68 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB mixed</td>
    <td style="white-space: nowrap; text-align: right">10.82 K</td>
    <td style="white-space: nowrap; text-align: right">92.43 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;179.32%</td>
    <td style="white-space: nowrap; text-align: right">48.25 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">870.13 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin mixed</td>
    <td style="white-space: nowrap;text-align: right">22.30 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB mixed</td>
    <td style="white-space: nowrap; text-align: right">10.82 K</td>
    <td style="white-space: nowrap; text-align: right">2.06x</td>
  </tr>

</table>
