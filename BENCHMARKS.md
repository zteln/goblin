# Benchmarks

Goblin v0.10.0 vs CubDB — 2026-06-03

## put

Benchmark

Benchmark run from 2026-06-03 19:02:19.210474Z UTC

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
    <td style="white-space: nowrap">1.19.0</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">28.1</td>
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
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">20.37</td>
    <td style="white-space: nowrap; text-align: right">49.09 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;103.90%</td>
    <td style="white-space: nowrap; text-align: right">34.52 ms</td>
    <td style="white-space: nowrap; text-align: right">289.96 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.39</td>
    <td style="white-space: nowrap; text-align: right">87.77 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.38%</td>
    <td style="white-space: nowrap; text-align: right">89.15 ms</td>
    <td style="white-space: nowrap; text-align: right">121.15 ms</td>
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
    <td style="white-space: nowrap;text-align: right">20.37</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.39</td>
    <td style="white-space: nowrap; text-align: right">1.79x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">208.90</td>
    <td style="white-space: nowrap; text-align: right">4.79 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;120.73%</td>
    <td style="white-space: nowrap; text-align: right">4.13 ms</td>
    <td style="white-space: nowrap; text-align: right">19.98 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">113.05</td>
    <td style="white-space: nowrap; text-align: right">8.85 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;13.72%</td>
    <td style="white-space: nowrap; text-align: right">8.65 ms</td>
    <td style="white-space: nowrap; text-align: right">16.21 ms</td>
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
    <td style="white-space: nowrap;text-align: right">208.90</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">113.05</td>
    <td style="white-space: nowrap; text-align: right">1.85x</td>
  </tr>

</table>




__Input: 1B__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.59 K</td>
    <td style="white-space: nowrap; text-align: right">278.34 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;45.04%</td>
    <td style="white-space: nowrap; text-align: right">254.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">828.45 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.42 K</td>
    <td style="white-space: nowrap; text-align: right">292.51 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;34.91%</td>
    <td style="white-space: nowrap; text-align: right">276.33 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">812.37 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">3.59 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.42 K</td>
    <td style="white-space: nowrap; text-align: right">1.05x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">985.15</td>
    <td style="white-space: nowrap; text-align: right">1.02 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;23.44%</td>
    <td style="white-space: nowrap; text-align: right">1.00 ms</td>
    <td style="white-space: nowrap; text-align: right">1.68 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">564.23</td>
    <td style="white-space: nowrap; text-align: right">1.77 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.65%</td>
    <td style="white-space: nowrap; text-align: right">1.75 ms</td>
    <td style="white-space: nowrap; text-align: right">2.37 ms</td>
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
    <td style="white-space: nowrap;text-align: right">985.15</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">564.23</td>
    <td style="white-space: nowrap; text-align: right">1.75x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.13 K</td>
    <td style="white-space: nowrap; text-align: right">319.00 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;33.62%</td>
    <td style="white-space: nowrap; text-align: right">298.08 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">838.85 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.04 K</td>
    <td style="white-space: nowrap; text-align: right">329.02 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;104.13%</td>
    <td style="white-space: nowrap; text-align: right">282.13 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1208.60 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">3.13 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.04 K</td>
    <td style="white-space: nowrap; text-align: right">1.03x</td>
  </tr>

</table>

## put_multi

Benchmark

Benchmark run from 2026-06-03 19:03:50.632822Z UTC

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
    <td style="white-space: nowrap">1.19.0</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">28.1</td>
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
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">3.18 K</td>
    <td style="white-space: nowrap; text-align: right">0.31 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.71%</td>
    <td style="white-space: nowrap; text-align: right">0.30 ms</td>
    <td style="white-space: nowrap; text-align: right">0.80 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.65 K</td>
    <td style="white-space: nowrap; text-align: right">1.54 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;18.27%</td>
    <td style="white-space: nowrap; text-align: right">1.52 ms</td>
    <td style="white-space: nowrap; text-align: right">2.49 ms</td>
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
    <td style="white-space: nowrap;text-align: right">3.18 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.65 K</td>
    <td style="white-space: nowrap; text-align: right">4.9x</td>
  </tr>

</table>




__Input: 100__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">739.97</td>
    <td style="white-space: nowrap; text-align: right">1.35 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;25.36%</td>
    <td style="white-space: nowrap; text-align: right">1.37 ms</td>
    <td style="white-space: nowrap; text-align: right">2.25 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">85.24</td>
    <td style="white-space: nowrap; text-align: right">11.73 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;104.80%</td>
    <td style="white-space: nowrap; text-align: right">9.31 ms</td>
    <td style="white-space: nowrap; text-align: right">68.22 ms</td>
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
    <td style="white-space: nowrap;text-align: right">739.97</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">85.24</td>
    <td style="white-space: nowrap; text-align: right">8.68x</td>
  </tr>

</table>




__Input: 100_000__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">3.86</td>
    <td style="white-space: nowrap; text-align: right">0.26 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.89%</td>
    <td style="white-space: nowrap; text-align: right">0.26 s</td>
    <td style="white-space: nowrap; text-align: right">0.31 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0813</td>
    <td style="white-space: nowrap; text-align: right">12.31 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">12.31 s</td>
    <td style="white-space: nowrap; text-align: right">12.31 s</td>
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
    <td style="white-space: nowrap;text-align: right">3.86</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0813</td>
    <td style="white-space: nowrap; text-align: right">47.45x</td>
  </tr>

</table>




__Input: 10_000__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">34.60</td>
    <td style="white-space: nowrap; text-align: right">28.90 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.39%</td>
    <td style="white-space: nowrap; text-align: right">29.40 ms</td>
    <td style="white-space: nowrap; text-align: right">35.21 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.02</td>
    <td style="white-space: nowrap; text-align: right">979.48 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.39%</td>
    <td style="white-space: nowrap; text-align: right">995.16 ms</td>
    <td style="white-space: nowrap; text-align: right">1077.62 ms</td>
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
    <td style="white-space: nowrap;text-align: right">34.60</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.02</td>
    <td style="white-space: nowrap; text-align: right">33.89x</td>
  </tr>

</table>




__Input: 1_000__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">280.64</td>
    <td style="white-space: nowrap; text-align: right">3.56 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.49%</td>
    <td style="white-space: nowrap; text-align: right">3.53 ms</td>
    <td style="white-space: nowrap; text-align: right">4.67 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.77</td>
    <td style="white-space: nowrap; text-align: right">102.31 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;2.90%</td>
    <td style="white-space: nowrap; text-align: right">102.32 ms</td>
    <td style="white-space: nowrap; text-align: right">107.50 ms</td>
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
    <td style="white-space: nowrap;text-align: right">280.64</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.77</td>
    <td style="white-space: nowrap; text-align: right">28.71x</td>
  </tr>

</table>

## get

Benchmark

Benchmark run from 2026-06-03 19:05:03.252942Z UTC

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
    <td style="white-space: nowrap">1.19.0</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">28.1</td>
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
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">164.93 K</td>
    <td style="white-space: nowrap; text-align: right">6.06 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;188.76%</td>
    <td style="white-space: nowrap; text-align: right">3.96 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">36.29 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">21.06 K</td>
    <td style="white-space: nowrap; text-align: right">47.49 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;108.60%</td>
    <td style="white-space: nowrap; text-align: right">38.13 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">193.13 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap;text-align: right">164.93 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">21.06 K</td>
    <td style="white-space: nowrap; text-align: right">7.83x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">180.58 K</td>
    <td style="white-space: nowrap; text-align: right">5.54 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;196.33%</td>
    <td style="white-space: nowrap; text-align: right">3.46 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">33.25 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">27.05 K</td>
    <td style="white-space: nowrap; text-align: right">36.97 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;27.37%</td>
    <td style="white-space: nowrap; text-align: right">35.88 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">69.79 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap;text-align: right">180.58 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">27.05 K</td>
    <td style="white-space: nowrap; text-align: right">6.68x</td>
  </tr>

</table>




__Input: 1GB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">4.25 K</td>
    <td style="white-space: nowrap; text-align: right">0.24 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;43.82%</td>
    <td style="white-space: nowrap; text-align: right">0.26 ms</td>
    <td style="white-space: nowrap; text-align: right">0.49 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.59 K</td>
    <td style="white-space: nowrap; text-align: right">1.70 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;53.67%</td>
    <td style="white-space: nowrap; text-align: right">1.81 ms</td>
    <td style="white-space: nowrap; text-align: right">4.14 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap;text-align: right">4.25 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.59 K</td>
    <td style="white-space: nowrap; text-align: right">7.25x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">182.52 K</td>
    <td style="white-space: nowrap; text-align: right">5.48 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;221.39%</td>
    <td style="white-space: nowrap; text-align: right">3.29 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">33.25 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">34.49 K</td>
    <td style="white-space: nowrap; text-align: right">29.00 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;59.87%</td>
    <td style="white-space: nowrap; text-align: right">24.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">60.79 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap;text-align: right">182.52 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">34.49 K</td>
    <td style="white-space: nowrap; text-align: right">5.29x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">154.56 K</td>
    <td style="white-space: nowrap; text-align: right">6.47 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;313.77%</td>
    <td style="white-space: nowrap; text-align: right">3.21 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">41.25 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">92.71 K</td>
    <td style="white-space: nowrap; text-align: right">10.79 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;69.19%</td>
    <td style="white-space: nowrap; text-align: right">8.83 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">26.75 &micro;s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap;text-align: right">154.56 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">92.71 K</td>
    <td style="white-space: nowrap; text-align: right">1.67x</td>
  </tr>

</table>

## get_multi

Benchmark

Benchmark run from 2026-06-03 19:06:22.440564Z UTC

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
    <td style="white-space: nowrap">1.19.0</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">28.1</td>
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
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">530.40</td>
    <td style="white-space: nowrap; text-align: right">1.89 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;7.29%</td>
    <td style="white-space: nowrap; text-align: right">1.87 ms</td>
    <td style="white-space: nowrap; text-align: right">2.27 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">19.29</td>
    <td style="white-space: nowrap; text-align: right">51.85 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;47.48%</td>
    <td style="white-space: nowrap; text-align: right">39.01 ms</td>
    <td style="white-space: nowrap; text-align: right">217.68 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap;text-align: right">530.40</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">19.29</td>
    <td style="white-space: nowrap; text-align: right">27.5x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">676.34</td>
    <td style="white-space: nowrap; text-align: right">1.48 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;7.36%</td>
    <td style="white-space: nowrap; text-align: right">1.47 ms</td>
    <td style="white-space: nowrap; text-align: right">1.81 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">26.12</td>
    <td style="white-space: nowrap; text-align: right">38.28 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.17%</td>
    <td style="white-space: nowrap; text-align: right">36.82 ms</td>
    <td style="white-space: nowrap; text-align: right">53.75 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap;text-align: right">676.34</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">26.12</td>
    <td style="white-space: nowrap; text-align: right">25.89x</td>
  </tr>

</table>




__Input: 1GB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">6.18</td>
    <td style="white-space: nowrap; text-align: right">0.162 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.73%</td>
    <td style="white-space: nowrap; text-align: right">0.150 s</td>
    <td style="white-space: nowrap; text-align: right">0.23 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.21</td>
    <td style="white-space: nowrap; text-align: right">4.70 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.55%</td>
    <td style="white-space: nowrap; text-align: right">4.70 s</td>
    <td style="white-space: nowrap; text-align: right">4.72 s</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap;text-align: right">6.18</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.21</td>
    <td style="white-space: nowrap; text-align: right">29.05x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.09 K</td>
    <td style="white-space: nowrap; text-align: right">0.92 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.18%</td>
    <td style="white-space: nowrap; text-align: right">0.91 ms</td>
    <td style="white-space: nowrap; text-align: right">1.15 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0146 K</td>
    <td style="white-space: nowrap; text-align: right">68.27 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.83%</td>
    <td style="white-space: nowrap; text-align: right">69.31 ms</td>
    <td style="white-space: nowrap; text-align: right">111.97 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap;text-align: right">1.09 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0146 K</td>
    <td style="white-space: nowrap; text-align: right">74.24x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">24.74 K</td>
    <td style="white-space: nowrap; text-align: right">0.0404 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.13%</td>
    <td style="white-space: nowrap; text-align: right">0.0395 ms</td>
    <td style="white-space: nowrap; text-align: right">0.0502 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.119 K</td>
    <td style="white-space: nowrap; text-align: right">8.42 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.57%</td>
    <td style="white-space: nowrap; text-align: right">7.90 ms</td>
    <td style="white-space: nowrap; text-align: right">12.61 ms</td>
  </tr>

</table>


Run Time Comparison

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Slower</th>
  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap;text-align: right">24.74 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.119 K</td>
    <td style="white-space: nowrap; text-align: right">208.31x</td>
  </tr>

</table>

## scan

Benchmark

Benchmark run from 2026-06-03 19:07:34.634994Z UTC

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
    <td style="white-space: nowrap">1.19.0</td>
  </tr><tr>
    <th style="white-space: nowrap">Erlang Version</th>
    <td style="white-space: nowrap">28.1</td>
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
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.10 K</td>
    <td style="white-space: nowrap; text-align: right">0.91 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;16.06%</td>
    <td style="white-space: nowrap; text-align: right">0.87 ms</td>
    <td style="white-space: nowrap; text-align: right">1.50 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0701 K</td>
    <td style="white-space: nowrap; text-align: right">14.26 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;60.35%</td>
    <td style="white-space: nowrap; text-align: right">14.11 ms</td>
    <td style="white-space: nowrap; text-align: right">31.20 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.10 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0701 K</td>
    <td style="white-space: nowrap; text-align: right">15.75x</td>
  </tr>

</table>




__Input: 10MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.06 K</td>
    <td style="white-space: nowrap; text-align: right">0.94 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;23.51%</td>
    <td style="white-space: nowrap; text-align: right">0.87 ms</td>
    <td style="white-space: nowrap; text-align: right">1.98 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.75 K</td>
    <td style="white-space: nowrap; text-align: right">1.34 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;54.59%</td>
    <td style="white-space: nowrap; text-align: right">1.34 ms</td>
    <td style="white-space: nowrap; text-align: right">2.62 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.06 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.75 K</td>
    <td style="white-space: nowrap; text-align: right">1.42x</td>
  </tr>

</table>




__Input: 1GB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">770.21</td>
    <td style="white-space: nowrap; text-align: right">1.30 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;25.27%</td>
    <td style="white-space: nowrap; text-align: right">1.29 ms</td>
    <td style="white-space: nowrap; text-align: right">2.02 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">76.55</td>
    <td style="white-space: nowrap; text-align: right">13.06 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;71.94%</td>
    <td style="white-space: nowrap; text-align: right">11.59 ms</td>
    <td style="white-space: nowrap; text-align: right">52.65 ms</td>
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
    <td style="white-space: nowrap;text-align: right">770.21</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">76.55</td>
    <td style="white-space: nowrap; text-align: right">10.06x</td>
  </tr>

</table>




__Input: 1MB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">6.29 K</td>
    <td style="white-space: nowrap; text-align: right">158.89 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;45.38%</td>
    <td style="white-space: nowrap; text-align: right">158.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">275.79 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.17 K</td>
    <td style="white-space: nowrap; text-align: right">853.31 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.14%</td>
    <td style="white-space: nowrap; text-align: right">833.83 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1423.15 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">6.29 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.17 K</td>
    <td style="white-space: nowrap; text-align: right">5.37x</td>
  </tr>

</table>




__Input: 1kB__

Run Time

<table style="width: 1%">
  <tr>
    <th>Name</th>
    <th style="text-align: right">IPS</th>
    <th style="text-align: right">Average</th>
    <th style="text-align: right">Devitation</th>
    <th style="text-align: right">Median</th>
    <th style="text-align: right">99th&nbsp;%</th>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">192.32 K</td>
    <td style="white-space: nowrap; text-align: right">5.20 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;276.44%</td>
    <td style="white-space: nowrap; text-align: right">2.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">34.42 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">87.15 K</td>
    <td style="white-space: nowrap; text-align: right">11.47 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;65.49%</td>
    <td style="white-space: nowrap; text-align: right">9.25 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">29.13 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">192.32 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">87.15 K</td>
    <td style="white-space: nowrap; text-align: right">2.21x</td>
  </tr>

</table>
