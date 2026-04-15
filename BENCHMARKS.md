# Benchmarks

Goblin v0.9.0 vs CubDB — 2026-04-14

## put

Benchmark

Benchmark run from 2026-04-14 16:50:10.231530Z UTC

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
    <td style="white-space: nowrap; text-align: right">20.82</td>
    <td style="white-space: nowrap; text-align: right">48.03 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;258.07%</td>
    <td style="white-space: nowrap; text-align: right">30.93 ms</td>
    <td style="white-space: nowrap; text-align: right">1036.55 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.55</td>
    <td style="white-space: nowrap; text-align: right">86.57 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.73%</td>
    <td style="white-space: nowrap; text-align: right">89.10 ms</td>
    <td style="white-space: nowrap; text-align: right">110.10 ms</td>
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
    <td style="white-space: nowrap;text-align: right">20.82</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.55</td>
    <td style="white-space: nowrap; text-align: right">1.8x</td>
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
    <td style="white-space: nowrap; text-align: right">236.08</td>
    <td style="white-space: nowrap; text-align: right">4.24 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;308.66%</td>
    <td style="white-space: nowrap; text-align: right">3.77 ms</td>
    <td style="white-space: nowrap; text-align: right">5.07 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">114.12</td>
    <td style="white-space: nowrap; text-align: right">8.76 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.60%</td>
    <td style="white-space: nowrap; text-align: right">8.64 ms</td>
    <td style="white-space: nowrap; text-align: right">13.01 ms</td>
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
    <td style="white-space: nowrap;text-align: right">236.08</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">114.12</td>
    <td style="white-space: nowrap; text-align: right">2.07x</td>
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
    <td style="white-space: nowrap; text-align: right">3.77 K</td>
    <td style="white-space: nowrap; text-align: right">265.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;100.33%</td>
    <td style="white-space: nowrap; text-align: right">227.54 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">876.13 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.56 K</td>
    <td style="white-space: nowrap; text-align: right">281.14 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;37.69%</td>
    <td style="white-space: nowrap; text-align: right">263.04 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">802.17 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">3.77 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.56 K</td>
    <td style="white-space: nowrap; text-align: right">1.06x</td>
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
    <td style="white-space: nowrap; text-align: right">722.48</td>
    <td style="white-space: nowrap; text-align: right">1.38 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;23.23%</td>
    <td style="white-space: nowrap; text-align: right">1.34 ms</td>
    <td style="white-space: nowrap; text-align: right">2.50 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">605.03</td>
    <td style="white-space: nowrap; text-align: right">1.65 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;15.34%</td>
    <td style="white-space: nowrap; text-align: right">1.63 ms</td>
    <td style="white-space: nowrap; text-align: right">2.52 ms</td>
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
    <td style="white-space: nowrap;text-align: right">722.48</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">605.03</td>
    <td style="white-space: nowrap; text-align: right">1.19x</td>
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
    <td style="white-space: nowrap; text-align: right">3.51 K</td>
    <td style="white-space: nowrap; text-align: right">285.21 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;35.48%</td>
    <td style="white-space: nowrap; text-align: right">266.54 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">793.19 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.50 K</td>
    <td style="white-space: nowrap; text-align: right">285.34 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;78.27%</td>
    <td style="white-space: nowrap; text-align: right">239.04 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1008.43 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">3.51 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">3.50 K</td>
    <td style="white-space: nowrap; text-align: right">1.0x</td>
  </tr>

</table>

## put_multi

Benchmark

Benchmark run from 2026-04-14 16:51:44.515964Z UTC

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
    <td style="white-space: nowrap; text-align: right">3.32 K</td>
    <td style="white-space: nowrap; text-align: right">0.30 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;36.66%</td>
    <td style="white-space: nowrap; text-align: right">0.28 ms</td>
    <td style="white-space: nowrap; text-align: right">0.75 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.70 K</td>
    <td style="white-space: nowrap; text-align: right">1.43 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;14.54%</td>
    <td style="white-space: nowrap; text-align: right">1.39 ms</td>
    <td style="white-space: nowrap; text-align: right">1.98 ms</td>
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
    <td style="white-space: nowrap;text-align: right">3.32 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.70 K</td>
    <td style="white-space: nowrap; text-align: right">4.73x</td>
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
    <td style="white-space: nowrap; text-align: right">940.42</td>
    <td style="white-space: nowrap; text-align: right">1.06 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.25%</td>
    <td style="white-space: nowrap; text-align: right">0.99 ms</td>
    <td style="white-space: nowrap; text-align: right">1.89 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">104.78</td>
    <td style="white-space: nowrap; text-align: right">9.54 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.19%</td>
    <td style="white-space: nowrap; text-align: right">9.32 ms</td>
    <td style="white-space: nowrap; text-align: right">11.96 ms</td>
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
    <td style="white-space: nowrap;text-align: right">940.42</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">104.78</td>
    <td style="white-space: nowrap; text-align: right">8.98x</td>
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
    <td style="white-space: nowrap; text-align: right">4.42</td>
    <td style="white-space: nowrap; text-align: right">0.23 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.91%</td>
    <td style="white-space: nowrap; text-align: right">0.23 s</td>
    <td style="white-space: nowrap; text-align: right">0.25 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0797</td>
    <td style="white-space: nowrap; text-align: right">12.55 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">12.55 s</td>
    <td style="white-space: nowrap; text-align: right">12.55 s</td>
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
    <td style="white-space: nowrap;text-align: right">4.42</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0797</td>
    <td style="white-space: nowrap; text-align: right">55.4x</td>
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
    <td style="white-space: nowrap; text-align: right">42.69</td>
    <td style="white-space: nowrap; text-align: right">0.0234 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;12.57%</td>
    <td style="white-space: nowrap; text-align: right">0.0237 s</td>
    <td style="white-space: nowrap; text-align: right">0.0342 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.94</td>
    <td style="white-space: nowrap; text-align: right">1.06 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.15%</td>
    <td style="white-space: nowrap; text-align: right">1.08 s</td>
    <td style="white-space: nowrap; text-align: right">1.12 s</td>
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
    <td style="white-space: nowrap;text-align: right">42.69</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.94</td>
    <td style="white-space: nowrap; text-align: right">45.18x</td>
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
    <td style="white-space: nowrap; text-align: right">312.71</td>
    <td style="white-space: nowrap; text-align: right">3.20 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;12.15%</td>
    <td style="white-space: nowrap; text-align: right">3.19 ms</td>
    <td style="white-space: nowrap; text-align: right">4.38 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.41</td>
    <td style="white-space: nowrap; text-align: right">106.32 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;4.11%</td>
    <td style="white-space: nowrap; text-align: right">105.48 ms</td>
    <td style="white-space: nowrap; text-align: right">118.83 ms</td>
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
    <td style="white-space: nowrap;text-align: right">312.71</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.41</td>
    <td style="white-space: nowrap; text-align: right">33.25x</td>
  </tr>

</table>

## get

Benchmark

Benchmark run from 2026-04-14 16:53:07.995750Z UTC

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
    <td style="white-space: nowrap; text-align: right">148.04 K</td>
    <td style="white-space: nowrap; text-align: right">6.75 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;129.73%</td>
    <td style="white-space: nowrap; text-align: right">6.33 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">10.71 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">23.83 K</td>
    <td style="white-space: nowrap; text-align: right">41.97 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;57.82%</td>
    <td style="white-space: nowrap; text-align: right">37.08 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">186.40 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">148.04 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">23.83 K</td>
    <td style="white-space: nowrap; text-align: right">6.21x</td>
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
    <td style="white-space: nowrap; text-align: right">158.59 K</td>
    <td style="white-space: nowrap; text-align: right">6.31 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;139.14%</td>
    <td style="white-space: nowrap; text-align: right">5.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">10.50 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">25.45 K</td>
    <td style="white-space: nowrap; text-align: right">39.29 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.01%</td>
    <td style="white-space: nowrap; text-align: right">37.58 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">78.38 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">158.59 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">25.45 K</td>
    <td style="white-space: nowrap; text-align: right">6.23x</td>
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
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">5.06 K</td>
    <td style="white-space: nowrap; text-align: right">197.81 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;35.31%</td>
    <td style="white-space: nowrap; text-align: right">211.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">353.48 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">3.91 K</td>
    <td style="white-space: nowrap; text-align: right">255.59 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;44.60%</td>
    <td style="white-space: nowrap; text-align: right">279.88 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">529.34 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">5.06 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">3.91 K</td>
    <td style="white-space: nowrap; text-align: right">1.29x</td>
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
    <td style="white-space: nowrap; text-align: right">149.81 K</td>
    <td style="white-space: nowrap; text-align: right">6.68 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;148.77%</td>
    <td style="white-space: nowrap; text-align: right">5.92 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">19.67 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">32.35 K</td>
    <td style="white-space: nowrap; text-align: right">30.91 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;59.28%</td>
    <td style="white-space: nowrap; text-align: right">27.67 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">65.79 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">149.81 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">32.35 K</td>
    <td style="white-space: nowrap; text-align: right">4.63x</td>
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
    <td style="white-space: nowrap; text-align: right">173.41 K</td>
    <td style="white-space: nowrap; text-align: right">5.77 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;167.89%</td>
    <td style="white-space: nowrap; text-align: right">5.46 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">9.25 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">89.55 K</td>
    <td style="white-space: nowrap; text-align: right">11.17 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;70.60%</td>
    <td style="white-space: nowrap; text-align: right">9 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">28.54 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">173.41 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">89.55 K</td>
    <td style="white-space: nowrap; text-align: right">1.94x</td>
  </tr>

</table>

## get_multi

Benchmark

Benchmark run from 2026-04-14 16:54:33.035318Z UTC

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
    <td style="white-space: nowrap; text-align: right">531.33</td>
    <td style="white-space: nowrap; text-align: right">1.88 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.20%</td>
    <td style="white-space: nowrap; text-align: right">1.86 ms</td>
    <td style="white-space: nowrap; text-align: right">2.27 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">25.50</td>
    <td style="white-space: nowrap; text-align: right">39.22 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.25%</td>
    <td style="white-space: nowrap; text-align: right">38.20 ms</td>
    <td style="white-space: nowrap; text-align: right">61.21 ms</td>
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
    <td style="white-space: nowrap;text-align: right">531.33</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">25.50</td>
    <td style="white-space: nowrap; text-align: right">20.84x</td>
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
    <td style="white-space: nowrap; text-align: right">671.84</td>
    <td style="white-space: nowrap; text-align: right">1.49 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.56%</td>
    <td style="white-space: nowrap; text-align: right">1.47 ms</td>
    <td style="white-space: nowrap; text-align: right">1.77 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">23.85</td>
    <td style="white-space: nowrap; text-align: right">41.93 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.63%</td>
    <td style="white-space: nowrap; text-align: right">38.77 ms</td>
    <td style="white-space: nowrap; text-align: right">75.59 ms</td>
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
    <td style="white-space: nowrap;text-align: right">671.84</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">23.85</td>
    <td style="white-space: nowrap; text-align: right">28.17x</td>
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
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">11.16</td>
    <td style="white-space: nowrap; text-align: right">89.59 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.28%</td>
    <td style="white-space: nowrap; text-align: right">88.39 ms</td>
    <td style="white-space: nowrap; text-align: right">116.75 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">6.00</td>
    <td style="white-space: nowrap; text-align: right">166.61 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;16.04%</td>
    <td style="white-space: nowrap; text-align: right">167.62 ms</td>
    <td style="white-space: nowrap; text-align: right">224.29 ms</td>
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
    <td style="white-space: nowrap;text-align: right">11.16</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">6.00</td>
    <td style="white-space: nowrap; text-align: right">1.86x</td>
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
    <td style="white-space: nowrap; text-align: right">800.19</td>
    <td style="white-space: nowrap; text-align: right">1.25 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.64%</td>
    <td style="white-space: nowrap; text-align: right">1.24 ms</td>
    <td style="white-space: nowrap; text-align: right">1.51 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">31.62</td>
    <td style="white-space: nowrap; text-align: right">31.63 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.85%</td>
    <td style="white-space: nowrap; text-align: right">31.70 ms</td>
    <td style="white-space: nowrap; text-align: right">42.13 ms</td>
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
    <td style="white-space: nowrap;text-align: right">800.19</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">31.62</td>
    <td style="white-space: nowrap; text-align: right">25.31x</td>
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
    <td style="white-space: nowrap; text-align: right">3.26 K</td>
    <td style="white-space: nowrap; text-align: right">0.31 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.59%</td>
    <td style="white-space: nowrap; text-align: right">0.30 ms</td>
    <td style="white-space: nowrap; text-align: right">0.39 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.117 K</td>
    <td style="white-space: nowrap; text-align: right">8.56 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;15.41%</td>
    <td style="white-space: nowrap; text-align: right">7.80 ms</td>
    <td style="white-space: nowrap; text-align: right">13.06 ms</td>
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
    <td style="white-space: nowrap;text-align: right">3.26 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.117 K</td>
    <td style="white-space: nowrap; text-align: right">27.9x</td>
  </tr>

</table>

## scan

Benchmark

Benchmark run from 2026-04-14 16:55:55.635436Z UTC

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
    <td style="white-space: nowrap; text-align: right">1.03 K</td>
    <td style="white-space: nowrap; text-align: right">0.97 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;18.34%</td>
    <td style="white-space: nowrap; text-align: right">0.91 ms</td>
    <td style="white-space: nowrap; text-align: right">1.64 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0504 K</td>
    <td style="white-space: nowrap; text-align: right">19.84 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;58.43%</td>
    <td style="white-space: nowrap; text-align: right">20.41 ms</td>
    <td style="white-space: nowrap; text-align: right">43.27 ms</td>
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
    <td style="white-space: nowrap; text-align: right">0.0504 K</td>
    <td style="white-space: nowrap; text-align: right">20.4x</td>
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
    <td style="white-space: nowrap; text-align: right">1.08 K</td>
    <td style="white-space: nowrap; text-align: right">0.93 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;15.65%</td>
    <td style="white-space: nowrap; text-align: right">0.89 ms</td>
    <td style="white-space: nowrap; text-align: right">1.57 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.58 K</td>
    <td style="white-space: nowrap; text-align: right">1.72 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;57.73%</td>
    <td style="white-space: nowrap; text-align: right">1.72 ms</td>
    <td style="white-space: nowrap; text-align: right">3.77 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.08 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.58 K</td>
    <td style="white-space: nowrap; text-align: right">1.86x</td>
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
    <td style="white-space: nowrap; text-align: right">774.02</td>
    <td style="white-space: nowrap; text-align: right">1.29 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;19.69%</td>
    <td style="white-space: nowrap; text-align: right">1.29 ms</td>
    <td style="white-space: nowrap; text-align: right">1.92 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">67.57</td>
    <td style="white-space: nowrap; text-align: right">14.80 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;83.44%</td>
    <td style="white-space: nowrap; text-align: right">12.53 ms</td>
    <td style="white-space: nowrap; text-align: right">57.79 ms</td>
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
    <td style="white-space: nowrap;text-align: right">774.02</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">67.57</td>
    <td style="white-space: nowrap; text-align: right">11.45x</td>
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
    <td style="white-space: nowrap; text-align: right">5.21 K</td>
    <td style="white-space: nowrap; text-align: right">191.88 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;44.51%</td>
    <td style="white-space: nowrap; text-align: right">191.79 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">340.33 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.09 K</td>
    <td style="white-space: nowrap; text-align: right">920.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;19.34%</td>
    <td style="white-space: nowrap; text-align: right">872.96 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1638.68 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">5.21 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.09 K</td>
    <td style="white-space: nowrap; text-align: right">4.8x</td>
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
    <td style="white-space: nowrap; text-align: right">179.89 K</td>
    <td style="white-space: nowrap; text-align: right">5.56 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;195.00%</td>
    <td style="white-space: nowrap; text-align: right">5.25 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">8.50 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">84.64 K</td>
    <td style="white-space: nowrap; text-align: right">11.82 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;55.40%</td>
    <td style="white-space: nowrap; text-align: right">9.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">30.04 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">179.89 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">84.64 K</td>
    <td style="white-space: nowrap; text-align: right">2.13x</td>
  </tr>

</table>
