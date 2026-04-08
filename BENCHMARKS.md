# Benchmarks

Goblin v0.8.0 vs CubDB — 2026-04-08

## put

Benchmark

Benchmark run from 2026-04-08 16:59:15.636832Z UTC

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
    <td style="white-space: nowrap; text-align: right">14.63</td>
    <td style="white-space: nowrap; text-align: right">68.36 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;194.45%</td>
    <td style="white-space: nowrap; text-align: right">46.82 ms</td>
    <td style="white-space: nowrap; text-align: right">1055.68 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.61</td>
    <td style="white-space: nowrap; text-align: right">86.15 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.64%</td>
    <td style="white-space: nowrap; text-align: right">87.54 ms</td>
    <td style="white-space: nowrap; text-align: right">107.39 ms</td>
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
    <td style="white-space: nowrap;text-align: right">14.63</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.61</td>
    <td style="white-space: nowrap; text-align: right">1.26x</td>
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
    <td style="white-space: nowrap; text-align: right">163.50</td>
    <td style="white-space: nowrap; text-align: right">6.12 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;103.55%</td>
    <td style="white-space: nowrap; text-align: right">5.85 ms</td>
    <td style="white-space: nowrap; text-align: right">7.56 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">126.05</td>
    <td style="white-space: nowrap; text-align: right">7.93 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.12%</td>
    <td style="white-space: nowrap; text-align: right">7.80 ms</td>
    <td style="white-space: nowrap; text-align: right">13.56 ms</td>
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
    <td style="white-space: nowrap;text-align: right">163.50</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">126.05</td>
    <td style="white-space: nowrap; text-align: right">1.3x</td>
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
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">9.76 K</td>
    <td style="white-space: nowrap; text-align: right">102.42 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;26.70%</td>
    <td style="white-space: nowrap; text-align: right">98.42 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">170.54 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.96 K</td>
    <td style="white-space: nowrap; text-align: right">510.36 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;39.63%</td>
    <td style="white-space: nowrap; text-align: right">464 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1488.03 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">9.76 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.96 K</td>
    <td style="white-space: nowrap; text-align: right">4.98x</td>
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
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">983.77</td>
    <td style="white-space: nowrap; text-align: right">1.02 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;41.78%</td>
    <td style="white-space: nowrap; text-align: right">0.89 ms</td>
    <td style="white-space: nowrap; text-align: right">3.06 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">421.49</td>
    <td style="white-space: nowrap; text-align: right">2.37 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;21.59%</td>
    <td style="white-space: nowrap; text-align: right">2.37 ms</td>
    <td style="white-space: nowrap; text-align: right">4.01 ms</td>
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
    <td style="white-space: nowrap;text-align: right">983.77</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">421.49</td>
    <td style="white-space: nowrap; text-align: right">2.33x</td>
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
    <td style="white-space: nowrap; text-align: right">9.12 K</td>
    <td style="white-space: nowrap; text-align: right">109.65 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;34.16%</td>
    <td style="white-space: nowrap; text-align: right">104.88 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">184.82 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.88 K</td>
    <td style="white-space: nowrap; text-align: right">531.34 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;35.96%</td>
    <td style="white-space: nowrap; text-align: right">485.13 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1351.34 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">9.12 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.88 K</td>
    <td style="white-space: nowrap; text-align: right">4.85x</td>
  </tr>

</table>

## put_multi

Benchmark

Benchmark run from 2026-04-08 17:00:48.355529Z UTC

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
    <td style="white-space: nowrap; text-align: right">1.62 K</td>
    <td style="white-space: nowrap; text-align: right">616.69 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;25.50%</td>
    <td style="white-space: nowrap; text-align: right">581.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1283.90 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.01 K</td>
    <td style="white-space: nowrap; text-align: right">991.11 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.83%</td>
    <td style="white-space: nowrap; text-align: right">933.33 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1684.65 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">1.62 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">1.01 K</td>
    <td style="white-space: nowrap; text-align: right">1.61x</td>
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
    <td style="white-space: nowrap; text-align: right">989.74</td>
    <td style="white-space: nowrap; text-align: right">1.01 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;33.52%</td>
    <td style="white-space: nowrap; text-align: right">0.89 ms</td>
    <td style="white-space: nowrap; text-align: right">1.89 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">104.80</td>
    <td style="white-space: nowrap; text-align: right">9.54 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;7.28%</td>
    <td style="white-space: nowrap; text-align: right">9.44 ms</td>
    <td style="white-space: nowrap; text-align: right">11.61 ms</td>
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
    <td style="white-space: nowrap;text-align: right">989.74</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">104.80</td>
    <td style="white-space: nowrap; text-align: right">9.44x</td>
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
    <td style="white-space: nowrap; text-align: right">3.44</td>
    <td style="white-space: nowrap; text-align: right">0.29 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.96%</td>
    <td style="white-space: nowrap; text-align: right">0.29 s</td>
    <td style="white-space: nowrap; text-align: right">0.30 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0778</td>
    <td style="white-space: nowrap; text-align: right">12.86 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">12.86 s</td>
    <td style="white-space: nowrap; text-align: right">12.86 s</td>
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
    <td style="white-space: nowrap;text-align: right">3.44</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0778</td>
    <td style="white-space: nowrap; text-align: right">44.26x</td>
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
    <td style="white-space: nowrap; text-align: right">33.42</td>
    <td style="white-space: nowrap; text-align: right">0.0299 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.21%</td>
    <td style="white-space: nowrap; text-align: right">0.0301 s</td>
    <td style="white-space: nowrap; text-align: right">0.0352 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.94</td>
    <td style="white-space: nowrap; text-align: right">1.07 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.98%</td>
    <td style="white-space: nowrap; text-align: right">1.10 s</td>
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
    <td style="white-space: nowrap;text-align: right">33.42</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.94</td>
    <td style="white-space: nowrap; text-align: right">35.71x</td>
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
    <td style="white-space: nowrap; text-align: right">232.78</td>
    <td style="white-space: nowrap; text-align: right">4.30 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.91%</td>
    <td style="white-space: nowrap; text-align: right">4.26 ms</td>
    <td style="white-space: nowrap; text-align: right">5.48 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.44</td>
    <td style="white-space: nowrap; text-align: right">105.96 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;5.23%</td>
    <td style="white-space: nowrap; text-align: right">104.61 ms</td>
    <td style="white-space: nowrap; text-align: right">122.20 ms</td>
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
    <td style="white-space: nowrap;text-align: right">232.78</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.44</td>
    <td style="white-space: nowrap; text-align: right">24.67x</td>
  </tr>

</table>

## get

Benchmark

Benchmark run from 2026-04-08 17:02:10.464530Z UTC

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
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">22.78 K</td>
    <td style="white-space: nowrap; text-align: right">0.0439 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;65.37%</td>
    <td style="white-space: nowrap; text-align: right">0.0381 ms</td>
    <td style="white-space: nowrap; text-align: right">0.194 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.105 K</td>
    <td style="white-space: nowrap; text-align: right">9.55 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;1.97%</td>
    <td style="white-space: nowrap; text-align: right">9.49 ms</td>
    <td style="white-space: nowrap; text-align: right">10.14 ms</td>
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
    <td style="white-space: nowrap;text-align: right">22.78 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.105 K</td>
    <td style="white-space: nowrap; text-align: right">217.43x</td>
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
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">24.46 K</td>
    <td style="white-space: nowrap; text-align: right">40.89 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;31.81%</td>
    <td style="white-space: nowrap; text-align: right">38.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">83.56 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">1.05 K</td>
    <td style="white-space: nowrap; text-align: right">955.20 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;5.81%</td>
    <td style="white-space: nowrap; text-align: right">950.69 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1030.32 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">24.46 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">1.05 K</td>
    <td style="white-space: nowrap; text-align: right">23.36x</td>
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
    <td style="white-space: nowrap; text-align: right">3.99 K</td>
    <td style="white-space: nowrap; text-align: right">0.25 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;51.00%</td>
    <td style="white-space: nowrap; text-align: right">0.27 ms</td>
    <td style="white-space: nowrap; text-align: right">0.52 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.110 K</td>
    <td style="white-space: nowrap; text-align: right">9.06 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;29.35%</td>
    <td style="white-space: nowrap; text-align: right">7.97 ms</td>
    <td style="white-space: nowrap; text-align: right">15.98 ms</td>
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
    <td style="white-space: nowrap;text-align: right">3.99 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.110 K</td>
    <td style="white-space: nowrap; text-align: right">36.14x</td>
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
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">31.83 K</td>
    <td style="white-space: nowrap; text-align: right">31.42 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;34.45%</td>
    <td style="white-space: nowrap; text-align: right">29.17 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">67.92 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">8.24 K</td>
    <td style="white-space: nowrap; text-align: right">121.35 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;7.87%</td>
    <td style="white-space: nowrap; text-align: right">120.21 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">139.18 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">31.83 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">8.24 K</td>
    <td style="white-space: nowrap; text-align: right">3.86x</td>
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
    <td style="white-space: nowrap; text-align: right">129.05 K</td>
    <td style="white-space: nowrap; text-align: right">7.75 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;90.95%</td>
    <td style="white-space: nowrap; text-align: right">7.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">11.54 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">87.15 K</td>
    <td style="white-space: nowrap; text-align: right">11.47 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;81.76%</td>
    <td style="white-space: nowrap; text-align: right">8.96 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">30.13 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">129.05 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">87.15 K</td>
    <td style="white-space: nowrap; text-align: right">1.48x</td>
  </tr>

</table>

## get_multi

Benchmark

Benchmark run from 2026-04-08 17:03:48.686970Z UTC

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
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">20.61</td>
    <td style="white-space: nowrap; text-align: right">0.0485 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;25.69%</td>
    <td style="white-space: nowrap; text-align: right">0.0420 s</td>
    <td style="white-space: nowrap; text-align: right">0.0902 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.21</td>
    <td style="white-space: nowrap; text-align: right">4.82 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.24%</td>
    <td style="white-space: nowrap; text-align: right">4.82 s</td>
    <td style="white-space: nowrap; text-align: right">4.82 s</td>
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
    <td style="white-space: nowrap;text-align: right">20.61</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.21</td>
    <td style="white-space: nowrap; text-align: right">99.24x</td>
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
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">24.14</td>
    <td style="white-space: nowrap; text-align: right">41.43 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.72%</td>
    <td style="white-space: nowrap; text-align: right">40.28 ms</td>
    <td style="white-space: nowrap; text-align: right">56.39 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">2.09</td>
    <td style="white-space: nowrap; text-align: right">478.41 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.21%</td>
    <td style="white-space: nowrap; text-align: right">478.23 ms</td>
    <td style="white-space: nowrap; text-align: right">480.69 ms</td>
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
    <td style="white-space: nowrap;text-align: right">24.14</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">2.09</td>
    <td style="white-space: nowrap; text-align: right">11.55x</td>
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
    <td style="white-space: nowrap; text-align: right">6.40</td>
    <td style="white-space: nowrap; text-align: right">0.156 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.68%</td>
    <td style="white-space: nowrap; text-align: right">0.155 s</td>
    <td style="white-space: nowrap; text-align: right">0.184 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.127</td>
    <td style="white-space: nowrap; text-align: right">7.90 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">7.90 s</td>
    <td style="white-space: nowrap; text-align: right">7.90 s</td>
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
    <td style="white-space: nowrap;text-align: right">6.40</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.127</td>
    <td style="white-space: nowrap; text-align: right">50.61x</td>
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
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">33.94</td>
    <td style="white-space: nowrap; text-align: right">29.47 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.12%</td>
    <td style="white-space: nowrap; text-align: right">28.96 ms</td>
    <td style="white-space: nowrap; text-align: right">35.47 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">17.04</td>
    <td style="white-space: nowrap; text-align: right">58.67 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;1.62%</td>
    <td style="white-space: nowrap; text-align: right">58.50 ms</td>
    <td style="white-space: nowrap; text-align: right">63.39 ms</td>
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
    <td style="white-space: nowrap;text-align: right">33.94</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">17.04</td>
    <td style="white-space: nowrap; text-align: right">1.99x</td>
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
    <td style="white-space: nowrap; text-align: right">1.14 K</td>
    <td style="white-space: nowrap; text-align: right">0.88 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.00%</td>
    <td style="white-space: nowrap; text-align: right">0.87 ms</td>
    <td style="white-space: nowrap; text-align: right">0.96 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.120 K</td>
    <td style="white-space: nowrap; text-align: right">8.34 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;12.72%</td>
    <td style="white-space: nowrap; text-align: right">7.85 ms</td>
    <td style="white-space: nowrap; text-align: right">12.22 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.14 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.120 K</td>
    <td style="white-space: nowrap; text-align: right">9.48x</td>
  </tr>

</table>

## scan

Benchmark

Benchmark run from 2026-04-08 17:05:10.294659Z UTC

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
    <td style="white-space: nowrap; text-align: right">1.02 K</td>
    <td style="white-space: nowrap; text-align: right">0.98 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;16.61%</td>
    <td style="white-space: nowrap; text-align: right">0.93 ms</td>
    <td style="white-space: nowrap; text-align: right">1.61 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0576 K</td>
    <td style="white-space: nowrap; text-align: right">17.35 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;60.70%</td>
    <td style="white-space: nowrap; text-align: right">16.49 ms</td>
    <td style="white-space: nowrap; text-align: right">37.00 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.02 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0576 K</td>
    <td style="white-space: nowrap; text-align: right">17.7x</td>
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
    <td style="white-space: nowrap; text-align: right">1.04 K</td>
    <td style="white-space: nowrap; text-align: right">0.96 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;19.52%</td>
    <td style="white-space: nowrap; text-align: right">0.90 ms</td>
    <td style="white-space: nowrap; text-align: right">1.63 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.65 K</td>
    <td style="white-space: nowrap; text-align: right">1.53 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;56.51%</td>
    <td style="white-space: nowrap; text-align: right">1.52 ms</td>
    <td style="white-space: nowrap; text-align: right">3.17 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.04 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.65 K</td>
    <td style="white-space: nowrap; text-align: right">1.59x</td>
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
    <td style="white-space: nowrap; text-align: right">767.24</td>
    <td style="white-space: nowrap; text-align: right">1.30 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;21.60%</td>
    <td style="white-space: nowrap; text-align: right">1.30 ms</td>
    <td style="white-space: nowrap; text-align: right">1.99 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">26.54</td>
    <td style="white-space: nowrap; text-align: right">37.67 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;61.43%</td>
    <td style="white-space: nowrap; text-align: right">36.43 ms</td>
    <td style="white-space: nowrap; text-align: right">87.43 ms</td>
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
    <td style="white-space: nowrap;text-align: right">767.24</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">26.54</td>
    <td style="white-space: nowrap; text-align: right">28.9x</td>
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
    <td style="white-space: nowrap; text-align: right">5.71 K</td>
    <td style="white-space: nowrap; text-align: right">175.11 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;44.56%</td>
    <td style="white-space: nowrap; text-align: right">174.46 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">308.71 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.06 K</td>
    <td style="white-space: nowrap; text-align: right">946.96 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;19.73%</td>
    <td style="white-space: nowrap; text-align: right">881.33 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1633.25 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">5.71 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">1.06 K</td>
    <td style="white-space: nowrap; text-align: right">5.41x</td>
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
    <td style="white-space: nowrap; text-align: right">148.86 K</td>
    <td style="white-space: nowrap; text-align: right">6.72 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;130.06%</td>
    <td style="white-space: nowrap; text-align: right">6.42 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">10.25 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">83.77 K</td>
    <td style="white-space: nowrap; text-align: right">11.94 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;63.53%</td>
    <td style="white-space: nowrap; text-align: right">9.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">30.67 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">148.86 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">83.77 K</td>
    <td style="white-space: nowrap; text-align: right">1.78x</td>
  </tr>

</table>
