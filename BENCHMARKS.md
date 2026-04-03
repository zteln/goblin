# Benchmarks

Goblin v0.8.0 vs CubDB — 2026-04-03

## put

Benchmark

Benchmark run from 2026-04-03 15:39:05.384371Z UTC

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
    <td style="white-space: nowrap; text-align: right">16.46</td>
    <td style="white-space: nowrap; text-align: right">60.74 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;93.76%</td>
    <td style="white-space: nowrap; text-align: right">46.45 ms</td>
    <td style="white-space: nowrap; text-align: right">341.33 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.42</td>
    <td style="white-space: nowrap; text-align: right">87.53 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.14%</td>
    <td style="white-space: nowrap; text-align: right">90.48 ms</td>
    <td style="white-space: nowrap; text-align: right">102.53 ms</td>
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
    <td style="white-space: nowrap;text-align: right">16.46</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">11.42</td>
    <td style="white-space: nowrap; text-align: right">1.44x</td>
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
    <td style="white-space: nowrap; text-align: right">141.60</td>
    <td style="white-space: nowrap; text-align: right">7.06 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;290.62%</td>
    <td style="white-space: nowrap; text-align: right">5.71 ms</td>
    <td style="white-space: nowrap; text-align: right">11.20 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">124.37</td>
    <td style="white-space: nowrap; text-align: right">8.04 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;37.12%</td>
    <td style="white-space: nowrap; text-align: right">7.77 ms</td>
    <td style="white-space: nowrap; text-align: right">10.51 ms</td>
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
    <td style="white-space: nowrap;text-align: right">141.60</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put/3</td>
    <td style="white-space: nowrap; text-align: right">124.37</td>
    <td style="white-space: nowrap; text-align: right">1.14x</td>
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
    <td style="white-space: nowrap; text-align: right">9.64 K</td>
    <td style="white-space: nowrap; text-align: right">103.70 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;37.53%</td>
    <td style="white-space: nowrap; text-align: right">100.25 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">165.77 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.89 K</td>
    <td style="white-space: nowrap; text-align: right">529.51 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;87.88%</td>
    <td style="white-space: nowrap; text-align: right">446.08 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1786.16 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">9.64 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">1.89 K</td>
    <td style="white-space: nowrap; text-align: right">5.11x</td>
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
    <td style="white-space: nowrap; text-align: right">1.01 K</td>
    <td style="white-space: nowrap; text-align: right">0.99 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;41.90%</td>
    <td style="white-space: nowrap; text-align: right">0.88 ms</td>
    <td style="white-space: nowrap; text-align: right">2.88 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">0.41 K</td>
    <td style="white-space: nowrap; text-align: right">2.46 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;14.28%</td>
    <td style="white-space: nowrap; text-align: right">2.47 ms</td>
    <td style="white-space: nowrap; text-align: right">3.61 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.01 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">0.41 K</td>
    <td style="white-space: nowrap; text-align: right">2.5x</td>
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
    <td style="white-space: nowrap; text-align: right">9.60 K</td>
    <td style="white-space: nowrap; text-align: right">104.20 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;31.29%</td>
    <td style="white-space: nowrap; text-align: right">100.96 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">164.71 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">2.15 K</td>
    <td style="white-space: nowrap; text-align: right">464.69 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;49.02%</td>
    <td style="white-space: nowrap; text-align: right">408.42 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1524.24 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">9.60 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.put/3</td>
    <td style="white-space: nowrap; text-align: right">2.15 K</td>
    <td style="white-space: nowrap; text-align: right">4.46x</td>
  </tr>

</table>

## put_multi

Benchmark

Benchmark run from 2026-04-03 15:40:44.051947Z UTC

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
    <td style="white-space: nowrap; text-align: right">1.73 K</td>
    <td style="white-space: nowrap; text-align: right">0.58 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;28.90%</td>
    <td style="white-space: nowrap; text-align: right">0.53 ms</td>
    <td style="white-space: nowrap; text-align: right">1.28 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.97 K</td>
    <td style="white-space: nowrap; text-align: right">1.03 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;28.00%</td>
    <td style="white-space: nowrap; text-align: right">0.91 ms</td>
    <td style="white-space: nowrap; text-align: right">1.81 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.73 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.97 K</td>
    <td style="white-space: nowrap; text-align: right">1.79x</td>
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
    <td style="white-space: nowrap; text-align: right">980.78</td>
    <td style="white-space: nowrap; text-align: right">1.02 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;34.24%</td>
    <td style="white-space: nowrap; text-align: right">0.92 ms</td>
    <td style="white-space: nowrap; text-align: right">1.85 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">97.31</td>
    <td style="white-space: nowrap; text-align: right">10.28 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;27.44%</td>
    <td style="white-space: nowrap; text-align: right">9.12 ms</td>
    <td style="white-space: nowrap; text-align: right">18.45 ms</td>
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
    <td style="white-space: nowrap;text-align: right">980.78</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">97.31</td>
    <td style="white-space: nowrap; text-align: right">10.08x</td>
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
    <td style="white-space: nowrap; text-align: right">3.56</td>
    <td style="white-space: nowrap; text-align: right">0.28 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.83%</td>
    <td style="white-space: nowrap; text-align: right">0.28 s</td>
    <td style="white-space: nowrap; text-align: right">0.30 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0775</td>
    <td style="white-space: nowrap; text-align: right">12.90 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">12.90 s</td>
    <td style="white-space: nowrap; text-align: right">12.90 s</td>
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
    <td style="white-space: nowrap;text-align: right">3.56</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.0775</td>
    <td style="white-space: nowrap; text-align: right">45.88x</td>
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
    <td style="white-space: nowrap; text-align: right">34.44</td>
    <td style="white-space: nowrap; text-align: right">0.0290 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;9.30%</td>
    <td style="white-space: nowrap; text-align: right">0.0291 s</td>
    <td style="white-space: nowrap; text-align: right">0.0383 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.71</td>
    <td style="white-space: nowrap; text-align: right">1.41 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;17.09%</td>
    <td style="white-space: nowrap; text-align: right">1.44 s</td>
    <td style="white-space: nowrap; text-align: right">1.67 s</td>
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
    <td style="white-space: nowrap;text-align: right">34.44</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.71</td>
    <td style="white-space: nowrap; text-align: right">48.62x</td>
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
    <td style="white-space: nowrap; text-align: right">231.88</td>
    <td style="white-space: nowrap; text-align: right">4.31 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;11.17%</td>
    <td style="white-space: nowrap; text-align: right">4.31 ms</td>
    <td style="white-space: nowrap; text-align: right">5.63 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.55</td>
    <td style="white-space: nowrap; text-align: right">104.76 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;5.93%</td>
    <td style="white-space: nowrap; text-align: right">103.83 ms</td>
    <td style="white-space: nowrap; text-align: right">129.38 ms</td>
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
    <td style="white-space: nowrap;text-align: right">231.88</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.put_multi/2</td>
    <td style="white-space: nowrap; text-align: right">9.55</td>
    <td style="white-space: nowrap; text-align: right">24.29x</td>
  </tr>

</table>

## get

Benchmark

Benchmark run from 2026-04-03 15:42:25.372023Z UTC

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
    <td style="white-space: nowrap; text-align: right">24.08 K</td>
    <td style="white-space: nowrap; text-align: right">0.0415 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;54.92%</td>
    <td style="white-space: nowrap; text-align: right">0.0373 ms</td>
    <td style="white-space: nowrap; text-align: right">0.177 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.107 K</td>
    <td style="white-space: nowrap; text-align: right">9.38 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;1.22%</td>
    <td style="white-space: nowrap; text-align: right">9.34 ms</td>
    <td style="white-space: nowrap; text-align: right">9.84 ms</td>
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
    <td style="white-space: nowrap;text-align: right">24.08 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.107 K</td>
    <td style="white-space: nowrap; text-align: right">225.85x</td>
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
    <td style="white-space: nowrap; text-align: right">25.43 K</td>
    <td style="white-space: nowrap; text-align: right">39.33 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.31%</td>
    <td style="white-space: nowrap; text-align: right">37.67 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">76.79 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">1.05 K</td>
    <td style="white-space: nowrap; text-align: right">950.05 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;8.05%</td>
    <td style="white-space: nowrap; text-align: right">943 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">1023.17 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">25.43 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">1.05 K</td>
    <td style="white-space: nowrap; text-align: right">24.16x</td>
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
    <td style="white-space: nowrap; text-align: right">4.48 K</td>
    <td style="white-space: nowrap; text-align: right">0.22 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;46.29%</td>
    <td style="white-space: nowrap; text-align: right">0.25 ms</td>
    <td style="white-space: nowrap; text-align: right">0.47 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.111 K</td>
    <td style="white-space: nowrap; text-align: right">8.99 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;30.59%</td>
    <td style="white-space: nowrap; text-align: right">7.80 ms</td>
    <td style="white-space: nowrap; text-align: right">15.56 ms</td>
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
    <td style="white-space: nowrap;text-align: right">4.48 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">0.111 K</td>
    <td style="white-space: nowrap; text-align: right">40.3x</td>
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
    <td style="white-space: nowrap; text-align: right">33.91 K</td>
    <td style="white-space: nowrap; text-align: right">29.49 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;34.86%</td>
    <td style="white-space: nowrap; text-align: right">25.38 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">59.88 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">8.25 K</td>
    <td style="white-space: nowrap; text-align: right">121.24 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.28%</td>
    <td style="white-space: nowrap; text-align: right">120.33 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">133.79 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">33.91 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get/2</td>
    <td style="white-space: nowrap; text-align: right">8.25 K</td>
    <td style="white-space: nowrap; text-align: right">4.11x</td>
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
    <td style="white-space: nowrap; text-align: right">126.57 K</td>
    <td style="white-space: nowrap; text-align: right">7.90 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;94.88%</td>
    <td style="white-space: nowrap; text-align: right">7.67 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">11.42 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">84.07 K</td>
    <td style="white-space: nowrap; text-align: right">11.89 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;268.74%</td>
    <td style="white-space: nowrap; text-align: right">9.04 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">32.54 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">126.57 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get/2</td>
    <td style="white-space: nowrap; text-align: right">84.07 K</td>
    <td style="white-space: nowrap; text-align: right">1.51x</td>
  </tr>

</table>

## get_multi

Benchmark

Benchmark run from 2026-04-03 15:44:41.380673Z UTC

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
    <td style="white-space: nowrap; text-align: right">25.72</td>
    <td style="white-space: nowrap; text-align: right">0.0389 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;3.33%</td>
    <td style="white-space: nowrap; text-align: right">0.0386 s</td>
    <td style="white-space: nowrap; text-align: right">0.0452 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.21</td>
    <td style="white-space: nowrap; text-align: right">4.83 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.23%</td>
    <td style="white-space: nowrap; text-align: right">4.83 s</td>
    <td style="white-space: nowrap; text-align: right">4.84 s</td>
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
    <td style="white-space: nowrap;text-align: right">25.72</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.21</td>
    <td style="white-space: nowrap; text-align: right">124.29x</td>
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
    <td style="white-space: nowrap; text-align: right">25.53</td>
    <td style="white-space: nowrap; text-align: right">39.17 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;4.67%</td>
    <td style="white-space: nowrap; text-align: right">38.75 ms</td>
    <td style="white-space: nowrap; text-align: right">46.16 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">2.09</td>
    <td style="white-space: nowrap; text-align: right">479.05 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.25%</td>
    <td style="white-space: nowrap; text-align: right">478.96 ms</td>
    <td style="white-space: nowrap; text-align: right">481.89 ms</td>
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
    <td style="white-space: nowrap;text-align: right">25.53</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">2.09</td>
    <td style="white-space: nowrap; text-align: right">12.23x</td>
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
    <td style="white-space: nowrap; text-align: right">7.29</td>
    <td style="white-space: nowrap; text-align: right">0.137 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;10.35%</td>
    <td style="white-space: nowrap; text-align: right">0.138 s</td>
    <td style="white-space: nowrap; text-align: right">0.168 s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.128</td>
    <td style="white-space: nowrap; text-align: right">7.83 s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;0.00%</td>
    <td style="white-space: nowrap; text-align: right">7.83 s</td>
    <td style="white-space: nowrap; text-align: right">7.83 s</td>
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
    <td style="white-space: nowrap;text-align: right">7.29</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.128</td>
    <td style="white-space: nowrap; text-align: right">57.06x</td>
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
    <td style="white-space: nowrap; text-align: right">35.24</td>
    <td style="white-space: nowrap; text-align: right">28.38 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;6.10%</td>
    <td style="white-space: nowrap; text-align: right">28.03 ms</td>
    <td style="white-space: nowrap; text-align: right">37.68 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">16.95</td>
    <td style="white-space: nowrap; text-align: right">59.01 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;1.47%</td>
    <td style="white-space: nowrap; text-align: right">58.96 ms</td>
    <td style="white-space: nowrap; text-align: right">62.54 ms</td>
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
    <td style="white-space: nowrap;text-align: right">35.24</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">16.95</td>
    <td style="white-space: nowrap; text-align: right">2.08x</td>
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
    <td style="white-space: nowrap; text-align: right">1.11 K</td>
    <td style="white-space: nowrap; text-align: right">0.90 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;5.88%</td>
    <td style="white-space: nowrap; text-align: right">0.89 ms</td>
    <td style="white-space: nowrap; text-align: right">1.03 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.101 K</td>
    <td style="white-space: nowrap; text-align: right">9.89 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;14.76%</td>
    <td style="white-space: nowrap; text-align: right">9.94 ms</td>
    <td style="white-space: nowrap; text-align: right">13.31 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.11 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.get_multi/2</td>
    <td style="white-space: nowrap; text-align: right">0.101 K</td>
    <td style="white-space: nowrap; text-align: right">10.94x</td>
  </tr>

</table>

## scan

Benchmark

Benchmark run from 2026-04-03 15:46:22.135056Z UTC

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
    <td style="white-space: nowrap; text-align: right">1.05 K</td>
    <td style="white-space: nowrap; text-align: right">0.96 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;14.05%</td>
    <td style="white-space: nowrap; text-align: right">0.92 ms</td>
    <td style="white-space: nowrap; text-align: right">1.58 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0593 K</td>
    <td style="white-space: nowrap; text-align: right">16.85 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;57.08%</td>
    <td style="white-space: nowrap; text-align: right">16.41 ms</td>
    <td style="white-space: nowrap; text-align: right">34.44 ms</td>
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
    <td style="white-space: nowrap;text-align: right">1.05 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.0593 K</td>
    <td style="white-space: nowrap; text-align: right">17.64x</td>
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
    <td style="white-space: nowrap; text-align: right">0.92 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;16.36%</td>
    <td style="white-space: nowrap; text-align: right">0.88 ms</td>
    <td style="white-space: nowrap; text-align: right">1.57 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">0.67 K</td>
    <td style="white-space: nowrap; text-align: right">1.49 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;56.68%</td>
    <td style="white-space: nowrap; text-align: right">1.48 ms</td>
    <td style="white-space: nowrap; text-align: right">2.98 ms</td>
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
    <td style="white-space: nowrap; text-align: right">0.67 K</td>
    <td style="white-space: nowrap; text-align: right">1.62x</td>
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
    <td style="white-space: nowrap; text-align: right">814.01</td>
    <td style="white-space: nowrap; text-align: right">1.23 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;19.25%</td>
    <td style="white-space: nowrap; text-align: right">1.23 ms</td>
    <td style="white-space: nowrap; text-align: right">1.83 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">18.73</td>
    <td style="white-space: nowrap; text-align: right">53.39 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;91.37%</td>
    <td style="white-space: nowrap; text-align: right">37.32 ms</td>
    <td style="white-space: nowrap; text-align: right">210.19 ms</td>
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
    <td style="white-space: nowrap;text-align: right">814.01</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">Goblin.scan/2</td>
    <td style="white-space: nowrap; text-align: right">18.73</td>
    <td style="white-space: nowrap; text-align: right">43.46x</td>
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
    <td style="white-space: nowrap; text-align: right">5.78 K</td>
    <td style="white-space: nowrap; text-align: right">0.173 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;43.90%</td>
    <td style="white-space: nowrap; text-align: right">0.173 ms</td>
    <td style="white-space: nowrap; text-align: right">0.30 ms</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">0.57 K</td>
    <td style="white-space: nowrap; text-align: right">1.75 ms</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;32.01%</td>
    <td style="white-space: nowrap; text-align: right">1.99 ms</td>
    <td style="white-space: nowrap; text-align: right">3.32 ms</td>
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
    <td style="white-space: nowrap;text-align: right">5.78 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">0.57 K</td>
    <td style="white-space: nowrap; text-align: right">10.14x</td>
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
    <td style="white-space: nowrap; text-align: right">146.15 K</td>
    <td style="white-space: nowrap; text-align: right">6.84 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;128.21%</td>
    <td style="white-space: nowrap; text-align: right">6.58 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">11.25 &micro;s</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">79.78 K</td>
    <td style="white-space: nowrap; text-align: right">12.53 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">&plusmn;58.10%</td>
    <td style="white-space: nowrap; text-align: right">9.50 &micro;s</td>
    <td style="white-space: nowrap; text-align: right">31.38 &micro;s</td>
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
    <td style="white-space: nowrap;text-align: right">146.15 K</td>
    <td>&nbsp;</td>
  </tr>

  <tr>
    <td style="white-space: nowrap">CubDB.select/2</td>
    <td style="white-space: nowrap; text-align: right">79.78 K</td>
    <td style="white-space: nowrap; text-align: right">1.83x</td>
  </tr>

</table>
