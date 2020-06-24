# SABD 2019/2020 second project
Authors: Marco Balletti, Francesco Marino

<h2>Project structure descritption</h2>

### data

Folder containing the input dataset as a CSV file (`dataset.csv`).

### docker-env

Folder containing scripts and file for a container based execution of the project architecture:

1. `start-dockers.sh` creates the Kafka Cluster and necessary Kafka topics,
2. `stop-dockers.sh` stops and deletes the Kafka Cluster after the created topics deletion and
3. `docker-compose.yml` is the Docker Compose file used to create the container infrastructure.

### Documentation

Folder containing benchmark results (under `Benchmark` directory), project report and presentation slides.

### Results

Folder containing Flink computation results as CSV files:

1. `query1_daily.csv` containing the output of the first query evaluated by daily windows,
2. `query1_weekly.csv` containing the output of the first query evaluated by weekly windows,
3. `query1_monthly.csv` containing the output of the first query evaluated by monthly windows,
4. `query2_daily.csv` containing the output of the second query evaluated by daily windows,
5. `query2_weekly.csv` containing the output of the second query evaluated by weekly windows,
6. `query3_daily.csv` containing the output of the third query evaluated by daily windows and
7. `query3_weekly.csv` containing the output of the third query evaluated by weekly windows.

**Results are evaluated from the entire dataset content**

### src

This directory contains in its subdirectories Java code for:

1. creation of Kafka Topic producer for input data,
2. creation of a Flink topology to run a DSP analysis of the three queries,
3. creation of a Kafka Streams topology to run an alternative DSP analysis of the same three queries and
4. creation of several Kafka topic consumers for DSP output saving.

---

<h2>Java Project structure description</h2>

It is recommended to open the entire directory with an IDE for better code navigation. Java project part was developed using JetBrains' IntelliJ IDEA.

In the main folder there are processing architecture launchers:

* `ConsumersLauncher.java` that launches consumers for Kafka Streams and Flink outputs,
* `FlinkDSPMain.java` that starts Flink data stream processing,
* `KafkaStreamsDSPMain.java` that starts Kafka Streams processing and
* `ProducerLauncher.java` used to start a producer that reads from file and publish tuples to Kafka topics simulating a real time data source.

### flink_dsp package

This package contains classes for queries' topologies building and execution using Flink as DSP framework.

#### flink_dsp.query1 package

* `AverageDelayAggregator.java` used to aggregate data for the first query using daily, weekly and monthly windows,
* `AverageDelayOutcome.java` representing the aggregation result,
* `AverageDelayProcessWindow.java` used to set correctly windows' start times,
* `MonthlyWindowAssigner.java` contains a custom thumbling window assigner for tuples separation by event time month (this was necessary due to differences in month durations) and
* `Query1TopologyBuilder.java` that builds the topology of the first query.

#### flink_dsp.query2 package

* `ReasonRankingAggregator.java` used to aggregate data for the second query using daily and weekly windows,
* `ReasonRankingOutcome.java` representing the aggregation result,
* `ReasonRankingProcessWindow.java` used to set correctly windows' start times and
* `Query2TopologyBuilder.java` that builds the topology of the second query.

#### flink_dsp.query3 package

* `CompanyRankingAggregator.java` used to aggregate data for the third query using daily and weekly windows,
* `CompanyRankingOutcome.java` representing the aggregation result,
* `CompanyRankingProcessWindow.java` used to set correctly windows' start times and
* `Query3TopologyBuilder.java` that builds the topology of the third query.

### kafka_pubsub package

This package contains configurations for the Kafka publish-subscribe service and classes for Consumers and Producers instantiation:

* `KafkaClusterConfig.java` containing topics name and properties builders (for publishers and subscribers),
* `KafkaParametricConsumers.java` used to create and start consumers registered to Kafka topics (of DSP outputs) and
* `KafkaSingleProducer.java` creates a producer that publishes DSP input tuples to Kafka topics.

### kafkastreams_dsp package

This package contains classes for queries' topologies building and execution using Kafka Streams as DSP library and the `KafkaStreamsConfig.java` used to get properties for the stream processing library execution.

#### kafkastreams_dsp.queries package

This package contains classes for queries' topologies creation:

* `Query1TopologyBuilder.java` that builds the topology of the first query,
* `Query2TopologyBuilder.java` that builds the topology of the second query and
* `Query3TopologyBuilder.java` that builds the topology of the third query.

#### kafkastreams_dsp.windows package

This package contains custom Kafka Streams windows:

* `CustomTimeWindows.java` that is an abstract class representing a generic custom duration time window,
* `DailyTimeWindows.java` that implements a daily time window aligned to a given time zone,
* `MonthlyTimeWindows.java` that implements a monthly time window (aligned to the first day of a month in a given time zone) and
* `WeeklyTimeWindows.java` implementing a weekly time window (starts on Monday and ends on Sunday aligned to a given time zone).

### utility package

This package contains classes needed for queries' execution support, in particular:

* `BusData.java` structure representing tuple information needed for evaluation,
* `DataCommonTransformation.java` containing common method needed for queries processing and
* `OutputFormatter.java` needed for query outcomes formatting in order to be published on Kafka.

#### utility.accumulators package

This package contains classes used as accumulators for both Flink and Kafka Streams processing:

* `AverageDelayAccumulator.java` used for average delay statistics grouped by neighbourhood (first query),
* `AverageDelayStatistics.java` used to maintain information about per neighbourhood delay (first query),
* `CompanyRankingAccumulator.java` used for company name ranking on delay basis (third query) and
* `ReasonRankingAccumulator.java` needed for delay reason rankings (second query).

#### utility.benchmarks package

This package contains utilities for latency and throughput evaluation:

* `BenchmarkFlinkSink.java` representing a sink that can be used in Flink topology to evaluate performances and
* `SynchronizedCounter.java` that is a static counter for benchmark evaluation (counts tuples and time).

#### utility.delay package

This package contains utilities for delay string parsing and delay type ranking:

* `DelayFormatException.java` that is a custom Java Exception for failure on gaining information from delay strings,
* `DealyInfo.java` representing a single parsed delay information,
* `DelayParsingUtility.java` that contains delay strings parsing logic and
* `DelayScorer.java` used to assign a score on delay and reason basis (third query).

#### utility.serdes package

This package contains data serialization and deserialization utilities:

* `FlinkStringToKafkaSerializer.java` needed to serialize Flink output strings for publication Kafka topics,
* `JsonPOJODeserializer.java` used to deserialize custom object from JSON format,
* `JsonPOJOSerializer.java` used to serialize custom object to JSON format and
* `SerDesBuilders.java` used to build ser-des for Kafka Streams.