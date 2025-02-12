# SABD 2019/2020 second project
Authors: [Marco Balletti](https://github.com/marcobaleno96), [Francesco Marino](https://github.com/fmarino-412)

<h2>Project structure descritption</h2>

### [data](data)

Folder containing the input dataset as a CSV file ([`dataset.csv`](data/dataset.csv)).

### [docker-env](docker-env)

Folder containing scripts and file for a container based execution of the project architecture:

1. [`start-dockers.sh`](docker-env/start-dockers.sh) creates the Kafka Cluster and necessary Kafka topics,
2. [`stop-dockers.sh`](docker-env/stop-dockers.sh) stops and deletes the Kafka Cluster after the created topics deletion and
3. [`docker-compose.yml`](docker-env/docker-compose.yml) is the Docker Compose file used to create the container infrastructure.

### [Documentation](Documentation)

Folder containing benchmark results (under [`Benchmark`](Documentation/Benchmark) directory), project report and presentation slides.

### [Results](Results)

Folder containing Flink computation results as CSV files:

1. [`query1_daily.csv`](Results/query1_daily.csv) containing the output of the first query evaluated by daily windows,
2. [`query1_weekly.csv`](Results/query1_weekly.csv) containing the output of the first query evaluated by weekly windows,
3. [`query1_monthly.csv`](Results/query1_monthly.csv) containing the output of the first query evaluated by monthly windows,
4. [`query2_daily.csv`](Results/query2_daily.csv) containing the output of the second query evaluated by daily windows,
5. [`query2_weekly.csv`](Results/query2_weekly.csv) containing the output of the second query evaluated by weekly windows,
6. [`query3_daily.csv`](Results/query3_daily.csv) containing the output of the third query evaluated by daily windows and
7. [`query3_weekly.csv`](Results/query3_weekly.csv) containing the output of the third query evaluated by weekly windows.

**Results are evaluated from the entire dataset content**

### [src](src)

This directory contains in its subdirectories Java code for:

1. creation of Kafka Topic producer for input data,
2. creation of a Flink topology to run a DSP analysis of the three queries,
3. creation of a Kafka Streams topology to run an alternative DSP analysis of the same three queries and
4. creation of several Kafka topic consumers for DSP output saving.

---

<h2>Java Project structure description</h2>

It is recommended to open the entire directory with an IDE for better code navigation. Java project part was developed using JetBrains' IntelliJ IDEA.

In the main folder there are processing architecture launchers:

* [`ConsumersLauncher.java`](src/main/java/ConsumersLauncher.java) that launches consumers for Kafka Streams and Flink outputs,
* [`FlinkDSPMain.java`](src/main/java/FlinkDSPMain.java) that starts Flink data stream processing,
* [`KafkaStreamsDSPMain.java`](src/main/java/KafkaStreamsDSPMain.java) that starts Kafka Streams processing and
* [`ProducerLauncher.java`](src/main/java/ProducerLauncher.java) used to start a producer that reads from file and publish tuples to Kafka topics simulating a real time data source.

### [flink_dsp package](src/main/java/flink_dsp)

This package contains classes for queries' topologies building and execution using Flink as DSP framework.

#### [flink_dsp.query1 package](src/main/java/flink_dsp/query1)

* [`AverageDelayAggregator.java`](src/main/java/flink_dsp/query1/AverageDelayAggregator.java) used to aggregate data for the first query using daily, weekly and monthly windows,
* [`AverageDelayOutcome.java`](src/main/java/flink_dsp/query1/AverageDelayOutcome.java) representing the aggregation result,
* [`AverageDelayProcessWindow.java`](src/main/java/flink_dsp/query1/AverageDelayProcessWindow.java) used to set correctly windows' start times,
* [`MonthlyWindowAssigner.java`](src/main/java/flink_dsp/query1/MonthlyWindowAssigner.java) contains a custom thumbling window assigner for tuples separation by event time month (this was necessary due to differences in month durations) and
* [`Query1TopologyBuilder.java`](src/main/java/flink_dsp/query1/Query1TopologyBuilder.java) that builds the topology of the first query.

#### [flink_dsp.query2 package](src/main/java/flink_dsp/query2)

* [`ReasonRankingAggregator.java`](src/main/java/flink_dsp/query2/ReasonRankingAggregator.java) used to aggregate data for the second query using daily and weekly windows,
* [`ReasonRankingOutcome.java`](src/main/java/flink_dsp/query2/ReasonRankingOutcome.java) representing the aggregation result,
* [`ReasonRankingProcessWindow.java`](src/main/java/flink_dsp/query2/ReasonRankingProcessWindow.java) used to set correctly windows' start times and
* [`Query2TopologyBuilder.java`](src/main/java/flink_dsp/query2/Query2TopologyBuilder.java) that builds the topology of the second query.

#### [flink_dsp.query3 package](src/main/java/flink_dsp/query3)

* [`CompanyRankingAggregator.java`](src/main/java/flink_dsp/query3/CompanyRankingAggregator.java) used to aggregate data for the third query using daily and weekly windows,
* [`CompanyRankingOutcome.java`](src/main/java/flink_dsp/query3/CompanyRankingOutcome.java) representing the aggregation result,
* [`CompanyRankingProcessWindow.java`](src/main/java/flink_dsp/query3/CompanyRankingProcessWindow.java) used to set correctly windows' start times and
* [`Query3TopologyBuilder.java`](src/main/java/flink_dsp/query3/Query3TopologyBuilder.java) that builds the topology of the third query.

### [kafka_pubsub package](src/main/java/kafka_pubsub)

This package contains configurations for the Kafka publish-subscribe service and classes for Consumers and Producers instantiation:

* [`KafkaClusterConfig.java`](src/main/java/kafka_pubsub/KafkaClusterConfig.java) containing topics name and properties builders (for publishers and subscribers),
* [`KafkaParametricConsumer.java`](src/main/java/kafka_pubsub/KafkaParametricConsumer.java) used to create and start consumers registered to Kafka topics (of DSP outputs) and
* [`KafkaSingleProducer.java`](src/main/java/kafka_pubsub/KafkaSingleProducer.java) creates a producer that publishes DSP input tuples to Kafka topics.

### [kafkastreams_dsp package](src/main/java/kafkastreams_dsp)

This package contains classes for queries' topologies building and execution using Kafka Streams as DSP library and the [`KafkaStreamsConfig.java`](src/main/java/kafkastreams_dsp/KafkaStreamsConfig.java) used to get properties for the stream processing library execution.

#### [kafkastreams_dsp.queries package](src/main/java/kafkastreams_dsp/queries)

This package contains classes for queries' topologies creation:

* [`Query1TopologyBuilder.java`](src/main/java/kafkastreams_dsp/queries/Query1TopologyBuilder.java) that builds the topology of the first query,
* [`Query2TopologyBuilder.java`](src/main/java/kafkastreams_dsp/queries/Query2TopologyBuilder.java) that builds the topology of the second query and
* [`Query3TopologyBuilder.java`](src/main/java/kafkastreams_dsp/queries/Query3TopologyBuilder.java) that builds the topology of the third query.

#### [kafkastreams_dsp.windows package](src/main/java/kafkastreams_dsp/windows)

This package contains custom Kafka Streams windows:

* [`CustomTimeWindows.java`](src/main/java/kafkastreams_dsp/windows/CustomTimeWindows.java) that is an abstract class representing a generic custom duration time window,
* [`DailyTimeWindows.java`](src/main/java/kafkastreams_dsp/windows/DailyTimeWindows.java) that implements a daily time window aligned to a given time zone,
* [`MonthlyTimeWindows.java`](src/main/java/kafkastreams_dsp/windows/MonthlyTimeWindows.java) that implements a monthly time window (aligned to the first day of a month in a given time zone) and
* [`WeeklyTimeWindows.java`](src/main/java/kafkastreams_dsp/windows/WeeklyTimeWindows.java) implementing a weekly time window (starts on Monday and ends on Sunday aligned to a given time zone).

### [utility package](src/main/java/utility)

This package contains classes needed for queries' execution support, in particular:

* [`BusData.java`](src/main/java/utility/BusData.java) structure representing tuple information needed for evaluation,
* [`DataCommonTransformation.java`](src/main/java/utility/DataCommonTransformation.java) containing common method needed for queries processing and
* [`OutputFormatter.java`](src/main/java/utility/OutputFormatter.java) needed for query outcomes formatting in order to be published on Kafka.

#### [utility.accumulators package](src/main/java/utility/accumulators)

This package contains classes used as accumulators for both Flink and Kafka Streams processing:

* [`AverageDelayAccumulator.java`](src/main/java/utility/accumulators/AverageDelayAccumulator.java) used for average delay statistics grouped by neighbourhood (first query),
* [`AverageDelayStatistics.java`](src/main/java/utility/accumulators/AverageDelayStatistics.java) used to maintain information about per neighbourhood delay (first query),
* [`CompanyRankingAccumulator.java`](src/main/java/utility/accumulators/CompanyRankingAccumulator.java) used for company name ranking on delay basis (third query) and
* [`ReasonRankingAccumulator.java`](src/main/java/utility/accumulators/ReasonRankingAccumulator.java) needed for delay reason rankings (second query).

#### [utility.benchmarks package](src/main/java/utility/benchmarks)

This package contains utilities for latency and throughput evaluation:

* [`BenchmarkFlinkSink.java`](src/main/java/utility/benchmarks/BenchmarkFlinkSink.java) representing a sink that can be used in Flink topology to evaluate performances and
* [`SynchronizedCounter.java`](src/main/java/utility/benchmarks/SynchronizedCounter.java) that is a static counter for benchmark evaluation (counts tuples and time).

#### [utility.delay package](src/main/java/utility/delay)

This package contains utilities for delay string parsing and delay type ranking:

* [`DelayFixes.java`](src/main/java/utility/delay/DelayFixes.java) this is an Enum for wrongly converted string correction in the dataset,
* [`DelayFormatException.java`](src/main/java/utility/delay/DelayFormatException.java) that is a custom Java Exception for failure on gaining information from delay strings,
* [`DealyInfo.java`](src/main/java/utility/delay/DelayInfo.java) representing a single parsed delay information,
* [`DelayParsingUtility.java`](src/main/java/utility/delay/DelayParsingUtility.java) that contains delay strings parsing logic and
* [`DelayScorer.java`](src/main/java/utility/delay/DelayScorer.java) used to assign a score on delay and reason basis (third query).

#### [utility.serdes package](src/main/java/utility/serdes)

This package contains data serialization and deserialization utilities:

* [`FlinkStringToKafkaSerializer.java`](src/main/java/utility/serdes/FlinkStringToKafkaSerializer.java) needed to serialize Flink output strings for publication Kafka topics,
* [`JsonPOJODeserializer.java`](src/main/java/utility/serdes/JsonPOJODeserializer.java) used to deserialize custom object from JSON format,
* [`JsonPOJOSerializer.java`](src/main/java/utility/serdes/JsonPOJOSerializer.java) used to serialize custom object to JSON format and
* [`SerDesBuilders.java`](src/main/java/utility/serdes/SerDesBuilders.java) used to build ser-des for Kafka Streams.
