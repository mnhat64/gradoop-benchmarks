/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmarks.grouping;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.benchmarks.AbstractRunner;
import org.gradoop.benchmarks.tpgm.AggregationBenchmark;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MaxTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MinTime;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.TemporalGroupingKeys;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for the evaluation of the {@link KeyedGrouping} operator both on EPGM and TPGM graphs.
 */
public class KeyedGroupingBenchmark extends AbstractRunner {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare output path to csv file with execution results
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to specify keyed grouping config
   */
  private static final String OPTION_CONFIG = "g";
  /**
   * Option to specify whether the benchmark operates on a temporal graph or not
   */
  private static final String OPTION_TEMPORAL = "t";
  /**
   * Index of the selected config
   */
  private static int SELECTED_CONFIG;
  /**
   * Path to the directory that contains {@link CSVDataSource} or {@link TemporalCSVDataSource} compatible
   * graph data
   */
  private static String INPUT_PATH;
  /**
   * Path to the directory the resulting graph data will be written to
   */
  private static String OUTPUT_PATH;
  /**
   * Path to the statistics csv file
   */
  private static String CSV_PATH;
  /**
   * Flag which determines whether keyed grouping is applied to TPGM or EPGM graphs
   */
  private static boolean IS_TEMPORAL;
  /**
   * {@link TimeDimension} that is considered in the context of this benchmark.
   */
  private static final TimeDimension DIMENSION = TimeDimension.VALID_TIME;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true, "Path to input csv directory");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true, "Path to output directory");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv result file (will be created if not available).");
    OPTIONS.addRequiredOption(OPTION_CONFIG, "config", true, "Select predefined configuration");
    OPTIONS.addOption(OPTION_TEMPORAL, "temporal", false, "Apply temporal keyed grouping to input graph");
  }

  /**
   * Main program to run the benchmark.
   * <p>
   * Example: {@code $ /path/to/flink run -c org.gradoop.benchmarks.grouping.KeyedGroupingBenchmark
   * /path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv -g 1 --temporal}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, AggregationBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    readCMDArguments(cmd);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    if (IS_TEMPORAL) {
      // Apply keyed grouping operation on TPGM graph
      TemporalGradoopConfig cfg = TemporalGradoopConfig.createConfig(env);

      TemporalDataSource source = new TemporalCSVDataSource(INPUT_PATH, cfg);
      TemporalGraph graph = source.getTemporalGraph();

      TemporalGraph groupedGraph = graph.callForGraph(getTPGMGroupingConfig(SELECTED_CONFIG));

      TemporalCSVDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, cfg);
      sink.write(groupedGraph, true);

    } else {
      // Apply keyed grouping operation on EPGM graph
      GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

      DataSource source = new CSVDataSource(INPUT_PATH, cfg);
      LogicalGraph graph = source.getLogicalGraph();

      LogicalGraph groupedGraph = graph.callForGraph(getEPGMGroupingConfig(SELECTED_CONFIG));

      CSVDataSink sink = new CSVDataSink(OUTPUT_PATH, cfg);
      sink.write(groupedGraph, true);
    }

    env.execute(KeyedGrouping.class.getSimpleName() + " - C: " + SELECTED_CONFIG + "- P: "
      + env.getParallelism());

    writeStatistics(env);
  }

  /**
   * Returns a specific {@link KeyedGrouping} object which will be applied on a {@link TemporalGraph}.
   * <p>
   * This method is meant to be easily extendable in order to provide multiple keyed grouping configurations.
   *
   * @param select the selected keyed grouping configuration
   * @return the selected {@link KeyedGrouping} object
   */
  private static KeyedGrouping<
    TemporalGraphHead,
    TemporalVertex,
    TemporalEdge,
    TemporalGraph,
    TemporalGraphCollection> getTPGMGroupingConfig(int select) {

    List<KeyFunction<TemporalVertex, ?>> vertexKeys;
    List<KeyFunction<TemporalEdge, ?>> edgeKeys;
    List<AggregateFunction> vertexAggregateFunctions;
    List<AggregateFunction> edgeAggregateFunctions;

    switch (select) {
    case 1:
      vertexKeys = Arrays.asList(
        GroupingKeys.label(),
        TemporalGroupingKeys.timeStamp(
          DIMENSION,
          TimeDimension.Field.FROM,
          ChronoField.ALIGNED_WEEK_OF_YEAR));

      edgeKeys = Arrays.asList(
        GroupingKeys.label(),
        TemporalGroupingKeys.timeStamp(
          DIMENSION,
          TimeDimension.Field.FROM,
          ChronoField.ALIGNED_WEEK_OF_YEAR));

      vertexAggregateFunctions = Collections.singletonList(new Count("count"));

      edgeAggregateFunctions = Collections.singletonList(new Count("count"));
      break;

    case 2:
      vertexKeys = Arrays.asList(
        GroupingKeys.label(),
        TemporalGroupingKeys.timeStamp(
          DIMENSION,
          TimeDimension.Field.FROM,
          ChronoField.ALIGNED_WEEK_OF_MONTH));

      edgeKeys = Arrays.asList(
        GroupingKeys.label(),
        TemporalGroupingKeys.timeStamp(
          DIMENSION,
          TimeDimension.Field.FROM,
          ChronoField.ALIGNED_WEEK_OF_MONTH));

      vertexAggregateFunctions = Arrays.asList(
        new Count("count"),
        new MinTime("minTime", DIMENSION, TimeDimension.Field.FROM));

      edgeAggregateFunctions = Collections.singletonList(new Count("count"));
      break;

    case 3:
      vertexKeys = Collections.singletonList(GroupingKeys.label());

      edgeKeys = Arrays.asList(
        GroupingKeys.label(),
        TemporalGroupingKeys.timeStamp(
          DIMENSION,
          TimeDimension.Field.FROM,
          ChronoField.ALIGNED_DAY_OF_WEEK_IN_MONTH));

      vertexAggregateFunctions = Collections.singletonList(new Count("count"));

      edgeAggregateFunctions = Arrays.asList(
        new Count("count"),
        new MaxTime("maxTime", DIMENSION, TimeDimension.Field.FROM));
      break;

    default:
      throw new IllegalArgumentException("Unsupported config: " + select);
    }

    return new KeyedGrouping<>(vertexKeys, vertexAggregateFunctions, edgeKeys, edgeAggregateFunctions);
  }

  /**
   * Returns a specific {@link KeyedGrouping} object which will be applied on a {@link LogicalGraph}.
   * <p>
   * This method is meant to be easily extendable in order to provide multiple keyed grouping configurations.
   *
   * @param select the selected keyed grouping configuration
   * @return the selected {@link KeyedGrouping} object
   */
  private static KeyedGrouping<
    EPGMGraphHead,
    EPGMVertex,
    EPGMEdge,
    LogicalGraph,
    GraphCollection> getEPGMGroupingConfig(int select) {

    List<KeyFunction<EPGMVertex, ?>> vertexKeys;
    List<KeyFunction<EPGMEdge, ?>> edgeKeys;
    List<AggregateFunction> vertexAggregateFunctions;
    List<AggregateFunction> edgeAggregateFunctions;

    switch (select) {
    case 1:
      vertexKeys = Collections.singletonList(GroupingKeys.label());
      edgeKeys = Collections.singletonList(GroupingKeys.label());
      vertexAggregateFunctions = Collections.singletonList(new Count("count"));
      edgeAggregateFunctions = Collections.singletonList(new Count("count"));
      break;

    case 2:
      vertexKeys = Collections.singletonList(GroupingKeys.nothing());
      edgeKeys = Collections.singletonList(GroupingKeys.label());
      vertexAggregateFunctions = Collections.singletonList(new Count("count"));
      edgeAggregateFunctions = Collections.singletonList(new Count("count"));
      break;

    default:
      throw new IllegalArgumentException("Unsupported config: " + select);
    }

    return new KeyedGrouping<>(vertexKeys, vertexAggregateFunctions, edgeKeys, edgeAggregateFunctions);
  }

  /**
   * Read values from the given {@link CommandLine} object
   *
   * @param cmd the {@link CommandLine} object
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH = cmd.getOptionValue(OPTION_CSV_PATH);
    SELECTED_CONFIG = Integer.parseInt(cmd.getOptionValue(OPTION_CONFIG));
    IS_TEMPORAL = cmd.hasOption(OPTION_TEMPORAL);
  }

  /**
   * Method to create and add lines to a csv file which contains all necessary statistics about the
   * {@link KeyedGrouping} operator.
   *
   * @param env given {@link ExecutionEnvironment}
   * @throws IOException exception during file writing
   */
  private static void writeStatistics(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s%n",
        "Parallelism",
        "dataset",
        "config",
        "temporal",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s%n",
        env.getParallelism(),
        INPUT_PATH,
        SELECTED_CONFIG,
        IS_TEMPORAL,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }
}
