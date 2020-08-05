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

public class KeyedGroupingBenchmark extends AbstractRunner {

  /**
   * Option to declare path to input graph
   */
  private static final String OPTION_INPUT_PATH = "i";

  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";

  private static final String OPTION_CONFIG = "c";
  /**
   * Path to CSV log file
   */
  private static final String OPTION_STATISTICS_PATH = "csv";

  private static final String OPTION_TEMPORAL = "t";

  private static int SELECTED_CONFIG;

  private static String INPUT_PATH;

  private static String OUTPUT_PATH;

  private static String STATISTICS_PATH;

  private static boolean IS_TEMPORAL;

  private static TimeDimension DIMENSION = TimeDimension.VALID_TIME;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true, "Path to input csv directory");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true, "Path to output directory");
    OPTIONS.addRequiredOption(OPTION_STATISTICS_PATH, "statistics", true, "Path to statistics csv file");
    OPTIONS.addRequiredOption(OPTION_CONFIG, "config", true, "Select predefined configuration");
    OPTIONS.addOption(OPTION_TEMPORAL, "temporal", false, "Apply temporal keyed grouping to input graph");
  }


  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, AggregationBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read cmd arguments
    readCMDArguments(cmd);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    if (IS_TEMPORAL) {
      TemporalGradoopConfig cfg = TemporalGradoopConfig.createConfig(env);

      TemporalDataSource source = new TemporalCSVDataSource(INPUT_PATH, cfg);
      TemporalGraph graph = source.getTemporalGraph();

      TemporalGraph groupedGraph = graph.callForGraph(getTPGMGroupingConfig(SELECTED_CONFIG));

      TemporalCSVDataSink sink = new TemporalCSVDataSink(OUTPUT_PATH, cfg);
      sink.write(groupedGraph);
    } else {
      GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);

      DataSource source = new CSVDataSource(INPUT_PATH, cfg);
      LogicalGraph graph = source.getLogicalGraph();

      LogicalGraph groupedGraph = graph.callForGraph(getEPGMGroupingConfig(SELECTED_CONFIG));

      CSVDataSink sink = new CSVDataSink(OUTPUT_PATH, cfg);
      sink.write(groupedGraph);
    }

    env.execute(KeyedGrouping.class.getSimpleName() + " - P: " + env.getParallelism());

    writeStatistics(env);
  }

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
          TemporalGroupingKeys.timeStamp(DIMENSION, TimeDimension.Field.FROM, ChronoField.ALIGNED_WEEK_OF_YEAR));
        edgeKeys = Arrays.asList(
          GroupingKeys.label(),
          TemporalGroupingKeys.timeStamp(DIMENSION, TimeDimension.Field.FROM, ChronoField.ALIGNED_WEEK_OF_YEAR));

        vertexAggregateFunctions = Arrays.asList(
          new Count("count")
        );

        edgeAggregateFunctions = Arrays.asList(
          new Count("count")
        );
        break;
      default:
        throw new IllegalArgumentException("Unsupported config: " + select);
    }
    return new KeyedGrouping<>(vertexKeys, vertexAggregateFunctions, edgeKeys, edgeAggregateFunctions);
  }

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
      default:
        throw new IllegalArgumentException("Unsupported config: " + select);
    }

    return new KeyedGrouping<>(vertexKeys, vertexAggregateFunctions, edgeKeys, edgeAggregateFunctions);
  }

  private static void readCMDArguments(CommandLine cmd) {
    // read input output paths
    INPUT_PATH = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    STATISTICS_PATH = cmd.getOptionValue(OPTION_STATISTICS_PATH);

    String config = cmd.getOptionValue(OPTION_CONFIG);
    SELECTED_CONFIG = Integer.parseInt(config);

    IS_TEMPORAL = cmd.hasOption(OPTION_TEMPORAL);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeStatistics(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s",
        "Parallelism",
        "dataset",
        "config",
        "temporal",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s",
        env.getParallelism(),
        INPUT_PATH,
        SELECTED_CONFIG,
        IS_TEMPORAL,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(STATISTICS_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(STATISTICS_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }

}
