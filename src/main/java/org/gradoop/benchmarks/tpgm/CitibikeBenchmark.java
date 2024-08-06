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
package org.gradoop.benchmarks.tpgm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.benchmarks.utils.GradoopFormat;
import org.gradoop.benchmarks.utils.GridId;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.functions.predicates.Overlaps;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageDuration;
import org.gradoop.temporal.model.impl.operators.keyedgrouping.TemporalGroupingKeys;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.gradoop.temporal.model.api.TimeDimension.VALID_TIME;

/**
 * Dedicated program to benchmark a complex dataflow that contains numerous TPGM related transformations.
 * The benchmark is expected to be executed on the Citibike data set.
 */
public class CitibikeBenchmark extends BaseTpgmBenchmark {
  /**
   * Main program to run the benchmark.
   * <p>
   * Example: {@code $ /path/to/flink run -c org.gradoop.benchmarks.tpgm.CitibikeBenchmark
   * /path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, DiffBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    readBaseCMDArguments(cmd);

    TemporalGraph citibikeGraph = readTemporalGraph(INPUT_PATH, GradoopFormat.getByName(INPUT_FORMAT))
      // Snapshot
      .snapshot(new Overlaps(LocalDateTime.of(2017,1,1,0,0), LocalDateTime.of(2019,1,1,0,0)), VALID_TIME)
      // Transformation
      .transformVertices((TransformationFunction<TemporalVertex>) (temporalVertex, el1) -> {
        GridId gridId = new GridId();
        temporalVertex.setProperty("cellId", gridId.getKey(temporalVertex));
        return temporalVertex;
      })
      // Pattern matching
      .temporalQuery("MATCH (v1:Station {cellId: 2883})-[t1:Trip]->(v2:Station)-[t2:Trip]->(v3:Station) " +
        "WHERE v2.id != v1.id " +
        "AND v2.id != v3.id " +
        "AND v3.id != v1.id " +
        // A performance optimization trick
        //"AND v1.tx_to > 1970-01-01" +
        //"AND t1.bike_id = t2.bike_id " +
        "AND t1.val.precedes(t2.val) " +
        "AND t1.val.lengthAtLeast(Minutes(30)) " +
        "AND t2.val.lengthAtLeast(Minutes(30))")
      // Reduce collection to graph
      .reduce(new ReduceCombination<>())
      // Grouping
      .callForGraph(
        new KeyedGrouping<>(
          // Vertex grouping key functions
          Arrays.asList(
            GroupingKeys.label(),
            GroupingKeys.property("name"),
            GroupingKeys.property("cellId")),
          // Vertex aggregates
          null,
          // Edge grouping key functions
          Arrays.asList(
            GroupingKeys.label(),
            TemporalGroupingKeys.timeStamp(VALID_TIME, TimeDimension.Field.FROM, ChronoField.MONTH_OF_YEAR)),
          // Edge aggregates
          Arrays.asList(
            new Count("countTripsOfMonth"),
            new AverageDuration("avgTripDurationOfMonth", VALID_TIME))))
      // Subgraph
      .subgraph(
        v -> true,
        e -> e.getPropertyValue("countTripsOfMonth").getLong() >= 1)
      .verify();

    writeOrCountGraph(citibikeGraph, citibikeGraph.getConfig());

    ExecutionEnvironment env = citibikeGraph.getConfig().getExecutionEnvironment();

    env.execute(CitibikeBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exception during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String.format("%s|%s|%s", "Parallelism", "dataset", "Runtime(s)");

    String tail = String.format("%s|%s|%s", env.getParallelism(), INPUT_PATH,
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    writeToCSVFile(head, tail);
  }
}
