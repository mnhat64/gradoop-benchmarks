/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageDuration;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageEdgeDuration;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageVertexDuration;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MaxEdgeTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MaxTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MaxVertexTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MinEdgeTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MinTime;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.MinVertexTime;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized TPGM aggregation benchmark.
 */
public class AggregationBenchmark extends BaseTpgmBenchmark {

  /**
   * Main program to run the benchmark. Arguments are the available options.
   * Example: {@code /path/to/flink run -c org.gradoop.benchmarks.tpgm.AggregationBenchmark
   * path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, AggregationBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read cmd arguments
    readBaseCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    TemporalGradoopConfig conf = TemporalGradoopConfig.createConfig(env);

    // read graph
    TemporalDataSource source = new TemporalCSVDataSource(INPUT_PATH, conf);
    TemporalGraph graph = source.getTemporalGraph();

    // we consider valid times only
    TimeDimension timeDimension = TimeDimension.VALID_TIME;

    // get the diff
    TemporalGraph aggregate = graph.aggregate(
      new MinVertexTime("minVertexValidFrom", timeDimension, TimeDimension.Field.FROM),
      new MinEdgeTime("minEdgeValidFrom", timeDimension, TimeDimension.Field.FROM),
      new MaxTime("maxValidFrom", timeDimension, TimeDimension.Field.FROM),
      new MaxVertexTime("maxVertexValidTo", timeDimension, TimeDimension.Field.TO),
      new MaxEdgeTime("maxEdgeValidTo", timeDimension, TimeDimension.Field.TO),
      new MinTime("minValidTo", timeDimension, TimeDimension.Field.TO),
      new AverageDuration("avgDuration", timeDimension),
      new AverageEdgeDuration("avgEdgeDuration", timeDimension),
      new AverageVertexDuration("avgVertexDuration", timeDimension));

    // write graph
    writeOrCountGraph(aggregate, conf);

    // execute and write job statistics
    env.execute(AggregationBenchmark.class.getSimpleName() + " - P:" + env.getParallelism());
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

    String tail = String.format("%s|%s|%s",
      env.getParallelism(),
      INPUT_PATH,
      env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    writeToCSVFile(head, tail);
  }
}
