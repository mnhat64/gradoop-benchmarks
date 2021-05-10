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
package org.gradoop.benchmarks.utils;

import org.gradoop.benchmarks.AbstractRunner;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.temporal.model.impl.TemporalGraph;

/**
 * Converts a given graph into another gradoop format.
 */
public class GradoopFormatConverter extends AbstractRunner {

  /**
   * Converts a graph from one gradoop format to another.
   *
   * args[0] - path to input graph
   * args[1] - format of input graph (csv, indexed)
   * args[2] - path to output graph
   * args[3] - format of output graph (csv, indexed)
   * args[4] - 'temporal' if input and output is a TPGM graph
   *
   * @param args program arguments
   * @throws Exception on failure
   */
  public static void main(String[] args) throws Exception {
    if (args[4] != null && args[4].equals("temporal")) {
      TemporalGraph temporalGraph = readTemporalGraph(args[0], args[1]);
      writeTemporalGraph(temporalGraph, args[2], args[3]);
    } else {
      LogicalGraph graph = readLogicalGraph(args[0], args[1]);
      writeLogicalGraph(graph, args[2], args[3]);
    }
  }
}
