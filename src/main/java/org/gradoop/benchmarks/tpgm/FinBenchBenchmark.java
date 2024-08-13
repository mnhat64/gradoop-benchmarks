package org.gradoop.benchmarks.tpgm;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumEdgeProperty;
import org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGrouping;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;


public class FinBenchBenchmark extends BaseTpgmBenchmark {


    private static final String OPTION_QUERY_FROM = "f";
    private static final String OPTION_QUERY_TO = "t";
    private static final String OPTION_QUERY_TYPE = "y";
    private static final String OPTION_QUERY_ID = "id";
    private static final String OPTION_QUERY_THRESHOLD = "th";

    private static final String TYPE_QUERY_4 = "4";
    private static final String TYPE_QUERY_6 = "6";

    private static Long QUERY_FROM;
    private static Long QUERY_TO;
    private static String QUERY_TYPE;
    private static String QUERY_ID;
    private static Double QUERY_THRESHOLD;

    static {
        OPTIONS.addRequiredOption(OPTION_QUERY_TYPE, "type", true, "Used query type");
        OPTIONS.addOption(OPTION_QUERY_FROM, "from", true, "Used query from timestamp [ms]");
        OPTIONS.addOption(OPTION_QUERY_TO, "to", true, "Used query to timestamp [ms]");
        OPTIONS.addOption(OPTION_QUERY_ID, "id", true, "Account ID to look for");
        OPTIONS.addOption(OPTION_QUERY_THRESHOLD, "threshold", true, "Minimum threshold for transactions");
    }

    /**
     * Main program to run the benchmark. Arguments are the available options.
     * Example: {@code /path/to/flink run -c org.gradoop.benchmarks.tpgm.FinBenchBenchmark
     * path/to/gradoop-benchmarks.jar -i hdfs:///graph -o hdfs:///output -c results.csv
     * -y 4 -f 1664803797282000 -t 1719319393000 -id 4902731144346222798 -th 10}
     *
     * @param args program arguments
     * @throws Exception in case of error
     */

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArguments(args, SnapshotBenchmark.class.getName());

        if (cmd == null) {
            return;
        }

        readBaseCMDArguments(cmd);
        readCMDArguments(cmd);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TemporalGradoopConfig conf = TemporalGradoopConfig.createConfig(env);

        TemporalDataSource source = new TemporalCSVDataSource(INPUT_PATH, conf);
        TemporalGraph graph = source.getTemporalGraph();

        switch (QUERY_TYPE) {
            case TYPE_QUERY_4:
                tsr4(graph, QUERY_ID, QUERY_FROM, QUERY_TO, QUERY_THRESHOLD).writeAsText(OUTPUT_PATH + "/result.txt" , FileSystem.WriteMode.OVERWRITE);
                break;
            case TYPE_QUERY_6:
                tsr6(graph, QUERY_ID, QUERY_FROM, QUERY_TO, QUERY_THRESHOLD).writeAsText(OUTPUT_PATH + "/result.txt", FileSystem.WriteMode.OVERWRITE);
                break;
            default:
                throw new IllegalArgumentException("The given query type '" + QUERY_TYPE + "' is not supported.");
        }

        env.execute(FinBenchBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
        writeCSV(env);
    }

    private static void readCMDArguments(CommandLine cmd) {
        String queryFrom        = cmd.getOptionValue(OPTION_QUERY_FROM);
        QUERY_FROM              = queryFrom == null ? null : Long.valueOf(queryFrom);

        String queryTo          = cmd.getOptionValue(OPTION_QUERY_TO);
        QUERY_TO                = queryTo == null ? null : Long.valueOf(queryTo);

        String queryThreshold   = cmd.getOptionValue(OPTION_QUERY_THRESHOLD);
        QUERY_THRESHOLD         = Double.valueOf(queryThreshold);

        QUERY_TYPE              = cmd.getOptionValue(OPTION_QUERY_TYPE);
        QUERY_ID                = cmd.getOptionValue(OPTION_QUERY_ID);
    }

    private static void writeCSV(ExecutionEnvironment env) throws IOException {
        String head = String
                .format("%s|%s|%s|%s|%s|%s",
                        "Parallelism",
                        "dataset",
                        "query-type",
                        "from(ms)",
                        "to(ms)",
                        "Runtime(ms)");

        String tail = String
                .format("%s|%s|%s|%s|%s|%s",
                        env.getParallelism(),
                        INPUT_PATH,
                        QUERY_TYPE,
                        QUERY_FROM,
                        QUERY_TO,
                        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.MILLISECONDS));
        writeToCSVFile(head, tail);
    }

    private static DataSet<Tuple3<String, Long, Double>> tsr4(TemporalGraph graph, String id, long startTime, long endTime, double threshold) {
        return graph
                .fromTo(startTime, endTime)
                .subgraph(v -> v.getLabel().equals("Account"), e -> (e.getLabel().equals("Transfer") && e.getProperties().get("SourceID").getString().equals(id) && e.getProperties().get("Amount").getDouble() > threshold))
                .callForGraph(
                        new KeyedGrouping<>(
                                Arrays.asList(
                                        GroupingKeys.property("ID")),
                                null,
                                Arrays.asList(
                                        GroupingKeys.property("TargetID")),
                                Arrays.asList(
                                        new Count("Transfer Count"),
                                        new SumEdgeProperty("Amount", "Transfer Sum"))))
                .getEdges()
                .map(edge -> new Tuple3<>(edge.getProperties().get("TargetID").toString(), edge.getPropertyValue("Transfer Count").getLong(), edge.getPropertyValue("Transfer Sum").getDouble()))
                .returns(Types.TUPLE(Types.STRING, Types.LONG, Types.DOUBLE))
                .sortPartition(2, Order.DESCENDING).setParallelism(1)
                .sortPartition(0, Order.ASCENDING).setParallelism(1);
    }

    private static DataSet<Long> tsr6(TemporalGraph graph, String id, long startTime, long endTime, double threshold) throws Exception {
        return graph.fromTo(startTime, endTime)
                .subgraph(v -> true,  e -> (e.getLabel().equals("Transfer") && e.getProperties().get("Amount").getDouble() > threshold))
                .query(String.format(
                        "MATCH (src:Account {ID: \"%s\"})<-[e1:Transfer]-(mid:Account)-[e2:Transfer]->(dst:Account {IsBlocked: true})\n " +
                                "WHERE src.ID <> dst.ID", id))
                .getEdges()
                .map(edge -> edge.getProperties().get("TargetID").getLong())
                .distinct()
                .sortPartition(value -> value, Order.ASCENDING).setParallelism(1);
    }
}
