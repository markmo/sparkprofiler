package io.metamorphic.sparkprofiler;

import io.metamorphic.analysiscommons.models.ColumnType;
import io.metamorphic.analysiscommons.models.DatasetMetrics;
import io.metamorphic.analysiscommons.models.TermFrequency;
import io.metamorphic.sparkprofiler.models.ColumnStats;
import io.metamorphic.sparkprofiler.models.FirstPassStatsModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.metamorphic.analysiscommons.models.MetricTypes.*;

/**
 * Created by markmo on 4/07/2015.
 */
public class SparkAnalysisServiceImpl implements SparkAnalysisService {

    private static final Log log = LogFactory.getLog(SparkAnalysisServiceImpl.class);

    private WeakHashMap<String, JavaSparkContext> connections = new WeakHashMap<>();

    private JavaSparkContext connect(String sourceName, String master) {
        if (log.isDebugEnabled()) {
            log.debug("Connecting to Spark master: " + master);
        }
        SparkConf conf = new SparkConf().setAppName(sourceName).setMaster(master);
        conf.set("spark.broadcast.compress", "false");
        conf.set("spark.shuffle.compress", "false");
        conf.set("spark.shuffle.spill.compress", "false");
        return new JavaSparkContext(conf);
    }

    public DatasetMetrics analyze(String sourceName, String master, String filepath) {
        if (log.isDebugEnabled()) {
            log.debug("Starting analysis of: " + sourceName);
        }
        JavaSparkContext sc;
        if (!connections.containsKey(master)) {
            sc = connect(sourceName, master);
            connections.put(master, sc);
        } else {
            sc = connections.get(master);
        }
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().parquet(filepath);
        FirstPassStatsModel firstPassStats = getFirstPassStat(df);
        StructType schema = df.schema();
        StructField[] fields = schema.fields();
        DatasetMetrics datasetMetrics = new DatasetMetrics(sourceName, "ParquetFile");
        firstPassStats.getColumnStatsMap().entrySet().forEach(e -> {
            int columnIdx = e.getKey();
            ColumnStats columnStats = e.getValue();
            StructField field = fields[columnIdx];
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_NULL_COUNT, columnStats.getNulls());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_BLANK_COUNT, columnStats.getEmpties());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_ROW_COUNT, columnStats.getTotalCount());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_DISTINCT_VALUES_COUNT, columnStats.getUniqueValues());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_HIGHEST_VALUE, columnStats.getMaxLong());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_LOWEST_VALUE, columnStats.getMinLong());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_SUM, columnStats.getSumLong());
            List<TermFrequency> topValues = columnStats.getTopNValues().getTopNCountsForColumnArray().stream().map(c ->
                new TermFrequency(String.valueOf(c.getLeft()), c.getRight() != null ? c.getRight().intValue() : 0)
            ).collect(Collectors.toList());
            datasetMetrics.addColumnMetric(field.name(), columnIdx, ColumnType.VARCHAR, MEASURE_TOP_5, topValues);
        });
        return datasetMetrics;
    }

    public void shutdown() {
        connections.values().forEach(JavaSparkContext::close);
    }

    private FirstPassStatsModel getFirstPassStat(DataFrame df) {
        StructType schema = df.schema();
        JavaPairRDD<Tuple2<Integer, Object>, Long> columnValueCounts = df.javaRDD()
            .flatMapToPair(row ->
                // ((columnIdx, cellValue), count)
                IntStream.range(0, schema.length())
                        .boxed()
                        .map(idx -> new Tuple2<>(new Tuple2<>(idx, row.get(idx)), 1L))
                        .collect(Collectors.toList())
            ).reduceByKey((a, b) -> a + b);
        return columnValueCounts.mapPartitions(it -> {
            FirstPassStatsModel model = new FirstPassStatsModel();
            it.forEachRemaining(t ->
                    model.add(t._1()._1(), t._1()._2(), t._2())
            );
            ArrayList<FirstPassStatsModel> models = new ArrayList<>();
            models.add(model);
            return models;
        }).reduce(FirstPassStatsModel::add);
    }
}
