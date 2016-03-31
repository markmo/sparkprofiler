package io.metamorphic.sparkprofiler;

import io.metamorphic.analysiscommons.models.DatasetMetrics;

/**
 * Created by markmo on 26/10/2015.
 */
public interface SparkAnalysisService {

    DatasetMetrics analyze(String sourceName, String master, String filepath);

    void shutdown();
}
