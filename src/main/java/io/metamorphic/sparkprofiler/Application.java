package io.metamorphic.sparkprofiler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.metamorphic.analysiscommons.models.DatasetMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by markmo on 26/10/2015.
 */
public class Application {

    private static final Log log = LogFactory.getLog(Application.class);

    /**
     *
     * @param args String[] arg0: dataset name, arg1: spark master, arg2: parquet file path
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        SparkAnalysisServiceImpl service = new SparkAnalysisServiceImpl();
        DatasetMetrics metrics = service.analyze(args[0], args[1], args[2]);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        if (log.isDebugEnabled()) {
            log.debug(mapper.writeValueAsString(metrics));
        }
        System.out.println(mapper.writeValueAsString(metrics));
    }
}
