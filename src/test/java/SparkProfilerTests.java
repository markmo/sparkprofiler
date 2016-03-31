import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.metamorphic.analysiscommons.models.DatasetMetrics;
import io.metamorphic.sparkprofiler.SparkAnalysisServiceImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by markmo on 31/03/2016.
 */
public class SparkProfilerTests {

    @Test
    public void testFileShouldReturnCorrectResults() throws Exception {
        SparkAnalysisServiceImpl service = new SparkAnalysisServiceImpl();
        String path = getClass().getResource("Customer_Demographics.parquet").getPath();
        System.out.println("File path: " + path);
        DatasetMetrics metrics = service.analyze("Test", "local[2]", path);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        System.out.println(mapper.writeValueAsString(metrics));

        assertEquals(
                "Distinct value count of 'age40to44' column should be 2",
                2L, (long)metrics.getColumnMetricsMap().get("age40to44").getMetricsMap().get("Distinct values count").getValue()
        );
    }

}
