import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExpeditionsTest extends SharedJavaSparkContext {
    final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    @Test
    public void expeditionValidTest() {

        // Creating test samples
        List<String> csvEntry = Arrays.asList("1,A,1,4,Chromium,1", "2,B,3,2,Titanium,2", "3,A,2,7,Titanium,2");
        JavaRDD<String> sampleRDDInput = jsc().parallelize(csvEntry);
        JavaPairRDD<String, Integer> result = Expeditions.getMineralAndQuantityPair(COMMA_DELIMITER, sampleRDDInput);

        // Creating the expected output
        List<Tuple2<String, Integer>> expectedInput = Arrays.asList(new Tuple2<>("Chromium", 4), new Tuple2<>("Titanium", 2), new Tuple2<>("Titanium", 7));
        JavaPairRDD<String, Integer> expectedRDD = jsc().parallelizePairs(expectedInput);

        // Creating ClassTag that allows class reflection
        ClassTag<Tuple2<String, Integer>> tag = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);

        // Run the assertions on the result and expected
        JavaRDDComparisons.assertRDDEquals(JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag), JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));
    }

    @Test
    public void verifyFailureTest() {

        // Creating test samples
        List<String> csvEntry = Arrays.asList("1,A,1,4,Chromium,1", "2,B,3,2,Titanium,2");
        JavaRDD<String> sampleRDDInput = jsc().parallelize(csvEntry);
        JavaPairRDD<String, Integer> result = Expeditions.getMineralAndQuantityPair(COMMA_DELIMITER, sampleRDDInput);

        // Creating the expected output
        List<Tuple2<String, Integer>> expectedInput = Arrays.asList(new Tuple2<>("Chromium", 4), new Tuple2<>("Titanium", 7));
        JavaPairRDD<String, Integer> expectedRDD = jsc().parallelizePairs(expectedInput);

        // Creating ClassTag that allows class reflection
        ClassTag<Tuple2<String, Integer>> tag = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);

        // Obtaining Tuple2 lists that don't match
        Option<Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>>> compareWithOrder = JavaRDDComparisons.compareRDDWithOrder(JavaRDD.fromRDD(JavaPairRDD.toRDD(result), tag), JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));

        // Creating non matching objects
        Option<Tuple2<String, Integer>> correctTuple = Option.apply(new Tuple2<>("Titanium", 2));
        Option<Tuple2<String, Integer>> wrongTuple = Option.apply(new Tuple2<>("Titanium", 7));

        Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>> wrongValueCombination = new Tuple2<>(correctTuple, wrongTuple);

        // Run the assertions on the right and wrong values
        Option<Tuple2<Option<Tuple2<String, Integer>>, Option<Tuple2<String, Integer>>>> wrongValue = Option.apply(wrongValueCombination);
        assertEquals(wrongValue, compareWithOrder);
    }

}