
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.util.Map;

public class Expeditions {
    public static void main(String[] args) throws Exception{
        final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf config = new SparkConf().setAppName("kinaxisExpedition").setMaster("local[3]");
        JavaSparkContext  sparkContext  = new JavaSparkContext(config);
        JavaRDD<String> entries = sparkContext.textFile("data/expeditions.csv");
        JavaRDD<String> clearedEntries = entries.filter(line -> !line.contains("Mineral"));

        JavaPairRDD<String, Integer> mineralAndQuantityPair =
                getMineralAndQuantityPair(COMMA_DELIMITER, clearedEntries);

        JavaPairRDD<String, Iterable<Integer>> expeditionTrip = mineralAndQuantityPair.groupByKey();

        FileWriter outputWriter = new FileWriter("output/minerals.csv", true);
        outputWriter.append("Minerals");
        outputWriter.append(",");
        outputWriter.append("Quantity");
        outputWriter.append("\n");

        for (Map.Entry<String, Iterable<Integer>> expedition : expeditionTrip.collectAsMap().entrySet()) {

            //Computing the total quantity of each minerals
            int quantity = 0 ;
            for (Integer qty:expedition.getValue()){
                quantity += qty;
            }

            //appending the minerals and total quantity to the output file
            outputWriter.append(expedition.getKey());
            outputWriter.append(",");
            outputWriter.append(""+quantity);
            outputWriter.append("\n");
        }
        outputWriter.flush();
        outputWriter.close();

    }

    static JavaPairRDD<String, Integer> getMineralAndQuantityPair(String COMMA_DELIMITER, JavaRDD<String> clearedEntries) {
        return clearedEntries.mapToPair( minerals -> new Tuple2<>(minerals.split(COMMA_DELIMITER)[4],
                Integer.parseInt(minerals.split(COMMA_DELIMITER)[3])));
    }
}
