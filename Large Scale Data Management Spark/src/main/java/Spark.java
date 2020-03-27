import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;


public class Spark {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        double begin = System.nanoTime();

        // Compute a list showing the rating for each title using also title.ratings.tsv.gz

        JavaPairRDD<String, Float> mr2 = sc.textFile("/Users/guilhermeviveiros/Desktop/4ano/4ano-2semestre/GGCD/Large Scale Data Management Spark/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])))

                //Compute a list of films with rating of at least 9.0, sorted by rating.
                .filter( l ->  l._2 > 9.0);


        double end = System.nanoTime();

        //System.out.println("Time in the mini file = " + (begin-end));

        List<Tuple2<String, Float>> votes = mr2.collect();

        System.out.println(votes);
    }
}
