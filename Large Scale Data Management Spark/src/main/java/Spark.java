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

        JavaPairRDD<String, Integer> mr = sc.textFile("/Users/guilhermeviveiros/Desktop/4ano/4ano-2semestre/GGCD/Large Scale Data Management Spark/title.basics.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .map(l -> l[8])
                .filter(l -> !l.equals("\\N"))
                .flatMap(l -> Arrays.asList(l.split(",")).iterator())
                .mapToPair(l -> new Tuple2<>(l, 1))
                .foldByKey(0, (v1, v2) -> v1 + v2);

        //List<Tuple2<String, Integer>> genres = mr.collect();

        double end = System.nanoTime();

        //System.out.println("Time in the mini file = " + (begin-end));

        // Compute a list showing the rating for each title using also title.ratings.tsv.gz

        JavaPairRDD<String, Float> mr2 = sc.textFile("/Users/guilhermeviveiros/Desktop/4ano/4ano-2semestre/GGCD/Large Scale Data Management Spark/title.ratings.tsv.bz2")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .filter(l -> !l.equals("\\N"))
                .mapToPair(l -> new Tuple2<>(l[0], Float.parseFloat(l[1])))
                .filter( l ->  l._2 > 9.0);




        //.filter( (fst,snd) -> fst.toString().equals("ola"));




        //.foldByKey(0, (v1, v2) -> v1 + v2);
        //Compute a list of films with rating of at least 9.0, sorted by rating.


                //.filter(l -> !l.equals("\\N"))
                //.flatMap(l -> Arrays.asList(l.split(",")).iterator())
                //.mapToPair(l -> new Tuple2<>(l, 1))
                //foldByKey(0, (v1, v2) -> v1 + v2);

        List<Tuple2<String, Float>> votes = mr2.collect();

        System.out.println(votes);
    }
}
