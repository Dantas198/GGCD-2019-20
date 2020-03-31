import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ex3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> left = sc.textFile("file///C:/Users/MarcoFilipeLeitãoDan/Desktop/4ºAno/2ºsemestre/GGCD/downloads/imdbMicro/title.basics.tsv")
                .map(l -> l.split("\\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));


        JavaPairRDD<String, String> right = sc.textFile("file///C:/Users/MarcoFilipeLeitãoDan/Desktop/4ºAno/2ºsemestre/GGCD/downloads/imdbMicro/title.ratings.tsv")
                .map(l -> l.split("\\t"))
                .filter(l -> !l[0].equals("tconst"))
                .mapToPair(l -> new Tuple2<>(l[0], l[1]));


        JavaPairRDD<String, Tuple2<String, String>> joinedValues = left.join(right);
        joinedValues.collect().forEach(t -> System.out.println(t._2.toString()));
    }
}
