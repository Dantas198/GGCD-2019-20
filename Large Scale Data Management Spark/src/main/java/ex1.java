import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ex1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("g0spark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> mr = sc.textFile("file///C:/Users/MarcoFilipeLeitãoDan/Desktop/4ºAno/2ºsemestre/GGCD/downloads/imdbMicro/title.basics.tsv")
                .map(l -> l.split("\t"))
                .filter(l -> !l[0].equals("tconst"))
                .map(l -> l[8])
                .filter(l -> !l.equals("\\N"))
                .flatMap(l -> Arrays.asList(l.split(",")).iterator())
                .mapToPair(l -> new Tuple2<>(l, 1))
                .foldByKey(0, (v1, v2) -> v1 + v2);

        List<Tuple2<String, Integer>> genres = mr.collect();
        System.out.println(genres.toString());
    }
}
