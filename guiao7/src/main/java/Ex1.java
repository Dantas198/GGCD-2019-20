import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class Ex1 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("simplestream");

        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(60));
        sc.socketTextStream("localhost", 12345)
                .foreachRDD(rdd -> rdd.saveAsTextFile("/Users/dantas/Documents/GitHub/GGCD-2019-20/guiao7/ex1Files"));

        sc.start();
        sc.awaitTermination();
    }
}
