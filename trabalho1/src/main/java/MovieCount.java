import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;


import java.io.IOException;

public class MovieCount {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() !=  0){
                String[] words = value.toString().split("\\t");

                if(words[3].equals("actor") || words[3].equals("actress") || words[3].equals("self")){
                    context.write(new Text(words[2]), new LongWritable(1));
                }
            }
        }
    }

    public static class MyReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable value: values) {
                sum+=value.get();
            }
            byte[] row = key.getBytes();
            Put put = new Put(row);
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("c"), Bytes.toBytes(sum));
            context.write(new ImmutableBytesWritable(row), put);
        }
    }

    public static void executeJob(Configuration conf, String name, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(MovieCount.class);
        job.setMapperClass(MyMapper.class);

        //combiner
        job.setCombinerClass(LongSumReducer.class);

        //job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);

        TableMapReduceUtil.initTableReducerJob(output, MyReducer.class, job);
        //job.setNumReduceTasks(2);

        job.waitForCompletion(true);
    }

}
