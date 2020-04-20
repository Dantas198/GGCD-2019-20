import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class LoadActorInfo {
    public static class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() != 0) {
                String[] words = value.toString().split("\\t");

                // nome de colunas pequenos para tentar diminuir ao máximo o espaço ocupado
                Put put = new Put(Bytes.toBytes(words[0]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("n"), Bytes.toBytes(words[1]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("b"), Bytes.toBytes(words[2]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("d"), Bytes.toBytes(words[3]));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(words[0])), put);
            }
        }
    }

    public static void executeJob(Configuration conf, String name, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(LoadActorInfo.class);
        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, output);
        job.waitForCompletion(true);
    }
}
