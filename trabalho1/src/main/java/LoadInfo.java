import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
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

public class LoadInfo {
    public static void createTable(Connection conn, String tableName, String... families) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf(tableName));
        for(String s : families)
            t.addFamily(new HColumnDescriptor(s));
        admin.createTable(t);
        admin.close();
    }

    public static class LoadActorInfoMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
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

    public static class LoadMovieInfoMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() !=  0){
                String[] words = value.toString().split("\\t");

                Put put = new Put(Bytes.toBytes(words[0]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("tt"), Bytes.toBytes(words[1]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pt"), Bytes.toBytes(words[2]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ot"), Bytes.toBytes(words[3]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ia"), Bytes.toBytes(words[4]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sy"), Bytes.toBytes(words[5]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ey"), Bytes.toBytes(words[6]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rtm"), Bytes.toBytes(words[7]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("g"), Bytes.toBytes(words[8]));
                context.write(new ImmutableBytesWritable(Bytes.toBytes(words[0])), put);
            }
        }
    }

    public static void executeLoadMovieJob(Configuration conf, String name, String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        executeLoadJob(conf, LoadMovieInfoMapper.class, name, input, output);
    }

    public static void executeLoadActorJob(Configuration conf, String name, String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        executeLoadJob(conf, LoadActorInfoMapper.class, name, input, output);
    }

    public static void executeLoadJob(Configuration conf, Class<? extends Mapper> mapper, String name, String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(LoadInfo.class);
        job.setMapperClass(mapper);
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
