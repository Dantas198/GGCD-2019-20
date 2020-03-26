package database;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class DB {





    public static class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() !=  0){
                String[] words = value.toString().split("\\t");

                Put put = new Put(Bytes.toBytes(words[0]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("titleType"), Bytes.toBytes(words[1]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("primaryTitle"), Bytes.toBytes(words[2]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("originalTitle"), Bytes.toBytes(words[3]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("isAdult"), Bytes.toBytes(words[4]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("startYear"), Bytes.toBytes(words[5]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("endYear"), Bytes.toBytes(words[6]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("runTimeMinutes"), Bytes.toBytes(words[7]));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("genres"), Bytes.toBytes(words[8]));

                for(int i=1;i<11;i++){
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vote_"+i), Bytes.toBytes(0));

                }

                context.write(new ImmutableBytesWritable(Bytes.toBytes(words[0])), put);

            }
        }
    }




    public static void main(String[] args) throws Exception{


        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zoo");
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("filmes"));
        t.addFamily(new HColumnDescriptor("info"));
        admin.createTable(t);
        admin.close();

        Job job = Job.getInstance(conf, "guiao4_test");
        job.setJarByClass(DB.class);
        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, "./data/title.basics.tsv.gz");
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "filmes");
        job.waitForCompletion(true);


    }

}
