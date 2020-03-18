import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Step1 {

    /*
    Versão em que a chave de cada linha é um chave composta por nome do filme + qualidade do voto: (Avatar&1)....(Avatar&10)
    Justifica-se a sua utilização por motivos de concorrência nas votações. E será a utilizada nos outros "steps"
     */
    public static void compositeKey(Table ht) throws IOException {
        for(int i=0; i<10; i++) {
            for(int j=1; j<=10; j++) {
                byte[] rowKey = Bytes.add(Bytes.toBytes("FILME" + i), Bytes.toBytes("&" + j));
                Put put = new Put(rowKey);
                put.addColumn(Bytes.toBytes("qual"), Bytes.toBytes("q"),
                        Bytes.toBytes(0L));
                ht.put(put);
            }
        }
    }

    /*
    Versão em que existe um qualificador para cada tipo de voto.
     */
    public static void multiQualifier(Table ht) throws IOException {
        for(int i=0; i<10; i++) {
            Put put = new Put(Bytes.toBytes("FILME" + i));
            for(int j = 1; j<=10; j++)
                put.addColumn(Bytes.toBytes("qual"), Bytes.toBytes("q"+j),
                        Bytes.toBytes(0L));
            ht.put(put);
        }
    }


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        // criar tabela
        Admin admin = conn.getAdmin();
        HTableDescriptor t = new HTableDescriptor(TableName.valueOf("filmes"));
        t.addFamily(new HColumnDescriptor("qual"));
        admin.createTable(t);
        admin.close();

        Table ht = conn.getTable(TableName.valueOf("filmes"));
        compositeKey(ht);
        ht.close();
        conn.close();
    }
}
