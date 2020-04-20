package utils;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextFloatPair implements WritableComparable<TextFloatPair> {
    public Text first;
    public FloatWritable second;

    public TextFloatPair(){
        this.first=new Text();
        this.second=new FloatWritable();
    }

    public TextFloatPair(Text first, FloatWritable second) {
        //super();
        this.first = first;
        this.second = second;
    }
    public TextFloatPair(String first, Float second){
        this.first=new Text(first);
        this.second=new FloatWritable(second);
    }

    public TextFloatPair(TextFloatPair tp){
        this.first = new Text(tp.getFirst());
        this.second = new FloatWritable(tp.getSecond().get());
    }

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public FloatWritable getSecond() {
        return second;
    }

    public void setSecond(FloatWritable second) {
        this.second = second;
    }
    public void set(Text first,FloatWritable second){
        this.first=first;
        this.second=second;
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return first.hashCode()*163+second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if(obj instanceof TextFloatPair){
            TextFloatPair tp=(TextFloatPair)obj;
            return first.equals(tp.getFirst())&&second.equals(tp.getSecond());
        }
        return false;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return first+"\t"+second.toString();
    }


    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        first.readFields(in);
        second.readFields(in);
    }


    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        first.write(out);
        second.write(out);
    }


    public int compareTo(TextFloatPair tp) {
        // TODO Auto-generated method stub
        int cmp=first.compareTo(tp.getFirst());
        if(cmp!=0)
            return cmp;
        return - second.compareTo(tp.getSecond()); //reversed
    }
}
