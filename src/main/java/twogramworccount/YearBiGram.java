package twogramworccount;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearBiGram implements Writable {
    protected String year;
    protected String w1;
    protected String w2;

    public boolean isEmpty() {
        return empty;
    }

    protected boolean empty;

    public YearBiGram() {
        this.year = null;
        this.w1 = null;
        this.w2 = null;
        this.empty = false;
    }

    public YearBiGram(String year, String w1, String w2) {
        this.year = year;
        this.w1 = w1;
        this.w2 = w2;
        this.empty = false;
    }

    public YearBiGram(boolean empty) {
        this.empty = empty;
    }

    public String getYear() {
        return year;
    }

    public String getW1() {
        return w1;
    }

    public String getW2() {
        return w2;
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        w1 = in.readUTF();
        w2 = in.readUTF();
        empty = in.readBoolean();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(w1);
        out.writeUTF(w2);
        out.writeBoolean(empty);
    }

    @Override
    public String toString() {
        if (isEmpty()){
            return "-2000" +'\t'+"was"+'\t'+"was";
        }else{
            return year +'\t'+w1+'\t'+w2;
        }
    }
}
