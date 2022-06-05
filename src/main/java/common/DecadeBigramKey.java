package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
public class DecadeBigramKey implements WritableComparable<DecadeBigramKey>{
    protected String decade;
    protected String w1;
    protected String w2;
    public static final String STAR = "*";

    public DecadeBigramKey() {
        this.decade = null;
        this.w1 = null;
        this.w2 = null;
    }

    public DecadeBigramKey(String decade, String w1, String w2) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
    }

    public DecadeBigramKey(String decade, String w1) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = STAR;
    }

    public String getDecade() {
        return decade;
    }

    public String getW1() {
        return w1;
    }

    public String getW2() {
        return w2;
    }

    public void readFields(DataInput in) throws IOException {
        decade = in.readUTF();
        w1 = in.readUTF();
        w2 = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(decade);
        out.writeUTF(w1);
        out.writeUTF(w2);
    }

    public int compareTo(DecadeBigramKey other) {
        if(decade.equals(other.decade) && w1.equals(other.w1) && w2.equals(other.w2)){
            return 0;
        }else if(decade.compareTo(other.decade)!=0){
            return decade.compareTo(other.decade);
        }else if(!w1.equals(other.w1)){
            return w1.compareTo(other.w1);
        }else if(w2.equals(STAR)){
            return -1;
        }else if(other.w2.equals(STAR)){
            return 1;
        }else{
            return w2.compareTo(other.w2);
        }
    }

    @Override
    public String toString() {
        return decade+'\t'+w1+'\t'+w2;
    }
}
