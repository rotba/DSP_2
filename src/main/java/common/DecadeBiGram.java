package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
public class DecadeBiGram implements WritableComparable<DecadeBiGram>{
    protected String decade;
    protected String w1;
    protected String w2;
    protected boolean keyIsW2;
    private static final String STAR = "*";

    public DecadeBiGram() {
        this.decade = null;
        this.w1 = null;
        this.w2 = null;
        this.keyIsW2 = false;
    }

    public DecadeBiGram(String decade, String w1, String w2, boolean keyIsW2) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
        this.keyIsW2 = keyIsW2;
    }

    public DecadeBiGram(String decade, String w1, boolean keyIsW2) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = STAR;
        this.keyIsW2 = keyIsW2;
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

    public boolean isKeyIsW2() {
        return keyIsW2;
    }

    public void readFields(DataInput in) throws IOException {
        decade = in.readUTF();
        w1 = in.readUTF();
        w2 = in.readUTF();
        keyIsW2 = in.readBoolean();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(decade);
        out.writeUTF(w1);
        out.writeUTF(w2);
        out.writeBoolean(keyIsW2);
    }

    public int compareTo(DecadeBiGram other) {
        if(decade.compareTo(other.decade)!=0){
            return decade.compareTo(other.decade);
        }else if(w2.equals(STAR) && other.w2.equals(STAR)){
            return w1.compareTo(other.w1);
        }else if(w2.equals(STAR)){
            return 1;
        }else if(other.w2.equals(STAR)){
            return -1;
        }else if(w1.compareTo(other.w1) == 0){
            return w2.compareTo(other.w2);
        }else{
            return w1.compareTo(other.w1);
        }
    }

    @Override
    public String toString() {
        return decade+'\t'+w1+'\t'+w2;
    }
}
