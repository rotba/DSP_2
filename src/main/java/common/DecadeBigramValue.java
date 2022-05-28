package common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DecadeBigramValue implements Writable {
    public int c1;
    public int c2;
    public int c12;
    public int N;

    public DecadeBigramValue() {
    }

    public DecadeBigramValue(int c1, int c2, int c12, int n) {
        this.c1 = c1;
        this.c2 = c2;
        this.c12 = c12;
        N = n;
    }

    public int getC2() {
        return c2;
    }

    public int getC12() {
        return c12;
    }

    public int getN() {
        return N;
    }

    public int getC1() {
        return c1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(c1);
        dataOutput.writeInt(c2);
        dataOutput.writeInt(c12);
        dataOutput.writeInt(N);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        c1 = dataInput.readInt();
        c2 = dataInput.readInt();
        c12 = dataInput.readInt();
        N = dataInput.readInt();
    }

    @Override
    public String toString() {
        return c1 + "\t"+c2+"\t"+c12+"\t"+N;
    }
}
