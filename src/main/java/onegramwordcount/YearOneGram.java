package onegramwordcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearOneGram implements Writable {
    protected String year;
    protected String w;

    public boolean isEmpty() {
        return empty;
    }

    protected boolean empty;

    public YearOneGram() {
        this.year = null;
        this.w = null;
        this.empty = false;
    }

    public YearOneGram(String year, String w) {
        this.year = year;
        this.w = w;
        this.empty = false;
    }

    public YearOneGram(boolean empty) {
        this.empty = empty;
    }

    public String getYear() {
        return year;
    }

    public String getW() {
        return w;
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        w = in.readUTF();
        empty = in.readBoolean();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(w);
        out.writeBoolean(empty);
    }

    @Override
    public String toString() {
        return "YearOneGram{" +
                "year='" + year + '\'' +
                ", w='" + w + '\'' +
                ", empty=" + empty +
                '}';
    }
}
