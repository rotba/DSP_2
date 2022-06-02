package onegramwordcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearOneGram implements Writable {
    protected String year;
    protected String w;

    public YearOneGram() {
        this.year = null;
        this.w = null;
    }

    public YearOneGram(String year, String w) {
        this.year = year;
        this.w = w;
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
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeUTF(w);
    }

    @Override
    public String toString() {
        return "YearOneGram{" +
                "year='" + year + '\'' +
                ", w='" + w + '\'' +
                '}';
    }
}
