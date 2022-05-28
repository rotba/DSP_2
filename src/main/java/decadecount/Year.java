package decadecount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Year implements Writable {
    protected String year;

    public Year() {
        this.year = null;
    }

    public Year(String year) {
        this.year = year;
    }

    public String getYear() {
        return year;
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
    }
}
