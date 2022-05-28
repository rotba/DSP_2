package likelihood;

import common.DecadeBigramKey;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LikelihoodAndDecadeBigramKey implements WritableComparable<LikelihoodAndDecadeBigramKey> {
    double likelihood;
    DecadeBigramKey dbk;

    public LikelihoodAndDecadeBigramKey() {
        likelihood = -1.0;
        dbk = null;
    }

    public LikelihoodAndDecadeBigramKey(double likelyhood, DecadeBigramKey dbk) {
        this.likelihood = likelyhood;
        this.dbk = dbk;
    }

    public double getLikelihood() {
        return likelihood;
    }

    public DecadeBigramKey getDbk() {
        return dbk;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(likelihood);
        dbk.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        likelihood = dataInput.readDouble();
        dbk = new DecadeBigramKey();
        dbk.readFields(dataInput);
    }

    @Override
    public int compareTo(LikelihoodAndDecadeBigramKey o) {
        return Double.compare(likelihood, o.getLikelihood());
    }
}
