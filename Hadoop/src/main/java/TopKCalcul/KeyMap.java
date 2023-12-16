package TopKCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyMap implements WritableComparable<KeyMap>{

    private String dateType;
    private String statistic;

    public KeyMap(String dateType, String statistic) {
        this.dateType = dateType;
        this.statistic = statistic;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        this.dateType = arg0.readUTF();
        this.statistic = arg0.readUTF();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        arg0.writeUTF(this.dateType);
        arg0.writeUTF(this.statistic);
    }

    @Override
    public int compareTo(KeyMap o) {
        int compare = this.dateType.compareTo(o.dateType);
        if (compare == 0) {
            compare = this.statistic.compareTo(o.statistic);
        }
        return compare;
    }
    
    @Override
    public String toString() {
        return this.dateType + "_" + this.statistic;
    }
}
