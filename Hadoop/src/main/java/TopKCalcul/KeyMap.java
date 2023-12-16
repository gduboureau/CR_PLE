package TopKCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * The KeyMap class represents a key used for mapping in the TopK calculation.
 * It implements the WritableComparable interface to allow serialization and comparison.
 */
public class KeyMap implements WritableComparable<KeyMap>{

    private String dateType;
    private String statistic;

    /**
     * Constructs a KeyMap object with the specified date type and statistic.
     * 
     * @param dateType the date type
     * @param statistic the statistic
     */
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

    /**
     * Compares this KeyMap object with the specified KeyMap object for order.
     * Returns a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
     *
     * @param o the KeyMap object to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
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
