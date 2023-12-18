package TopKCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Represents a deck descriptor.
 * 
 * This class implements the WritableComparable interface and represents a deck descriptor
 * with a string of cards and a corresponding value. It provides methods to read and write
 * the deck descriptor to a data input/output stream, as well as getters and setters for
 * the cards and value properties. It also implements the compareTo method for comparing
 * deck descriptors based on their values.
 */
public class DeckDescriptor implements WritableComparable<DeckDescriptor>{

    private String cards;
    private double value;

    public DeckDescriptor(String cards, double value) {
        this.cards = cards;
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.cards = in.readUTF();
        this.value = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(cards);
        out.writeDouble(value);
    }

    public String getCards() {
        return cards;
    }

    public void setCards(String cards) {
        this.cards = cards;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }


    /**
     * Compares this DeckDescriptor object with the specified DeckDescriptor object for order.
     * Returns a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
     *
     * @param o the DeckDescriptor object to be compared
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object
     */
    @Override
    public int compareTo(DeckDescriptor o) {
        return Double.compare(o.value, this.value);
    }

    @Override   
    public String toString() {
        return cards + "_" + value;
    }
    
}

