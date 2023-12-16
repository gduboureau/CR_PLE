package TopKCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

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


    @Override
    public int compareTo(DeckDescriptor o) {
        return Double.compare(o.value, this.value);
    }

    @Override   
    public String toString() {
        return cards + "_" + value;
    }
    
}
