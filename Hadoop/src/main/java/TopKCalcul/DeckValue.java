package TopKCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DeckValue implements Writable{

    private String cards;
    private double value;
    private String line;

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public DeckValue(String cards, double value, String line) {
        this.cards = cards;
        this.value = value;
        this.line = line;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.cards = in.readUTF();
        this.value = in.readDouble();
        this.line = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(cards);
        out.writeDouble(value);
        out.writeUTF(line);
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
    
}
