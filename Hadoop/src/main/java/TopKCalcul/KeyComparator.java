package TopKCalcul;

import java.util.Comparator;

public class KeyComparator implements Comparator<DeckValue> {
    @Override
    public int compare(DeckValue o1, DeckValue o2) {
        int valueComparison = Double.compare(o1.getValue(), o2.getValue());
        if (valueComparison != 0) {
            return valueComparison;
        }
        return o1.getCards().compareTo(o2.getCards());
    }
}