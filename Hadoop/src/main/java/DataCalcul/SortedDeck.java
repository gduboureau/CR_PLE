package DataCalcul;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SortedDeck {

    static String sortDeck(String deck) {

        List<String> cardPairs = new ArrayList<>();
        for (int i = 0; i < deck.length(); i += 2) {
            cardPairs.add(deck.substring(i, i + 2));
        }

        Collections.sort(cardPairs);

        StringBuilder sortedHex = new StringBuilder();
        for (String cardPair : cardPairs) {
            sortedHex.append(cardPair);
        }

        return sortedHex.toString();
    }

}

