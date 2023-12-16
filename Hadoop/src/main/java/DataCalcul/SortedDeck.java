package DataCalcul;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The SortedDeck class provides a method to sort a deck of cards represented as a string.
 */
public class SortedDeck {

    /**
     * Sorts a deck of cards represented as a string.
     * 40154168021e6551 and 02151e4041516568 are considered equal.
     * @param deck the deck of cards to be sorted
     * @return the sorted deck of cards as a string

     */
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

