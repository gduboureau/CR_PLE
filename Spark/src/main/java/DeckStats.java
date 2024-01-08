import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * The DeckStats class represents the statistics of a deck in a Clash Royal game.
 * It implements the Writable interface to allow serialization and deserialization of the object.
 */
public class DeckStats implements Writable{

    private double useDeck;
    private double bestClan;
    private double diffForceWin;
    private double winDeck;
    private Set<String> players; 

    /**
     * Constructs a new DeckStats object with default values.
     */
    public DeckStats() {
        this.players = new HashSet<>();
        this.useDeck = 0;
        this.bestClan = 0;
        this.diffForceWin = 0;
        this.winDeck = 0;
    }

    /**
     * Constructs a new DeckStats object with the specified values.
     * 
     * @param useDeck the usage of the deck
     * @param bestClan the best clan of the deck
     * @param diffForceWin the difference in force to win with the deck
     * @param winDeck the win rate of the deck
     */
    public DeckStats(double useDeck, double bestClan, double diffForceWin, double winDeck) {
        this.useDeck = useDeck;
        this.bestClan = bestClan;
        this.diffForceWin = diffForceWin;
        this.winDeck = winDeck;
        this.players = new HashSet<>();
    }

    /**
     * Reads the fields of the DeckStats object from the specified DataInput.
     * 
     * @param dataInput the DataInput to read from
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        useDeck = dataInput.readDouble();
        bestClan = dataInput.readDouble();
        diffForceWin = dataInput.readDouble();
        winDeck = dataInput.readDouble();
        
        int numPlayers = dataInput.readInt();
        players = new HashSet<>();

        for (int i = 0; i < numPlayers; i++) {
            String player = dataInput.readUTF();
            players.add(player);
        }
    }
    
    /**
     * Writes the fields of the DeckStats object to the specified DataOutput.
     * 
     * @param dataOutput the DataOutput to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(useDeck);
        dataOutput.writeDouble(bestClan);
        dataOutput.writeDouble(diffForceWin);
        dataOutput.writeDouble(winDeck);
        
        dataOutput.writeInt(players.size());
        for (String player : players) {
            dataOutput.writeUTF(player);
        }
    }
    
   
    public double getUseDeck() {
        return useDeck;
    }


    public void setUseDeck(double useDeck) {
        this.useDeck = useDeck;
    }

  
    public double getBestClan() {
        return bestClan;
    }

 
    public void setBestClan(double bestClan) {
        this.bestClan = bestClan;
    }


    public double getDiffForceWin() {
        return diffForceWin;
    }

 
    public void setDiffForceWin(double diffForceWin) {
        this.diffForceWin = diffForceWin;
    }


    public double getWinDeck() {
        return winDeck;
    }


    public void setWinDeck(double winDeck) {
        this.winDeck = winDeck;
    }


    public void setPlayers(Set<String> players) {
        this.players = players;
    }


    public Set<String> getPlayers() {
        return players;
    }


    public void addPlayer(String player) {
        players.add(player);
    }

    /**
     * Returns a string representation of the DeckStats object.
     * The string is in the format "useDeck,bestClan,diffForceWin,winDeck,nbPlayers".
     * 
     * @return a string representation of the DeckStats object
     */
    @Override
    public String toString() {
        return useDeck + "," + bestClan + "," + diffForceWin + "," + winDeck + "," + players.size();
    }
    
}
