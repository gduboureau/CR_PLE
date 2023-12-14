package DataCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class DeckStats implements Writable{

    private double useDeck;
    private double bestClan;
    private double diffForceWin;
    private double winDeck;
    private Set<String> players; 
    

    public DeckStats() {
        this.players = new HashSet<>();
        this.useDeck = 0;
        this.bestClan = 0;
        this.diffForceWin = 0;
        this.winDeck = 0;
    }

    public DeckStats(double useDeck, double bestClan, double diffForceWin, double winDeck) {
        this.useDeck = useDeck;
        this.bestClan = bestClan;
        this.diffForceWin = diffForceWin;
        this.winDeck = winDeck;
        this.players = new HashSet<>();
    }

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

    @Override
    public String toString() {
        return "useDeck : " + useDeck + ", bestClan : " + bestClan + ", diffForceWin : " + diffForceWin + ", winDeck : " + winDeck + ", players : " + players.size();
    }
    
}
