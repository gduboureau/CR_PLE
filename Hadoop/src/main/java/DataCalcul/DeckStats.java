package DataCalcul;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class DeckStats implements Writable{

    private double winDeck;
    private double useDeck;
    private double diffForceWin;
    private double bestClan;
    private Set<String> players;

    public DeckStats() {
        this.winDeck = 0;
        this.useDeck = 0;
        this.diffForceWin = 0;
        this.bestClan = 0;
        this.players = new HashSet<String>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(winDeck);
        out.writeDouble(useDeck);
        out.writeDouble(diffForceWin);
        out.writeDouble(bestClan);
        
        out.writeInt(players.size());
        for (String player : players) {
            out.writeUTF(player);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.bestClan = in.readDouble();
        this.diffForceWin = in.readDouble();
        this.useDeck = in.readDouble();
        this.winDeck = in.readDouble();
        
        int playersSize = in.readInt();
        players = new HashSet<String>();
        for (int i = 0; i < playersSize; i++) {
            players.add(in.readUTF());
        }
    }

    public void setWinDeck(double winDeck) {
        this.winDeck = winDeck;
    }

    public void setUseDeck(double useDeck) {
        this.useDeck = useDeck;
    }

    public void setDiffForceWin(double diffForceWin) {
        this.diffForceWin = diffForceWin;
    }

    public void setBestClan(double bestClan) {
        this.bestClan = bestClan;
    }

    public void setPlayers(Set<String> players) {
        this.players = players;
    }

    public double getWinDeck() {
        return winDeck;
    }

    public double getUseDeck() {
        return useDeck;
    }


    public double getDiffForceWin() {
        return diffForceWin;
    }

    public double getBestClan() {
        return bestClan;
    }

    public void addPlayer(String player){
        this.players.add(player);
    }

    public Set<String> getPlayers() {
        return players;
    }

    @Override
    public String toString() {
        return "DeckStats [bestClan=" + bestClan + ", diffForceWin=" + diffForceWin + ", players=" + players.size()
                + ", useDeck=" + useDeck + ", winDeck=" + winDeck + "]";
    }
    
}
