import DataCalcul.*;
import TopKCalcul.TopK;

public class Driver {

    public static void main(String[] args) throws Exception {

        WinDeck.mainDeck(args, "result/WinDeck");
        UseDeck.JobUseDeck(args, "result/UseDeck");
        UniquePlayerUse.JobUniquePlayerUse(args, "result/UniquePlayerUse");

        TopK.mainTopK("result/WinDeck/part-r-00000", "resultTopK/TopKWinDeck", Integer.parseInt(args[1]));
        TopK.mainTopK("result/UseDeck/part-r-00000", "resultTopK/TopKUseDeck", Integer.parseInt(args[1]));
        TopK.mainTopK("result/UniquePlayerUse/part-r-00000", "resultTopK/TopKUniquePlayerUse", Integer.parseInt(args[1]));
    }
}
