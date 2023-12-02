import DataCalcul.*;
import TopKCalcul.TopK;

public class Driver {

    public static void main(String[] args) throws Exception {

        WinDeck.mainDeck(args, "DataPLE/result/WinDeck");
        UseDeck.JobUseDeck(args, "DataPLE/result/UseDeck");
        UniquePlayerUse.JobUniquePlayerUse(args, "DataPLE/result/UniquePlayerUse");
        BestClanWin.JobBestClanWin(args, "DataPLE/result/BestClanWin");

        TopK.mainTopK("DataPLE/result/WinDeck/part-r-00000", "DataPLE/resultTopK/TopKWinDeck", Integer.parseInt(args[1]));
        TopK.mainTopK("DataPLE/result/UseDeck/part-r-00000", "DataPLE/resultTopK/TopKUseDeck", Integer.parseInt(args[1]));
        TopK.mainTopK("DataPLE/result/UniquePlayerUse/part-r-00000", "DataPLE/resultTopK/TopKUniquePlayerUse", Integer.parseInt(args[1]));
        TopK.mainTopK("DataPLE/result/BestClanWin/part-r-00000", "DataPLE/resultTopK/TopKBestClanWin", Integer.parseInt(args[1]));
    }
}
