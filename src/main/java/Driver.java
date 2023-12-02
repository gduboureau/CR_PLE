import DataCalcul.WinDeck;
import TopKCalcul.TopK;

public class Driver {

    public static void main(String[] args) throws Exception {
        WinDeck.mainDeck(args, "result/WinDeck");
        TopK.mainTopKWeeks("result/WinDeck/part-r-00000", "resultTopK/TopKWinDeck", Integer.parseInt(args[1]));
    }
}
