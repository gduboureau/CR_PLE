import java.io.IOException;

import DataBase.HBase;
import DataCalcul.BestClanWin;
import DataCalcul.DiffForceWin;
import DataCalcul.UniquePlayerUse;
import DataCalcul.UseDeck;
import DataCalcul.WinDeck;
import TopKCalcul.TopK;

import org.apache.commons.cli.*;

public class Driver {

    public void doMapReduce(String input, int k) throws Exception{
        WinDeck.mainDeck(input, "DataPLE/result/WinDeck");
        UseDeck.JobUseDeck(input, "DataPLE/result/UseDeck");
        UniquePlayerUse.JobUniquePlayerUse(input, "DataPLE/result/UniquePlayerUse");
        BestClanWin.JobBestClanWin(input, "DataPLE/result/BestClanWin");
        DiffForceWin.JobDiffForceWin(input, "DataPLE/result/DiffForceWin");

        TopK.mainTopK("DataPLE/result/WinDeck/part-r-00000", "DataPLE/resultTopK/TopKWinDeck", k);
        TopK.mainTopK("DataPLE/result/UseDeck/part-r-00000", "DataPLE/resultTopK/TopKUseDeck", k);
        TopK.mainTopK("DataPLE/result/UniquePlayerUse/part-r-00000", "DataPLE/resultTopK/TopKUniquePlayerUse", k);
        TopK.mainTopK("DataPLE/result/BestClanWin/part-r-00000", "DataPLE/resultTopK/TopKBestClanWin", k);
        TopK.mainTopK("DataPLE/result/DiffForceWin/part-r-00000", "DataPLE/resultTopK/TopKDiffForceWin", k);
    }

    public void doHBase() throws IOException{
        String[] files = { 
                        "DataPLE/resultTopK/TopKWinDeck/part-r-00000",
                        "DataPLE/resultTopK/TopKUseDeck/part-r-00000",
                        "DataPLE/resultTopK/TopKUniquePlayerUse/part-r-00000",
                        "DataPLE/resultTopK/TopKBestClanWin/part-r-00000",
                        "DataPLE/resultTopK/TopKDiffForceWin/part-r-00000"
                        };

        HBase.mainHBase(files);
    }

    public static void main(String[] args) throws Exception {

        int k = 10;

        Options options = new Options();
        options.addOption("h", "help", false, "Afficher l'aide");
        options.addOption("mapreduce", true, "Faire uniquement les traitements map reduce");
        options.addOption("hbase", false, "Faire uniquement hbase");
        options.addOption("k", true, "Valeur de k");
        options.addOption("default", false, "Effectuer tous les traitements par défaut (option par défaut, peut être spécifiée explicitement)");

        
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        
        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Driver", options);
            System.exit(0);
        }

        if (cmd.hasOption("k")) {
            String value = cmd.getOptionValue("k");
            if (value == null || value.isEmpty()) {
                System.err.println("L'option -k nécessite la spécification d'un nombre entier positif.");
                System.exit(1);
            }
            try {
                k = Integer.parseInt(cmd.getOptionValue("k"));
                if (k <= 0) {
                    System.err.println("La valeur de k doit être un nombre entier positif.");
                    System.exit(1);
                }
            } catch (NumberFormatException e) {
                System.err.println("La valeur de k doit être un nombre entier positif.");
                System.exit(1);
            }
        }
        
        Driver driver = new Driver();
        if (cmd.hasOption("mapreduce")) {
            String input = cmd.getOptionValue("mapreduce");
            if(input == null || input.isEmpty()){ 
                System.err.println("L'option -mapreduce nécessite la spécification du fichier d'entrée.");
                System.exit(1);
            }else{
                driver.doMapReduce(input, k);
            }
        }else if(cmd.hasOption("hbase")){
            driver.doHBase();
        }else{
             String input = cmd.getOptionValue("mapreduce");
            if(input == null || input.isEmpty()){ 
                System.err.println("L'option -mapreduce nécessite la spécification du fichier d'entrée.");
                System.exit(1);
            }else{
                driver.doMapReduce(input, k);
            }
            driver.doHBase();
        }
    }

}
