import java.io.IOException;

import DataBase.HBase;
import DataCalcul.*;

import org.apache.commons.cli.*;

public class Driver {

    public void doMapReduce(String input, int k) throws Exception{
        StatsCalculs.mainStatsCalculs(input, "DataPLE/resultStatsCalculs");
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
        String inputFile = "/user/auber/data_ple/clashroyale/gdc_battles.nljson";

        Options options = new Options();
        options.addOption("h", "help", false, "Afficher l'aide");
        options.addOption("mapreduce", false, "Faire uniquement les traitements map reduce");
        options.addOption("hbase", false, "Faire uniquement hbase");
        options.addOption("k", true, "Valeur de k");
        options.addOption("default", false, "Effectuer tous les traitements par défaut (option par défaut, peut être spécifiée explicitement ou non)");

        
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
            driver.doMapReduce(inputFile, k);
        }else if(cmd.hasOption("hbase")){
            driver.doHBase();
        }else{
            driver.doMapReduce(inputFile, k);
            driver.doHBase();
        }
    }

}
