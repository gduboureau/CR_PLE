package DataCalcul;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkStatsCalculs {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkStatsCalculs");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Charger le fichier séquence résultant de la partie I
        JavaPairRDD<Text, DeckStats> inputRDD = sc.sequenceFile("DataPLE/resultStatsCalculs/", Text.class, DeckStats.class);

           // Générer les combinaisons de 1 à 8 cartes
           JavaPairRDD<String, List<String>> combinationsRDD = inputRDD.flatMapToPair(record -> {

            String deck = record._1.toString();

            if (deck.startsWith("WEEK") || deck.startsWith("MONTH")) {
                deck.split("_")[2].trim();
            }else{
                deck.split("_")[1].trim();
            }

            List<String> cards = new ArrayList<>();
            for (int i = 0; i < deck.length(); i += 2) {
                String cardCode = deck.substring(i, i + 2);
                cards.add(cardCode);
            }

            List<Tuple2<String, List<String>>> combinations = new ArrayList<>();

            // Générer les combinaisons de 1 à 8 cartes
            for (int i = 1; i <= 8; i++) {
                List<List<String>> result = generateCombinations(cards, i);
                for (List<String> combo : result) {
                    combinations.add(new Tuple2<>(String.join(",", combo), cards));
                }
            }
            return combinations.iterator();
        });

        System.out.println("Nombre de combinaisons : " + combinationsRDD.count());

        // Calculer les statistiques pour chaque combinaison
        // JavaPairRDD<String, DeckStats> resultRDD = combinationsRDD
        //         .reduceByKey((stats1, stats2) -> {
        //             DeckStats combinedStats = new DeckStats();
        //             combinedStats.setUseDeck(stats1._1.getUseDeck() + stats2._1.getUseDeck());
        //             combinedStats.setWinDeck(stats1._1.getWinDeck() + stats2._1.getWinDeck());
        //             combinedStats.setDiffForceWin(stats1._1.getDiffForceWin() + stats2._1.getDiffForceWin());
        //             combinedStats.setBestClan(Math.max(stats1._1.getBestClan(), stats2._1.getBestClan()));
        //             combinedStats.getPlayers().addAll(stats1._1.getPlayers());
        //             combinedStats.getPlayers().addAll(stats2._1.getPlayers());
        //             System.out.println("Combined stats for " + stats1._2 + " and " + stats2._2);
        //             return new Tuple2<>(combinedStats, stats1._2);
        //         })
        //         .mapValues(statsTuple -> statsTuple._1);

        // Enregistrer les résultats dans un nouveau fichier séquence
        // resultRDD.saveAsTextFile("DataPLE/SparkStatsCalculs/");

        sc.stop();
        sc.close();
    }

    private static List<List<String>> generateCombinations(List<String> elements, int k) {
        List<List<String>> combinations = new ArrayList<>();
        generateCombinationsHelper(elements, k, 0, new ArrayList<String>(), combinations);
        return combinations;
    }

    private static void generateCombinationsHelper(List<String> elements, int k, int start, List<String> current, List<List<String>> combinations) {
        if (k == 0) {
            combinations.add(new ArrayList<>(current));
            return;
        }

        for (int i = start; i < elements.size(); i++) {
            current.add(elements.get(i));
            generateCombinationsHelper(elements, k - 1, i + 1, current, combinations);
            current.remove(current.size() - 1);
        }
    }
}