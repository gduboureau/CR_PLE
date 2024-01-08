package DataCalcul;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkStatsCalculs {

    public static void mainSpark() {
        SparkConf conf = new SparkConf().setAppName("SparkStatsCalculs");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Charger le fichier séquence résultant de la partie I
        JavaPairRDD<Text, DeckStats> inputRDD = sc.sequenceFile("DataPLE/resultStatsCalculs/", Text.class, DeckStats.class);

           // Générer les combinaisons de 1 à 8 cartes
        JavaPairRDD<Text, Tuple2<DeckStats, List<String>>> combinationsRDD = inputRDD.flatMapToPair(record -> {
            String[] cards = record._1.toString().split("_")[1].split("");
            List<Tuple2<Text, Tuple2<DeckStats, List<String>>>> result = new ArrayList<>();

            // Générer les combinaisons de 1 à 8 cartes
            for (int i = 1; i <= 8; i++) {
                List<List<String>> combos = generateCombinations(cards, i);
                for (List<String> combo : combos) {
                    String comboStr = String.join("", combo);
                    result.add(new Tuple2<>(new Text("COMBO_" + comboStr), new Tuple2<>(record._2, combo)));
                }
            }
            return result.iterator();
        });

        // Calculer les statistiques pour chaque combinaison
        JavaPairRDD<Text, DeckStats> resultRDD = combinationsRDD
                .reduceByKey((stats1, stats2) -> {
                    DeckStats combinedStats = new DeckStats();
                    combinedStats.setUseDeck(stats1._1.getUseDeck() + stats2._1.getUseDeck());
                    combinedStats.setWinDeck(stats1._1.getWinDeck() + stats2._1.getWinDeck());
                    combinedStats.setDiffForceWin(stats1._1.getDiffForceWin() + stats2._1.getDiffForceWin());
                    combinedStats.setBestClan(Math.max(stats1._1.getBestClan(), stats2._1.getBestClan()));
                    combinedStats.getPlayers().addAll(stats1._1.getPlayers());
                    combinedStats.getPlayers().addAll(stats2._1.getPlayers());
                    return new Tuple2<>(combinedStats, stats1._2);
                })
                .mapValues(statsTuple -> statsTuple._1);

        // Enregistrer les résultats dans un nouveau fichier séquence
        resultRDD.saveAsTextFile("DataPLE/SparkStatsCalculs/");

        sc.stop();
        sc.close();
    }

        // Fonction pour générer toutes les combinaisons de k éléments à partir d'une liste
    private static List<List<String>> generateCombinations(String[] elements, int k) {
        List<List<String>> combinations = new ArrayList<>();
        generateCombinationsHelper(elements, k, 0, new ArrayList<>(), combinations);
        return combinations;
    }

    private static void generateCombinationsHelper(String[] elements, int k, int start, List<String> current, List<List<String>> combinations) {
        if (k == 0) {
            combinations.add(new ArrayList<>(current));
            return;
        }

        for (int i = start; i < elements.length; i++) {
            current.add(elements[i]);
            generateCombinationsHelper(elements, k - 1, i + 1, current, combinations);
            current.remove(current.size() - 1);
        }
    }
}