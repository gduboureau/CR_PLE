package DataCalcul;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkStatsCalculs {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkStatsCalculs");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Charger le fichier séquence résultant de la partie I
        JavaPairRDD<Text, DeckStats> inputRDD = sc.sequenceFile("DataPLE/resultStatsCalculs/", Text.class, DeckStats.class);

        JavaRDD<Tuple2<String, DeckStats>> ngramRdd = inputRDD.flatMap(x -> {
            Text key = x._1();
            DeckStats value = x._2();
            String cardId = extractCardId(key.toString());
            String[] substrings = cardId.split("(?<=\\G..)");
            List<String> allCombinations = getAllCombinationsForCard(substrings);

            return allCombinations.stream()
                    .map(combination -> new Tuple2<>(combination, value))
                    .iterator();
        });

        JavaPairRDD<String, DeckStats> ngramRddPair = ngramRdd.mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2()));

        JavaPairRDD<String, DeckStats> ngramRddByStatistic = ngramRddPair.reduceByKey((value1, value2) -> {
            DeckStats result = new DeckStats();
            result.setUseDeck(value1.getUseDeck() + value2.getUseDeck());
            result.setWinDeck(value1.getWinDeck() + value2.getWinDeck());
            result.setDiffForceWin(value1.getDiffForceWin() + value2.getDiffForceWin());
            result.setBestClan(Math.max(value1.getBestClan(), value2.getBestClan()));

            Set<String> uniquePlayers = new HashSet<>(value1.getPlayers());
            uniquePlayers.addAll(value2.getPlayers());
            result.setPlayers(uniquePlayers);

            return result;
        });

        // Enregistrer les résultats dans un nouveau fichier séquence
        ngramRddByStatistic.saveAsObjectFile("DataPLE/SparkStatsCalculs/");

        sc.close();
    }

    private static String extractCardId(String keyString) {
        // Extraire le card id de la clé en fonction de votre format de clé
        String[] parts = keyString.split("_");
        if (parts.length == 3) {
            return parts[2];
        } else {
            return parts[1]; // Gérer le cas où le card id n'est pas présent dans la clé
        }
    }

    public static List<String> getAllCombinationsForCard(String[] elements) {
        List<List<String>> allCombinations = new ArrayList<>();
        generateCombinations(elements, 0, new ArrayList<>(), allCombinations);

        return allCombinations.stream()
                .map(combination -> String.join("", combination))
                .collect(Collectors.toList());
    }

    public static void generateCombinations(String[] elements, int index, List<String> current, List<List<String>> allCombinations) {
        if (index == elements.length) {
            if (!current.isEmpty()) {
                allCombinations.add(new ArrayList<>(current));
            }
            return;
        }

        current.add(elements[index]);
        generateCombinations(elements, index + 1, current, allCombinations);

        current.remove(current.size() - 1);
        generateCombinations(elements, index + 1, current, allCombinations);
    }
}
