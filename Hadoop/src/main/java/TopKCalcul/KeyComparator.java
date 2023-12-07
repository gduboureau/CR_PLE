package TopKCalcul;

import java.util.Comparator;

public class KeyComparator implements Comparator<String> {
    @Override
    public int compare(String key1, String key2) {
        String[] parts1 = key1.split(",");
        String[] parts2 = key2.split(",");

        // Compare the values after the comma
        int valueComparison = Double.compare(Double.parseDouble(parts1[1]), Double.parseDouble(parts2[1]));

        // If values are equal compare the names before the comma
        //Example : MONTH_10_0809233f48666954,150.0
        if (valueComparison == 0) {
            return parts1[0].compareTo(parts2[0]);
        } else {
            return valueComparison;
        }
    }
}