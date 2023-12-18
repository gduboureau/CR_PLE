/**
 * This class represents a HBase database and provides methods for creating tables, loading data, and performing operations on the database.
 */
package DataBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HBase {

    // Constants for table name and column families
    private static final String TABLE_NAME = "vloustau:CRdata";
    private static final String FAMILY_WIN = "nb_win";
    private static final String FAMILY_USE = "nb_use";
    private static final String FAMILY_UNIQUE_PLAYER = "nb_uniquePlayer";
    private static final String FAMILY_BEST_CLAN = "best_clan";
    private static final String FAMILY_DIFF_FORCE = "diff_force";

    /**
     * Creates a table in the HBase database with the specified column families.
     *
     * @param admin The HBase admin object.
     * @throws IOException If an I/O error occurs.
     */
    public static void createTable(Admin admin) throws IOException {
        // Create table descriptor with column families
        TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(TABLE_NAME))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(FAMILY_WIN)))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(FAMILY_USE)))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(FAMILY_UNIQUE_PLAYER)))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(FAMILY_BEST_CLAN)))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(FAMILY_DIFF_FORCE)))
                .build();
        System.out.println("Creating Table " + tableDescriptor.getTableName() + " ...");
        createOrOverwrite(admin, tableDescriptor);
        System.out.println("Done....");
    }

    /**
     * Loads data from a file into the HBase database.
     *
     * @param connection The HBase connection object.
     * @param filepath   The path of the file to load data from.
     * @param fs         The Hadoop file system object.
     * @throws FileNotFoundException If the file specified by the filepath does not exist.
     * @throws IOException           If an I/O error occurs.
     */
    private static void loadData(Connection connection, String filepath, FileSystem fs) throws FileNotFoundException, IOException {
        System.out.println("Loading data from " + filepath + " ...");

        try (FSDataInputStream fsDataInputStream = fs.open(new org.apache.hadoop.fs.Path(filepath));
             BufferedReader br = new BufferedReader(new java.io.InputStreamReader(fsDataInputStream))) {

            Path path = Paths.get(filepath);
            String folderName = path.getParent().getFileName().toString();
            System.out.println("Loading data for " + folderName + " ...");

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            String line;
            String rowKey;

            // Read the file line by line
            while ((line = br.readLine()) != null) {
                String[] token = line.split("\\s+");
                String key = token[0];
                String value = token[1];
                String[] partKey = key.split("_");

                if (partKey.length == 3) { // If we have a week or a month
                    rowKey = partKey[0];
                } else { // If we have a global
                    rowKey = partKey[0] + "_" + partKey[1];
                }

                // Get the cardId and the statId
                String cardId = partKey[partKey.length - 1];
                String statId = partKey[partKey.length - 2];

                table.put(fillTable(statId, rowKey, cardId, value));
            }

            System.out.println("Done....");

            table.close();
        }
    }

    /**
     * Fills a Put object with data to be inserted into the HBase table.
     *
     * @param statId  The statId value.
     * @param rowKey  The rowKey value.
     * @param cardId  The cardId value.
     * @param value   The value to be inserted.
     * @return The Put object with the data.
     * @throws IOException If an I/O error occurs.
     */
    private static Put fillTable(String statId, String rowKey, String cardId, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        KeyValue keyValue;
        switch (statId) {
            case "winDeck":
                keyValue = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(FAMILY_WIN), Bytes.toBytes(cardId), Bytes.toBytes(value));
                put.add(keyValue);
                return put;
            case "useDeck":
                keyValue = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(FAMILY_USE), Bytes.toBytes(cardId), Bytes.toBytes(value));
                put.add(keyValue);
                return put;
            case "nbPlayers":
                keyValue = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(FAMILY_UNIQUE_PLAYER), Bytes.toBytes(cardId), Bytes.toBytes(value));
                put.add(keyValue);
                return put;
            case "bestClan":
                keyValue = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(FAMILY_BEST_CLAN), Bytes.toBytes(cardId), Bytes.toBytes(value));
                put.add(keyValue);
                return put;
            case "diffForceWin":
                keyValue = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(FAMILY_DIFF_FORCE), Bytes.toBytes(cardId), Bytes.toBytes(value));
                put.add(keyValue);
                return put;
            default:
                throw new IllegalArgumentException("StatId not recognized: " + statId);
        }
    }

    /**
     * Creates a table in the HBase database if it does not exist, or overwrites it if it already exists.
     *
     * @param admin The HBase admin object.
     * @param table The table descriptor.
     * @throws IOException If an I/O error occurs.
     */
    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    /**
     * Main method to execute HBase operations.
     *
     * @param input The input file path.
     * @throws IOException If an I/O error occurs.
     */
    public static void mainHBase(String input) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = null;
        Admin admin = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            FileSystem fs = FileSystem.get(config);
            System.out.println("Connecting....");
            createTable(admin);

            loadData(connection, input, fs);

        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            System.out.println("Closing....");
            admin.close();
            connection.close();
        }
    }
}
