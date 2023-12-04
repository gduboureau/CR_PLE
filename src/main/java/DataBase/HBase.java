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

    private static final String TABLE_NAME = "gduboureau:CRdata";

    private static final String FAMILY_WIN = "nb_win";
    private static final String FAMILY_USE = "nb_use"; 
    private static final String FAMILY_UNIQUE_PLAYER = "nb_uniquePlayer";
    private static final String FAMILY_BEST_CLAN = "best_clan";
    private static final String FAMILY_DIFF_FORCE = "diff_force";

    public static void createTable(Admin admin) throws IOException {
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

    private static void loadData(Connection connection, String filepath, FileSystem fs) throws FileNotFoundException, IOException{
        
        System.out.println("Loading data from " + filepath + " ...");

        try (FSDataInputStream fsDataInputStream = fs.open(new  org.apache.hadoop.fs.Path(filepath));
             BufferedReader br = new BufferedReader(new java.io.InputStreamReader(fsDataInputStream))) {

            Path path = Paths.get(filepath);
            String folderName = path.getParent().getFileName().toString();
            System.out.println("Loading data for " + folderName + " ...");

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            String line;
            String rowKey = null;
            int cptColumn = 1;
            while ((line = br.readLine()) != null) {
                String [] token = line.split("\\s+");
                String key = token[0];
                String value = token[1];
                String [] partKey = key.split("_");
                
                String currentRowKey;
                if (partKey.length == 2){
                    currentRowKey  = partKey[0];
                }else{
                    currentRowKey  = partKey[0] + "_" + partKey[1];
                }

                if (!currentRowKey.equals(rowKey)) {
                    rowKey = currentRowKey;
                    cptColumn = 1;
                }

                String cardId = partKey[partKey.length - 1];
        
                table.put(fillTable(folderName, rowKey, cardId, value, cptColumn));
                cptColumn++;
            }

            System.out.println("Done....");

            table.close();
        }

    }

    private static Put fillTable(String folderName, String rowKey, String cardId, String value, int cptColumn) {
        Put put = new Put(Bytes.toBytes(rowKey));
        switch (folderName) {
            case "TopKWinDeck":
                put.addColumn(Bytes.toBytes(FAMILY_WIN), Bytes.toBytes("cardId_" + cptColumn), Bytes.toBytes(cardId));
                put.addColumn(Bytes.toBytes(FAMILY_WIN), Bytes.toBytes("value_" + cptColumn), Bytes.toBytes(value));
                return put;
            case "TopKUseDeck":
                put.addColumn(Bytes.toBytes(FAMILY_USE), Bytes.toBytes("cardId_" + cptColumn), Bytes.toBytes(cardId));
                put.addColumn(Bytes.toBytes(FAMILY_USE), Bytes.toBytes("value_" + cptColumn), Bytes.toBytes(value));
                return put;
            case "TopKUniquePlayerUse":
                put.addColumn(Bytes.toBytes(FAMILY_UNIQUE_PLAYER), Bytes.toBytes("cardId_" + cptColumn), Bytes.toBytes(cardId));
                put.addColumn(Bytes.toBytes(FAMILY_UNIQUE_PLAYER), Bytes.toBytes("value_" + cptColumn), Bytes.toBytes(value));
                return put;
            case "TopKBestClanWin":
                put.addColumn(Bytes.toBytes(FAMILY_BEST_CLAN), Bytes.toBytes("cardId_" + cptColumn), Bytes.toBytes(cardId));
                put.addColumn(Bytes.toBytes(FAMILY_BEST_CLAN), Bytes.toBytes("value_" + cptColumn), Bytes.toBytes(value));
                return put;
            case "TopKDiffForceWin":
                put.addColumn(Bytes.toBytes(FAMILY_DIFF_FORCE), Bytes.toBytes("cardId_" + cptColumn), Bytes.toBytes(cardId));
                put.addColumn(Bytes.toBytes(FAMILY_DIFF_FORCE), Bytes.toBytes("value_" + cptColumn), Bytes.toBytes(value));
                return put;
            default:
                throw new IllegalArgumentException("Folder name not recognized: " + folderName);
        }
    }

    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void mainHBase(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        Connection connection = null;
        Admin admin = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            FileSystem fs = FileSystem.get(config);
            System.out.println("Connecting....");
            createTable(admin);

            loadData(connection, args[0], fs);
            loadData(connection, args[1], fs);
            loadData(connection, args[2], fs);
            loadData(connection, args[3], fs);
            loadData(connection, args[4], fs);

        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            System.out.println("Closing....");
            admin.close();
            connection.close();
        }
    }
    
}

 