import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
import java.util.Map;
import java.util.LinkedHashMap;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;

public class DataReader {

    /**
     * Values specific to the file being read in.
     */
    private static final String INPUT_FILE = "./data.csv";
    private static final String OUTPUT_FILE = "./output.txt";
    private static final String DELIMITER = ",";
    private static final int MMR_INDEX = 1;
    private static final int PING_INDEX = 13;

    /**
     * Configurable integers which dictate the size of the bucket the value will fall into.
     * ie. For ping: 0-12.49 => 0, 12.5-37.49 => 1, etc.
     */
    private static final int MMR_BUCKET_SIZE = 1;
    private static final int PING_BUCKET_SIZE = 25;
    private static final float BETA = 0.8f;

    /**
     * Stores the edges between the buckets and the players (both ways)
     */
    private static Map<String, List<String>> mEdges;

    /**
     * Assigns the edges between the player and the buckets that they fall into.
     * Also assigns the bucket to have an edge with the player.
     */
    private static void assignEdges(int counter, float mmr, float ping) {
        int mmrBucket = Math.round(mmr / MMR_BUCKET_SIZE);
        int pingBucket = Math.round(ping / PING_BUCKET_SIZE);

        String playerId = "player" + counter;
        String mmrId = "mmr" + mmrBucket;
        String pingId = "ping" + pingBucket;

        if(mEdges.get(playerId) == null) {
            List<String> tempList = new ArrayList<>();
            tempList.add(mmrId);
            tempList.add(pingId);
            mEdges.put(playerId, tempList);
        }

        if(mEdges.get(mmrId) == null) {
            List<String> tempList = new ArrayList<>();
            tempList.add(playerId);
            mEdges.put(mmrId, tempList);
        } else {
            List<String> tempList = mEdges.get(mmrId);
            tempList.add(playerId);
            mEdges.put(mmrId, tempList);
        }

        if(mEdges.get(pingId) == null) {
            List<String> tempList = new ArrayList<>();
            tempList.add(playerId);
            mEdges.put(pingId, tempList);
        } else {
            List<String> tempList = mEdges.get(pingId);
            tempList.add(playerId);
            mEdges.put(pingId, tempList);
        }
    }

    /**
     * Reads in the CSV file and populates the edges map
     */
    private static void readInCSV() {
        BufferedReader br = null;
        String line = "";
        boolean title = true;
        int counter = 0;

        try {
            br = new BufferedReader(new FileReader(INPUT_FILE));
            while ((line = br.readLine()) != null) {
                if(!title) {
                    String[] elements = line.split(DELIMITER);
                    float mmr = Float.parseFloat(elements[MMR_INDEX]);
                    float ping = Float.parseFloat(elements[PING_INDEX]);

                    assignEdges(counter, mmr, ping);
                    counter++;
                    if (counter == 4) {
                        break;
                    }
                } else {
                    title = false;
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.toString());
        }
    }

    /**
     * Calculate the initial transition matrix and writes the indexes and values to a file that
     * are non-zero.
     */
    private static void calcTransitionMatrix() {
        File file = new File(OUTPUT_FILE);
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            System.err.println("Error: " + e.toString());
        }

        int i = 0;
        for (Map.Entry<String, List<String>> outterEntry : mEdges.entrySet()) {
            int j = 0;
            List<String> edges = outterEntry.getValue();
            for (Map.Entry<String, List<String>> innerEntry : mEdges.entrySet()) {
                if(edges.contains(innerEntry.getKey())){
                    float val = 1.0f / (outterEntry.getValue().size() / BETA);

                    String transLine = "A" + "," + i + "," + j + "," + val;
                    try(FileWriter fw = new FileWriter(OUTPUT_FILE, true);
                    BufferedWriter bw = new BufferedWriter(fw);
                    PrintWriter out = new PrintWriter(bw)){
                        out.println(transLine);
                    } catch (IOException e) {
                        System.err.println("Error: " + e.toString());
                    }
                }
                ++j;
            }
            ++i;
        }

    }

    /**
     * Writes the initial matrix to a file
     */
    private static void calcInitialMatrix() {
        for(int k=0; k < mEdges.size(); ++k) {
            String vLine = "B" + "," + "0" + "," + k + "," + "0.2";
            try(FileWriter fw = new FileWriter(OUTPUT_FILE, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)){
                out.println(vLine);
            } catch (IOException e) {
                System.err.println("Error: " + e.toString());
            }
        }
    }

    /**
     * The main function which drives the parsing of the CSV file.
     */
    public static void main(String[] args) {
        mEdges = new LinkedHashMap<String, List<String>>();
        readInCSV();
        calcTransitionMatrix();
        calcInitialMatrix();
    }
}
