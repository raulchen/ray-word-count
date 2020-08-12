import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A local word-count example.
 */
public class WordCount {

  public static final int NUM_FILES = 10;

  /**
   * Count the occurrence of each word in a file.
   */
  public static Map<String, Integer> countWordsInFile(String fileName) {
    Map<String, Integer> counts = new HashMap<>();
    try {
      for (String line : Files.readAllLines(Paths.get(fileName), Charset.defaultCharset())) {
        for (String word : line.split("\\s+")) {
          counts.put(word, counts.getOrDefault(word, 0) + 1);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return counts;
  }

  public static void main(String[] args) {
    Map<String, Integer> totalCounts = new HashMap<>();
    for (int i = 0; i < NUM_FILES; i++) {
      // Count words in each file and add them to the final result.
      // This runs sequentially, and is the bottleneck of this program.
      countWordsInFile("files/" + i + ".txt").forEach((word, count) -> {
        totalCounts.put(word, totalCounts.getOrDefault(word, 0) + count);
      });
    }
    // Print the top 10 most frequent words in all files.
    totalCounts.entrySet().stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .limit(10).forEach(entry -> {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    });
  }
}
