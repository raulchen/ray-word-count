import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A distributed word-count example with a distributed counter.
 */
public class DistributedWordCount2 {

  /**
   * This is an ordinary Java counter class. Ray can turn it into a distributed counter.
   */
  public static class Counter {

    private static Map<String, Integer> totalCounts = new HashMap<>();

    /**
     * Update the counts.
     */
    public void update(Map<String, Integer> counts) {
      counts.forEach((word, count) -> {
        totalCounts.put(word, totalCounts.getOrDefault(word, 0) + count);
      });
    }

    /**
     * Get the top N words and their counts.
     */
    public Map<String, Integer> getTopN(int n) {
      return totalCounts.entrySet().stream()
          .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
          .limit(n).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }

  public static void countWordsInFileAndUpdateCounter(String file, ActorHandle<Counter> counter)
      throws IOException {
    Map<String, Integer> counts = WordCount.countWordsInFile(file);
    counter.task(Counter::update, counts).remote();
  }

  public static void main(String[] args) throws IOException {
    // Initialize Ray runtime.
    Ray.init();
    // Create a counter actor.
    // The actor runs in a remote process. It can be used as a distributed counter.
    ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();
    for (int i = 0; i < WordCount.NUM_FILES; i++) {
      // Count the words in each file and update the counter actor.
      Ray.task(DistributedWordCount2::countWordsInFileAndUpdateCounter,
          "files/" + i + ".txt", counter).remote();
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    do {
      System.out.println("Press enter to query current top 10 words, press q to quit:");
      String input = reader.readLine();
      if (input.equals("q")) {
        break;
      } else {
        ObjectRef<Map<String, Integer>> top10Words = counter.task(Counter::getTopN, 10).remote();
        System.out.println(top10Words.get());
      }
    } while (true);
  }
}
