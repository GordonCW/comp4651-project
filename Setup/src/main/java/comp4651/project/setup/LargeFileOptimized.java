package comp4651.project.setup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.Math;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class LargeFileOptimized {
  private static int[] generateRandomNumbers(int n, int start, int end) {
    int[] randomNumbers = new int[n];
    for (int i = 0; i < n; ++i) {
      randomNumbers[i] = (int)(Math.random() * (end - start) + start);
    }
    return randomNumbers;
  }

  public static void main(String args[]) throws IOException {
    // compare both read from large file, online vs by batch
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataInputStream in = null;

    int N = Integer.valueOf(args[0]);
    int numHost = Integer.valueOf(args[1]);
    int numDomain = Integer.valueOf(args[2]);
    int numText = Integer.valueOf(args[3]);

    int[] h = generateRandomNumbers(N, 1, numHost);
    int[] d = generateRandomNumbers(N, 1, numDomain);
    int[] t = generateRandomNumbers(N, 1, numText);

    // read from large file, unoptimized
    Instant start = Instant.now();

    for (int i = 0; i < N; ++i) {
      String filename = "/outputs/host" + h[i] + "/domain" + d[i] + "/";
      filename += "result.txt";

      in = fs.open(new Path(filename));
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      IOUtils.copyBytes(in, out, 4096);
      String readContent = new String(out.toByteArray(), "UTF-8");

      String thatLine = "/inputs/host" + h[i] + "/domain" + d[i] + "/";
      thatLine += t[i] + ".txt";

      if (!readContent.contains(thatLine)) {
        System.out.println("content doesn't exist");
      }

      in.close();
    }

    Instant end = Instant.now();

    Duration timeElapsed = Duration.between(start, end);
    System.out.println("Time taken for unoptimized large files: " + timeElapsed.toMillis() +" milliseconds");



    // read from large file, optimized
    start = Instant.now();

    // preprocessing the random queries
    ArrayList<List<Integer>> sortedTuples = new ArrayList<List<Integer>>();

    for (int i = 0; i < N; ++i) {
      List<Integer> text = Arrays.asList(h[i], d[i], t[i]);
      sortedTuples.add(text);
    }

    sortedTuples.sort(new Comparator<List<Integer>>() {
      @Override
      public int compare(List<Integer> l1, List<Integer> l2) {
        if (l1.get(0) < l2.get(0)) return -1;
        if (l1.get(0) > l2.get(0)) return 1;

        if (l1.get(1) < l2.get(1)) return -1;
        if (l1.get(1) > l2.get(1)) return 1;

        if (l1.get(2) < l2.get(2)) return -1;
        if (l1.get(2) > l2.get(2)) return 1;

        return 0;
      }
    });

    for (int i = 0; i < N; ++i) {
      int host = sortedTuples.get(i).get(0);
      int domain = sortedTuples.get(i).get(1);
      int text = sortedTuples.get(i).get(2);

      String filename = "/outputs/host" + host + "/domain" + domain + "/";
      filename += "result.txt";

      in = fs.open(new Path(filename));
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      IOUtils.copyBytes(in, out, 4096);
      String readContent = new String(out.toByteArray());

      String thatLine = "/inputs/host" + host + "/domain" + domain + "/";
      thatLine += text + ".txt";

      if (!readContent.contains(thatLine)) {
        System.out.println("content doesn't exist");
      }

      in.close();
    }

    end = Instant.now();
    timeElapsed = Duration.between(start, end);
    System.out.println("Time taken for optimized large files: " + timeElapsed.toMillis() +" milliseconds");

    fs.close();
  }
}
