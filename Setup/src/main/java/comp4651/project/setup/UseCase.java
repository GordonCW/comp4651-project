package comp4651.project.setup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.Math;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class UseCase {
  private static int[] generateRandomNumbers(int n, int start, int end) {
    int[] randomNumbers = new int[n];
    for (int i = 0; i < n; ++i) {
      randomNumbers[i] = (int)(Math.random() * (end - start) + start);
    }
    return randomNumbers;
  }

  public static void main(String args[]) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataInputStream in = null;

    int N = Integer.valueOf(args[0]);
    int numHost = Integer.valueOf(args[1]);
    int numDomain = Integer.valueOf(args[2]);
    int numText = Integer.valueOf(args[3]);

    int[] h = generateRandomNumbers(N, 1, numHost);
    int[] d = generateRandomNumbers(N, 1, numDomain);
    int[] t = generateRandomNumbers(N, 1, numText);

    Instant start = Instant.now();

    for (int i = 0; i < N; ++i) {
      String filename = "/inputs/host" + h[i] + "/domain" + d[i] + "/";
      filename += t[i] + ".txt";

      in = fs.open(new Path(filename));
      ByteArrayOutputStream out = new ByteArrayOutputStream();

      IOUtils.copyBytes(in, out, 4096);
      String readContent = new String(out.toByteArray(), "UTF-8");

      in.close();
    }

    Instant end = Instant.now();

    Duration timeElapsed = Duration.between(start, end);
    System.out.println("Time taken for small files: " + timeElapsed.toMillis() +" milliseconds");


    // read from large file, not optimized
    start = Instant.now();

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
        System.out.println(readContent);
        System.out.println();
        System.out.println(thatLine);
        System.out.println("content doesn't exist");
      }

      in.close();
    }

    end = Instant.now();

    timeElapsed = Duration.between(start, end);
    System.out.println("Time taken for unoptimized large files: " + timeElapsed.toMillis() +" milliseconds");

    fs.close();
  }
}
