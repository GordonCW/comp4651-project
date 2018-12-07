package comp4651.project.setup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;


public class Test {
  public static void main(String args[]) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    FSDataInputStream in = null;

    for (int i = 1; i <= 7; ++i) {
      for (int j = 1; j <= 8; ++j) {

        String expectedContent = "";
        for (int k = 1; k <= 10; ++k) {
          expectedContent +=  "/inputs/host" + String.valueOf(i) + "/";
          expectedContent += "domain" + String.valueOf(j) + "/";
          expectedContent += String.valueOf(k) + ".txt";
          expectedContent += "\n";
        }

        String result = "/outputs/host" + String.valueOf(i) + "/";
        result += "domain" + String.valueOf(j) + "/";
        result += "result.txt";

        Path path = new Path(result);

        // check if file exists or not
        if (fs.exists(path)) {
          in = fs.open(path);

          byte[] readByte = new byte[in.available()];
          in.read(readByte);
          String readContent = new String(readByte);

          if (readContent.equals(expectedContent)) {
            System.out.println("okay");
          } else {
            System.out.println("not okay: different content");
          }

          in.close();
        } else {
          System.out.println("not okay: non-existent file");
        }
      }
    }

    fs.close();
  }
}
