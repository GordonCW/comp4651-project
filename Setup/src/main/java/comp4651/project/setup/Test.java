package comp4651.project.setup;

import java.io.IOException;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;


public class Test {
  public static void main(String args[]) throws IOException {
    int hostNum = Integer.valueOf(args[0]);
    int domainNum = Integer.valueOf(args[1]);
    int fileNum = Integer.valueOf(args[2]);
    int lineNum = Integer.valueOf(args[3]);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    FSDataInputStream in = null;

    for (int i = 1; i <= hostNum; ++i) {
      for (int j = 1; j <= domainNum; ++j) {

        StringBuilder builder = new StringBuilder();

        for (int k = 1; k <= fileNum; ++k) {
          for (int l = 0; l < lineNum; ++l) {
            builder.append("/inputs/host" + String.valueOf(i) + "/");
            builder.append("domain" + String.valueOf(j) + "/");
            builder.append(String.valueOf(k) + ".txt\n");
          }
        }

        String expectedContent = builder.toString();

        String result = "/outputs/host" + String.valueOf(i) + "/";
        result += "domain" + String.valueOf(j) + "/result.txt";

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
