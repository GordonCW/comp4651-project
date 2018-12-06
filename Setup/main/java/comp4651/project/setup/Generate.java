package comp4651.project.setup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


public class Generate {
  public static void main(String args[]) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // cleanup the root
    fs.delete(new Path("."), true);

    FSDataOutputStream out = null;

    for (int i = 1; i <= 7; ++i) {
      for (int j = 1; j <= 8; ++j) {
        for (int k = 1; k <= 10; ++k) {
          String filename =  "/inputs/host" + String.valueOf(i) + "/";
          filename += "domain" + String.valueOf(j) + "/";
          filename += String.valueOf(k) + ".txt";

          out = fs.create(new Path(filename));
          out.writeBytes(filename);
          System.out.println(filename);
          out.close();
        }
      }
    }

    fs.close();
  }
}
