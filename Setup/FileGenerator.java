import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


public class FileGenerator {
  // not yet tested...
  public void generateFiles(String directory, String[] filenames) {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    if (!fs.exists(directory)) {
      fs.mkdirs(directory);
    } else {
      // cleanup the folder

    }

    FSDataOutputStream out = null;

    for (String filename : filenames) {
      String textPath;

      if (directory.charAt(directory.length() - 1) != '/') {
        textPath = directory + "/" + filename;
      } else {
        textPath = directory + filename;
      }

      out = fs.create(new Path(textPath));
      out.writeBytes(textPath);
      out.close();
    }

    fs.close();
  }
}
