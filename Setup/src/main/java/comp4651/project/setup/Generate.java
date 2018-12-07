package comp4651.project.setup;

import java.io.IOException;
import java.lang.Thread;
import java.lang.StringBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


public class Generate {
	public static int domainNum;
	public static int fileNum;
	public static int lineNum;
	
	static class CreateFile extends Thread {
		CreateFile(int i) {
			this.i = i;
		}
		
		int i;
		
		public void run() {
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				FSDataOutputStream out = null;
				for (int j = 1; j <= domainNum; ++j) {
					for (int k = 1; k <= fileNum; ++k) {
						String filename =  "/inputs/host" + String.valueOf(i) + "/";
						filename += "domain" + String.valueOf(j) + "/";
						filename += String.valueOf(k) + ".txt";

						out = fs.create(new Path(filename));
						StringBuilder builder = new StringBuilder();
						for (int l = 0; l < lineNum; ++l) {
						  builder.append(l + ", " + filename);
						}
						out.writeBytes(builder.toString());
						out.close();
					}
					System.out.println("host" + i + ", " + "domain" + j);
				}
				fs.close();
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
	
  public static void main(String args[]) throws IOException {
    int hostNum = Integer.valueOf(args[0]);
	domainNum = Integer.valueOf(args[1]);
	fileNum = Integer.valueOf(args[2]);
	lineNum = Integer.valueOf(args[3]);
	
    // cleanup the root
	FileSystem fs = FileSystem.get(new Configuration());
    fs.delete(new Path("/inputs"), true);

    for (int i = 1; i <= hostNum; ++i) {
	  new CreateFile(i).start();
    }
  }
}
