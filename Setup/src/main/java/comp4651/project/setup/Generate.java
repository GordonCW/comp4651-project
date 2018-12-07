package comp4651.project.setup;

import java.io.IOException;
import java.lang.Thread;
import java.lang.StringBuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;


public class Generate {
	public static int domainNum;
	public static int fileNum;
	public static int lineNum;

	static class CreateFile extends Thread {
		CreateFile(int i, FileSystem fs) {
			this.i = i;
			this.fs = fs;
		}

		int i;
		FileSystem fs;

		public void run() {
			try {
				FSDataOutputStream out = null;
				for (int j = 1; j <= domainNum; ++j) {
					for (int k = 1; k <= fileNum; ++k) {
						String filename =  "/inputs/host" + String.valueOf(i) + "/";
						filename += "domain" + String.valueOf(j) + "/";
						filename += String.valueOf(k) + ".txt";

						out = fs.create(new Path(filename));
						StringBuilder builder = new StringBuilder();
						for (int l = 0; l < lineNum; ++l) {
						  builder.append(l + ", " + filename + "\n");
						}
						out.writeBytes(builder.toString());
						out.close();
					}
					System.out.println("host" + i + ", " + "domain" + j);
				}
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
		}
	}

  public static void main(String args[]) throws IOException, InterruptedException {
		int hostNum = Integer.valueOf(args[0]);
		domainNum = Integer.valueOf(args[1]);
		fileNum = Integer.valueOf(args[2]);
		lineNum = Integer.valueOf(args[3]);

    // cleanup the root
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("/inputs"), true);

		List<CreateFile> threads = new ArrayList<CreateFile>();
    for (int i = 1; i <= hostNum; ++i) {
			CreateFile cF = new CreateFile(i, fs);
			cF.start();
			threads.add(cF);
    }

		for (CreateFile cF: threads) {
			cF.join();
		}
  }
}
