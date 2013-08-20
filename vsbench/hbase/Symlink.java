/**
 * Provides a tool to create symlinks on HDFS
 */
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

class Symlink {
	public static void main(String args[]) throws Exception {
		FileContext ctx = FileContext.getFileContext();

		for (String arg : args) {
			System.out.println(arg);
		}
		ctx.createSymlink(new Path(args[0]), new Path(args[1]), false);
	}
}
