/**
 * Provides a tool to create symlinks on HDFS
 */
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

class Symlink {
	public static void main(String args[]) throws Exception {
		FileContext ctx = FileContext.getFileContext();

		if (args.length < 2) {
			System.out.println("Symlink <src> <src> ... <tgt>");
		}

		String tgt = args[args.length - 1];
		for (int i = 0; i < args.length - 1; i++) {
			String src = args[i];
			ctx.createSymlink(new Path(src), new Path(tgt), false);
		}
	}
}
