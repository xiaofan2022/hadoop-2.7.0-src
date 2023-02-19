import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileTest {
	
	public static void main(String[] args) throws IOException {
		Configuration configuration=new Configuration();

		FileSystem fileSystem=FileSystem.newInstance(configuration);

		/**
		 * 创建目录
		 *
		 * HDFS的目录树
		 *
		 */
		fileSystem.mkdirs(new Path("/usr/hive/warehouse/test/mydata"));

		//完成写数据流程
		FSDataOutputStream fsous=fileSystem.create(new Path("/user.txt"));

		//TODO HdfsDataOutputStream
		fsous.write("fdsafdsafdsafs".getBytes());

	
	}

}
