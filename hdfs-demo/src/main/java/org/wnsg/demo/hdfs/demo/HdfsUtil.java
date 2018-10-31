package org.wnsg.demo.hdfs.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsUtil {
	FileSystem fs = null;

	@Before
	public void init(URI uri, Configuration conf, String user)
			throws IOException, InterruptedException, URISyntaxException {
		// 构造一个hdfs的客户端
		fs = FileSystem.get(uri, conf, user);
	}

	/*
	 * 从本地上传文件到hdfs中
	 */
	@Test
	public void testUpload(String sourceFile,  String destDir) throws IllegalArgumentException, IOException {
		fs.copyFromLocalFile(new Path(sourceFile), new Path(destDir));
		fs.close();
	}

	/*
	 * 从hdfs中下载文件到本地
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(false, new Path("/docker"), new Path("/Users/cl/Downloads/"), true);
		fs.close();
	}

	/*
	 * 文件夹操作
	 */
	@Test
	public void testDir() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/aaa"));
		System.out.println("创建了一个文件夹：/aaa");

		boolean exists = fs.exists(new Path("/aaa"));
		System.out.println("/aaa文件夹存在否？" + exists);

		fs.copyFromLocalFile(new Path("/Users/cl/Downloads/input.txt"), new Path("/aaa"));
		System.out.println("成功上传了一个文件到/aaa目录下");

		fs.delete(new Path("/aaa"), true);
		System.out.println("已经将/aaa目录删除");

		boolean exists2 = fs.exists(new Path("/aaa"));
		System.out.println("/aaa文件夹存在否？" + exists2);
		fs.close();
	}

	/*
	 * 文件信息查看
	 */
	@Test
	public void testFileStatus() throws FileNotFoundException, IllegalArgumentException, IOException {
		// 只能列出文件信息
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
		}

		System.out.println("-----------------------");
		// 能列出文件和文件夹信息
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus f : listStatus) {
			String type = "-";
			if (f.isDirectory())
				type = "d";
			System.out.println(type + "\t" + f.getPath().getName());
		}
		fs.close();
	}

	@Test
	public void testOthers() throws IllegalArgumentException, IOException {
		// 文件偏移量信息
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(new Path("/docker"), 0, 143588167);
		for (BlockLocation location : fileBlockLocations) {
			System.out.println(location.getOffset());
			System.out.println(location.getNames()[0]);
		}

		// 修改文件名
		fs.rename(new Path("/docker"), new Path("/docker.tgz"));

		// 修改一个文件的副本数量
		fs.setReplication(new Path("/docker.tgz"), (short) 2);
		fs.close();
	}
}