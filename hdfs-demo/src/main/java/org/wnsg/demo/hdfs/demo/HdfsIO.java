package org.wnsg.demo.hdfs.demo;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsIO {
	FileSystem fs = null;

	@Before
	public void init(URI uri, Configuration conf, String user)
			throws IOException, InterruptedException, URISyntaxException {
		// 构造一个hdfs的客户端
		fs = FileSystem.get(uri, conf, user);
	}

	/*
	 * 下载文件
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fs.open(new Path("/docker"));
		FileOutputStream out = new FileOutputStream("/Users/cl/Downloads/docker");
		IOUtils.copyBytes(in, out, new Configuration());
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
		fs.close();
	}

	/*
	 * 上传文件
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException {
		FileInputStream in = new FileInputStream("/Users/cl/Downloads/docker");
		FSDataOutputStream out = fs.create(new Path("/docker"));
		IOUtils.copyBytes(in, out, new Configuration());
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
		fs.close();
	}

	/*
	 * 从指定偏移量读取hdfs中的文件数据 在分布式数据处理时，可以将数据分片来分配给不同的节点处理
	 */
	@Test
	public void testSeek() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fs.open(new Path("/docker"));
		in.seek(6);// 定位，设置起始偏移量
		FileOutputStream out = new FileOutputStream("/Users/cl/Downloads/docker");
		IOUtils.copyBytes(in, out, new Configuration());
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
		fs.close();
	}
}