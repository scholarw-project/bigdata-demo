package org.wnsg.demo.hdfs.demo;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	URI uri =  null;
    	try {
			uri = new URI("hdfs://localhost:8020");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	Configuration conf = new Configuration();
    	String user = "wangsg";
    	
    	HdfsUtil hdfsUtil = new HdfsUtil();
    	try {
			hdfsUtil.init(uri, conf, user);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	try {
			hdfsUtil.testUpload("/Users/wangsg/a.txt", "/");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}
