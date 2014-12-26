package cc.twittertools.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class UserWordCountDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("user-wordcount");
	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("c");
	
	private static final Logger log = Logger.getLogger(WordCountDAO.class);

	 private HTablePool pool;
	 
	 public UserWordCountDAO(HTablePool pool) {
		 this.pool = pool;
	 }
	 
	 public void CreateTable() throws IOException, ZooKeeperConnectionException{
		 Configuration conf = HBaseConfiguration.create();
		  
		 HBaseAdmin hbase = new HBaseAdmin(conf);
		 HTableDescriptor[] wordcounts = hbase.listTables("wordcount");
		  
		 if(wordcounts.length != 0){ //Drop Table if Exists
		 	 hbase.disableTable(TABLE_NAME);
			 hbase.deleteTable(TABLE_NAME);
		 }
		 
		 HTableDescriptor wordcount = new HTableDescriptor(TABLE_NAME);
		 hbase.createTable(wordcount);
		 // Cannot edit a stucture on an active table.
		 hbase.disableTable(TABLE_NAME);
		 HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY);
		 hbase.addColumn(TABLE_NAME, columnFamily);
		 hbase.enableTable(TABLE_NAME);
		 
		 hbase.close();
	 }
}
