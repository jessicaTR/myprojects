import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class HBaseHomework {
	public static void main(String[] args){
		PropertyConfigurator.configure("log4j.properties");
		Logger logger = LogManager.getRootLogger();     
		
//		Configuration conf =  HBaseConfiguration.create();
		Configuration conf =  new Configuration();
//	    conf.set("fs.defaultFS", "hdfs://ubuntu:9000/");
//	    conf.set("mapreduce.framework.name", "local");
//	    conf.set("mapred.job.tracker", "ubuntu:9001");
//	    conf.set("hbase.zookeeper.quorum", "ubuntu");
	    
        conf.addResource(new Path("D:\\DBOR_WS1\\PointCarbon\\HBaseDemo\\hbase-site.xml"));
        conf.addResource(new Path("D:\\DBOR_WS1\\PointCarbon\\HBaseDemo\\core-site.xml"));
        conf.addResource(new Path("D:\\DBOR_WS1\\PointCarbon\\HBaseDemo\\hdfs-site.xml"));
        conf.addResource(new Path("D:\\DBOR_WS1\\PointCarbon\\HBaseDemo\\ssl-client.xml"));
       
        storeFileToTable(conf); 
        queryScaled(conf);
        minuteSumTAQ(conf);
        minuteSumTAS(conf);
			  
		

	}
	
	public static void storeFileToTable(Configuration conf){
	try{
		HBaseAdmin admin = new HBaseAdmin(conf);			
		HTableDescriptor htd = new HTableDescriptor("jli_taq");
		HColumnDescriptor cf = new HColumnDescriptor("tck");			 
		cf.setTimeToLive(65535);//seconds  生命周期， 插入多久后被删掉，默认是一个月，会放到archive里一段，然后还是会被清掉
		cf.setMaxVersions(3);
		htd.addFamily(cf);//	
		
		cf = new HColumnDescriptor("others");			 
		cf.setTimeToLive(65535);
		cf.setMaxVersions(3);
		htd.addFamily(cf);//	
		
		
		admin.createTable(htd);
		
		htd = new HTableDescriptor("jli_tas");
		
		cf = new HColumnDescriptor("tck");			 
		cf.setTimeToLive(65535);
		cf.setMaxVersions(3);
		htd.addFamily(cf);//		
		
		cf = new HColumnDescriptor("trd");			 
		cf.setTimeToLive(65535);
		cf.setMaxVersions(3);
		htd.addFamily(cf);//	
		
		cf = new HColumnDescriptor("others");			 
		cf.setTimeToLive(65535);
		cf.setMaxVersions(3);
		htd.addFamily(cf);//	
		
		admin.createTable(htd);
		
		htd = new HTableDescriptor("jli_factor");
		
		cf = new HColumnDescriptor("factor");			 
		cf.setTimeToLive(65535);
		cf.setMaxVersions(3);
		htd.addFamily(cf);//		
		
		admin.createTable(htd);
		
		String[] taq_columns={"VhBaseTime","VhSeqNum","VhExchgTime","VhRecvTime","VhTimeSeqNum","VhTimeFields","VhFlags","VhSubType","RecordKey","ReceivedTime","RTL_Wrap","RTL","Sub_RTL","RuleSetVersion","RuleID","RuleVersionID","RuleClauseNo","BID","BIDSIZE","ASK","ASKSIZE","UPLIMIT","LOLIMIT"};
	    String[] tas_columns={"VhBaseTime","VhSeqNum","VhExchgTime","VhRecvTime","VhTimeSeqNum","VhTimeFields","VhFlags","VhSubType","RecordKey","ReceivedTime","COLLECT_DATETIME","RTL_Wrap","RTL","Sub_RTL","RuleSetVersion","RuleID","RuleVersionID","RuleClauseNo","SOURCE_DATETIME","SEQNUM","TRDPRC_1","TRDVOL_1","ACVOL_1","VWAP","BID","BIDSIZE","ASK","ASKSIZE","PRCTCK_1","TRD_QUAL_1","TRD_QUAL_2","TRD_QUAL_4","PCTCHNG"};
	    String[] fac_columns={"RIC","Source","FieldName","Factor","EffectiveTime"};
		File2HBase.store(conf,"jli_taq","/user/hdev/8006234/", "ashx_taq.log",taq_columns);
		File2HBase.store(conf,"jli_taq","/user/hdev/8006234/", "ori_taq.log",taq_columns);
		File2HBase.store(conf,"jli_tas","/user/hdev/8006234/", "ashx_tas.log",taq_columns);
		File2HBase.store(conf,"jli_tas","/user/hdev/8006234/", "ori_tas.log",taq_columns);
		File2HBase.store(conf,"jli_factor","/user/hdev/8006234/", "ScalingFactor.log",fac_columns);
		
    } catch (MasterNotRunningException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ZooKeeperConnectionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ClassNotFoundException | InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	}
	
	public static void queryScaled(Configuration conf){
		/*
		 * TAQ: BIDSIZE, ASKSIZE
		 * TAS: BIDSIZE, ASKSIZE, TRDPRC_1, TRDVOL, ACVOL
		 */
		try {			
			HTable factorTable = new HTable(conf,"jli_factor");

			Scan scanFactor = new Scan();
			scanFactor.addFamily("factor".getBytes());
			ResultScanner scanFactorRes= factorTable.getScanner(scanFactor);
			for(Result facRes: scanFactorRes){//scan each row of factor, then query data from taq/tas which meets the effective time
				String factorStr = facRes.getValue("factor".getBytes(), "Factor".getBytes()).toString();
				Integer factor = Integer.parseInt(factorStr);
				String effectTime = facRes.getValue("factor".getBytes(), "EffectiveTime".getBytes()).toString();
				String ric = facRes.getValue("factor".getBytes(), "RIC".getBytes()).toString();
				scanTaqTas(conf, "jli_taq",  "tck",  "BIDSIZE", ric,   effectTime,  factor);
				scanTaqTas(conf, "jli_taq",  "tck",  "ASKSIZE", ric,   effectTime,  factor);
				scanTaqTas(conf, "jli_tas",  "tck",  "BIDSIZE", ric,   effectTime,  factor);
				scanTaqTas(conf, "jli_tas",  "tck",  "ASKSIZE", ric,   effectTime,  factor);
				scanTaqTas(conf, "jli_tas",  "trd",  "TRDPRC_1", ric,   effectTime,  factor);
				scanTaqTas(conf, "jli_tas",  "trd",  "TRDVOL_1", ric,   effectTime,  factor);
				scanTaqTas(conf, "jli_tas",  "trd",  "ACVOL_1", ric,   effectTime,  factor);
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	public static void scanTaqTas(Configuration conf,String tablename, String cf, String qual,String ric, String effectTime, int factor) throws IOException{
		
		HTable table = new HTable(conf,tablename);			
	
		Scan scan = new Scan();
		scan.addColumn(cf.getBytes(),qual.getBytes());
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator((ric+effectTime).getBytes()));
		
		scan.setFilter(filter1);
		ResultScanner scanner1 = table.getScanner(scan);
		System.out.println("Query scaled "+tablename+" "+qual+":");
		for(Result res:scanner1){
			String val = res.getValue(cf.getBytes(),qual.getBytes()).toString();
			Long bidsize = Long.parseLong(val);
			System.out.println(bidsize*factor);
		}
		table.close();
	}
	
	public void createSumTable(Configuration conf){
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			HTableDescriptor htd = new HTableDescriptor("jli_sum");
			HColumnDescriptor cf = new HColumnDescriptor("cf");			 
			cf.setTimeToLive(65535);//seconds  生命周期， 插入多久后被删掉，默认是一个月，会放到archive里一段，然后还是会被清掉
			cf.setMaxVersions(3);
			htd.addFamily(cf);
		
			admin.createTable(htd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
		
	}
	
	public static void minuteSumTAS(Configuration conf){
		try {
			HTable table = new HTable(conf,"jli_tas".getBytes());
			HTable sumTable = new HTable(conf,"jli_sum".getBytes());
			Scan scanFactor = new Scan();
			scanFactor.addFamily("trd".getBytes());
			ResultScanner scanRes= table.getScanner(scanFactor);
			float highPrice=0;
			float lowPrice=0;
			float openPrice=0;
			float closePrice=0;
			long sumVol=0;
			int count=0;
			int rowNum=0;
			String timeStr="00000000000000.000";					
			String minuteStr ="000000000000";
			for(Result res: scanRes){
				
				String priceStr = res.getValue("trd".getBytes(), "TRDPRC_1".getBytes()).toString();
				float price = Float.parseFloat(priceStr);
				String volStr = res.getValue("trd".getBytes(), "TRDVOL_1".getBytes()).toString();
				int vol = Integer.parseInt(priceStr);
				String ric = res.getRow().toString().substring(0,6);
				String thisTimeStr = res.getRow().toString().substring(6, 23);
				String thisMinuteStr=res.getRow().toString().substring(6, 18);
				if(rowNum==0){
					timeStr = thisTimeStr;
					minuteStr = thisMinuteStr;
					highPrice = price;
					lowPrice = price;
					openPrice = price;
					closePrice = price;
					sumVol = vol; 
					count ++;
				}else{
					if(!thisMinuteStr.equals(minuteStr)){
						
						closePrice = price;
						
						insertSumTable(sumTable,ric, "H_TRDPRC_1",minuteStr, Float.toString(highPrice));
						insertSumTable(sumTable,ric, "L_TRDPRC_1",minuteStr, Float.toString(lowPrice));
						insertSumTable(sumTable,ric, "O_TRDPRC_1",minuteStr, Float.toString(openPrice));
						insertSumTable(sumTable,ric, "C_TRDPRC_1",minuteStr, Float.toString(closePrice));
						insertSumTable(sumTable,ric, "S_TRDVOL_1",minuteStr, Float.toString(sumVol));
						insertSumTable(sumTable,ric, "N_TRDVOL_1",minuteStr, Float.toString(count));
						
//						String rowkey=ric+minuteStr+"H_TRDPRC_1";
//						Put put1 = new Put(rowkey.getBytes());
//						put1.add("cf".getBytes(),"H_TRDPRC_1".getBytes(),Float.toString(highPrice).getBytes());
//						table.put(put1);
//						
//						rowkey=ric+minuteStr+"L_TRDPRC_1";
//						Put put2 = new Put(rowkey.getBytes());
//						put2.add("cf".getBytes(),"L_TRDPRC_1".getBytes(),Float.toString(lowPrice).getBytes());
//						table.put(put2);
//						
//						
//						rowkey=ric+minuteStr+"O_TRDPRC_1";
//						Put put3 = new Put(rowkey.getBytes());
//						put3.add("cf".getBytes(),"O_TRDPRC_1".getBytes(),Float.toString(openPrice).getBytes());
//						table.put(put3);
//						
//						
//						rowkey=ric+minuteStr+"C_TRDPRC_1";
//						Put put4 = new Put(rowkey.getBytes());
//						put4.add("cf".getBytes(),"C_TRDPRC_1".getBytes(),Float.toString(closePrice).getBytes());
//						table.put(put4);
//						
//						rowkey=ric+minuteStr+"S_TRDVOL_1";
//						Put put5 = new Put(rowkey.getBytes());
//						put5.add("cf".getBytes(),"S_TRDVOL_1".getBytes(),Float.toString(sumVol).getBytes());
//						table.put(put5);
						
						
						minuteStr = thisMinuteStr;
						timeStr = thisTimeStr;
						highPrice = price;
						lowPrice = price;
						openPrice = price;
						closePrice = price;
						sumVol = vol; 
						count++;
						
						
					}else{
						if(price>highPrice) highPrice = price;
						if(price<lowPrice)  lowPrice = price;
						sumVol +=vol;
						count++;
						
					}
				}
				
			}
			table.close();
			sumTable.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void insertSumTable(HTable table,String ric, String field,String minute, String value){
		
		 
		try {			 
			String rowkey=ric+minute+field;
			Put put = new Put(rowkey.getBytes());
			put.add("cf".getBytes(),field.getBytes(), value.getBytes());
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
    public static void minuteSumTAQ(Configuration conf){
    	
    	try {
			HTable table = new HTable(conf,"jli_taq".getBytes());
			HTable sumTable = new HTable(conf,"jli_sum".getBytes());
			Scan scanFactor = new Scan();
			scanFactor.addFamily("trd".getBytes());
			ResultScanner scanRes= table.getScanner(scanFactor);
			float highBid=0;
			float lowBid=0;
			float highAsk=0;
			float lowAsk=0;
			float openBid=0;
			float closeBid=0;
			float openAsk=0;
			float closeAsk=0;
			long sumBidsize=0;
			long sumAsksize=0;
			int countBid = 0;
			int countAsk = 0;
			int rowNum=0;
			String timeStr="00000000000000.000";					
			String minuteStr ="000000000000";
			for(Result res: scanRes){
				
				String bidStr = res.getValue("tck".getBytes(), "BID".getBytes()).toString();
				String askStr = res.getValue("tck".getBytes(), "ASK".getBytes()).toString();
				float bid = Float.parseFloat(bidStr);
				float ask = Float.parseFloat(askStr);
				String bidsizeStr = res.getValue("tck".getBytes(), "BIDSIZE".getBytes()).toString();
				String asksizeStr = res.getValue("tck".getBytes(), "ASKSIZE".getBytes()).toString();
				long bidsize = Integer.parseInt(bidsizeStr);
				long asksize = Integer.parseInt(asksizeStr);
				
				String ric = res.getRow().toString().substring(0,6);				
				String thisMinuteStr=res.getRow().toString().substring(6, 18);
				if(rowNum==0){
					
					minuteStr = thisMinuteStr;
					highBid = bid;
					lowBid = bid;
					highAsk = ask;
					lowAsk = ask;
					openBid = bid;
					openAsk = ask;
					closeBid = bid;
					closeAsk = ask;
					sumBidsize = bidsize;
					sumAsksize = asksize;
					countBid++;
					countAsk++;
				}else{
					if(!thisMinuteStr.equals(minuteStr)){
						
						closeBid = bid;
						closeAsk = ask;
						
						insertSumTable(sumTable,ric, "H_BID",minuteStr, Float.toString(highBid));
						insertSumTable(sumTable,ric, "L_BID",minuteStr, Float.toString(lowBid));
						insertSumTable(sumTable,ric, "O_BID",minuteStr, Float.toString(openBid));
						insertSumTable(sumTable,ric, "C_BID",minuteStr, Float.toString(closeBid));
						insertSumTable(sumTable,ric, "N_BID",minuteStr, Float.toString(countBid));
						insertSumTable(sumTable,ric, "S_BIDSIZE",minuteStr, Float.toString(sumBidsize));
						
						insertSumTable(sumTable,ric, "H_ASK",minuteStr, Float.toString(highAsk));
						insertSumTable(sumTable,ric, "L_ASK",minuteStr, Float.toString(lowAsk));
						insertSumTable(sumTable,ric, "O_ASK",minuteStr, Float.toString(openAsk));
						insertSumTable(sumTable,ric, "C_ASK",minuteStr, Float.toString(closeAsk));
						insertSumTable(sumTable,ric, "N_ASK",minuteStr, Float.toString(countAsk));
						insertSumTable(sumTable,ric, "S_ASKSIZE",minuteStr, Float.toString(sumAsksize));
															
						
						minuteStr = thisMinuteStr;		
						
						highBid = bid;
						lowBid = bid;
						openBid = bid;
						closeBid = bid;
						sumBidsize = bidsize; 
						countBid++;
						
						highAsk = ask;
						lowAsk = ask;
						openAsk = ask;
						closeAsk = ask;
						sumAsksize = asksize; 
						countAsk++;
						
						
					}else{
						if(bid>highBid) highBid = bid;
						if(bid<lowBid)  lowBid = bid;
						if(ask>highAsk) highAsk = ask;
						if(ask<lowAsk)  lowAsk = ask;
						
						sumBidsize +=bidsize;
						sumAsksize +=asksize;
						
						countBid++;
						countAsk++;
						
					}
				}
				
			}
			
			table.close();
			sumTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
