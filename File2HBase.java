import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class File2HBase {
  static String filename;
  static String[] COLUMNS;

  public static class MapperClass extends
      Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    //列名
//    public static final String[] COLUMNS = { "card", "type",
//        "amount", "time", "many" };
  
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] cloumnVals = value.toString().split(",");
      String rowkey ;
      if(filename.contains("ScalingFactor")) {
    	  rowkey = cloumnVals[0] +cloumnVals[1]+cloumnVals[2];
      }else{
    	  rowkey = (filename.split("_")[0]).replace("-", ".").toUpperCase()+cloumnVals[0] ;
      }
      Put put = new Put(rowkey.getBytes());
      String cf;
      for (int i = 0; i < cloumnVals.length; i++) {
    	if(filename.contains("ScalingFactor")) {
    		put.add("factor".getBytes(), COLUMNS[i].getBytes(),cloumnVals[i].getBytes());
    	}else if(!filename.contains("ScalingFactor") ){
	    	if(COLUMNS[i].equals("BID") || COLUMNS[i].equals("BIDSIZE") || COLUMNS[i].equals("ASK") ||COLUMNS[i].equals("ASKSIZE")) cf="tck";
	    	else if(COLUMNS[i].equals("TRDPRC_1") || COLUMNS[i].equals("TRDVOL_1") || COLUMNS[i].equals("ACVOL_1") ) cf="trd";
	    	else cf="others";
	        put.add(cf.getBytes(), COLUMNS[i].getBytes(),cloumnVals[i].getBytes());
    	}
      }
      context.write(new ImmutableBytesWritable(rowkey.getBytes()), put);
    }
  }
  
  @SuppressWarnings("deprecation")
  public static void store(Configuration conf,String tablename,String filepath, String file,String[] columns) throws IOException, InterruptedException, ClassNotFoundException {
//    Configuration conf = new Configuration();
//    conf.set("fs.defaultFS", "hdfs://ubuntu:9000/");
//    conf.set("mapreduce.framework.name", "local");
//    conf.set("mapred.job.tracker", "ubuntu:9001");
//    conf.set("hbase.zookeeper.quorum", "ubuntu");
	filename = file;
	COLUMNS = columns;
    Job job = new Job(conf,"file2hbase");
    job.setJarByClass(File2HBase.class);
    job.setMapperClass(MapperClass.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(TableOutputFormat.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
//    FileInputFormat.addInputPath(job, new Path("hdfs://ubuntu:9000/user/hdev/8006234/ashx_taq.log"));
    FileInputFormat.addInputPath(job, new Path(filepath+file));
    System.out.println(job.waitForCompletion(true) ? 0 : 1);
  }
}
