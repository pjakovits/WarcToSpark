package org.ut.scicloud.estnltk;

import scala.Tuple2;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;
import nl.surfsara.warcutils.WarcInputFormat;

public class SparkWarcStreamingReader {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: SparkWarcStreamingReader <input folder> <output folder>  <optional_separator>");
			System.exit(1);
		}

		final String outputfolder = args[1];
		
		SparkConf conf = new SparkConf();
		
		//Configure how old files (based on timestamps) are considered as imput files. Default is 60 seconds, which may cause problems when copying big or many files.  
		conf.set("spark.streaming.minRememberDuration", "600"); //600 sec

		conf.setAppName("Spark Warq Streaming Reader");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(10000));

		
		//Read in warc records
		//Ignore files containing string "COPYING"
		JavaPairInputDStream<LongWritable, WarcRecord> warcStreamingRecords = ssc.fileStream(args[0],  LongWritable.class, WarcRecord.class, WarcInputFormat.class, new Function<Path,Boolean>(){
			private static final long serialVersionUID = -500894852906147792L;
			@Override
			public Boolean call(Path v1) throws Exception {
				return (!v1.toString().contains("COPYING"));
			}}, true);
		
		// Extract raw Html
		// Output: (Text key, Text val)
		// key: protocol + "::" + hostname + "::" + urlpath + "::" + parameters + "::" + date
		// val: raw html string
		JavaPairDStream<String, String> htmlRecords = warcStreamingRecords.mapToPair(new WarcToHtmlFunction());

		//Filter out records with empty keys. 
		JavaPairDStream<String, String> filtered = htmlRecords.filter(new Function<Tuple2<String, String>,Boolean>(){
			private static final long serialVersionUID = 268198219452528658L;

			@Override
			public Boolean call(Tuple2<String, String> s) throws Exception {
				return !s._1().equals("");
			}
		});

		//Store results in (compressed) TextOutput format
		//Using foreachRDD to avoid writing out empty results when there is no actual output. 
		filtered.foreachRDD(new Function<JavaPairRDD<String, String>, Void>(){
			private static final long serialVersionUID = -7959326603587611867L;

			@Override
			public Void call(JavaPairRDD<String, String> rdd) throws Exception {
				if(!rdd.isEmpty()){

					JavaPairRDD<Text, Text> rdd2 = rdd.mapToPair(new PairFunction<Tuple2<String, String>,Text, Text>(){
						private static final long serialVersionUID = -7458294460957741075L;
						@Override
						public Tuple2<Text, Text> call(Tuple2<String, String> t) throws Exception {
							return new Tuple2<>(new Text(t._1()),new Text(t._2()));
						}});
					
					Configuration hadoopconf = new Configuration();
					
					//Define a Custom Key & Value separator, so it would not conflict with any actual string in the extracted HTML
					//hadoopconf.set("mapreduce.output.textoutputformat.separator", "*_*_*_*_*_*_*_*_");
					hadoopconf.set("mapreduce.output.fileoutputformat.compress", "true");
					hadoopconf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
					hadoopconf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
					
					rdd2.saveAsNewAPIHadoopFile(outputfolder+"/out_"+System.currentTimeMillis(), Text.class, Text.class, TextOutputFormat.class, hadoopconf);
				}
				return null;
			}
		});

		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}
}
