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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;
import nl.surfsara.warcutils.WarcInputFormat;

public class SparkWarcReader {
	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: SparkWarcReader <input folder> <output folder> <optional_separator>");
			System.exit(1);
		}
		
		Configuration hadoopconf = new Configuration();
		
		if(args.length > 2){
			//Define a Custom Key & Value separator
			hadoopconf.set("mapreduce.output.textoutputformat.separator", args[2]);
		}
		
		hadoopconf.set("mapreduce.output.fileoutputformat.compress", "true");
		hadoopconf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.GzipCodec");
		hadoopconf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
		
		SparkConf conf = new SparkConf();
		
		//Output compression. Comment out to disable compression. 
		//conf.set("spark.hadoop.mapred.output.compress", "true");
		//conf.set("mapred.output.compression.codec","lz4");
		
		conf.setAppName("Spark Warq Reader");
		JavaSparkContext ctx = new JavaSparkContext(conf);

		//Read in all warc records
		JavaPairRDD <LongWritable, WarcRecord> warcRecords = ctx.newAPIHadoopFile(args[0], WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

		// Extract raw Html using WarcToHtmlM2P function
		// Output: (Text key, Text val)
		// key: protocol + "::" + hostname + "::" + urlpath + "::" + parameters + "::" + date
		// val: raw html string
		JavaPairRDD<String, String> htmlRecords = warcRecords.mapToPair(new WarcToHtmlFunction());
			
		//Filter out records with empty keys. 
		JavaPairRDD<String, String> filtered = htmlRecords.filter(new Function<Tuple2<String, String>,Boolean>(){
			private static final long serialVersionUID = 268198219452528658L;

			@Override
			public Boolean call(Tuple2<String, String> s) throws Exception {
				return !s._1().equals("");
			}
		});
		
		//cast String values into Text writable type for Hadoop API. 
		filtered.mapToPair(new PairFunction<Tuple2<String,String>, Text, Text>(){
			private static final long serialVersionUID = -788808740444185356L;

			@Override
			public Tuple2<Text, Text> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<>(new Text(t._1()), new Text(t._2()));
			}
		});
		
		//Store results in (compressed) TextOutput format
		filtered.saveAsNewAPIHadoopFile(args[1], Text.class, Text.class, TextOutputFormat.class, hadoopconf);
		
		ctx.close();
	}
}
