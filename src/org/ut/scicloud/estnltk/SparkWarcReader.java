package org.ut.scicloud.estnltk;

import scala.Tuple2;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
			System.err.println("Usage: SparkWarcReader <input> <output>");
			System.exit(1);
		}

		Configuration hadoopconf = new Configuration();
		SparkConf conf = new SparkConf();

		//Output compression. Comment out to disable compression. 
		conf.set("spark.hadoop.mapred.output.compress", "true");
		conf.set("mapred.output.compression.codec","lz4");

		conf.setAppName("Spark Warq Reader");
		JavaSparkContext ctx = new JavaSparkContext(conf);

		//Read in all warc records
		JavaPairRDD <LongWritable, WarcRecord> warcRecords = ctx.newAPIHadoopFile(args[0], WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

		// Extract raw Html
		// Output: (Text key, Text val)
		// key: protocol + "::" + hostname + "::" + urlpath + "::" + parameters + "::" + date
		// val: raw html string

		JavaPairRDD<Text, Text> htmlRecords = warcRecords.mapToPair(new PairFunction<Tuple2<LongWritable, WarcRecord>, Text, Text>() {
			private static final long serialVersionUID = 413002664707012422L;

			public Tuple2<Text,Text> call(Tuple2<LongWritable, WarcRecord> val)  {

				//LongWritable key = val._1;  
				WarcRecord value = val._2;

				HttpHeader httpHeader = value.getHttpHeader();

				if (httpHeader == null) {
					// WarcRecord failed to parse httpHeader
				} else {
					if (httpHeader.contentType != null && value.header.warcTargetUriUri != null && httpHeader.contentType.contains("text/html")) {

						String host = value.header.warcTargetUriStr;

						if (host != null){	

							String keytext = "";
							String htmlraw = "";
							String encoding = "UTF-8";

							try {
								URL url = new URL(host);
								String protocol = url.getProtocol();
								String hostname = url.getHost();
								String urlpath = url.getPath();
								String param = url.getQuery();
								String date = httpHeader.getHeader("date").value;
								
								String pattern_in = "EEE, d MMM yyyy HH:mm:ss z";
								String pattern_out = "yyyymmddhhmmss";
								SimpleDateFormat format = new SimpleDateFormat(pattern_in,Locale.ENGLISH);
								SimpleDateFormat formatout = new SimpleDateFormat(pattern_out,Locale.ENGLISH);
								String newdate = formatout.format(format.parse(date));
								
								//ZonedDateTime datetime =  ZonedDateTime.parse(date, DateTimeFormatter.ofPattern());
								//String newdate = datetime.format(DateTimeFormatter.ofPattern("yyyymmddhhmmss"));

								keytext = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + newdate;

							} catch (MalformedURLException | ParseException e1) {
								e1.printStackTrace();
							}

							//Try to read record encoding
							String contentType = httpHeader.getProtocolContentType();
							if (contentType != null && contentType.contains("charset")) {
								String[] blocks = contentType.split(";");

								for(String block : blocks){
									if(block.contains("charset")){
										int idx = block.indexOf('=');
										encoding = block.substring(idx+1, block.length());
									}
								}
							}

							try {
								htmlraw = IOUtils.toString(httpHeader.getPayloadInputStream(), encoding);
							} catch (IOException e) {
								e.printStackTrace();
							} 

							return new Tuple2<>(new Text(keytext), new Text(htmlraw));
						}
						
					}
				}

				return new Tuple2<>(new Text(""),new Text(""));
			}
		});

		//Filter out records with empty keys. 
		JavaPairRDD<Text, Text> filtered = htmlRecords.filter(new Function<Tuple2<Text, Text>,Boolean>(){
			private static final long serialVersionUID = 268198219452528658L;

			@Override
			public Boolean call(Tuple2<Text, Text> s) throws Exception {
				return !s._1().toString().equals("");
			}
		});

		//Store results in (compressed) Sequencefile format
		filtered.saveAsHadoopFile(args[1], Text.class, Text.class, SequenceFileOutputFormat.class);

		ctx.close();
	}
}
