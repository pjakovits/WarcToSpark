package org.ut.scicloud.estnltk;

import scala.Tuple2;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
			System.err.println("Usage: SparkWarcReader <input> <output>");
			System.exit(1);
		}


		SparkConf conf = new SparkConf();

		//Output compression. Comment out to disable compression. 
		conf.set("spark.hadoop.mapred.output.compress", "true");
		conf.set("mapred.output.compression.codec","lz4");

		conf.setAppName("Spark Warq Reader");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

		//Read in warc records
		JavaPairInputDStream<LongWritable, WarcRecord> warcStreamingRecords = ssc.fileStream(args[0],  LongWritable.class, WarcRecord.class,WarcInputFormat.class);


		// Extract raw Html
		// Output: (Text key, Text val)
		// key: protocol + "::" + hostname + "::" + urlpath + "::" + parameters + "::" + date
		// val: raw html string

		JavaPairDStream<String, String> htmlRecords = warcStreamingRecords.mapToPair(new PairFunction<Tuple2<LongWritable, WarcRecord>, String, String>() {
			private static final long serialVersionUID = 413002664707012422L;

			public Tuple2<String,String> call(Tuple2<LongWritable, WarcRecord> val)  {

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

								ZonedDateTime datetime =  ZonedDateTime.parse(date, DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss z"));
								String newdate = datetime.format(DateTimeFormatter.ofPattern("yyyymmddhhmmss"));

								keytext = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + newdate;

							} catch (MalformedURLException e1) {
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

							return new Tuple2<>(keytext,htmlraw);
						}
					}
				}
				return new Tuple2<>("","");
			}
		});

		//Filter out records with empty keys. 
		JavaPairDStream<String, String> filtered = htmlRecords.filter(new Function<Tuple2<String, String>,Boolean>(){
			private static final long serialVersionUID = 268198219452528658L;

			@Override
			public Boolean call(Tuple2<String, String> s) throws Exception {
				return !s._1().equals("");
			}
		});

		//Store results in (compressed) Sequencefile format
		//Using foreachRDD to avoid writing out empty results when there is no input. 
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

					rdd2.saveAsHadoopFile(args[1]+System.currentTimeMillis(), Text.class, Text.class, SequenceFileOutputFormat.class);
				}
				return null;
			}

		});

		ssc.start();
		ssc.awaitTermination();
		ssc.close();

	}
}
