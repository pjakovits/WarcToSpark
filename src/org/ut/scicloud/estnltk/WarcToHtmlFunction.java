package org.ut.scicloud.estnltk;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.function.PairFunction;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;

import scala.Tuple2;

public class WarcToHtmlFunction implements PairFunction<Tuple2<LongWritable, WarcRecord>, String, String> {
	private static final long serialVersionUID = 413002664707012422L;

	public Tuple2<String,String> call(Tuple2<LongWritable, WarcRecord> val)  {

		WarcRecord value = val._2;
		HttpHeader httpHeader = value.getHttpHeader();

		if (httpHeader == null) {
			// WarcRecord failed to parse httpHeader
		} else {
			if (httpHeader.contentType != null && value.header.warcTargetUriUri != null && httpHeader.contentType.contains("text/html")) {

				String host = value.header.warcTargetUriStr;

				if (host != null){	
					
					String keytext = host;
					String htmlraw = "";
					String encoding = "UTF-8";

					try {
						URL url = new URL(host);
						String protocol = url.getProtocol();
						String hostname = url.getHost();
						String urlpath = url.getPath();
						String param = url.getQuery();
						
						keytext = protocol + "::" + hostname + "::" + urlpath + "::" + param;
								
					} catch (MalformedURLException | NullPointerException e1) {
						e1.printStackTrace();
					}
					
					try {
						String date = httpHeader.getHeader("date").value;
						
						String pattern_in = "EEE, d MMM yyyy HH:mm:ss z";
						String pattern_out = "yyyymmddhhmmss";
						SimpleDateFormat format = new SimpleDateFormat(pattern_in,Locale.ENGLISH);
						SimpleDateFormat formatout = new SimpleDateFormat(pattern_out,Locale.ENGLISH);
						String newdate = formatout.format(format.parse(date));							

						keytext += "::" + newdate;
						
					} catch ( ParseException | NullPointerException e1) {
						e1.printStackTrace();
					}
					

					//Try to read record encoding
					String contentType = httpHeader.getProtocolContentType();
					contentType = contentType.replaceAll("\"","");
					contentType = contentType.replaceAll("\'","");
					
					if (contentType != null && contentType.contains("charset")) {
						String[] blocks = contentType.split(";");
						
						for(String block : blocks){
							if(block.contains("charset")){
								if(block.contains("=")){
									int idx = block.indexOf('=');
									encoding = block.substring(idx+1, block.length());										
								}
								else if(block.contains(":")){
									int idx = block.indexOf(':');
									encoding = block.substring(idx+1, block.length());										
								}
							}
						}
					}

					try {
						htmlraw = IOUtils.toString(httpHeader.getPayloadInputStream(), encoding);
						
						//Remove all whitespaces except single space. Resulting sting should be single line.
						//This also allows us to keep using /t as a Key and Value separator afterwards 
						htmlraw = htmlraw.replaceAll("\r"," ");
						htmlraw = htmlraw.replaceAll("\t"," ");
						htmlraw = htmlraw.replaceAll("\\s{2,}"," ");
						htmlraw = htmlraw.replaceAll("\n","");
						
						return new Tuple2<>(keytext, htmlraw);
						
					} catch (IOException e) {
						//e.printStackTrace();
					} catch (IllegalCharsetNameException e) {
						//e.printStackTrace();
					} catch (UnsupportedCharsetException e) {
						//e.printStackTrace();
					}
				}
			}
		}
		
		//Return empty (indicating a failure to parse) result, which should be filtered out later. 
		return new Tuple2<>("","");
	}
}
