import org.apache.http.*;
import org.apache.http.client.methods.*;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.httpclient.URIException;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.message.*;
import org.apache.http.client.entity.*;
import org.apache.http.impl.client.*;
import org.apache.http.client.*;

import java.util.*;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map.Entry;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


public class MainGeoTableUpload {
	
	public static String MD5(String md5){
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] array = md.digest(md5.getBytes());
			StringBuffer sb = new StringBuffer();
			for(int i = 0; i < array.length; ++i){
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
			}
			return sb.toString();
		}catch(NoSuchAlgorithmException e){
			System.out.println();
		}
		return null;
	}
	
	public static String toQueryString(Map<?,?> data){
		StringBuffer queryString = new StringBuffer();
		try{
			for(Entry<?,?>pair:data.entrySet()){
				queryString.append(pair.getKey() + "=");
				String[] ss = pair.getValue().toString().split(",");
				if(ss.length > 1){
					for(String s : ss){
						queryString.append(URIUtil.encodeQuery(s, "UTF-8") + ",");
					}
					queryString.deleteCharAt(queryString.length() - 1);
					queryString.append("&");
				}
				else{
					queryString.append(URIUtil.encodeQuery((String) pair.getValue(), "UTF-8") + "&");
				}			
			}
			if(queryString.length() > 0){
				queryString.deleteCharAt(queryString.length()-1);
			}
			return queryString.toString();
		}catch(URIException e){
			System.out.println(e.toString());
			return null;
		}

	}
	
	public static String inStream2String(InputStream is){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		int len = -1;
		try{
			while((len = is.read(buf)) != -1){
				baos.write(buf,0,len);
			}
			return new String(baos.toByteArray(),"UTF-8");
		}catch(Exception e){
			return null;
		}
	}
	
	public static void createTable(String tableName){
		try{
			String sk = "qohftqeYagRGRrRn9wEzL1duazvCZDXb";
			LinkedHashMap<String,String> paramsMap = new LinkedHashMap<String,String>();
			paramsMap.put("geotype", "1");
			paramsMap.put("ak", "tkhDt60OqqXfPUf1kFMKZkxphS7Qvs9T");
			paramsMap.put("name", tableName);
			paramsMap.put("is_published", "1");
			
			Map<String,String> treeMap = new TreeMap<String,String>(paramsMap);
			String paramsStr = toQueryString(treeMap);
			String wholeStr = new String("/geodata/v3/geotable/create?" + paramsStr + sk);
			String tempStr  = URLEncoder.encode(wholeStr,"UTF-8");
			String sn = MD5(tempStr);
			
			HttpPost post =  new HttpPost("http://api.map.baidu.com/geodata/v3/geotable/create");
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			params.add(new BasicNameValuePair("geotype","1"));
			params.add(new BasicNameValuePair("ak","tkhDt60OqqXfPUf1kFMKZkxphS7Qvs9T"));
			params.add(new BasicNameValuePair("name",tableName));
			params.add(new BasicNameValuePair("is_published","1"));
			params.add(new BasicNameValuePair("sn",sn));
			HttpEntity formEntity = new UrlEncodedFormEntity(params);
			post.setEntity(formEntity);
			HttpClient httpclient = new DefaultHttpClient();
			httpclient.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
			HttpResponse response = httpclient.execute(post);
			InputStream is = response.getEntity().getContent();
			String result = inStream2String(is);
			System.out.println(result.toString());
			
		}catch(ClientProtocolException e1){
			System.out.println(e1.toString());
		
		}catch(IOException e2){
			System.out.println(e2.toString());
			
		}
	}
	
	public static void postPOI(String tableId, String poiFile){
		try{
			File csvFile = new File(poiFile);
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line = "";
			String[] lineFields = new String[7];
			while((line = br.readLine()) != null){
				StringTokenizer st = new StringTokenizer(line,",");
				int i = 0;
				while(st.hasMoreTokens()){
					lineFields[i++] = st.nextToken();
				}
				String sk = "qohftqeYagRGRrRn9wEzL1duazvCZDXb";
				LinkedHashMap<String,String> paramsMap = new LinkedHashMap<String,String>();
				paramsMap.put("geotable_id", tableId);
				paramsMap.put("ak", "tkhDt60OqqXfPUf1kFMKZkxphS7Qvs9T");
				paramsMap.put("latitude", lineFields[3]);
				paramsMap.put("longitude", lineFields[2]);
				paramsMap.put("coord_type", lineFields[6]);
				paramsMap.put("title", lineFields[0]);
				paramsMap.put("address", lineFields[1]);
				paramsMap.put("occurTime", lineFields[5]);
				paramsMap.put("msisdn", lineFields[4]);
				paramsMap.put("tags", "shenzhen");
						
				Map<String,String> treeMap = new TreeMap<String,String>(paramsMap);
				String paramsStr = toQueryString(treeMap);
				String wholeStr = new String("/geodata/v3/poi/create?" + paramsStr + sk);
				String tempStr  = URLEncoder.encode(wholeStr,"UTF-8");
				String sn = MD5(tempStr);
				
				HttpPost post =  new HttpPost("http://api.map.baidu.com/geodata/v3/poi/create");
				List<NameValuePair> params = new ArrayList<NameValuePair>();
				params.add(new BasicNameValuePair("geotable_id", tableId));
				params.add(new BasicNameValuePair("ak","tkhDt60OqqXfPUf1kFMKZkxphS7Qvs9T"));
				params.add(new BasicNameValuePair("latitude", lineFields[3]));
				params.add(new BasicNameValuePair("longitude", lineFields[2]));
				params.add(new BasicNameValuePair("coord_type", lineFields[6]));
				params.add(new BasicNameValuePair("title", lineFields[0]));
				params.add(new BasicNameValuePair("address", lineFields[1]));
				params.add(new BasicNameValuePair("occurTime", lineFields[5]));
				params.add(new BasicNameValuePair("msisdn", lineFields[4]));
				params.add(new BasicNameValuePair("tags", "shenzhen"));
				params.add(new BasicNameValuePair("sn",sn));
				HttpEntity formEntity = new UrlEncodedFormEntity(params);
				post.setEntity(formEntity);
				HttpClient httpclient = new DefaultHttpClient();
				httpclient.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
				HttpResponse response = httpclient.execute(post);
				InputStream is = response.getEntity().getContent();
				String result = inStream2String(is);
				System.out.println(result.toString());
			}
			br.close();
		}catch(ClientProtocolException e1){
			System.out.println(e1.toString());	
		}catch(IOException e2){
			System.out.println(e2.toString());
		}
	}

	public static void createColumn(String tableId,String columnName, String columnKey, String columnType, String columnSorted, String columnSearch, String columnIndex,String ... maxLength){
		try{
			String sk = "qohftqeYagRGRrRn9wEzL1duazvCZDXb";
			LinkedHashMap<String,String> paramsMap = new LinkedHashMap<String,String>();
			paramsMap.put("geotable_id", tableId);
			if(maxLength!=null){
				paramsMap.put("max_length", maxLength[0]);
			}		
			paramsMap.put("ak", "tkhDt60OqqXfPUf1kFMKZkxphS7Qvs9T");
			paramsMap.put("name", columnName);
			paramsMap.put("key", columnKey);
			paramsMap.put("type", columnType);
			paramsMap.put("is_sortfilter_field", columnSorted);
			paramsMap.put("is_search_field", columnSearch);
			paramsMap.put("is_index_field", columnIndex);
			
			Map<String,String> treeMap = new TreeMap<String,String>(paramsMap);
			String paramsStr = toQueryString(treeMap);
			String wholeStr = new String("/geodata/v3/column/create?" + paramsStr + sk);
			String tempStr  = URLEncoder.encode(wholeStr,"UTF-8");
			String sn = MD5(tempStr);
			
			HttpPost post =  new HttpPost("http://api.map.baidu.com/geodata/v3/column/create");
			List<NameValuePair> params = new ArrayList<NameValuePair>();
			params.add(new BasicNameValuePair("geotable_id", tableId));
			if(maxLength!=null){
				params.add(new BasicNameValuePair("max_length", maxLength[0]));
			}
			params.add(new BasicNameValuePair("ak","tkhDt60OqqXfPUf1kFMKZkxphS7Qvs9T"));
			params.add(new BasicNameValuePair("name", columnName));
			params.add(new BasicNameValuePair("key", columnKey));
			params.add(new BasicNameValuePair("type", columnType));
			params.add(new BasicNameValuePair("is_sortfilter_field", columnSorted));
			params.add(new BasicNameValuePair("is_search_field", columnSearch));
			params.add(new BasicNameValuePair("is_index_field", columnIndex));
			params.add(new BasicNameValuePair("sn",sn));
			HttpEntity formEntity = new UrlEncodedFormEntity(params);
			post.setEntity(formEntity);
			HttpClient httpclient = new DefaultHttpClient();
			httpclient.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
			HttpResponse response = httpclient.execute(post);
			InputStream is = response.getEntity().getContent();
			String result = inStream2String(is);
			System.out.println(result.toString());
			
		}catch(ClientProtocolException e1){
			System.out.println(e1.toString());
		
		}catch(IOException e2){
			System.out.println(e2.toString());
		}
	}
	
	public static void main(String[] args){
		createTable("szPoints");
		String[] maxLength = {"20"};
		createColumn("144071","msisdn", "msisdn", "3", "0", "1", "0",maxLength);
		createColumn("144071","occurTime", "occurTime", "3", "0", "1", "0",maxLength);
		postPOI("144071", "/home/wongkongtao/BaiduMapAPI/databox_144071.csv");
	}

}
