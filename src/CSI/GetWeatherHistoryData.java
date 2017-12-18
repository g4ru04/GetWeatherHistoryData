package CSI;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import com.google.gson.Gson;

public class GetWeatherHistoryData {
	//logger.debug(CommonMethodForUpdate.JSONStringify(taipeiSunShine));
	public static class ConfigForUpdate{
		String dbURLbatch ;
		String dbUserName ;
		String dbPassword ;
		String tmpDIR = "temporary";
		String projectPath ;
		
	 	String deleteOrNot = "true" ;
		
		String needSP = "sp_insert_data_predict_weather";
		String needTB = "tb_data_predict_weather";
	}
	
	private static final Logger logger = LogManager.getLogger(GetWeatherHistoryData.class);
	
	//name code lat lng url 17個站點是手動找的 從 全台542個點之中找的 離目前所有有人流的捷運站最近的17個
	//條件篩選: 不是全區統計類的 有雨量 離捷運站最近
	static String observeStationJsonObj = 
			    "{\"C0AC80\":{\"name\":\"文山\",\"code\":\"C0AC80\",\"lat\":\"25.002350\",\"lng\":\"121.575728\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AC80&stname=%E6%96%87%E5%B1%B1&datepicker=\"},"
				+"\"C0AC70\":{\"name\":\"信義\",\"code\":\"C0AC70\",\"lat\":\"25.037822\",\"lng\":\"121.564597\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AC70&stname=%E4%BF%A1%E7%BE%A9&datepicker=\"},"
				+"\"C0A9A0\":{\"name\":\"大直\",\"code\":\"C0A9A0\",\"lat\":\"25.078047\",\"lng\":\"121.542853\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A9A0&stname=%E5%A4%A7%E7%9B%B4&datepicker=\"},"
				+"\"C0A9F0\":{\"name\":\"內湖\",\"code\":\"C0A9F0\",\"lat\":\"25.079422\",\"lng\":\"121.575450\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A9F0&stname=%E5%85%A7%E6%B9%96&datepicker=\"},"
				+"\"C0A9G0\":{\"name\":\"南港\",\"code\":\"C0A9G0\",\"lat\":\"25.055431\",\"lng\":\"121.602906\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A9G0&stname=%E5%8D%97%E6%B8%AF&datepicker=\"},"
				+"\"C0AH10\":{\"name\":\"永和\",\"code\":\"C0AH10\",\"lat\":\"25.011250\",\"lng\":\"121.508111\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AH10&stname=%E6%B0%B8%E5%92%8C&datepicker=\"},"
				+"\"C0A9I1\":{\"name\":\"三重\",\"code\":\"C0A9I1\",\"lat\":\"25.056667\",\"lng\":\"121.488492\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A9I1&stname=%E4%B8%89%E9%87%8D&datepicker=\"},"
				+"\"C0A9B0\":{\"name\":\"石牌\",\"code\":\"C0A9B0\",\"lat\":\"25.116342\",\"lng\":\"121.513817\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A9B0&stname=%E7%9F%B3%E7%89%8C&datepicker=\"},"
				+"\"C0A980\":{\"name\":\"社子\",\"code\":\"C0A980\",\"lat\":\"25.109508\",\"lng\":\"121.469681\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A980&stname=%E7%A4%BE%E5%AD%90&datepicker=\"},"
				+"\"C0AD10\":{\"name\":\"八里\",\"code\":\"C0AD10\",\"lat\":\"25.150211\",\"lng\":\"121.403947\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AD10&stname=%E5%85%AB%E9%87%8C&datepicker=\"},"
				+"\"C0A580\":{\"name\":\"屈尺\",\"code\":\"C0A580\",\"lat\":\"24.922422\",\"lng\":\"121.546333\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A580&stname=%E5%B1%88%E5%B0%BA&datepicker=\"},"
				+"\"C0AG90\":{\"name\":\"中和\",\"code\":\"C0AG90\",\"lat\":\"24.992647\",\"lng\":\"121.490444\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AG90&stname=%E4%B8%AD%E5%92%8C&datepicker=\"},"
				+"\"C0ACA0\":{\"name\":\"新莊\",\"code\":\"C0ACA0\",\"lat\":\"25.051478\",\"lng\":\"121.446756\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0ACA0&stname=%E6%96%B0%E8%8E%8A&datepicker=\"},"
				+"\"C0C680\":{\"name\":\"龜山\",\"code\":\"C0C680\",\"lat\":\"25.028460\",\"lng\":\"121.386560\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0C680&stname=%E9%BE%9C%E5%B1%B1&datepicker=\"},"
				+"\"C0AD30\":{\"name\":\"蘆洲\",\"code\":\"C0AD30\",\"lat\":\"25.086594\",\"lng\":\"121.472331\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AD30&stname=%E8%98%86%E6%B4%B2&datepicker=\"},"
				+"\"C0A520\":{\"name\":\"山佳\",\"code\":\"C0A520\",\"lat\":\"24.974944\",\"lng\":\"121.402008\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0A520&stname=%E5%B1%B1%E4%BD%B3&datepicker=\"},"
				+"\"C0AD40\":{\"name\":\"土城\",\"code\":\"C0AD40\",\"lat\":\"24.973208\",\"lng\":\"121.445169\",\"url\":\"http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=C0AD40&stname=%E5%9C%9F%E5%9F%8E&datepicker=\"}"
				+"}";
	
	static String taipeiSunShineUrl = "http://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station=466920&stname=%25E8%2587%25BA%25E5%258C%2597&datepicker=";
	static JSONObject taipeiSunShine = null;
	
    public static void main(String[] args){
    	
    	logger.info("開啟工作-抓取中央氣象局歷史天氣資料");
//    	logger.info("(註:此工作可能因該網站response過慢而失敗 此屬正常現象 對該網站做1000次連線實測結果(連結成功且順暢, 連線時間>5s, 連線時間>20s, 連線無效)=(977,9,10,4)");
    	Date begin=new Date();
    	String from_time = "2017-09-01";
    	String till_time = "2017-10-19";
    	
    	try{
    		logger.info("指令> 抓取中央氣象局-歷史天氣資料[ "+ from_time +" ~ "+ till_time +" ] 進度: 讀取設定檔(1/5)");
    		
    		String sysString = GetWeatherHistoryData.class.getProtectionDomain().getCodeSource().getLocation().getPath(); 
			String projectPath = sysString.split(sysString.split("/")[sysString.split("/").length - 1])[0];
			
			ConfigForUpdate myConfig = CommonMethodForUpdate.getConfigs(projectPath);
			myConfig.projectPath = projectPath;
			
			logger.info("指令> 抓取中央氣象局-歷史天氣資料[ "+ from_time +" ~ "+ till_time +" ] 進度: 下載檔案(2/5)");
			
			File destFolder = new File(projectPath+"/"+myConfig.tmpDIR);
			logger.debug("檔案暫存位置: '"+destFolder.getAbsolutePath()+"'");
			destFolder.mkdirs();
			
    		List<String> urlList = getAllUrl(from_time,till_time);
    		int successDownload = 0 ;
    		//下載所有資料
    		for(int i=0;i<urlList.size();i++){
    			String day = urlList.get(i).split("datepicker=")[1];
    	    	String code = urlList.get(i).split("&stname")[0].split("viewMain&station=")[1];
    	    	if("466920".equals(code)){
    	    		code = "TaipeiSunShine";
    	    	}
    	    	
    			if(CommonMethodForUpdate.isValidURL(urlList.get(i))){
    				if(CommonMethodForUpdate.downlodFileSuccess(
    						urlList.get(i),
    					destFolder+"/"+day+"_"+code+".html"
    				)){
    					successDownload++;
    				};
    			}
    			if( successDownload!=0 && successDownload%50==0 ){
    				logger.debug("已下載檔案數量: "+successDownload+"/"+urlList.size());
    			}
    		}
    		logger.debug("總下載檔案數量: "+successDownload+"/"+urlList.size());
			
    		logger.info("指令> 抓取中央氣象局-歷史天氣資料[ "+ from_time +" ~ "+ till_time +" ] 進度: 解析資料，臺北全區天氣資料(3/5)");
    		File folder = new File(destFolder.getAbsolutePath());
    		String[] tempList = folder.list();
    		taipeiSunShine = new JSONObject();
    		File temp = null;
    		for (int i = 0; i < tempList.length; i++) {
    			temp = new File(folder + "/" + tempList[i]);
    			if (temp.isFile() && ("TaipeiSunShine".equals(tempList[i].split("_")[1].split(".html")[0]))) {
    				//取臺北全體日照資料
    				getTaipeiSunShineHr(temp,myConfig);
    			}
    		}
    		
			logger.info("指令> 抓取中央氣象局-歷史天氣資料[ "+ from_time +" ~ "+ till_time +" ] 進度: 解析資料，匯入資料庫(4/5)");
			
			JSONArray batchElement= new JSONArray();
    		File temp2 = null;
    		int i;
    		for (i = 0; i < tempList.length; i++) {
    			temp2 = new File(folder + "/" + tempList[i]);
    			if (temp.isFile()&& (!"TaipeiSunShine".equals(tempList[i].split("_")[1].split(".html")[0]))) {
    				//取17關測點資料
    				parseWeatherHistoryFile(batchElement,temp2,myConfig);
    			}
    			
//    			if( i%50==0 ){
//    				logger.debug("解析資料進度: "+i+"/"+tempList.length);
//    			}
    		}
//    		logger.debug("總解析資料量: "+i+"/"+tempList.length+" 開始匯入資料。");
    		CommonMethodForUpdate.batchCMD("call sp_insert_data_predict_weather(?,?,?,?,?,?,?,?,?,?,?,?)",batchElement,myConfig);
    		
			logger.info("指令> 抓取中央氣象局-歷史天氣資料[ "+ from_time +" ~ "+ till_time +" ] 進度: 刪除暫存檔(5/5)");
			if("true".equals(myConfig.deleteOrNot)){
				CommonMethodForUpdate.deleteFolder(destFolder.getAbsolutePath());
			}else{
				logger.info("設定檔設為不刪除。");
			}
			logger.info("完成工作-抓取中央氣象局歷史天氣資料");
			
    	}catch(Exception e){
    		e.printStackTrace();
    		logger.info("異常中止-抓取中央氣象局歷史天氣資料");
    		logger.debug("處理資料發生異常，請檢查現行檔案格式是否變更: "+e.toString());
    	}finally{
    		logger.info("工作總耗時: "+(new Date().getTime()-begin.getTime())+" ms");
    	}
    	
    }
    
    public static JSONObject getTaipeiSunShineHr(File file,ConfigForUpdate myConfig){
    	
    	String day = file.getName().split("_")[0];
//    	logger.debug("讀取臺北全區天氣資料-"+day);
    	
    	try {
			Document xmlDoc = Jsoup.parse(file,"UTF-8","");
			Elements trs = xmlDoc.select("#MyTable tr[class!='first_tr'][class!='second_tr']");
	        for(int j=0;j<trs.size();j++){
	        	taipeiSunShine.put(
	        		day+"_"+trs.get(j).getElementsByTag("td").get(0).text().replace("\u00a0", ""),
	        		trs.get(j).getElementsByTag("td").get(12).text().replace("\u00a0", "")
	        	);
	        }
	        
	        return taipeiSunShine;
	        
		} catch (IOException e) {
			
			logger.debug("取得臺北日照資料錯誤: "+e.toString());
			e.printStackTrace();
			return null;
			
		}
    }
    

    public static boolean parseWeatherHistoryFile(JSONArray batchElement, File file,ConfigForUpdate myConfig){
    	try {
    		
	    	String day = file.getName().split("_")[0];
	    	String code = file.getName().split("_")[1].split(".html")[0];
	    	
	    	if("TaipeiSunShine".equals(code)){
	    		return false;
	    	}
	    	JSONObject jsonobj = new JSONObject(observeStationJsonObj);
	        Document xmlDoc = Jsoup.parse(file,"UTF-8","");
	        Elements trs = xmlDoc.select("#MyTable tr[class!='first_tr'][class!='second_tr']");
	        for(int j=0;j<trs.size();j++){
	        	
	        	//以下丟溫度資料
	        	Map<String,String> voT = new LinkedHashMap<String,String>();
	        	voT.put("a", day);
	        	voT.put("b", trs.get(j).getElementsByTag("td").get(0).text().replace("\u00a0", ""));
	        	voT.put("c", "T");
				voT.put("d", trs.get(j).getElementsByTag("td").get(3).text().replace("\u00a0", ""));
				voT.put("e", "C");
				voT.put("f", "");
				voT.put("g", "");
				voT.put("h", code);
				voT.put("i", jsonobj.getJSONObject(code).getString("name"));
				voT.put("j", jsonobj.getJSONObject(code).getString("lat"));
				voT.put("k", jsonobj.getJSONObject(code).getString("lng"));
				voT.put("l", "");
				batchElement.put(voT);
				
				//以下丟天氣資料
				String rainStr = trs.get(j).getElementsByTag("td").get(10).text().replace("\u00a0", "");
				String sunnyStr = taipeiSunShine.getString(day+"_"+trs.get(j).getElementsByTag("td").get(0).text().replace("\u00a0", ""));
				Double rain = 0.0,
						sunny = 0.0;
				if(taipeiSunShine==null || "X".equals(rainStr) || "V".equals(rainStr) || "T".equals(rainStr) || "/".equals(rainStr) || "".equals(rainStr)){
					// X表故障 T表過小但是有 /表不明 V表不定
					continue;
				}
				
				if("".equals(sunnyStr)){
					sunny = 0.0;
				}else{
					sunny = Double.parseDouble(sunnyStr);
				}
				
				rain = Double.parseDouble(rainStr);
				
				String climate="不明";
				if(rain > 0.0){
					climate = "雨";
				}else if(sunny > 0.2){
					climate = "晴";
				}else{
					climate = "陰";
				}
				
				String climate_code = "";
				climate_code = climate=="晴" ? "1" : climate_code;
				climate_code = climate=="陰" ? "3" : climate_code;
				climate_code = climate=="雨" ? "4" : climate_code;
				climate_code = climate=="不明" ? "0" : climate_code;
				
				Map<String,String> voWx = new LinkedHashMap<String,String>();
	        	voWx.put("a", day);
	        	voWx.put("b", String.format("%02d",Long.parseLong(trs.get(j).getElementsByTag("td").get(0).text().replace("\u00a0", ""))));
	        	voWx.put("c", "Wx");
				voWx.put("d", climate);
				voWx.put("e", "");
				voWx.put("f", "天氣圖示代碼");
				voWx.put("g", climate_code);
				voWx.put("h", code);
				voWx.put("i", jsonobj.getJSONObject(code).getString("name"));
				voWx.put("j", jsonobj.getJSONObject(code).getString("lat"));
				voWx.put("k", jsonobj.getJSONObject(code).getString("lng"));
				voWx.put("l", "");
				batchElement.put(voWx);
	        }
    	} catch (IOException e) {
    		logger.debug("解析歷史天氣資料失敗: "+e.toString());
			e.printStackTrace();
			return false;
		}
		return true;
    }
    
    public static List<String> getDayUrl(Date d){
    	
    	List<String> urlList = new ArrayList<String>();
		String dayStr = new SimpleDateFormat("yyyy-MM-dd").format(d);
		JSONObject jsonObj = new JSONObject(observeStationJsonObj);
		
		for(Iterator<String> iter = jsonObj.keys(); iter.hasNext();) {
			String key = (String)iter.next();
			urlList.add(
				jsonObj.getJSONObject(key).getString("url")+dayStr
			);
		}
		
    	urlList.add(taipeiSunShineUrl+dayStr);
    	return urlList;
		
    }
    public static List<String> getAllUrl(String csv_from_time,String csv_till_time){
    	List<String> urlList = new ArrayList<String>();
		SimpleDateFormat my_format = new SimpleDateFormat("yyyy-MM-dd");
		try{
			Calendar from_time = Calendar.getInstance();
			from_time.setTime(my_format.parse(csv_from_time));
			Calendar till_time = Calendar.getInstance();
			till_time.setTime(my_format.parse(csv_till_time));
			
			urlList.addAll(getDayUrl(from_time.getTime()));
			while(from_time.compareTo(till_time)<0){
				from_time.add(Calendar.DATE, 1);
				urlList.addAll(getDayUrl(from_time.getTime()));
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return urlList ; 
    }
    
    
}