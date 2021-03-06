package CSI;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;

import CSI.GetWeatherHistoryData.ConfigForUpdate;

public class CommonMethodForUpdate {
	private static final Logger logger = LogManager.getLogger(CommonMethodForUpdate.class);
	public static ConfigForUpdate getConfigs(String projectPath){
		ConfigForUpdate myConfig = new ConfigForUpdate();
		try{
			String config_path = projectPath+"/config.properties";
			
			if(!(new File(config_path).exists())){
				logger.info("設定檔: "+config_path+" 不存在");
				return myConfig;
			}
			BufferedReader reader = new BufferedReader(new FileReader(config_path));
			reader.readLine();
			String line = null; 
			while((line=reader.readLine())!=null){
				String item[] = line.split("=");
				
				if(item.length == 2 && (!"#".equals(line.substring(0,1)))){
					myConfig.tmpDIR = item[0].trim().equals("tmpDIR") ? item[1].trim() : myConfig.tmpDIR ;
					myConfig.dbURLbatch = item[0].trim().equals("dbURL") ? item[1].trim() : myConfig.dbURLbatch + "?useUnicode=true&characterEncoding=utf-8&useSSL=false&useServerPrepStmts=false&rewriteBatchedStatements=true" ;
					myConfig.dbUserName = item[0].trim().equals("dbUserName") ? item[1].trim() : myConfig.dbUserName ;
					myConfig.dbPassword = item[0].trim().equals("dbPassword") ? item[1].trim() : myConfig.dbPassword ;
					myConfig.deleteOrNot = item[0].trim().equals("deleteOrNot") ? item[1].trim() : myConfig.deleteOrNot ;
				}
			}
			reader.close();
		}catch(Exception e){
			logger.debug("設定檔讀取異常");
		}
		return myConfig;
	}
    
    public static boolean batchCMD(String sp,JSONArray requestElements,ConfigForUpdate my_config) throws Exception{
		int batchCount = 0;
		Connection conn;
		PreparedStatement psts;
		try{
	        Class.forName("com.mysql.jdbc.Driver");  
	        conn = (Connection) DriverManager.getConnection(my_config.dbURLbatch, my_config.dbUserName,my_config.dbPassword);  
	        conn.setAutoCommit(false);
	        psts = conn.prepareStatement(sp);  
	        Date begin=new Date();
	        for( int i=0 ; requestElements!=null && i<requestElements.length() ; i++ ){
	        	JSONObject element = requestElements.getJSONObject(i);
	        	int j=0;
				for (Iterator<String> iter = element.keys(); iter.hasNext();) {
			        String key = (String)iter.next();
			        psts.setString(++j, element.getString(key));
				}
				psts.addBatch();
	            if((++batchCount)%(11*10000)==0){
	            	
	            	logger.info("batch資料庫處理資料量達 "+batchCount+" 筆");
	            	psts.executeBatch();
		        	conn.commit();
		        	psts.clearBatch();
		        	
		        }
			}
	        psts.executeBatch();
	        conn.commit();
	        Date end=new Date();

	        logger.info("共處理 "+batchCount+" 筆資料，耗時: "+(end.getTime()-begin.getTime())+" ms");
	        conn.close();  
	        return true;
		}catch(Exception e){
			logger.debug("資料庫連線異常，檢查資料庫 sp: "+my_config.needSP+" table: "+my_config.needTB);
			throw new Exception("批次MysqlCMD失敗: "+e.toString());
		}
    }
    
    public static String JSONStringify(Object object){
		Gson gson = new Gson();
		return  gson.toJson(object);
	}
    
    public static boolean isValidURL(String urlStr) {
		try {
			InputStream in = new URL(urlStr).openStream();
			in.close();
		} catch (Exception e) {
			try {
				java.util.concurrent.TimeUnit.SECONDS.sleep(1);
				InputStream in = new URL(urlStr).openStream();
				in.close();
			} catch (Exception e1) {
				try {
					java.util.concurrent.TimeUnit.SECONDS.sleep(1);
					InputStream in = new URL(urlStr).openStream();
					in.close();
				} catch (Exception e2) {
					logger.info("嘗試連線三次失敗: " + e.toString());
					e.printStackTrace();
					return false;
				}
			}
		}
		return true;
	}
   
	public static boolean downlodFileSuccess(String downloadStr, String downloadPosition) {
		
		try {
			FileUtils.copyURLToFile(new URL(downloadStr), new File(downloadPosition));
		} catch (Exception e) {
			try {
				java.util.concurrent.TimeUnit.SECONDS.sleep(1);
				FileUtils.copyURLToFile(new URL(downloadStr), new File(downloadPosition));
			} catch (Exception e1) {
				try {
					java.util.concurrent.TimeUnit.SECONDS.sleep(1);
					FileUtils.copyURLToFile(new URL(downloadStr), new File(downloadPosition));
				} catch (Exception e2) {
					logger.info("嘗試三次下載檔案依然無效: " + e.toString());
					e.printStackTrace();
					return false;
				}
			}
		}
		return true;
	}
	
	public static boolean deleteFolder(String folder) {
		
		File file = new File(folder);
		if (!file.exists()) {
			return true;
		}
		if (!file.isDirectory()) {
			return true;
		}
		String[] tempList = file.list();
		File temp = null;
		for (int i = 0; i < tempList.length; i++) {
			if (folder.endsWith(File.separator)) {
				temp = new File(folder + tempList[i]);
			} else {
				temp = new File(folder + File.separator + tempList[i]);
			}
			if (temp.isFile()) {
				temp.delete();
			}
			if (temp.isDirectory()) {
				deleteFolder(folder + "/" + tempList[i]);
				delFolder(folder + "/" + tempList[i]);
			}
		}
		return true;
		
	}

	public static void delFolder(String folderPath) {
		
		try {
			deleteFolder(folderPath);
			String filePath = folderPath;
			filePath = filePath.toString();
			java.io.File myFilePath = new java.io.File(filePath);
			myFilePath.delete();
		} catch (Exception e) {
			logger.debug("清空資料夾操作出錯 ");
			e.printStackTrace();
		}
		
	}
	
}
