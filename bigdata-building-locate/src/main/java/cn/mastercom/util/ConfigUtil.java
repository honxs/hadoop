package cn.mastercom.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {

	public static  Properties readPath(String configName) throws FileNotFoundException, IOException{

		InputStream in = null;
		in = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(configName);
		if(in == null){
			in = ConfigUtil.class.getClassLoader().getResourceAsStream(configName);
		}
		if(in == null){
			in = ConfigUtil.class.getResourceAsStream(configName);
		}
	/*	if(in == null){
			in = new PathMatchingResourcePatternResolver().getResource("classpath*:"+configName).getInputStream();
		}*/
		Properties properties = new Properties();
		try{

			properties.load(in);
		}catch (IOException o){
			o.printStackTrace();
		}finally {
			in.close();
		}
		return properties;
	}

}
