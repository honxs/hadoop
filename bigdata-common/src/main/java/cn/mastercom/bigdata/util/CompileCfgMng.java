package cn.mastercom.bigdata.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

//import com.thoughtworks.xstream.XStream;
//import com.thoughtworks.xstream.io.xml.DomDriver;

public class CompileCfgMng
{
	private Map<String, CompileHelper> mng;

	private static CompileCfgMng instance;
	public static CompileCfgMng getInstance()
	{
		if (instance == null)
		{
			instance = new CompileCfgMng();
		}
		return instance;
	}

	private CompileCfgMng()
	{
		mng = new HashMap<String, CompileHelper>();
	}

//	public void init(String path)
//	{
//		String xmlData = "";
//		StringBuffer sb = new StringBuffer();
//		String tmStr = "";
//
//		FileReader reader = null;
//		BufferedReader br = null;
//
//		try
//		{
//			reader = new FileReader(path);
//			br = new BufferedReader(reader);
//
//			while ((tmStr = br.readLine()) != null)
//			{
//				sb.append(tmStr + "/n");
//			}
//
//			xmlData = sb.toString();
//
//			br.close();
//			reader.close();
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		finally
//		{
//
//		}
//
//        XStream xStream = new XStream(new DomDriver());  
//        xStream.autodetectAnnotations(true); 
//		xStream.alias("complie list", CompileList.class);
//
//		CompileList compileList = (CompileList) xStream.fromXML(xmlData);
//		for (CompileInfo compileInfo : compileList.compileList)
//		{
//			CompileHelper ch = new CompileHelper();
//			ch.initFormatMark(compileList.compile_static_mark);
//			ch.initMark(compileInfo.compile_mark);
//			mng.put(compileInfo.name, ch);
//		}
//	}
	
	public void init_txt(String path)
	{
		String tmStr = "";
		String format_mark = "";
		String compile_mark = "";

		FileReader reader = null;
		BufferedReader br = null;

		try
		{
			reader = new FileReader(path);
			br = new BufferedReader(reader);

			while ((tmStr = br.readLine()) != null)
			{
				if(tmStr.indexOf("##") >= 0)
				{
					continue;
				}
				else if(tmStr.indexOf("#FROMAT_MARK:") >= 0)
				{
					format_mark = tmStr.substring(tmStr.indexOf("#FROMAT_MARK:") + "#FROMAT_MARK:".length());
				}
				else if(tmStr.indexOf("#COMPILE_MARK:") >= 0)
				{
					compile_mark = tmStr.substring(tmStr.indexOf("#COMPILE_MARK:") + "#COMPILE_MARK:".length());
				}
				
			}
			
			CompileHelper ch = new CompileHelper();
			ch.initFormatMark(format_mark);
			ch.initMark(compile_mark);
			mng.put("default", ch);

			br.close();
			reader.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{

		}
	}
	
	public boolean init(InputStream inputStream) throws IOException
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));  
		String tmStr = "";
		String format_mark = "";
		String compile_mark = "";
		
		while ((tmStr = br.readLine()) != null)
		{
			if(tmStr.indexOf("#FROMAT_MARK:") >= 0)
			{
				format_mark = tmStr.substring(tmStr.indexOf("#FROMAT_MARK:") + "#FROMAT_MARK:".length());
			}
			else if(tmStr.indexOf("#COMPILE_MARK:") >= 0)
			{
				compile_mark = tmStr.substring(tmStr.indexOf("#COMPILE_MARK:") + "#COMPILE_MARK:".length());
			}
		}
		CompileHelper ch = new CompileHelper();
		ch.initFormatMark(format_mark);
		ch.initMark(compile_mark);
		mng.put("default", ch);
		
		return true;
	}
	

//	public void saveXml(String path, CompileList value)
//	{
//        XStream xStream = new XStream(new DomDriver());  
//        xStream.autodetectAnnotations(true); 
//		xStream.alias("complie list", CompileList.class);	
//		
//		String xml = xStream.toXML(value);
//		if(xml.length() == 0)
//		{
//			return;
//		}
//		
//		try
//		{
//			File file = new File(path);
//			if (file.exists())
//			{
//				file.delete();
//			}
//			file.createNewFile();
//
//			FileWriter fileWritter = new FileWriter(file.getPath());
//			fileWritter.write(xml);
//			fileWritter.flush();
//			fileWritter.close();
//		}
//		catch (IOException e)
//		{
//			e.printStackTrace();
//		}
//	}
//	
	public CompileHelper getCompile(String name)
	{
		return mng.get(name);
	}
	


}
