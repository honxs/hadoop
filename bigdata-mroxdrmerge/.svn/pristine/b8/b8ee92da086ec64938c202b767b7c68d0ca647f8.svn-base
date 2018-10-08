package cn.mastercom.bigdata.StructData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataAdapterConf
{
	private Map<String, ParseItem> parseItemMap = null;

	public DataAdapterConf()
	{
		parseItemMap = new HashMap<String, ParseItem>();
	}

	public boolean init(String confPath) throws IOException
	{
		try
		{
			FileReader reader = new FileReader(confPath);
			BufferedReader br = new BufferedReader(reader);
			String str = "";
			while ((str = br.readLine()) != null)
			{
				str = str.trim();
				parseData(str);
			}
			br.close();
			reader.close();
		}
		catch (IOException e)
		{
			throw e;
		}
		
		return true;
	}
	
	private int curPos = 0;
	private ParseItem curParseItem = null;
	private void parseData(String parseStr)
	{
		if(curPos == 0 && parseStr.indexOf("#TYPENAME:") >= 0)
		{
			String typeName = parseStr.substring(parseStr.indexOf(":") + 1);
			curParseItem = parseItemMap.get(typeName);
			if(curParseItem == null)
			{
				curParseItem = new ParseItem(typeName);
				parseItemMap.put(typeName, curParseItem);
			}
			curPos = 1;
		}
		else if(parseStr.equals("#BEGIN"))
		{
			if(curPos == 1)
			{
				curPos = 2;
			}
		}
		else if(parseStr.equals("#END"))
		{
			if(curPos == 2)
			{
				curPos = 0;
			}	
		}
		else 
		{
			if(curPos == 2)
			{
				String[] strs = parseStr.split("=");
				curParseItem.addColum(strs[0].trim(), Integer.parseInt(strs[1].trim()));
			}
		}
		
	}
	
	
	public class ParseItem
	{
		private String parseType = "";
		private Map<String, Integer> columPosMap;
		
		public ParseItem(String parseType)
		{
			this.parseType = parseType;
			columPosMap = new HashMap<String, Integer>();
			
		}
		
		public String getParseType()
		{
			return parseType;
		}
		
		public Map<String, Integer> getColumPosMap()
		{
			return columPosMap;
		}
		
		public void addColum(String name, int pos)
		{
			if(pos < 0)
			{
				return;
			}
			columPosMap.put(name, pos);
		}
		
	}
	
	
	

}
