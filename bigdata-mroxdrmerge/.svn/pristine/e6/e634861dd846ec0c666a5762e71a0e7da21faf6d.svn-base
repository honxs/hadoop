package cn.mastercom.bigdata.standalone.local;

import java.io.File;

import org.dom4j.Document;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;


public class ConfigHelper
{

	private Document m_doc;

	public ConfigHelper(String cfgFile)
	{
		File file = new File(cfgFile);
		try
		{
			if (file.exists())
			{
				SAXReader reader = new SAXReader();
				m_doc = reader.read(file);// 读取XML文件
			}
		}
		catch (Exception e)
		{
			m_doc = null;
		}
	}

	/*
	 * key样式: //root/item
	 */
	public String readAsString(String key)
	{
		if (m_doc == null)
			return null;
		try
		{
			Node node = m_doc.selectSingleNode(key);
			if (node == null)
				return null;
			return node.getText();
		}
		catch (Exception e)
		{
			return null;
		}
	}

	/*
	 * key样式: //root/item
	 */
	public Integer readAsInteger(String key, Integer defaultValue)
	{
		String str = readAsString(key);
		if (str == null)
			return defaultValue;
		try
		{
			return Integer.parseInt(str);
		}
		catch (Exception e)
		{
			return defaultValue;
		}
	}
}
