package cn.mastercom.bigdata.util.hadoop.mapred;



public class MultiTypeItem
{
	public int dataType;
	public String typeName;
	public String fileName;
	public String basePath;
	
	public MultiTypeItem()
	{
		dataType = -1;
		typeName = "";
		fileName = "";
		basePath = "";
	}
	
	public String getPathName()
	{
		return basePath + "/" + typeName;
	}
}
