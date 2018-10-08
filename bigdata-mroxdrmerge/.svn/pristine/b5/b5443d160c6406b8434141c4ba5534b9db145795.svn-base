package cn.mastercom.bigdata.xdr.prepare.stat;

import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum XdrPrepareTablesEnum implements IOutPutPathEnum
{
	xdrLocation("location", "TB_LOCATION_01");

	private int index;
	private String fileName;
	private String dirName;

	private XdrPrepareTablesEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = MroBsTablesEnum.class.getName().hashCode() + ordinal();
	}

	@Override
	public String getPath(String hPath, String outData)
	{
		return getBasePath(hPath, outData) + "/" + dirName + "_" + outData;
	}
    
	@Override
	public String getHourPath(String hPath, String outData, String hour) 
	{
		if (hour == null)
			return getPath(hPath, outData);
		return getHourBasePath(hPath, outData, hour) + "/" + dirName + "_" + outData;
	}
	@Override
	public String getFileName()
	{
		return fileName;
	}

	@Override
	public String getDirName()
	{
		return dirName;
	}

	@Override
	public int getIndex()
	{
		return index;
	}
	
	public static String getBasePath(String hPath, String date)
	{
		return hPath + "/xdr_prepare/data_01_" + date;
	}
	
	public static String getHourBasePath(String hPath, String date, String hour)
	{
		if (hour == null)
			return getBasePath(hPath, date);
		return hPath + "/xdr_prepare/data_01_" + date + "/" + hour;
	}
}
