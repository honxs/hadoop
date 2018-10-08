package cn.mastercom.bigdata.stat.userResident.enmus;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

/**
 * 常驻用户的另一个输出目录,为每一个输出目录创建一个单独的枚举类
 * 该枚举类里面生成的为配置表
 * @author xmr
 *
 */
public enum ResidentConfigTablesEnum implements IOutPutPathEnum
{   
	Merge_Resident_User("mergeResidentUser","tb_mr_user_location"),
	User_Resident_Location("newMergeResidentUser","tb_user_resident_location")
	;
	private int index;
	private String fileName;
	private String dirName;

	private ResidentConfigTablesEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = ResidentConfigTablesEnum.class.getName().hashCode() + ordinal();
	}

	@Override
	public String getPath(String hPath, String outData)
	{
		return getBasePath(hPath) + "/" + dirName;
	}
	
	@Override
	public String getHourPath(String hPath, String outData, String hour) 
	{
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
	
	public static String getBasePath(String hPath)
	{
		return hPath + "/resident_config";
	}
	
	public static String getHourBasePath(String hPath, String date, String hour)
	{
		return hPath + "/resident_config/data_01_" + date + "/" + hour;
	}
}
