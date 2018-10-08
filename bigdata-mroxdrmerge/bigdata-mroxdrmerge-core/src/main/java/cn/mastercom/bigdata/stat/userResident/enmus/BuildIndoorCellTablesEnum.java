package cn.mastercom.bigdata.stat.userResident.enmus;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

/**
 * 常驻用户的另一个输出目录,为每一个输出目录创建一个单独的枚举类
 * 该枚举类里面生成的为配置表
 * @author xmr
 *
 */
public enum BuildIndoorCellTablesEnum implements IOutPutPathEnum
{   
	Build_Indoor_Cell("buildIndoorCell", "tb_cfg_building_indoor_cell"),
	Build_pos_Indoor_Cell("buildPosIndoorCell", "tb_cfg_building_pos_indoor_cell"),
	User_Resident_Indoor("buildUserResidentIndoor", "tb_user_resident_indoor")
	;
	private int index;
	private String fileName;
	private String dirName;

	private BuildIndoorCellTablesEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = BuildIndoorCellTablesEnum.class.getName().hashCode() + ordinal();
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
