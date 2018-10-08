package cn.mastercom.bigdata.stat.userResident.enmus;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

/**
 * 常驻用户的另一个输出目录,为每一个输出目录创建一个单独的枚举类
 * 该枚举类里面生成的为配置表
 * @author xmr
 *
 */
public enum ResidentUserTablesEnum implements IOutPutPathEnum
{   
	Merge_Resident_User("MergeResidentUser","tb_mr_user_location_dd"),
	Merge_Resident_User_UnEncrypt("MergeResidentUserUnEncrypt","tb_mr_user_location_unencrypt_dd"),
	User_Resident_Location("UserResidentLocation","tb_user_resident_location_dd"),
	User_Resident_Location_UnEncrypt("UserResidentLocationUnEncrypt","tb_user_resident_location_unencrypt_dd"),
	Build_Indoor_Cell("cfgBuildIndoorCell", "tb_cfg_building_indoor_cell_dd"),
	Build_pos_Indoor_Cell("cfgBuildPosIndoorCell", "tb_cfg_building_pos_indoor_cell_dd"),
	User_Resident_Indoor("UserResidentIndoor", "tb_user_resident_indoor_dd")
	;
	private int index;
	private String fileName;
	private String dirName;

	private ResidentUserTablesEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = ResidentUserTablesEnum.class.getName().hashCode() + ordinal();
	}

	@Override
	public String getPath(String hPath, String outData)
	{
		return getBasePath(hPath, outData) + "/" + dirName + "_" + outData;
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
	
	public static String getBasePath(String hPath, String date)
	{
		return hPath + "/resident_user/data_01_" + date;
	}
	
	public static String getHourBasePath(String hPath, String date, String hour)
	{
		return hPath + "/resident_user/data_01_" + date + "/" + hour;
	}
}
