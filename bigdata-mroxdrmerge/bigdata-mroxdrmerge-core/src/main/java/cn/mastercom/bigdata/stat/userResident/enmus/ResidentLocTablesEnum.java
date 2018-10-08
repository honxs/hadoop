package cn.mastercom.bigdata.stat.userResident.enmus;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

/**
 * @TODO 因为常驻用户的表格单独占据了一个输出目录
 * 因此,为了代码的扩展性和可维护性,单独
 * 将常驻用户的表格整理出一个枚举类
 * @author xmr
 *
 */
public enum ResidentLocTablesEnum implements IOutPutPathEnum
{
	xdrcellhourTime("userLocationTemp","tb_mr_user_location_temp_dd"), //后缀有temp的
	resident_user("mrUserLocation","tb_mr_user_location_dd"),
	xdrcellhourTime_gsm("userLocationGsmTemp","tb_mr_user_location_gsm_temp_dd"), //后缀有temp的
	xdrcellhourTime_td("userLocationTdTemp","tb_mr_user_location_td_temp_dd"), //后缀有temp的
	;
	
	private int index;
	private String fileName;
	private String dirName;

	private ResidentLocTablesEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = ResidentLocTablesEnum.class.getName().hashCode() + ordinal();
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
		return hPath + "/resident_loc/data_01_" + date;
	}
	
	public static String getHourBasePath(String hPath, String date, String hour)
	{
		return hPath + "/resident_loc/data_01_" + date + "/" + hour;
	}

}
