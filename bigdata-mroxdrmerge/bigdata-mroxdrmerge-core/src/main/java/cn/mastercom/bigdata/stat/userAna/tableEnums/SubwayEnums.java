package cn.mastercom.bigdata.stat.userAna.tableEnums;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum SubwayEnums implements IOutPutPathEnum
{

	SUBWAY_TMP_POTENTIAL_USER("tbSubwayTmpPotentialUser","subway_data_potential_user_dd_"),
	SUBWAY_TRAIN_INFO("tbSubwaySection","tb_subway_data_traininfo_yd_dd_"),
	SUBWAY_LOCATION_POINT("tbSubwayLocPoint","tb_subway_data_locpoint_dd_"),
	SUBWAY_IMSI("tbSubwayImsi","tb_subway_data_imsi_dd_"),
	SUBWAY_NOVER_TIME("tbSubwayNoCoverTime","tb_subway_data_nocover_timespan_dd_")
	;
	
	private int index;
	private String fileName;
	private String dirName;

	private SubwayEnums(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = SubwayEnums.class.getName().hashCode() + ordinal();
	}

	@Override
	public String getPath(String sPath, String outData)
	{
		return getBasePath(sPath, outData) + "/" + dirName + outData;
	}
	
	@Override
	public String getHourPath(String hPath, String outData, String hour) 
	{
		return getHourBasePath(hPath, outData, hour) + "/" + dirName + "_" + outData;
	}

	/**
	 * 返回输入路径
	 * 
	 * @param path
	 * @return
	 */
	public String getPath(String path)
	{
		return path;
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
	
	public static String getBasePath(String sPath, String outData)
	{
		return sPath + "/subway_user_ana/data_01_" + outData;
	}

	public static String getHourBasePath(String hPath, String date, String hour)
	{
		return hPath + "/subway_user_ana/data_01_" + date + "/" + hour;
	}
}
