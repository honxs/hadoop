package cn.mastercom.bigdata.stat.userAna.tableEnums;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum HsrEnums implements IOutPutPathEnum
{
	HSR_TMP_POTENTIAL_USER("tbHsrTmpPotentialUser","hsr_data_potential_user_dd_"),
	HSR_TRAIN_INFO("tbHsrSection","tb_hsr_data_traininfo_yd_dd_"),
	HSR_LOCATION_POINT("tbHsrLocPoint","tb_hsr_data_locpoint_dd_"),
	HSR_IMSI("tbHsrImsi","tb_hsr_data_imsi_dd_"),
	HSR_NOVER_TIME("tbHsrNoCoverTime","tb_hsr_data_nocover_timespan_dd_"),
	HSR_SEG_INFO("tbHsrSegInfo","tb_hsr_data_seginfo_dd_"),
	HSR_SEG_NOCOVER("tbHsrSegNoCover","tb_hsr_data_seg_nocover_dd_"),
	HSR_XDR_INFO("tbHsrXdr","tb_hsr_data_xdrinfo_dd_"),
	HSR_USER_AREA("tbHsrUserArea","tb_hsr_data_user_area_dd_"),
	//
	//test
	HSR_XDR("tbHsrXdr", "tb_hsr_xdr_");
	
	private int index;
	private String fileName;
	private String dirName;

	private HsrEnums(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = HsrEnums.class.getName().hashCode() + ordinal();
	}

	@Override
	public String getPath(String hPath, String outDate)
	{
		return getBasePath(hPath, outDate) + "/" + dirName + outDate;
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

	public static String getBasePath(String hPath, String date)
	{
		return hPath + "/hsr_user_ana/data_01_" + date;
	}

	public static String getHourBasePath(String hPath, String date, String hour)
	{
		return hPath + "/hsr_user_ana/data_01_" + date + "/" + hour;
	}

}
