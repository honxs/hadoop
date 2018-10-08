package cn.mastercom.bigdata.mro.stat.tableEnum;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum MroCsOTTTableEnum implements IOutPutPathEnum
{
	mrosample("mrosample","TB_SIGNAL_SAMPLE_01"),
	mrocell("mrocell","TB_SIGNAL_CELL_01"),
	mrocellfreq("mrocellfreq","TB_FREQ_SIGNAL_CELL_01"),
	mrocellgrid("mrocellgrid","TB_SIGNAL_CELLGRID_01"),
	mrogrid("mrogrid","TB_SIGNAL_GRID_01"),
	griddt("griddt","TB_DTSIGNAL_GRID_01"),
	griddtfreq("griddtfreq","TB_FREQ_DTSIGNAL_GRID_01"),
	gridcqt("gridcqt","TB_CQTSIGNAL_GRID_01"),
	gridcqtfreq("gridcqtfreq","TB_FREQ_CQTSIGNAL_GRID_01"),
	sampledt("sampledt","TB_DTSIGNAL_SAMPLE_01"),
	sampledtex("sampledtex","TB_DTEXSIGNAL_SAMPLE_01"),
	samplecqt("samplecqt","TB_CQTSIGNAL_SAMPLE_01"),
	useractcell("useractcell","TB_SIG_USER_BEHAVIOR_LOC_MR_01"),
	tenmrogrid("tenmrogrid","TB_SIGNAL_GRID10_01"),
	tengriddt("tengriddt","TB_DTSIGNAL_GRID10_01"),
	tengriddtfreq("tengriddtfreq","TB_FREQ_DTSIGNAL_GRID10_01"),
	tengridcqt("tengridcqt","TB_CQTSIGNAL_GRID10_01"),
	tengridcqtfreq("tengridcqtfreq","TB_FREQ_CQTSIGNAL_GRID10_01"),
	tenmrocellgrid("tenmrocellgrid","TB_SIGNAL_CELLGRID10_01"),
	MrOutGrid("mrOutGrid","TB_MR_OUTGRID_01"),
	MrBuild("mrBuild","TB_MR_BUILD_01"),
	MrInGrid("mrInGrid","TB_MR_INGRID_01"),
	MrBuildCell("mrBuildCell","TB_MR_BUILD_CELL_01"),
	MrInGridCell("mrInGridCell","TB_MR_INGRID_CELL_01"),
	MrStatCell("mrStatCell","TB_STAT_MR_CELL_01"),
	MrOutGridCell("mrOutGridCell","TB_MR_OUTGRID_CELL_01"),
	MrOutGridCellNc("mrOutGridCellNc","TB_MR_OUTGRID_CELL_NC_01"),
	MrInGridCellNc("mrInGridCellNc","TB_MR_INGRID_CELL_NC_01"),
	MrBuildCellNc("mrBuildCellNc","TB_MR_BUILD_CELL_NC_01"),
	TopicCellIsolated("topicCellIsolated","TB_TOPIC_CELL_ISOLATED_01"),
	LTfreqcellByImei("LTfreqcellByImei","TB_FREQ_SIGNAL_LT_CELL_BYIMEI_01"),
	LTtenFreqByImeiDt("LTtenFreqByImeiDt","TB_FREQ_DTSIGNAL_LT_GRID10_BYIMEI_01"),
	LTtenFreqByImeiCqt("LTtenFreqByImeiCqt","TB_FREQ_CQTSIGNAL_LT_GRID10_BYIMEI_01"),
	DXfreqcellByImei("DXfreqcellByImei","TB_FREQ_SIGNAL_DX_CELL_BYIMEI_01"),
	DXtenFreqByImeiDt("DXtenFreqByImeiDt","TB_FREQ_DTSIGNAL_DX_GRID10_BYIMEI_01"),
	DXtenFreqByImeiCqt("DXtenFreqByImeiCqt","TB_FREQ_CQTSIGNAL_DX_GRID10_BYIMEI_01"),
	tencellgriddt("tencellgriddt","TB_DTSIGNAL_CELLGRID10_01"),
	tencellgridcqt("tencellgridcqt","TB_CQTSIGNAL_CELLGRID10_01"),
	mrvap("mrvap","TB_SIGNAL_MR_VAP_01"),
	mrloclib("loclib","TB_LOC_LIB_01"),
	xdrloclib("xdrloclib","XDR_LOC_LIB_01")
	;

	private String fileName;
	private String dirName;
	private int  index;

	private MroCsOTTTableEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index=MroCsOTTTableEnum.class.getName().hashCode()+ordinal();
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
		return hPath + "/mro_loc/data_01_" + date;
	}

	
	public static String getHourBasePath(String hPath, String date, String hour)
	{
		if (hour == null)
			return getBasePath(hPath, date);
		return hPath + "/mro_loc/data_01_" + date + "/" + hour;
	}
}
