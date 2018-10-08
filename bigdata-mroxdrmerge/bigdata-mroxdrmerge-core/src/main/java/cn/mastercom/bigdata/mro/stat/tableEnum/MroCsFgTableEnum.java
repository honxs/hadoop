package cn.mastercom.bigdata.mro.stat.tableEnum;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum MroCsFgTableEnum implements IOutPutPathEnum
{
	fpmrosample("fpmrosample","TB_FGSIGNAL_SAMPLE_01"),
	fpmrocell("fpmrocell","TB_FGSIGNAL_CELL_01"),
	fpmrocellfreq("fpmrocellfreq","TB_FGFREQ_SIGNAL_CELL_01"),
	fpmrocellgrid("fpmrocellgrid","TB_FGSIGNAL_CELLGRID_01"),
	fpmrogrid("fpmrogrid","TB_FGSIGNAL_GRID_01"),
	fpmroMore("fpmroMore","MRO_FGMORE_01"),
	fpgriddt("fpgriddt","TB_FGDTSIGNAL_GRID_01"),
	fpgriddtfreq("fpgriddtfreq","TB_FGFREQ_DTSIGNAL_GRID_01"),
	fpgridcqt("fpgridcqt","TB_FGCQTSIGNAL_GRID_01"),
	fpgridcqtfreq("fpgridcqtfreq","TB_FGFREQ_CQTSIGNAL_GRID_01"),
	fpsampledt("fpsampledt","TB_FGDTSIGNAL_SAMPLE_01"),
	fpsampledtex("fpsampledtex","TB_FGDTEXSIGNAL_SAMPLE_01"),
	fpsamplecqt("fpsamplecqt","TB_FGCQTSIGNAL_SAMPLE_01"),
	fpuseractcell("fpuseracstcell","TB_FGSIG_USER_BEHAVIOR_LOC_MR_01"),
	fptenmrogrid("fptenmrogrid","TB_FGSIGNAL_GRID10_01"),
	fptengriddt("fptengriddt","TB_FGDTSIGNAL_GRID10_01"),
	fptengriddtfreq("fptengriddtfreq","TB_FGFREQ_DTSIGNAL_GRID10_01"),
	fptengridcqt("fptengridcqt","TB_FGCQTSIGNAL_GRID10_01"),
	fptengridcqtfreq("fptengridcqtfreq","TB_FGFREQ_CQTSIGNAL_GRID10_01"),
	fptenmrocellgrid("fptenmrocellgrid","TB_FGSIGNAL_CELLGRID10_01"),
	fpLTfreqcellByImei("fpLTfreqcellByImei","TB_FREQ_FGSIGNAL_LT_CELL_BYIMEI_01"),
	fpLTtenFreqByImeiDt("fpLTtenFreqByImeiDt","TB_FREQ_FGDTSIGNAL_LT_GRID10_BYIMEI_01"),
	fpLTtenFreqByImeiCqt("fpLTtenFreqByImeiCqt","TB_FREQ_FGCQTSIGNAL_LT_GRID10_BYIMEI_01"),
	fpDXfreqcellByImei("fpDXfreqcellByImei","TB_FREQ_FGSIGNAL_DX_CELL_BYIMEI_01"),
	fpDXtenFreqByImeiDt("fpDXtenFreqByImeiDt","TB_FREQ_FGDTSIGNAL_DX_GRID10_BYIMEI_01"),
	fpDXtenFreqByImeiCqt("fpDXtenFreqByImeiCqt","TB_FREQ_FGCQTSIGNAL_DX_GRID10_BYIMEI_01"),
	fptencellgriddt("fptencellgriddt","TB_FGDTSIGNAL_CELLGRID10_01"),
	fptencellgridcqt("fptencellgridcqt","TB_FGCQTSIGNAL_CELLGRID10_01"),
	indoorErr("indoorErr","tb_mr_cell_err_yd_dd"),
	AOATA_PATH("aoata","tb_aoata_sample_dd")
	;

	private int index;
	private String fileName;
	private String dirName;

	private MroCsFgTableEnum(String fileName, String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index=MroCsFgTableEnum.class.getName().hashCode()+ordinal();
	}

	@Override
	public String getPath(String hPath, String outData)
	{
		return getBasePath(hPath, outData) + "/" + dirName + "_" + outData;
	}
	
	@Override
	public String getHourPath(String hPath, String outData, String hour) 
	{	if (hour == null)
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
