package cn.mastercom.bigdata.mdt.stat;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
/**
 * 
 * @author xmr
 *
 */
public enum MdtTablesEnum implements IOutPutPathEnum
{
	
	mdtimm_insample_high("mdtimmInsampleHigh","tb_mdtimm_insample_high_dd"),
	mdtimm_insample_mid("mdtimmInsampleMid","tb_mdtimm_insample_mid_dd"),
	mdtimm_insample_low("mdtimmInsampleLow","tb_mdtimm_insample_low_dd"),
	mdtimm_outsample_high("mdtimmOutsampleHigh","tb_mdtimm_outsample_high_dd"),
	mdtimm_outsample_mid("mdtimmOutsampleMid","tb_mdtimm_outsample_mid_dd"),
	mdtimm_outsample_low("mdtimmOutsampleLow","tb_mdtimm_outsample_low_dd"),
	mdtimm_noLocSample("mdtimmNoLocSample","tb_mdtimm_noLocSample_dd"),
	mdtimm_outgrid_yd_high("mdtimmOutgridYdHigh","tb_mdtimm_outgrid_high_yd_dd"),
	mdtimm_outgrid_yd_mid("mdtimmOutgridYdMid","tb_mdtimm_outgrid_mid_yd_dd"),
	mdtimm_outgrid_yd_low("mdtimmOutgridYdLow","tb_mdtimm_outgrid_low_yd_dd"),
	mdtimm_outgrid_cell_high("mdtimmOutgridCellHigh","tb_mdtimm_outgrid_cell_high_dd"),
	mdtimm_outgrid_cell_mid("mdtimmOutgridCellMid","tb_mdtimm_outgrid_cell_mid_dd"),
	mdtimm_outgrid_cell_low("mdtimmOutgridCellLow","tb_mdtimm_outgrid_cell_low_dd"),
	mdtimm_ingrid_yd_high("mdtimmIngridYdHigh","tb_mdtimm_ingrid_high_yd_dd"),
	mdtimm_ingrid_yd_mid("mdtimmIngridYdMid","tb_mdtimm_ingrid_mid_yd_dd"),
	mdtimm_ingrid_yd_low("mdtimmIngridYdLow","tb_mdtimm_ingrid_low_yd_dd"),
	mdtimm_ingrid_cell_high("mdtimmIngridCellHigh","tb_mdtimm_ingrid_cell_high_dd"),
	mdtimm_ingrid_cell_mid("mdtimmIngridCellMid","tb_mdtimm_ingrid_cell_mid_dd"),
	mdtimm_ingrid_cell_low("mdtimmIngridCellLow","tb_mdtimm_ingrid_cell_low_dd"),
	mdtimm_building_yd_high("mdtimmBuildingYdHigh","tb_mdtimm_building_high_yd_dd"),
	mdtimm_building_yd_mid("mdtimmBuildingYdMid","tb_mdtimm_building_mid_yd_dd"),
	mdtimm_building_yd_low("mdtimmBuildingYdLow","tb_mdtimm_building_low_yd_dd"),
	mdtimm_building_cell_high("mdtimmBuildingCellHigh","tb_mdtimm_building_cell_high_dd"),
	mdtimm_building_cell_mid("mdtimmBuildingCellMid","tb_mdtimm_building_cell_mid_dd"),
	mdtimm_building_cell_low("mdtimmBuildingCellLow","tb_mdtimm_building_cell_low_dd"),
	mdtimm_cell_yd("mdtimmCellYd","tb_mdtimm_cell_yd_dd"),
	mdtimm_tac("mdtimmTac","tb_mdtimm_imei_dd"),
	mdtimm_area_sample_yd("mdtimmAreaSampleYd","tb_mdtimm_area_sample_yd_dd"),
	mdtimm_area_outgrid_yd("mdtimmAreaOutgridYd","tb_mdtimm_area_outgrid_yd_dd"),
	mdtimm_area_outgrid_cell("mdtimmAreaOutgridCell","tb_mdtimm_area_outgrid_cell_dd"),
	mdtimm_area_cell_yd("mdtimmAreaCellYd","tb_mdtimm_area_cell_yd_dd"),
	mdtimm_area_yd("mdtimmAreaYd","tb_mdtimm_area_yd_dd"),
	mdtimm_outgrid_fullnet_high_ydlt("mdtimmOutgridFullnetHighYdlt","tb_mdtimm_outgrid_fullnet_high_ydlt_dd"),
	mdtimm_outgrid_fullnet_mid_ydlt("mdtimmOutgridFullnetMidYdlt","tb_mdtimm_outgrid_fullnet_mid_ydlt_dd"),
	mdtimm_outgrid_fullnet_low_ydlt("mdtimmOutgridFullnetLowYdlt","tb_mdtimm_outgrid_fullnet_low_ydlt_dd"),
	mdtimm_outgrid_fullnet_high_yddx("mdtimmOutgridFullnetHighYddx","tb_mdtimm_outgrid_fullnet_high_yddx_dd"),
	mdtimm_outgrid_fullnet_mid_yddx("mdtimmOutgridFullnetMidYddx","tb_mdtimm_outgrid_fullnet_mid_yddx_dd"),
	mdtimm_outgrid_fullnet_low_yddx("mdtimmOutgridFullnetLowYddx","tb_mdtimm_outgrid_fullnet_low_yddx_dd"),
	mdtimm_ingrid_fullnet_high_ydlt("mdtimmIngridFullnetHighYdlt","tb_mdtimm_ingrid_fullnet_high_ydlt_dd"),
	mdtimm_ingrid_fullnet_mid_ydlt("mdtimmIngridFullnetMidYdlt","tb_mdtimm_ingrid_fullnet_mid_ydlt_dd"),
	mdtimm_ingrid_fullnet_low_ydlt("mdtimmIngridFullnetLowYdlt","tb_mdtimm_ingrid_fullnet_low_ydlt_dd"),
	mdtimm_ingrid_fullnet_high_yddx("mdtimmIngridFullnetHighYddx","tb_mdtimm_ingrid_fullnet_high_yddx_dd"),
	mdtimm_ingrid_fullnet_mid_yddx("mdtimmIngridFullnetMidYddx","tb_mdtimm_ingrid_fullnet_mid_yddx_dd"),
	mdtimm_ingrid_fullnet_low_yddx("mdtimmIngridFullnetLowYddx","tb_mdtimm_ingrid_fullnet_low_yddx_dd"),
	mdtimm_building_fullnet_high_ydlt("mdtimmBuildingFullnetHighYdlt","tb_mdtimm_building_fullnet_high_ydlt_dd"),
	mdtimm_building_fullnet_mid_ydlt("mdtimmBuildingFullnetMidYdlt","tb_mdtimm_building_fullnet_mid_ydlt_dd"),
	mdtimm_building_fullnet_low_ydlt("mdtimmBuildingFullnetLowYdlt","tb_mdtimm_building_fullnet_low_ydlt_dd"),
	mdtimm_building_fullnet_high_yddx("mdtimmBuildingFullnetHighYddx","tb_mdtimm_building_fullnet_high_yddx_dd"),
	mdtimm_building_fullnet_mid_yddx("mdtimmBuildingFullnetMidYddx","tb_mdtimm_building_fullnet_mid_yddx_dd"),
	mdtimm_building_fullnet_low_yddx("mdtimmBuildingFullnetLowYddx","tb_mdtimm_building_fullnet_low_yddx_dd"),
	mdtimm_cell_fullnet_ydlt("mdtimmCellFullnetYdlt","tb_mdtimm_cell_fullnet_ydlt_dd"),
	mdtimm_cell_fullnet_yddx("mdtimmCellFullnetYddx","tb_mdtimm_cell_fullnet_yddx_dd"),
	mdtimm_area_outgrid_fullnet_ydlt("mdtimmAreaOutgridFullnetYdlt","tb_mdtimm_area_outgrid_fullnet_ydlt_dd"),
	mdtimm_area_outgrid_fullnet_yddx("mdtimmAreaOutgridFullnetYddx","tb_mdtimm_area_outgrid_fullnet_yddx_dd"),
	mdtimm_area_cell_fullnet_ydlt("mdtimmAreaCellFullnetYdlt","tb_mdtimm_area_cell_fullnet_ydlt_dd"),
	mdtimm_area_cell_fullnet_yddx("mdtimmAreaCellFullnetYddx","tb_mdtimm_area_cell_fullnet_yddx_dd"),
	mdtimm_area_fullnet_ydlt("mdtimmAreaFullnetYdlt","tb_mdtimm_area_fullnet_ydlt_dd"),
	mdtimm_area_fullnet_yddx("mdtimmAreaFullnetYddx","tb_mdtimm_area_fullnet_yddx_dd"),
	mdtimm_outgrid_dx_high("mdtimmOutgridDxHigh","tb_mdtimm_outgrid_high_dx_dd"),
	mdtimm_outgrid_dx_mid("mdtimmOutgridDxMid","tb_mdtimm_outgrid_mid_dx_dd"),
	mdtimm_outgrid_dx_low("mdtimmOutgridDxLow","tb_mdtimm_outgrid_low_dx_dd"),
	mdtimm_ingrid_dx_high("mdtimmIngridDxHigh","tb_mdtimm_ingrid_high_dx_dd"),
	mdtimm_ingrid_dx_mid("mdtimmIngridDxMid","tb_mdtimm_ingrid_mid_dx_dd"),
	mdtimm_ingrid_dx_low("mdtimmIngridDxLow","tb_mdtimm_ingrid_low_dx_dd"),
	mdtimm_building_dx_high("mdtimmBuildingDxHigh","tb_mdtimm_building_high_dx_dd"),
	mdtimm_building_dx_mid("mdtimmBuildingDxMid","tb_mdtimm_building_mid_dx_dd"),
	mdtimm_building_dx_low("mdtimmBuildingDxLow","tb_mdtimm_building_low_dx_dd"),
	mdtimm_cell_dx("mdtimmCellDx","tb_mdtimm_cell_dx_dd"),
	mdtimm_area_outgrid_dx("mdtimmAreaOutgridDx","tb_mdtimm_area_outgrid_dx_dd"),
	mdtimm_area_cell_dx("mdtimmAreaCellDx","tb_mdtimm_area_cell_dx_dd"),
	mdtimm_area_dx("mdtimmAreaDx","tb_mdtimm_area_dx_dd"),
	mdtimm_outgrid_lt_high("mdtimmOutgridLtHigh","tb_mdtimm_outgrid_high_lt_dd"),
	mdtimm_outgrid_lt_mid("mdtimmOutgridLtMid","tb_mdtimm_outgrid_mid_lt_dd"),
	mdtimm_outgrid_lt_low("mdtimmOutgridLtLow","tb_mdtimm_outgrid_low_lt_dd"),
	mdtimm_ingrid_lt_high("mdtimmIngridLtHigh","tb_mdtimm_ingrid_high_lt_dd"),
	mdtimm_ingrid_lt_mid("mdtimmIngridLtMid","tb_mdtimm_ingrid_mid_lt_dd"),
	mdtimm_ingrid_lt_low("mdtimmIngridLtLow","tb_mdtimm_ingrid_low_lt_dd"),
	mdtimm_building_lt_high("mdtimmBuildingLtHigh","tb_mdtimm_building_high_lt_dd"),
	mdtimm_building_lt_mid("mdtimmBuildingLtMid","tb_mdtimm_building_mid_lt_dd"),
	mdtimm_building_lt_low("mdtimmBuildingLtLow","tb_mdtimm_building_low_lt_dd"),
	mdtimm_cell_lt("mdtimmCellLt","tb_mdtimm_cell_lt_dd"),
	mdtimm_area_outgrid_lt("mdtimmAreaOutgridLt","tb_mdtimm_area_outgrid_lt_dd"),
	mdtimm_area_cell_lt("mdtimmAreaCellLt","tb_mdtimm_area_cell_lt_dd"),
	mdtimm_area_lt("mdtimmAreaLt","tb_mdtimm_area_lt_dd"),
	mdtlog_insample_high("mdtlogInsampleHigh","tb_mdtlog_insample_high_dd"),
	mdtlog_insample_mid("mdtlogInsampleMid","tb_mdtlog_insample_mid_dd"),
	mdtlog_insample_low("mdtlogInsampleLow","tb_mdtlog_insample_low_dd"),
	mdtlog_outsample_high("mdtlogOutsampleHigh","tb_mdtlog_outsample_high_dd"),
	mdtlog_outsample_mid("mdtlogOutsampleMid","tb_mdtlog_outsample_mid_dd"),
	mdtlog_outsample_low("mdtlogOutsampleLow","tb_mdtlog_outsample_low_dd"),
	mdtlog_noLocSample("mdtlogNoLocSample","tb_mdtlog_noLocSample_dd"),
	mdtlog_outgrid_yd_high("mdtlogOutgridYdHigh","tb_mdtlog_outgrid_high_yd_dd"),
	mdtlog_outgrid_yd_mid("mdtlogOutgridYdMid","tb_mdtlog_outgrid_mid_yd_dd"),
	mdtlog_outgrid_yd_low("mdtlogOutgridYdLow","tb_mdtlog_outgrid_low_yd_dd"),
	mdtlog_outgrid_cell_high("mdtlogOutgridCellHigh","tb_mdtlog_outgrid_cell_high_dd"),
	mdtlog_outgrid_cell_mid("mdtlogOutgridCellMid","tb_mdtlog_outgrid_cell_mid_dd"),
	mdtlog_outgrid_cell_low("mdtlogOutgridCellLow","tb_mdtlog_outgrid_cell_low_dd"),
	mdtlog_ingrid_yd_high("mdtlogIngridYdHigh","tb_mdtlog_ingrid_high_yd_dd"),
	mdtlog_ingrid_yd_mid("mdtlogIngridYdMid","tb_mdtlog_ingrid_mid_yd_dd"),
	mdtlog_ingrid_yd_low("mdtlogIngridYdLow","tb_mdtlog_ingrid_low_yd_dd"),
	mdtlog_ingrid_cell_high("mdtlogIngridCellHigh","tb_mdtlog_ingrid_cell_high_dd"),
	mdtlog_ingrid_cell_mid("mdtlogIngridCellMid","tb_mdtlog_ingrid_cell_mid_dd"),
	mdtlog_ingrid_cell_low("mdtlogIngridCellLow","tb_mdtlog_ingrid_cell_low_dd"),
	mdtlog_building_yd_high("mdtlogBuildingYdHigh","tb_mdtlog_building_high_yd_dd"),
	mdtlog_building_yd_mid("mdtlogBuildingYdMid","tb_mdtlog_building_mid_yd_dd"),
	mdtlog_building_yd_low("mdtlogBuildingYdLow","tb_mdtlog_building_low_yd_dd"),
	mdtlog_building_cell_high("mdtlogBuildingCellHigh","tb_mdtlog_building_cell_high_dd"),
	mdtlog_building_cell_mid("mdtlogBuildingCellMid","tb_mdtlog_building_cell_mid_dd"),
	mdtlog_building_cell_low("mdtlogBuildingCellLow","tb_mdtlog_building_cell_low_dd"),
	mdtlog_cell_yd("mdtlogCellYd","tb_mdtlog_cell_yd_dd"),
	mdtlog_tac("mdtlogTac","tb_mdtlog_imei_dd"),
	mdtlog_area_sample_yd("mdtlogAreaSampleYd","tb_mdtlog_area_sample_yd_dd"),
	mdtlog_area_outgrid_yd("mdtlogAreaOutgridYd","tb_mdtlog_area_outgrid_yd_dd"),
	mdtlog_area_outgrid_cell("mdtlogAreaOutgridCell","tb_mdtlog_area_outgrid_cell_dd"),
	mdtlog_area_cell_yd("mdtlogAreaCellYd","tb_mdtlog_area_cell_yd_dd"),
	mdtlog_area_yd("mdtlogAreaYd","tb_mdtlog_area_yd_dd"),
	mdtlog_outgrid_fullnet_high_ydlt("mdtlogOutgridFullnetHighYdlt","tb_mdtlog_outgrid_fullnet_high_ydlt_dd"),
	mdtlog_outgrid_fullnet_mid_ydlt("mdtlogOutgridFullnetMidYdlt","tb_mdtlog_outgrid_fullnet_mid_ydlt_dd"),
	mdtlog_outgrid_fullnet_low_ydlt("mdtlogOutgridFullnetLowYdlt","tb_mdtlog_outgrid_fullnet_low_ydlt_dd"),
	mdtlog_outgrid_fullnet_high_yddx("mdtlogOutgridFullnetHighYddx","tb_mdtlog_outgrid_fullnet_high_yddx_dd"),
	mdtlog_outgrid_fullnet_mid_yddx("mdtlogOutgridFullnetMidYddx","tb_mdtlog_outgrid_fullnet_mid_yddx_dd"),
	mdtlog_outgrid_fullnet_low_yddx("mdtlogOutgridFullnetLowYddx","tb_mdtlog_outgrid_fullnet_low_yddx_dd"),
	mdtlog_ingrid_fullnet_high_ydlt("mdtlogIngridFullnetHighYdlt","tb_mdtlog_ingrid_fullnet_high_ydlt_dd"),
	mdtlog_ingrid_fullnet_mid_ydlt("mdtlogIngridFullnetMidYdlt","tb_mdtlog_ingrid_fullnet_mid_ydlt_dd"),
	mdtlog_ingrid_fullnet_low_ydlt("mdtlogIngridFullnetLowYdlt","tb_mdtlog_ingrid_fullnet_low_ydlt_dd"),
	mdtlog_ingrid_fullnet_high_yddx("mdtlogIngridFullnetHighYddx","tb_mdtlog_ingrid_fullnet_high_yddx_dd"),
	mdtlog_ingrid_fullnet_mid_yddx("mdtlogIngridFullnetMidYddx","tb_mdtlog_ingrid_fullnet_mid_yddx_dd"),
	mdtlog_ingrid_fullnet_low_yddx("mdtlogIngridFullnetLowYddx","tb_mdtlog_ingrid_fullnet_low_yddx_dd"),
	mdtlog_building_fullnet_high_ydlt("mdtlogBuildingFullnetHighYdlt","tb_mdtlog_building_fullnet_high_ydlt_dd"),
	mdtlog_building_fullnet_mid_ydlt("mdtlogBuildingFullnetMidYdlt","tb_mdtlog_building_fullnet_mid_ydlt_dd"),
	mdtlog_building_fullnet_low_ydlt("mdtlogBuildingFullnetLowYdlt","tb_mdtlog_building_fullnet_low_ydlt_dd"),
	mdtlog_building_fullnet_high_yddx("mdtlogBuildingFullnetHighYddx","tb_mdtlog_building_fullnet_high_yddx_dd"),
	mdtlog_building_fullnet_mid_yddx("mdtlogBuildingFullnetMidYddx","tb_mdtlog_building_fullnet_mid_yddx_dd"),
	mdtlog_building_fullnet_low_yddx("mdtlogBuildingFullnetLowYddx","tb_mdtlog_building_fullnet_low_yddx_dd"),
	mdtlog_cell_fullnet_ydlt("mdtlogCellFullnetYdlt","tb_mdtlog_cell_fullnet_ydlt_dd"),
	mdtlog_cell_fullnet_yddx("mdtlogCellFullnetYddx","tb_mdtlog_cell_fullnet_yddx_dd"),
	mdtlog_area_outgrid_fullnet_ydlt("mdtlogAreaOutgridFullnetYdlt","tb_mdtlog_area_outgrid_fullnet_ydlt_dd"),
	mdtlog_area_outgrid_fullnet_yddx("mdtlogAreaOutgridFullnetYddx","tb_mdtlog_area_outgrid_fullnet_yddx_dd"),
	mdtlog_area_cell_fullnet_ydlt("mdtlogAreaCellFullnetYdlt","tb_mdtlog_area_cell_fullnet_ydlt_dd"),
	mdtlog_area_cell_fullnet_yddx("mdtlogAreaCellFullnetYddx","tb_mdtlog_area_cell_fullnet_yddx_dd"),
	mdtlog_area_fullnet_ydlt("mdtlogAreaFullnetYdlt","tb_mdtlog_area_fullnet_ydlt_dd"),
	mdtlog_area_fullnet_yddx("mdtlogAreaFullnetYddx","tb_mdtlog_area_fullnet_yddx_dd"),
	mdtlog_outgrid_dx_high("mdtlogOutgridDxHigh","tb_mdtlog_outgrid_high_dx_dd"),
	mdtlog_outgrid_dx_mid("mdtlogOutgridDxMid","tb_mdtlog_outgrid_mid_dx_dd"),
	mdtlog_outgrid_dx_low("mdtlogOutgridDxLow","tb_mdtlog_outgrid_low_dx_dd"),
	mdtlog_ingrid_dx_high("mdtlogIngridDxHigh","tb_mdtlog_ingrid_high_dx_dd"),
	mdtlog_ingrid_dx_mid("mdtlogIngridDxMid","tb_mdtlog_ingrid_mid_dx_dd"),
	mdtlog_ingrid_dx_low("mdtlogIngridDxLow","tb_mdtlog_ingrid_low_dx_dd"),
	mdtlog_building_dx_high("mdtlogBuildingDxHigh","tb_mdtlog_building_high_dx_dd"),
	mdtlog_building_dx_mid("mdtlogBuildingDxMid","tb_mdtlog_building_mid_dx_dd"),
	mdtlog_building_dx_low("mdtlogBuildingDxLow","tb_mdtlog_building_low_dx_dd"),
	mdtlog_cell_dx("mdtlogCellDx","tb_mdtlog_cell_dx_dd"),
	mdtlog_area_outgrid_dx("mdtlogAreaOutgridDx","tb_mdtlog_area_outgrid_dx_dd"),
	mdtlog_area_cell_dx("mdtlogAreaCellDx","tb_mdtlog_area_cell_dx_dd"),
	mdtlog_area_dx("mdtlogAreaDx","tb_mdtlog_area_dx_dd"),
	mdtlog_outgrid_lt_high("mdtlogOutgridLtHigh","tb_mdtlog_outgrid_high_lt_dd"),
	mdtlog_outgrid_lt_mid("mdtlogOutgridLtMid","tb_mdtlog_outgrid_mid_lt_dd"),
	mdtlog_outgrid_lt_low("mdtlogOutgridLtLow","tb_mdtlog_outgrid_low_lt_dd"),
	mdtlog_ingrid_lt_high("mdtlogIngridLtHigh","tb_mdtlog_ingrid_high_lt_dd"),
	mdtlog_ingrid_lt_mid("mdtlogIngridLtMid","tb_mdtlog_ingrid_mid_lt_dd"),
	mdtlog_ingrid_lt_low("mdtlogIngridLtLow","tb_mdtlog_ingrid_low_lt_dd"),
	mdtlog_building_lt_high("mdtlogBuildingLtHigh","tb_mdtlog_building_high_lt_dd"),
	mdtlog_building_lt_mid("mdtlogBuildingLtMid","tb_mdtlog_building_mid_lt_dd"),
	mdtlog_building_lt_low("mdtlogBuildingLtLow","tb_mdtlog_building_low_lt_dd"),
	mdtlog_cell_lt("mdtlogCellLt","tb_mdtlog_cell_lt_dd"),
	mdtlog_area_outgrid_lt("mdtlogAreaOutgridLt","tb_mdtlog_area_outgrid_lt_dd"),
	mdtlog_area_cell_lt("mdtlogAreaCellLt","tb_mdtlog_area_cell_lt_dd"),
	mdtlog_area_lt("mdtlogAreaLt","tb_mdtlog_area_lt_dd")
	;

	public String fileName;
	public String dirName;
	public int index;
	private MdtTablesEnum(String fileName,String dirName)
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = MdtTablesEnum.class.getName().hashCode()+ordinal();
	}
	
	@Override
	public String getPath(String hPath, String outData)
	{
		return getBasePath(hPath, outData) + "/" + dirName + "_" + outData;
	}
	
	@Override
	public String getHourPath(String hPath, String outData, String hour) 
	{
		if (hour == null) return getPath(hPath, outData);
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
		if (hour == null) return getBasePath(hPath, date);
		return hPath + "/mro_loc/data_01_" + date + "/" + hour;
	}
}
