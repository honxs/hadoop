package cn.mastercom.bigdata.mro.stat.tableEnum;

import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;

public enum MroBsTablesEnum implements IOutPutPathEnum 
{
	mroInsampleHigh("inhighsample", "tb_mr_insample_high_dd"),
	mroInsampleMid("inmidsample", "tb_mr_insample_mid_dd"),
	mroInsampleLow("inlowsample", "tb_mr_insample_low_dd"),
	mroOutsampleHigh("outhighsample", "tb_mr_outsample_high_dd"),
	mroOutsampleMid("outmidsample", "tb_mr_outsample_mid_dd"),
	mroOutsampleLow("outlowsample", "tb_mr_outsample_low_dd"),
	mroNoLocSample("nolocsample", "tb_mr_noLocSample_dd"),

	mroYdOutgridHigh("highOutGrid", "tb_mr_outgrid_high_yd_dd"),
	mroYdOutgridMid("midOutGrid", "tb_mr_outgrid_mid_yd_dd"),
	mroYdOutgridLow("lowOutGrid", "tb_mr_outgrid_low_yd_dd"),
	mroOutgridCellHigh("highOutGridCell", "tb_mr_outgrid_cell_high_dd"),
	mroOutgridCellMid("midOutGridCell", "tb_mr_outgrid_cell_mid_dd"),
	mroOutgridCellLow("lowOutGridCell", "tb_mr_outgrid_cell_low_dd"),

	mroYdIngridHigh("highInGrid", "tb_mr_ingrid_high_yd_dd"),
	mroYdIngridMid("midInGrid", "tb_mr_ingrid_mid_yd_dd"),
	mroYdIngridLow("lowInGrid", "tb_mr_ingrid_low_yd_dd"),
	mroIngridCellHigh("highInGridCell", "tb_mr_ingrid_cell_high_dd"),
	mroIngridCellMid("midInGridCell", "tb_mr_ingrid_cell_mid_dd"),
	mroIngridCellLow("lowInGridCell", "tb_mr_ingrid_cell_low_dd"),

	mroYdOutgridHighAllFreq("ydHighAllFreqOutGrid", "tb_mr_outgrid_allfreq_high_yd_dd"),
	mroYdOutgridMidAllFreq("ydMidAllFreqOutGrid", "tb_mr_outgrid_allfreq_mid_yd_dd"),
	mroYdOutgridLowAllFreq("ydLowAllFreqOutGrid", "tb_mr_outgrid_allfreq_low_yd_dd"),
	mroYdIngridHighAllFreq("ydHighAllFreqInGrid", "tb_mr_ingrid_allfreq_high_yd_dd"),
	mroYdIngridMidAllFreq("ydMidAllFreqInGrid", "tb_mr_ingrid_allfreq_mid_yd_dd"),
	mroYdIngridLowAllFreq("ydLowAllFreqInGrid", "tb_mr_ingrid_allfreq_low_yd_dd"),

	mroYdBuildHigh("highBuild", "tb_mr_building_high_yd_dd"),
	mroYdBuildMid("midBuild", "tb_mr_building_mid_yd_dd"),
	mroYdBuildLow("lowBuild", "tb_mr_building_low_yd_dd"),
	mroYdBuildPosHigh("highBuildPos", "tb_mr_building_pos_high_yd_dd"),
	mroYdBuildPosMid("midBuildPos", "tb_mr_building_pos_mid_yd_dd"),
	mroYdBuildPosLow("lowBuildPos", "tb_mr_building_pos_low_yd_dd"),
	mroYdCell("cell", "tb_mr_cell_yd_dd"),
	mroDxOutgridHigh("dxHighOutGrid", "tb_mr_outgrid_high_dx_dd"),
	mroDxOutgridMid("dxMidOutGrid", "tb_mr_outgrid_mid_dx_dd"),
	mroDxOutgridLow("dxLowOutGrid", "tb_mr_outgrid_low_dx_dd"),
	mroDxIngridHigh("dxHighInGrid", "tb_mr_ingrid_high_dx_dd"),
	mroDxIngridMid("dxMidInGrid", "tb_mr_ingrid_mid_dx_dd"),
	mroDxIngridLow("dxLowInGrid", "tb_mr_ingrid_low_dx_dd"),

	mroDxOutgridHighAllFreq("dxHighAllFreqOutGrid", "tb_mr_outgrid_allfreq_high_dx_dd"),
	mroDxOutgridMidAllFreq("dxMidAllFreqOutGrid", "tb_mr_outgrid_allfreq_mid_dx_dd"),
	mroDxOutgridLowAllFreq("dxLowAllFreqOutGrid", "tb_mr_outgrid_allfreq_low_dx_dd"),
	mroDxIngridHighAllFreq("dxHighAllFreqInGrid", "tb_mr_ingrid_allfreq_high_dx_dd"),
	mroDxIngridMidAllFreq("dxMidAllFreqInGrid", "tb_mr_ingrid_allfreq_mid_dx_dd"),
	mroDxIngridLowAllFreq("dxLowAllFreqInGrid", "tb_mr_ingrid_allfreq_low_dx_dd"),

	mroDxBuildHigh("dxHighBuild", "tb_mr_building_high_dx_dd"),
	mroDxBuildMid("dxMidBuild", "tb_mr_building_mid_dx_dd"),
	mroDxBuildLow("dxLowBuild", "tb_mr_building_low_dx_dd"),
	mroDxCell("dxCell", "tb_mr_cell_dx_dd"),

	mroLtOutgridHigh("ltHighOutGrid", "tb_mr_outgrid_high_lt_dd"),
	mroLtOutgridMid("ltMidOutGrid", "tb_mr_outgrid_mid_lt_dd"),
	mroLtOutgridLow("ltLowOutGrid", "tb_mr_outgrid_low_lt_dd"),
	mroLtIngridHigh("ltHighInGrid", "tb_mr_ingrid_high_lt_dd"),
	mroLtIngridMid("ltMidInGrid", "tb_mr_ingrid_mid_lt_dd"),
	mroLtIngridLow("ltLowInGrid", "tb_mr_ingrid_low_lt_dd"),

	mroLtOutgridHighAllFreq("ltHighAllFreqOutGrid", "tb_mr_outgrid_allfreq_high_lt_dd"),
	mroLtOutgridMidAllFreq("ltMidAllFreqOutGrid", "tb_mr_outgrid_allfreq_mid_lt_dd"),
	mroLtOutgridLowAllFreq("ltLowAllFreqOutGrid", "tb_mr_outgrid_allfreq_low_lt_dd"),
	mroLtIngridHighAllFreq("ltHighAllFreqInGrid", "tb_mr_ingrid_allfreq_high_lt_dd"),
	mroLtIngridMidAllFreq("ltMidAllFreqInGrid", "tb_mr_ingrid_allfreq_mid_lt_dd"),
	mroLtIngridLowAllFreq("ltLowAllFreqInGrid", "tb_mr_ingrid_allfreq_low_lt_dd"),

	mroLtBuildHigh("ltHighBuild", "tb_mr_building_high_lt_dd"),
	mroLtBuildMid("ltMidBuild", "tb_mr_building_mid_lt_dd"),
	mroLtBuildLow("ltLowBuild", "tb_mr_building_low_lt_dd"),
	mroLtCell("ltCell", "tb_mr_cell_lt_dd"),

	mroYdAreaCell("sceneCell", "tb_mr_area_cell_yd_dd"),
	mroYdAreaGridCell("sceneCellGrid", "tb_mr_area_outgrid_cell_dd"),
	mroYdAreaGrid("sceneGrid", "tb_mr_area_outgrid_yd_dd"),
	mroYdArea("scene", "tb_mr_area_yd_dd"),
	mroAreaSample("sceneSample", "tb_mr_area_sample_dd"),
	vap("vaptbmr", "tb_mr_vap"), // 特殊用户采样点

	mroBuildCellHigh("highbuildcell", "tb_mr_building_cell_high_dd"),
	mroBuildCellMid("midbuildcell", "tb_mr_building_cell_mid_dd"),
	mroBuildCellLow("lowbuildcell", "tb_mr_building_cell_low_dd"),
	mroBuildCellPosHigh("ydhighbuildcellpos", "tb_mr_building_cell_pos_high_yd_dd"),
	mroBuildCellPosMid("ydmidbuildcellpos", "tb_mr_building_cell_pos_mid_yd_dd"),
	mroBuildCellPosLow("ydlowbuildcellpos", "tb_mr_building_cell_pos_low_yd_dd"),

	mroLtAreaCell("ltSceneCell", "tb_mr_area_cell_lt_dd"),
	mroLtAreaGrid("ltSceneGrid", "tb_mr_area_outgrid_lt_dd"),
	mroLtArea("ltScene", "tb_mr_area_lt_dd"),
	mroDxAreaCell("dxSceneCell", "tb_mr_area_cell_dx_dd"),
	mroDxAreaGrid("dxSceneGrid", "tb_mr_area_outgrid_dx_dd"),
	mroDxArea("dxScene", "tb_mr_area_dx_dd"),
	mroYdltOutgridHigh("ydltHighOutGrid", "tb_mr_outgrid_high_fullnet_ydlt_dd"),
	mroYdltOutgridMid("ydltMidOutGrid", "tb_mr_outgrid_mid_fullnet_ydlt_dd"),
	mroYdltOutgridLow("ydltLowOutGrid", "tb_mr_outgrid_low_fullnet_ydlt_dd"),

	mroYddxOutgridHigh("yddxHighOutGrid", "tb_mr_outgrid_high_fullnet_yddx_dd"),
	mroYddxOutgridMid("yddxMidOutGrid", "tb_mr_outgrid_mid_fullnet_yddx_dd"),
	mroYddxOutgridLow("yddxLowOutGrid", "tb_mr_outgrid_low_fullnet_yddx_dd"),

	mroYdltIngridHigh("ydltHighInGrid", "tb_mr_ingrid_high_fullnet_ydlt_dd"),
	mroYdltIngridMid("ydltMidInGrid", "tb_mr_ingrid_mid_fullnet_ydlt_dd"),
	mroYdltIngridLow("ydltLowInGrid", "tb_mr_ingrid_low_fullnet_ydlt_dd"),

	mroYddxIngridHigh("yddxHighInGrid", "tb_mr_ingrid_high_fullnet_yddx_dd"),
	mroYddxIngridMid("yddxMidInGrid", "tb_mr_ingrid_mid_fullnet_yddx_dd"),
	mroYddxIngridLow("yddxLowInGrid", "tb_mr_ingrid_low_fullnet_yddx_dd"),

	mroYdltBuildingHigh("ydltHighBuilding", "tb_mr_building_high_fullnet_ydlt_dd"),
	mroYdltBuildingMid("ydltMidBuilding", "tb_mr_building_mid_fullnet_ydlt_dd"),
	mroYdltBuildingLow("ydltLowBuilding", "tb_mr_building_low_fullnet_ydlt_dd"),
	mroYddxBuildingHigh("yddxHighBuilding", "tb_mr_building_high_fullnet_yddx_dd"),
	mroYddxBuildingMid("yddxMidBuilding", "tb_mr_building_mid_fullnet_yddx_dd"),
	mroYddxBuildingLow("yddxLowBuilding", "tb_mr_building_low_fullnet_yddx_dd"),
	mroYdltCell("ydltCell", "tb_mr_cell_fullnet_ydlt_dd"),
	mroYddxCell("yddxCell", "tb_mr_cell_fullnet_yddx_dd"),
	mroYdltAreaGrid("ydltAreaGrid", "tb_mr_area_outgrid_fullnet_ydlt_dd"),
	mroYddxAreaGrid("yddxAreaGrid", "tb_mr_area_outgrid_fullnet_yddx_dd"),
	mroYdltAreaCell("ydltAreaCell", "tb_mr_area_cell_fullnet_ydlt_dd"),
	mroYddxAreaCell("yddxAreaCell", "tb_mr_area_cell_fullnet_yddx_dd"),
	mroYdltArea("ydltArea", "tb_mr_area_fullnet_ydlt_dd"),
	mroYddxArea("yddxArea", "tb_mr_area_fullnet_yddx_dd"),
	mroUserCell("usercell", "tb_mr_user_cell_dd"),

	//基于高铁进行统计的表格
	mroHsrYdTrainSeg("hsrYdTrainSeg", "tb_hsr_data_trainsegment_yd_dd"),
	mroHsrLtTrainSeg("hsrLtTrainSeg", "tb_hsr_data_trainsegment_lt_dd"),
	mroHsrDxTrainSeg("hsrDxTrainSeg", "tb_hsr_data_trainsegment_dx_dd"),
	
	mroHsrYdTrainSegCell("hsrYdTrainSegCell", "tb_hsr_data_trainsegment_cell_yd_dd"),
	mroHsrYdGrid("hsrYdGrid", "tb_hsr_data_grid_yd_dd"),
	mroHsrLtGrid("hsrLtGrid", "tb_hsr_data_grid_lt_dd"),
	mroHsrDxGrid("hsrDxGrid", "tb_hsr_data_grid_dx_dd"),
	
	mroHsrYdGridCell("hsrYdGridCell", "tb_hsr_data_cellgrid_yd_dd"),
	mroHsrYdCell("hsrYdCell", "tb_hsr_data_cell_yd_dd"),
	mroHsrSample("hsrSample", "tb_hsr_data_samplingpoint_yd_dd"),//由于带sample字样的 会被入库程序导进GP，故在这做折衷处理

	mroHsrYdTrainsegNocover("hsrYdTrainSegNoCover", "tb_hsr_data_trainsegment_nocover_yd_dd"),
	mroHsrLtTrainsegNocover("hsrLtTrainSegNoCover", "tb_hsr_data_trainsegment_nocover_lt_dd"),
	mroHsrDxTrainsegNocover("hsrDxTrainSegNoCover", "tb_hsr_data_trainsegment_nocover_dx_dd"),
	mrImeiYd("ydimei", "tb_mr_imei_yd_dd"),
	mrImeiLt("ltimei", "tb_mr_imei_lt_dd"),
	mrImeiDx("dximei", "tb_mr_imei_dx_dd"),
	// 邻区栅格统计
	mroNbCellInGridHigh("nbHighInGridCell", "tb_mr_nbcell_ingrid_high_yd_dd"),
	mroNbCellInGridMid("nbMidInGridCell", "tb_mr_nbcell_ingrid_mid_yd_dd"),
	mroNbCellInGridLow("nbLowInGridCell", "tb_mr_nbcell_ingrid_low_yd_dd"),
	mroNbCellOutGridHigh("nbHighOutGridCell", "tb_mr_nbcell_outgrid_high_yd_dd"),
	mroNbCellOutGridMid("nbMidOutGridCell", "tb_mr_nbcell_outgrid_mid_yd_dd"),
	mroNbCellOutGridLow("nbLowOutGridCell", "tb_mr_nbcell_outgrid_low_yd_dd"),

	// ch add 栅格用户统计
	mroUserOutgridCellHigh("highUserOutCellGrid", "tb_mr_outgrid_cell_usercnt_high_yd_dd"),
	mroUserOutgridCellMid("midUserOutCellGrid", "tb_mr_outgrid_cell_usercnt_mid_yd_dd"),
	mroUserOutgridCellLow("lowUserOutCellGrid", "tb_mr_outgrid_cell_usercnt_low_yd_dd"),
	mroUserIngridCellHigh("highUserInCellGrid", "tb_mr_ingrid_cell_usercnt_high_yd_dd"),
	mroUserIngridCellMid("midUserInCellGrid", "tb_mr_ingrid_cell_usercnt_mid_yd_dd"),
	mroUserIngridCellLow("lowUserInCellGrid", "tb_mr_ingrid_cell_usercnt_low_yd_dd"),
	//常驻用户统计
	mroCellResident("cellResident", "tb_mr_cell_resident_yd_dd"),
	mroBuildPosUserHigh("highBuildPosUser", "tb_mr_building_pos_user_high_yd_dd"),
	mroBuildPosUserMid("midBuildPosUser", "tb_mr_building_pos_user_mid_yd_dd"),
	mroBuildPosUserLow("lowBuildPosUser", "tb_mr_building_pos_user_low_yd_dd"),
	mroResidentUserCell("residentUserCell", "tb_mr_resident_user_cell_yd_hd"),	
	mroruYdBuildMid("mrruMidBuild", "tb_mrru_building_mid_yd_dd"),
	mroruYdBuildPosMid("mrruMidBuildPos", "tb_mrru_building_pos_mid_yd_dd"),
	mroruBuildCellMid("mrruMidbuildcell", "tb_mrru_building_cell_mid_dd"),
	mroruBuildCellPosMid("mrruYdMidBuildCellPos", "tb_mrru_building_cell_pos_mid_yd_dd"),
	mroruBuildPosUserMid("mrrumidBuildPosUser", "tb_mrru_building_pos_user_mid_yd_dd"),

	xdrCountStat("xdrCount", "tb_xdr_user_count_yd_dd"),
	mroUserCellByMins("userCellex", "tb_mr_user_cell_yd_5md"),

	mroResidentSample("residentSample", "tb_mr_resident_sample_dd");

	private int index;
	private String fileName;
	private String dirName;

	private MroBsTablesEnum(String fileName, String dirName) {
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = MroBsTablesEnum.class.getName().hashCode() + ordinal();
	}

	@Override
	public String getPath(String hPath, String outData) {
		return getBasePath(hPath, outData) + "/" + dirName + "_" + outData;
	}

	@Override
	public String getHourPath(String hPath, String outData, String hour) {
		if (hour == null) return getPath(hPath, outData);
		return getHourBasePath(hPath, outData, hour) + "/" + dirName + "_" + outData;
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public String getDirName() {
		return dirName;
	}

	@Override
	public int getIndex() {
		return index;
	}

	public static String getBasePath(String hPath, String date) {
		return hPath + "/mro_loc/data_01_" + date;
	}

	public static String getHourBasePath(String hPath, String date, String hour) {
		if (hour == null) return getBasePath(hPath, date);
		return hPath + "/mro_loc/data_01_" + date + "/" + hour;
	}

}
