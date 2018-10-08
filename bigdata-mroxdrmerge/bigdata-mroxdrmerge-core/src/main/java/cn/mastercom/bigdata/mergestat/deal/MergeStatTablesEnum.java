package cn.mastercom.bigdata.mergestat.deal;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mdt.stat.MdtTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.project.enums.IOutPutPathEnum;
import cn.mastercom.bigdata.stat.userResident.enmus.ResidentLocTablesEnum;

/**
 * @Description:汇聚表格枚举类
 * @author xmr
 * @TODO  将需要进行汇聚的表格抽离出来,作为单独的枚举类
 * @time 2018年5月27日
 */
public enum MergeStatTablesEnum implements IOutPutPathEnum 
{
	//CsOTTTables
	mrocell("mrocell","TB_SIGNAL_CELL_01", MroCsOTTTableEnum.mrocell),
	mrocellfreq("mrocellfreq","TB_FREQ_SIGNAL_CELL_01", MroCsOTTTableEnum.mrocellfreq),
	mrocellgrid("mrocellgrid","TB_SIGNAL_CELLGRID_01", MroCsOTTTableEnum.mrocellgrid),
	mrogrid("mrogrid","TB_SIGNAL_GRID_01", MroCsOTTTableEnum.mrogrid),
	griddt("griddt","TB_DTSIGNAL_GRID_01", MroCsOTTTableEnum.griddt),
	griddtfreq("griddtfreq","TB_FREQ_DTSIGNAL_GRID_01", MroCsOTTTableEnum.griddtfreq),
	gridcqt("gridcqt","TB_CQTSIGNAL_GRID_01", MroCsOTTTableEnum.gridcqt),
	gridcqtfreq("gridcqtfreq","TB_FREQ_CQTSIGNAL_GRID_01", MroCsOTTTableEnum.gridcqtfreq),
	useractcell("useractcell","TB_SIG_USER_BEHAVIOR_LOC_MR_01", MroCsOTTTableEnum.useractcell),
	tenmrogrid("tenmrogrid","TB_SIGNAL_GRID10_01", MroCsOTTTableEnum.tenmrogrid),
	tengriddt("tengriddt","TB_DTSIGNAL_GRID10_01", MroCsOTTTableEnum.tengriddt),
	tengriddtfreq("tengriddtfreq","TB_FREQ_DTSIGNAL_GRID10_01", MroCsOTTTableEnum.tengriddtfreq),
	tengridcqt("tengridcqt","TB_CQTSIGNAL_GRID10_01", MroCsOTTTableEnum.tengridcqt),
	tengridcqtfreq("tengridcqtfreq","TB_FREQ_CQTSIGNAL_GRID10_01", MroCsOTTTableEnum.tengridcqtfreq),
	tenmrocellgrid("tenmrocellgrid","TB_SIGNAL_CELLGRID10_01", MroCsOTTTableEnum.tenmrocellgrid),
	MrOutGrid("mrOutGrid","TB_MR_OUTGRID_01", MroCsOTTTableEnum.MrOutGrid),
	MrBuild("mrBuild","TB_MR_BUILD_01", MroCsOTTTableEnum.MrBuild),
	MrInGrid("mrInGrid","TB_MR_INGRID_01", MroCsOTTTableEnum.MrInGrid),
	MrBuildCell("mrBuildCell","TB_MR_BUILD_CELL_01", MroCsOTTTableEnum.MrBuildCell),
	MrInGridCell("mrInGridCell","TB_MR_INGRID_CELL_01", MroCsOTTTableEnum.MrInGridCell),
	MrStatCell("mrStatCell","TB_STAT_MR_CELL_01", MroCsOTTTableEnum.MrStatCell),
	MrOutGridCell("mrOutGridCell","TB_MR_OUTGRID_CELL_01", MroCsOTTTableEnum.MrOutGridCell),
	MrOutGridCellNc("mrOutGridCellNc","TB_MR_OUTGRID_CELL_NC_01", MroCsOTTTableEnum.MrOutGridCellNc),
	MrInGridCellNc("mrInGridCellNc","TB_MR_INGRID_CELL_NC_01", MroCsOTTTableEnum.MrInGridCellNc),
	MrBuildCellNc("mrBuildCellNc","TB_MR_BUILD_CELL_NC_01", MroCsOTTTableEnum.MrBuildCellNc),
	TopicCellIsolated("topicCellIsolated","TB_TOPIC_CELL_ISOLATED_01", MroCsOTTTableEnum.TopicCellIsolated),
	LTfreqcellByImei("LTfreqcellByImei","TB_FREQ_SIGNAL_LT_CELL_BYIMEI_01", MroCsOTTTableEnum.LTfreqcellByImei),
	LTtenFreqByImeiDt("LTtenFreqByImeiDt","TB_FREQ_DTSIGNAL_LT_GRID10_BYIMEI_01", MroCsOTTTableEnum.LTtenFreqByImeiDt),
	LTtenFreqByImeiCqt("LTtenFreqByImeiCqt","TB_FREQ_CQTSIGNAL_LT_GRID10_BYIMEI_01", MroCsOTTTableEnum.LTtenFreqByImeiCqt),
	DXfreqcellByImei("DXfreqcellByImei","TB_FREQ_SIGNAL_DX_CELL_BYIMEI_01", MroCsOTTTableEnum.DXfreqcellByImei),
	DXtenFreqByImeiDt("DXtenFreqByImeiDt","TB_FREQ_DTSIGNAL_DX_GRID10_BYIMEI_01", MroCsOTTTableEnum.DXtenFreqByImeiDt),
	DXtenFreqByImeiCqt("DXtenFreqByImeiCqt","TB_FREQ_CQTSIGNAL_DX_GRID10_BYIMEI_01", MroCsOTTTableEnum.DXtenFreqByImeiCqt),
	tencellgriddt("tencellgriddt","TB_DTSIGNAL_CELLGRID10_01", MroCsOTTTableEnum.tencellgriddt),
	tencellgridcqt("tencellgridcqt","TB_CQTSIGNAL_CELLGRID10_01", MroCsOTTTableEnum.tencellgridcqt),

	
	//xdrLocTables
	xdrevent("xdrevent","TB_SIGNAL_EVENT_01",XdrLocTablesEnum.xdrevent),
	xdrcell("xdrcell","TB_SIGNAL_CELL_01", XdrLocTablesEnum.xdrcell),
	xdrcellgrid("xdrcellgrid","TB_SIGNAL_CELLGRID_01", XdrLocTablesEnum.xdrcellgrid),
	xdrcellgrid23g("xdrcellgrid23g","TB_23G_SIGNAL_CELLGRID_01", XdrLocTablesEnum.xdrcellgrid23g),
	xdrgrid("xdrgrid","TB_SIGNAL_GRID_01", XdrLocTablesEnum.xdrgrid),
	xdrgriduserhour("xdrgriduserhour","TB_SIGNAL_GRID_USER_HOUR_01", XdrLocTablesEnum.xdrgriduserhour),
	xdrgrid23g("xdrgrid23g","TB_23G_SIGNAL_GRID_01", XdrLocTablesEnum.xdrgrid23g),
	xdrLocation("xdrLocation","XDR_LOCATION_01", XdrLocTablesEnum.xdrLocation),
	xdrLocationSelectOut("xdrLocationSelectOut","XDR_LOCATION_SELECTOUT_01", XdrLocTablesEnum.xdrLocationSelectOut),
	xdrgriddt("xdrgriddt","TB_DTSIGNAL_GRID_01", XdrLocTablesEnum.xdrgriddt),
	xdrgriddt23g("xdrgriddt23g","TB_23G_DTSIGNAL_GRID_01", XdrLocTablesEnum.xdrgrid23g),
	xdrgridcqt("xdrgridcqt","TB_CQTSIGNAL_GRID_01", XdrLocTablesEnum.xdrgridcqt),
	xdrgridcqt23g("xdrgridcqt23g","TB_23G_CQTSIGNAL_GRID_01", XdrLocTablesEnum.xdrgridcqt23g),
	xdreventdt("xdreventdt","TB_DTSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventdt),
	xdreventdt23g("xdreventdt23g","TB_23G_DTSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventdt23g),
	xdreventdtex("xdreventdtex","TB_DTEXSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventdtex),
	xdreventdtex23g("xdreventdtex23g","TB_23G_DTEXSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventdtex23g),
	xdreventcqt("xdreventcqt","TB_CQTSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventcqt),
	xdreventcqt23g("xdreventcqt23g","TB_23G_CQTSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventcqt23g),
	xdruserinfo("xdruserinfo","TB_SIGNAL_USERINFO_01", XdrLocTablesEnum.xdruserinfo),
	xdruseract("xdruseract","TB_SIG_USER_BEHAVIOR_LOC_CELL_01", XdrLocTablesEnum.xdruseract),
	xdreventerr("xdreventerr","TB_ERRSIGNAL_EVENT_01", XdrLocTablesEnum.xdreventerr),
	xdrevtVap("xdrevtVap","TB_EVT_VAP_01", XdrLocTablesEnum.xdrevtVap),
	xdrhirail("xdrhirail","TB_XDR_HIRAIL_01", XdrLocTablesEnum.xdrhirail),
	xdrLocSpan("xdrLocSpan","TB_XDR_LOCATION_SPAN_01", XdrLocTablesEnum.xdrLocSpan),
	xdrcellhourTime("xdrcellhourTime","tb_mr_user_location_dd", ResidentLocTablesEnum.xdrcellhourTime),
	
	//CsFgTable
//	fpmrosample("fpmrosample","TB_FGSIGNAL_SAMPLE_01", MroCsFgTableEnum.fpmrosample),
	fpmrocell("fpmrocell","TB_FGSIGNAL_CELL_01", MroCsFgTableEnum.fpmrocell),
	fpmrocellfreq("fpmrocellfreq","TB_FGFREQ_SIGNAL_CELL_01", MroCsFgTableEnum.fpmrocellfreq),
	fpmrocellgrid("fpmrocellgrid","TB_FGSIGNAL_CELLGRID_01", MroCsFgTableEnum.fpmrocellgrid),
	fpmrogrid("fpmrogrid","TB_FGSIGNAL_GRID_01", MroCsFgTableEnum.fpmrogrid),
	fpmroMore("fpmroMore","MRO_FGMORE_01", MroCsFgTableEnum.fpmroMore),
	fpgriddt("fpgriddt","TB_FGDTSIGNAL_GRID_01", MroCsFgTableEnum.fpgriddt),
	fpgriddtfreq("fpgriddtfreq","TB_FGFREQ_DTSIGNAL_GRID_01", MroCsFgTableEnum.fpgriddtfreq),
	fpgridcqt("fpgridcqt","TB_FGCQTSIGNAL_GRID_01", MroCsFgTableEnum.fpgridcqt),
	fpgridcqtfreq("fpgridcqtfreq","TB_FGFREQ_CQTSIGNAL_GRID_01", MroCsFgTableEnum.fpgridcqtfreq),
	fpsampledt("fpsampledt","TB_FGDTSIGNAL_SAMPLE_01", MroCsFgTableEnum.fpsampledt),
	fpsampledtex("fpsampledtex","TB_FGDTEXSIGNAL_SAMPLE_01", MroCsFgTableEnum.fpsampledtex),
	fpsamplecqt("fpsamplecqt","TB_FGCQTSIGNAL_SAMPLE_01", MroCsFgTableEnum.fpsamplecqt),
	fpuseractcell("fpuseractcell","TB_FGSIG_USER_BEHAVIOR_LOC_MR_01", MroCsFgTableEnum.fpuseractcell),
	fptenmrogrid("fptenmrogrid","TB_FGSIGNAL_GRID10_01", MroCsFgTableEnum.fptenmrogrid),
	fptengriddt("fptengriddt","TB_FGDTSIGNAL_GRID10_01", MroCsFgTableEnum.fptengriddt),
	fptengriddtfreq("fptengriddtfreq","TB_FGFREQ_DTSIGNAL_GRID10_01", MroCsFgTableEnum.fptengriddtfreq),
	fptengridcqt("fptengridcqt","TB_FGCQTSIGNAL_GRID10_01", MroCsFgTableEnum.fptengridcqt),
	fptengridcqtfreq("fptengridcqtfreq","TB_FGFREQ_CQTSIGNAL_GRID10_01", MroCsFgTableEnum.fptengridcqtfreq),
	fptenmrocellgrid("fptenmrocellgrid","TB_FGSIGNAL_CELLGRID10_01", MroCsFgTableEnum.fptenmrocellgrid),
	fpLTfreqcellByImei("fpLTfreqcellByImei","TB_FREQ_FGSIGNAL_LT_CELL_BYIMEI_01", MroCsFgTableEnum.fpLTfreqcellByImei),
	fpLTtenFreqByImeiDt("fpLTtenFreqByImeiDt","TB_FREQ_FGDTSIGNAL_LT_GRID10_BYIMEI_01", MroCsFgTableEnum.fpLTtenFreqByImeiDt),
	fpLTtenFreqByImeiCqt("fpLTtenFreqByImeiCqt","TB_FREQ_FGCQTSIGNAL_LT_GRID10_BYIMEI_01", MroCsFgTableEnum.fpLTtenFreqByImeiCqt),
	fpDXfreqcellByImei("fpDXfreqcellByImei","TB_FREQ_FGSIGNAL_DX_CELL_BYIMEI_01", MroCsFgTableEnum.fpDXfreqcellByImei),
	fpDXtenFreqByImeiDt("fpDXtenFreqByImeiDt","TB_FREQ_FGDTSIGNAL_DX_GRID10_BYIMEI_01", MroCsFgTableEnum.fpDXtenFreqByImeiDt),
	fpDXtenFreqByImeiCqt("fpDXtenFreqByImeiCqt","TB_FREQ_FGCQTSIGNAL_DX_GRID10_BYIMEI_01", MroCsFgTableEnum.fpDXtenFreqByImeiCqt),
	fptencellgriddt("fptencellgriddt","TB_FGDTSIGNAL_CELLGRID10_01", MroCsFgTableEnum.fptencellgriddt),
	fptencellgridcqt("fptencellgridcqt","TB_FGCQTSIGNAL_CELLGRID10_01", MroCsFgTableEnum.fptencellgridcqt),
	indoorErr("indoorErr","tb_mr_cell_err_yd_dd", MroCsFgTableEnum.indoorErr),
	AOATA_PATH("aoata","tb_aoata_sample_dd", MroCsFgTableEnum.AOATA_PATH),
	
	//BsTables
	mroYdOutgridHigh("highOutGrid","tb_mr_outgrid_high_yd_dd", MroBsTablesEnum.mroYdOutgridHigh),
	mroYdOutgridMid("midOutGrid","tb_mr_outgrid_mid_yd_dd", MroBsTablesEnum.mroYdOutgridMid),
	mroYdOutgridLow("lowOutGrid","tb_mr_outgrid_low_yd_dd", MroBsTablesEnum.mroYdOutgridLow),
	mroOutgridCellHigh("highOutGridCell","tb_mr_outgrid_cell_high_dd", MroBsTablesEnum.mroOutgridCellHigh),
	mroOutgridCellMid("midOutGridCell","tb_mr_outgrid_cell_mid_dd", MroBsTablesEnum.mroOutgridCellMid),
	mroOutgridCellLow("lowOutGridCell","tb_mr_outgrid_cell_low_dd", MroBsTablesEnum.mroOutgridCellLow),
	
	mroYdIngridHigh("highInGrid","tb_mr_ingrid_high_yd_dd", MroBsTablesEnum.mroYdIngridHigh),
	mroYdIngridMid("midInGrid","tb_mr_ingrid_mid_yd_dd", MroBsTablesEnum.mroYdIngridMid),
	mroYdIngridLow("lowInGrid","tb_mr_ingrid_low_yd_dd", MroBsTablesEnum.mroYdIngridLow),
	mroIngridCellHigh("highInGridCell","tb_mr_ingrid_cell_high_dd", MroBsTablesEnum.mroIngridCellHigh),
	mroIngridCellMid("midInGridCell","tb_mr_ingrid_cell_mid_dd", MroBsTablesEnum.mroIngridCellMid),
	mroIngridCellLow("lowInGridCell","tb_mr_ingrid_cell_low_dd", MroBsTablesEnum.mroIngridCellLow),
	
	mroYdOutgridHighAllFreq("ydHighAllFreqOutGrid","tb_mr_outgrid_allfreq_high_yd_dd", MroBsTablesEnum.mroYdOutgridHighAllFreq),
	mroYdOutgridMidAllFreq("ydMidAllFreqOutGrid","tb_mr_outgrid_allfreq_mid_yd_dd", MroBsTablesEnum.mroYdOutgridMidAllFreq),
	mroYdOutgridLowAllFreq("ydLowAllFreqOutGrid","tb_mr_outgrid_allfreq_low_yd_dd", MroBsTablesEnum.mroYdOutgridLowAllFreq),
	mroYdIngridHighAllFreq("ydHighAllFreqInGrid","tb_mr_ingrid_allfreq_high_yd_dd", MroBsTablesEnum.mroYdIngridHighAllFreq),
	mroYdIngridMidAllFreq("ydMidAllFreqInGrid","tb_mr_ingrid_allfreq_mid_yd_dd", MroBsTablesEnum.mroYdIngridMidAllFreq),
	mroYdIngridLowAllFreq("ydLowAllFreqInGrid","tb_mr_ingrid_allfreq_low_yd_dd", MroBsTablesEnum.mroYdIngridLowAllFreq),
	
	mroYdBuildHigh("highBuild","tb_mr_building_high_yd_dd", MroBsTablesEnum.mroYdBuildHigh),
	mroYdBuildMid("midBuild","tb_mr_building_mid_yd_dd", MroBsTablesEnum.mroYdBuildMid),
	mroYdBuildLow("lowBuild","tb_mr_building_low_yd_dd", MroBsTablesEnum.mroYdBuildLow),
	mroYdBuildPosHigh("highBuildPos","tb_mr_building_pos_high_yd_dd", MroBsTablesEnum.mroYdBuildPosHigh),
	mroYdBuildPosMid("midBuildPos","tb_mr_building_pos_mid_yd_dd", MroBsTablesEnum.mroYdBuildPosMid),
	mroYdBuildPosLow("lowBuildPos","tb_mr_building_pos_low_yd_dd", MroBsTablesEnum.mroYdBuildPosLow),
	mroYdCell("cell","tb_mr_cell_yd_dd", MroBsTablesEnum.mroYdCell),
	mroDxOutgridHigh("dxHighOutGrid","tb_mr_outgrid_high_dx_dd", MroBsTablesEnum.mroDxOutgridHigh),
	mroDxOutgridMid("dxMidOutGrid","tb_mr_outgrid_mid_dx_dd", MroBsTablesEnum.mroDxOutgridMid),
	mroDxOutgridLow("dxLowOutGrid","tb_mr_outgrid_low_dx_dd", MroBsTablesEnum.mroDxOutgridLow),
	mroDxIngridHigh("dxHighInGrid","tb_mr_ingrid_high_dx_dd", MroBsTablesEnum.mroDxIngridHigh),
	mroDxIngridMid("dxMidInGrid","tb_mr_ingrid_mid_dx_dd", MroBsTablesEnum.mroDxIngridMid),
	mroDxIngridLow("dxLowInGrid","tb_mr_ingrid_low_dx_dd", MroBsTablesEnum.mroDxIngridLow),
	
	mroDxOutgridHighAllFreq("dxHighAllFreqOutGrid","tb_mr_outgrid_allfreq_high_dx_dd", MroBsTablesEnum.mroDxOutgridHighAllFreq),
	mroDxOutgridMidAllFreq("dxMidAllFreqOutGrid","tb_mr_outgrid_allfreq_mid_dx_dd", MroBsTablesEnum.mroDxOutgridMidAllFreq),
	mroDxOutgridLowAllFreq("dxLowAllFreqOutGrid","tb_mr_outgrid_allfreq_low_dx_dd", MroBsTablesEnum.mroDxOutgridLowAllFreq),
	mroDxIngridHighAllFreq("dxHighAllFreqInGrid","tb_mr_ingrid_allfreq_high_dx_dd", MroBsTablesEnum.mroDxIngridHighAllFreq),
	mroDxIngridMidAllFreq("dxMidAllFreqInGrid","tb_mr_ingrid_allfreq_mid_dx_dd", MroBsTablesEnum.mroDxIngridMidAllFreq),
	mroDxIngridLowAllFreq("dxLowAllFreqInGrid","tb_mr_ingrid_allfreq_low_dx_dd", MroBsTablesEnum.mroDxIngridLowAllFreq),
	
	mroDxBuildHigh("dxHighBuild","tb_mr_building_high_dx_dd", MroBsTablesEnum.mroDxBuildHigh),
	mroDxBuildMid("dxMidBuild","tb_mr_building_mid_dx_dd", MroBsTablesEnum.mroDxBuildMid),
	mroDxBuildLow("dxLowBuild","tb_mr_building_low_dx_dd", MroBsTablesEnum.mroDxBuildLow),
	mroDxCell("dxCell","tb_mr_cell_dx_dd", MroBsTablesEnum.mroDxCell),
	
	mroLtOutgridHigh("ltHighOutGrid","tb_mr_outgrid_high_lt_dd", MroBsTablesEnum.mroLtOutgridHigh),
	mroLtOutgridMid("ltMidOutGrid","tb_mr_outgrid_mid_lt_dd", MroBsTablesEnum.mroLtOutgridMid),
	mroLtOutgridLow("ltLowOutGrid","tb_mr_outgrid_low_lt_dd", MroBsTablesEnum.mroLtOutgridLow),
	mroLtIngridHigh("ltHighInGrid","tb_mr_ingrid_high_lt_dd", MroBsTablesEnum.mroLtIngridHigh),
	mroLtIngridMid("ltMidInGrid","tb_mr_ingrid_mid_lt_dd", MroBsTablesEnum.mroLtIngridMid),
	mroLtIngridLow("ltLowInGrid","tb_mr_ingrid_low_lt_dd", MroBsTablesEnum.mroLtIngridLow),
	
	mroLtOutgridHighAllFreq("ltHighAllFreqOutGrid","tb_mr_outgrid_allfreq_high_lt_dd", MroBsTablesEnum.mroLtOutgridHighAllFreq),
	mroLtOutgridMidAllFreq("ltMidAllFreqOutGrid","tb_mr_outgrid_allfreq_mid_lt_dd", MroBsTablesEnum.mroLtOutgridMidAllFreq),
	mroLtOutgridLowAllFreq("ltLowAllFreqOutGrid","tb_mr_outgrid_allfreq_low_lt_dd", MroBsTablesEnum.mroLtOutgridLowAllFreq),
	mroLtIngridHighAllFreq("ltHighAllFreqInGrid","tb_mr_ingrid_allfreq_high_lt_dd", MroBsTablesEnum.mroLtIngridHighAllFreq),
	mroLtIngridMidAllFreq("ltMidAllFreqInGrid","tb_mr_ingrid_allfreq_mid_lt_dd", MroBsTablesEnum.mroLtIngridMidAllFreq),
	mroLtIngridLowAllFreq("ltLowAllFreqInGrid","tb_mr_ingrid_allfreq_low_lt_dd", MroBsTablesEnum.mroLtIngridLowAllFreq),
	
	mroLtBuildHigh("ltHighBuild","tb_mr_building_high_lt_dd", MroBsTablesEnum.mroLtBuildHigh),
	mroLtBuildMid("ltMidBuild","tb_mr_building_mid_lt_dd", MroBsTablesEnum.mroLtBuildMid),
	mroLtBuildLow("ltLowBuild","tb_mr_building_low_lt_dd", MroBsTablesEnum.mroLtBuildLow),
	mroLtCell("ltCell","tb_mr_cell_lt_dd", MroBsTablesEnum.mroLtCell),
	
	mroYdAreaCell("sceneCell","tb_mr_area_cell_yd_dd", MroBsTablesEnum.mroYdAreaCell),
	mroYdAreaGridCell("sceneCellGrid","tb_mr_area_outgrid_cell_dd", MroBsTablesEnum.mroYdAreaGridCell),
	mroYdAreaGrid("sceneGrid","tb_mr_area_outgrid_yd_dd", MroBsTablesEnum.mroYdAreaGrid),
	mroYdArea("scene","tb_mr_area_yd_dd", MroBsTablesEnum.mroYdArea), 
	mroAreaSample("sceneSample","tb_mr_area_sample_dd", MroBsTablesEnum.mroAreaSample),  
	vap("vaptbmr","tb_mr_vap", MroBsTablesEnum.vap), // 特殊用户采样点
	
	mroBuildCellHigh("highbuildcell","tb_mr_building_cell_high_dd", MroBsTablesEnum.mroBuildCellHigh),
	mroBuildCellMid("midbuildcell","tb_mr_building_cell_mid_dd", MroBsTablesEnum.mroBuildCellMid),
	mroBuildCellLow("lowbuildcell","tb_mr_building_cell_low_dd", MroBsTablesEnum.mroBuildCellLow),
	mroBuildCellPosHigh("ydhighbuildcellpos","tb_mr_building_cell_pos_high_yd_dd", MroBsTablesEnum.mroBuildCellPosHigh),
	mroBuildCellPosMid("ydmidbuildcellpos","tb_mr_building_cell_pos_mid_yd_dd", MroBsTablesEnum.mroBuildCellPosMid),
	mroBuildCellPosLow("ydlowbuildcellpos","tb_mr_building_cell_pos_low_yd_dd", MroBsTablesEnum.mroBuildCellPosLow),
	
	mroLtAreaCell("ltSceneCell","tb_mr_area_cell_lt_dd", MroBsTablesEnum.mroLtAreaCell),
	mroLtAreaGrid("ltSceneGrid","tb_mr_area_outgrid_lt_dd", MroBsTablesEnum.mroLtAreaGrid),
	mroLtArea("ltScene","tb_mr_area_lt_dd", MroBsTablesEnum.mroLtArea),
	mroDxAreaCell("dxSceneCell","tb_mr_area_cell_dx_dd", MroBsTablesEnum.mroDxAreaCell),
	mroDxAreaGrid("dxSceneGrid","tb_mr_area_outgrid_dx_dd", MroBsTablesEnum.mroDxAreaGrid),
	mroDxArea("dxScene","tb_mr_area_dx_dd", MroBsTablesEnum.mroDxArea),
	mroYdltOutgridHigh("ydltHighOutGrid","tb_mr_outgrid_high_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltOutgridHigh),
	mroYdltOutgridMid("ydltMidOutGrid","tb_mr_outgrid_mid_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltOutgridMid),
	mroYdltOutgridLow("ydltLowOutGrid","tb_mr_outgrid_low_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltOutgridLow),
	
	mroYddxOutgridHigh("yddxHighOutGrid","tb_mr_outgrid_high_fullnet_yddx_dd", MroBsTablesEnum.mroYddxOutgridHigh),
	mroYddxOutgridMid("yddxMidOutGrid","tb_mr_outgrid_mid_fullnet_yddx_dd", MroBsTablesEnum.mroYddxOutgridMid),
	mroYddxOutgridLow("yddxLowOutGrid","tb_mr_outgrid_low_fullnet_yddx_dd", MroBsTablesEnum.mroYddxOutgridLow),
	
	mroYdltIngridHigh("ydltHighInGrid","tb_mr_ingrid_high_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltIngridHigh),
	mroYdltIngridMid("ydltMidInGrid","tb_mr_ingrid_mid_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltIngridMid),
	mroYdltIngridLow("ydltLowInGrid","tb_mr_ingrid_low_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltIngridLow),
	
	mroYddxIngridHigh("yddxHighInGrid","tb_mr_ingrid_high_fullnet_yddx_dd", MroBsTablesEnum.mroYddxIngridHigh),
	mroYddxIngridMid("yddxMidInGrid","tb_mr_ingrid_mid_fullnet_yddx_dd", MroBsTablesEnum.mroYddxIngridMid),
	mroYddxIngridLow("yddxLowInGrid","tb_mr_ingrid_low_fullnet_yddx_dd", MroBsTablesEnum.mroYddxIngridLow),
	
	mroYdltBuildingHigh("ydltHighBuilding","tb_mr_building_high_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltBuildingHigh),
	mroYdltBuildingMid("ydltMidBuilding","tb_mr_building_mid_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltBuildingMid),
	mroYdltBuildingLow("ydltLowBuilding","tb_mr_building_low_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltBuildingLow),
	mroYddxBuildingHigh("yddxHighBuilding","tb_mr_building_high_fullnet_yddx_dd", MroBsTablesEnum.mroYddxBuildingHigh),
	mroYddxBuildingMid("yddxMidBuilding","tb_mr_building_mid_fullnet_yddx_dd", MroBsTablesEnum.mroYddxBuildingMid),
	mroYddxBuildingLow("yddxLowBuilding","tb_mr_building_low_fullnet_yddx_dd", MroBsTablesEnum.mroYddxBuildingLow),
	mroYdltCell("ydltCell","tb_mr_cell_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltCell),
	mroYddxCell("yddxCell","tb_mr_cell_fullnet_yddx_dd", MroBsTablesEnum.mroYddxCell),
	mroYdltAreaGrid("ydltAreaGrid","tb_mr_area_outgrid_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltAreaGrid),
	mroYddxAreaGrid("yddxAreaGrid","tb_mr_area_outgrid_fullnet_yddx_dd", MroBsTablesEnum.mroYddxAreaGrid),
	mroYdltAreaCell("ydltAreaCell","tb_mr_area_cell_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltAreaCell),
	mroYddxAreaCell("yddxAreaCell","tb_mr_area_cell_fullnet_yddx_dd", MroBsTablesEnum.mroYddxAreaCell),
	mroYdltArea("ydltArea","tb_mr_area_fullnet_ydlt_dd", MroBsTablesEnum.mroYdltArea),
	mroYddxArea("yddxArea","tb_mr_area_fullnet_yddx_dd", MroBsTablesEnum.mroYddxArea),
	mroUserCell("usercell","tb_mr_user_cell_dd", MroBsTablesEnum.mroUserCell),  

	mrImeiYd("ydimei","tb_mr_imei_yd_dd", MroBsTablesEnum.mrImeiYd),
	mrImeiLt("ltimei","tb_mr_imei_lt_dd", MroBsTablesEnum.mrImeiLt),
	mrImeiDx("dximei","tb_mr_imei_dx_dd", MroBsTablesEnum.mrImeiDx),
	// 邻区栅格统计
	mroNbCellInGridHigh("nbHighInGridCell","tb_mr_nbcell_ingrid_high_yd_dd", MroBsTablesEnum.mroNbCellInGridHigh),
	mroNbCellInGridMid("nbMidInGridCell","tb_mr_nbcell_ingrid_mid_yd_dd", MroBsTablesEnum.mroNbCellInGridMid),
	mroNbCellInGridLow("nbLowInGridCell","tb_mr_nbcell_ingrid_low_yd_dd", MroBsTablesEnum.mroNbCellInGridLow),
	mroNbCellOutGridHigh("nbHighOutGridCell","tb_mr_nbcell_outgrid_high_yd_dd", MroBsTablesEnum.mroNbCellOutGridHigh),
	mroNbCellOutGridMid("nbMidOutGridCell","tb_mr_nbcell_outgrid_mid_yd_dd", MroBsTablesEnum.mroNbCellOutGridMid),
	mroNbCellOutGridLow("nbLowOutGridCell","tb_mr_nbcell_outgrid_low_yd_dd", MroBsTablesEnum.mroNbCellOutGridLow),
	
	// ch add 栅格用户统计
	mroUserOutgridCellHigh("highUserOutCellGrid","tb_mr_outgrid_cell_usercnt_high_yd_dd", MroBsTablesEnum.mroUserOutgridCellHigh),
	mroUserOutgridCellMid("midUserOutCellGrid","tb_mr_outgrid_cell_usercnt_mid_yd_dd", MroBsTablesEnum.mroUserOutgridCellMid),
	mroUserOutgridCellLow("lowUserOutCellGrid","tb_mr_outgrid_cell_usercnt_low_yd_dd", MroBsTablesEnum.mroUserOutgridCellLow),
	mroUserIngridCellHigh("highUserInCellGrid","tb_mr_ingrid_cell_usercnt_high_yd_dd", MroBsTablesEnum.mroUserIngridCellHigh),
	mroUserIngridCellMid("midUserInCellGrid","tb_mr_ingrid_cell_usercnt_mid_yd_dd", MroBsTablesEnum.mroUserIngridCellMid),
	mroUserIngridCellLow("lowUserInCellGrid","tb_mr_ingrid_cell_usercnt_low_yd_dd", MroBsTablesEnum.mroUserIngridCellLow),
	//常驻用户统计
	mroCellResident("cellResident","tb_mr_cell_resident_yd_dd", MroBsTablesEnum.mroCellResident),	
	   
	mroBuildingDirectionHigh("highBuildingDirection","tb_mr_building_pos_user_high_yd_dd", MroBsTablesEnum.mroBuildPosUserHigh),
	mroBuildingDirectionMid("midBuildingDirection","tb_mr_building_pos_user_mid_yd_dd", MroBsTablesEnum.mroBuildPosUserMid),
	mroBuildingDirectionLow("lowBuildingDirection","tb_mr_building_pos_user_low_yd_dd", MroBsTablesEnum.mroBuildPosUserLow),

	mroruYdBuildMid("mrruMidBuild","tb_mrru_building_mid_yd_dd", MroBsTablesEnum.mroruYdBuildMid),
	mroruYdBuildPosMid("mrruMidBuildPos","tb_mrru_building_pos_mid_yd_dd", MroBsTablesEnum.mroruYdBuildPosMid),
	mroruBuildCellMid("mrruMidbuildcell","tb_mrru_building_cell_mid_dd", MroBsTablesEnum.mroruBuildCellMid),
	mroruBuildCellPosMid("mrruYdMidBuildCellPos","tb_mrru_building_cell_pos_mid_yd_dd", MroBsTablesEnum.mroruBuildCellPosMid),
	mroruBuildPosUserMid("mrrumidBuildPosUser","tb_mrru_building_pos_user_mid_yd_dd", MroBsTablesEnum.mroruBuildPosUserMid),



	// EventTables
	/*ORIGINMME("tbEventOriginMme","/TB_EVENT_ORIGIN_MME_", TypeIoEvtEnum.ORIGINMME),
	ORIGINMW("tbEventOriginMw","/TB_EVENT_ORIGIN_MW_", TypeIoEvtEnum.ORIGINMW),
	ORIGINSV("tbEventOriginSv","/TB_EVENT_ORIGIN_SV_", TypeIoEvtEnum.ORIGINSV),
	ORIGINRX( "tbEventOriginRx","/TB_EVENT_ORIGIN_Rx_", TypeIoEvtEnum.ORIGINRX),
	ORIGINHTTP( "tbEventOriginS1uHttp","/TB_EVENT_ORIGIN_S1U_HTTP_", TypeIoEvtEnum.ORIGINHTTP),
	ORIGINMG("tbEventOriginMg","/TB_EVENT_ORIGIN_MG_", TypeIoEvtEnum.ORIGINMG),
	ORIGINRTP( "tbEventOriginRtp","/TB_EVENT_ORIGIN_RTP_", TypeIoEvtEnum.ORIGINRTP),
	ORIGINMOS_BEIJING("tbEventOriginMosBeiJing","/TB_EVENT_ORIGIN_MOS_BEIJING_", TypeIoEvtEnum.ORIGINMOS_BEIJING),
	ORIGINWJTDH_BEIJING("tbEventOriginMjtdhBeiJing","/TB_EVENT_ORIGIN_MJTDH_BEIJING_", TypeIoEvtEnum.ORIGINWJTDH_BEIJING),
	ORIGINIMS_MO("tbEventOriginImsMo","/TB_EVENT_ORIGIN_Ims_Mo_", TypeIoEvtEnum.ORIGINIMS_MO),
	ORIGINIMS_MT("tbEventOriginImsMt","/TB_EVENT_ORIGIN_Ims_Mt_", TypeIoEvtEnum.ORIGINIMS_MT),
	ORIGINCDR_QUALITY("tbEventOriginCdrQuality","/TB_EVENT_ORIGIN_Cdr_Quality_", TypeIoEvtEnum.ORIGINCDR_QUALITY),
	ORIGIN_Uu("tbEventOriginUu","/TB_EVENT_ORIGIN_UU_", TypeIoEvtEnum.ORIGIN_Uu),
	ORIGIN_MRO("tbEventOriginMro","/TB_EVENT_ORIGIN_MRO_LTE_SCPLR_", TypeIoEvtEnum.ORIGIN_MRO),
	ORIGIN_MDT_RLFHOF("tbEventOriginMdtRlfHof","/TB_EVENT_ORIGIN_Mdt_RlfHof_", TypeIoEvtEnum.ORIGIN_MDT_RLFHOF),
	ORIGIN_MDT_RLFHOF_OTHERPATH("tbEventOriginMdtRlfHofOtherPath","/TB_EVENT_ORIGIN_Mdt_RlfHof_OtherPath_", TypeIoEvtEnum.ORIGIN_MDT_RLFHOF_OTHERPATH),
	ORIGIN_MOS_SHARDING("tbEventOriginMosSharding","/TB_EVENT_ORIGIN_MOS_SHRDING", TypeIoEvtEnum.ORIGIN_MOS_SHARDING),*/
	
	/*IMSAMPLE("tbEventMidInSample","/tb_evt_insample_abnormal_mid_dd_", TypeIoEvtEnum.IMSAMPLE),
    OMSAMPLE("tbEventMidOutSample","/tb_evt_outsample_abnormal_mid_dd_", TypeIoEvtEnum.OMSAMPLE),
    ILSAMPLE("tbEventLowInSample","/tb_evt_insample_abnormal_low_dd_", TypeIoEvtEnum.ILSAMPLE),
    OLSAMPLE("tbEventLowOutSample","/tb_evt_outsample_abnormal_low_dd_", TypeIoEvtEnum.OLSAMPLE),
    IHSAMPLE("tbEventHighInSample","/tb_evt_insample_abnormal_high_dd_", TypeIoEvtEnum.IHSAMPLE),
    OHSAMPLE("tbEventHighOutSample","/tb_evt_outsample_abnormal_high_dd_", TypeIoEvtEnum.OHSAMPLE),
    IMSAMPLEALL("tbEventMidInSampleALL","/tb_evt_insample_mid_dd_", TypeIoEvtEnum.IMSAMPLEALL),
    OMSAMPLEALL("tbEventMidOutSampleALL","/tb_evt_outsample_mid_dd_", TypeIoEvtEnum.OMSAMPLEALL),
    ILSAMPLEALL("tbEventLowInSampleALL","/tb_evt_insample_low_dd_", TypeIoEvtEnum.ILSAMPLEALL),
    OLSAMPLEALL("tbEventLowOutSampleALL","/tb_evt_outsample_low_dd_", TypeIoEvtEnum.OLSAMPLEALL),
    IHSAMPLEALL("tbEventHighInSampleALL","/tb_evt_insample_high_dd_", TypeIoEvtEnum.IHSAMPLEALL),
    OHSAMPLEALL("tbEventHighOutSampleALL","/tb_evt_outsample_high_dd_", TypeIoEvtEnum.OHSAMPLEALL),*/
    OHGRIDEVT("tbEventHighOutGrid","tb_evt_outgrid_high_dd", TypeIoEvtEnum.OHGRIDEVT),
    OMGRIDEVT("tbEventMidOutGrid","tb_evt_outgrid_mid_dd", TypeIoEvtEnum.OMGRIDEVT),
    OLGRIDEVT("tbEventLowOutGrid","tb_evt_outgrid_low_dd", TypeIoEvtEnum.OLGRIDEVT),
    OHCELLGRIDEVT("tbEventHighOutCellGrid","tb_evt_outgrid_cell_high_dd", TypeIoEvtEnum.OHCELLGRIDEVT),
    OMCELLGRIDEVT("tbEventMidOutCellGrid","tb_evt_outgrid_cell_mid_dd", TypeIoEvtEnum.OMCELLGRIDEVT),
    OLCELLGRIDEVT("tbEventLowOutCellGrid","tb_evt_outgrid_cell_low_dd", TypeIoEvtEnum.OLCELLGRIDEVT),
    IHGRIDEVT("tbEventHighInGrid","tb_evt_ingrid_high_dd", TypeIoEvtEnum.IHGRIDEVT),
    IMGRIDEVT("tbEventMidInGrid","tb_evt_ingrid_mid_dd", TypeIoEvtEnum.IMGRIDEVT),
    ILGRIDEVT("tbEventLowInGrid","tb_evt_ingrid_low_dd", TypeIoEvtEnum.ILGRIDEVT),
    IHCELLGRIDEVT("tbEventHighInCellGrid","tb_evt_ingrid_cell_high_dd", TypeIoEvtEnum.IHCELLGRIDEVT),
    IMCELLGRIDEVT("tbEventMidInCellGrid","tb_evt_ingrid_cell_mid_dd", TypeIoEvtEnum.IMCELLGRIDEVT),
    ILCELLGRIDEVT("tbEventLowInCellGrid","tb_evt_ingrid_cell_low_dd", TypeIoEvtEnum.ILCELLGRIDEVT),
    BHGRIDEVT("tbEventHighBuildGrid","tb_evt_building_high_dd", TypeIoEvtEnum.BHGRIDEVT),
    BMGRIDEVT("tbEventMidBuildGrid","tb_evt_building_mid_dd", TypeIoEvtEnum.BMGRIDEVT),
    BLGRIDEVT("tbEventLowBuildGrid","tb_evt_building_low_dd", TypeIoEvtEnum.BLGRIDEVT),
    BHPOSEVT("tbEventHighBuildPos","tb_evt_building_pos_high_yd_dd", TypeIoEvtEnum.BHPOSEVT),
    BMPOSEVT("tbEventMidBuildPos","tb_evt_building_pos_mid_yd_dd", TypeIoEvtEnum.BMPOSEVT),
    BLPOSEVT("tbEventLowBuildPos","tb_evt_building_pos_low_yd_dd", TypeIoEvtEnum.BLPOSEVT),
    BHCELLGRIDEVT("tbEventHighBuildCellGrid","tb_evt_building_cell_high_dd", TypeIoEvtEnum.BHCELLGRIDEVT),
    BMCELLGRIDEVT("tbEventMidBuildCellGrid","tb_evt_building_cell_mid_dd", TypeIoEvtEnum.BMCELLGRIDEVT),
    BLCELLGRIDEVT("tbEventLowBuildCellGrid","tb_evt_building_cell_low_dd", TypeIoEvtEnum.BLCELLGRIDEVT),
    BHCELLPOSEVT("tbEventHighBuildCellPos","tb_evt_building_cell_pos_high_yd_dd", TypeIoEvtEnum.BHCELLPOSEVT),
    BMCELLPOSEVT("tbEventMidBuildCellPos","tb_evt_building_cell_pos_mid_yd_dd", TypeIoEvtEnum.BMCELLPOSEVT),
    BLCELLPOSEVT("tbEventLowBuildCellPos","tb_evt_building_cell_pos_low_yd_dd", TypeIoEvtEnum.BLCELLPOSEVT),
    CELLEVT("tbEventCell","tb_evt_cell_dd", TypeIoEvtEnum.CELLEVT),
    IMSIEVT("tbEventImsi","tb_evt_imsi_dd", TypeIoEvtEnum.IMSIEVT),
    AREAEVT("tbArea","tb_evt_area_dd", TypeIoEvtEnum.AREAEVT),
    AREACELLEVT("tbAreaCell","tb_evt_area_cell_dd", TypeIoEvtEnum.AREACELLEVT),
    AREACELLGRIDEVT("tbAreaGridCell","tb_evt_area_outgrid_cell_dd", TypeIoEvtEnum.AREACELLGRIDEVT),
    AREAGRIDEVT("tbAreaGrid","tb_evt_area_outgrid_dd", TypeIoEvtEnum.AREAGRIDEVT),
//	,HPSAMPLE("hpEvtSample", "/tb_hp_evt_samplingpoint_yd_dd_", TypeIoEvtEnum.HPSAMPLE)//++

    //MdtTables
  /*  mdtimm_insample_high("mdtimmInsampleHigh","tb_mdtimm_insample_high_dd", MdtTablesEnum.mdtimm_insample_high),
	mdtimm_insample_mid("mdtimmInsampleMid","tb_mdtimm_insample_mid_dd", MdtTablesEnum.mdtimm_insample_mid),
	mdtimm_insample_low("mdtimmInsampleLow","tb_mdtimm_insample_low_dd", MdtTablesEnum.mdtimm_insample_low),
	mdtimm_outsample_high("mdtimmOutsampleHigh","tb_mdtimm_outsample_high_dd", MdtTablesEnum.mdtimm_outsample_high),
	mdtimm_outsample_mid("mdtimmOutsampleMid","tb_mdtimm_outsample_mid_dd", MdtTablesEnum.mdtimm_outsample_mid),
	mdtimm_outsample_low("mdtimmOutsampleLow","tb_mdtimm_outsample_low_dd", MdtTablesEnum.mdtimm_outsample_low),
	mdtimm_noLocSample("mdtimmNoLocSample","tb_mdtimm_noLocSample_dd", MdtTablesEnum.mdtimm_noLocSample),*/
	mdtimm_outgrid_yd_high("mdtimmOutgridYdHigh","tb_mdtimm_outgrid_high_yd_dd", MdtTablesEnum.mdtimm_outgrid_yd_high),
	mdtimm_outgrid_yd_mid("mdtimmOutgridYdMid","tb_mdtimm_outgrid_mid_yd_dd", MdtTablesEnum.mdtimm_outgrid_yd_mid),
	mdtimm_outgrid_yd_low("mdtimmOutgridYdLow","tb_mdtimm_outgrid_low_yd_dd", MdtTablesEnum.mdtimm_outgrid_yd_low),
	mdtimm_outgrid_cell_high("mdtimmOutgridCellHigh","tb_mdtimm_outgrid_cell_high_dd", MdtTablesEnum.mdtimm_outgrid_cell_high),
	mdtimm_outgrid_cell_mid("mdtimmOutgridCellMid","tb_mdtimm_outgrid_cell_mid_dd", MdtTablesEnum.mdtimm_outgrid_cell_mid),
	mdtimm_outgrid_cell_low("mdtimmOutgridCellLow","tb_mdtimm_outgrid_cell_low_dd", MdtTablesEnum.mdtimm_outgrid_cell_low),
	mdtimm_ingrid_yd_high("mdtimmIngridYdHigh","tb_mdtimm_ingrid_high_yd_dd", MdtTablesEnum.mdtimm_ingrid_yd_high),
	mdtimm_ingrid_yd_mid("mdtimmIngridYdMid","tb_mdtimm_ingrid_mid_yd_dd", MdtTablesEnum.mdtimm_ingrid_yd_mid),
	mdtimm_ingrid_yd_low("mdtimmIngridYdLow","tb_mdtimm_ingrid_low_yd_dd", MdtTablesEnum.mdtimm_ingrid_yd_low),
	mdtimm_ingrid_cell_high("mdtimmIngridCellHigh","tb_mdtimm_ingrid_cell_high_dd", MdtTablesEnum.mdtimm_ingrid_cell_high),
	mdtimm_ingrid_cell_mid("mdtimmIngridCellMid","tb_mdtimm_ingrid_cell_mid_dd", MdtTablesEnum.mdtimm_ingrid_cell_mid),
	mdtimm_ingrid_cell_low("mdtimmIngridCellLow","tb_mdtimm_ingrid_cell_low_dd", MdtTablesEnum.mdtimm_ingrid_cell_low),
	mdtimm_building_yd_high("mdtimmBuildingYdHigh","tb_mdtimm_building_high_yd_dd", MdtTablesEnum.mdtimm_building_yd_high),
	mdtimm_building_yd_mid("mdtimmBuildingYdMid","tb_mdtimm_building_mid_yd_dd", MdtTablesEnum.mdtimm_building_yd_mid),
	mdtimm_building_yd_low("mdtimmBuildingYdLow","tb_mdtimm_building_low_yd_dd", MdtTablesEnum.mdtimm_building_yd_low),
	mdtimm_building_cell_high("mdtimmBuildingCellHigh","tb_mdtimm_building_cell_high_dd", MdtTablesEnum.mdtimm_building_cell_high),
	mdtimm_building_cell_mid("mdtimmBuildingCellMid","tb_mdtimm_building_cell_mid_dd", MdtTablesEnum.mdtimm_building_cell_mid),
	mdtimm_building_cell_low("mdtimmBuildingCellLow","tb_mdtimm_building_cell_low_dd", MdtTablesEnum.mdtimm_building_cell_low),
	mdtimm_cell_yd("mdtimmCellYd","tb_mdtimm_cell_yd_dd", MdtTablesEnum.mdtimm_cell_yd),
	mdtimm_tac("mdtimmTac","tb_mdtimm_imei_dd", MdtTablesEnum.mdtimm_tac),
//	mdtimm_area_sample_yd("mdtimmAreaSampleYd","tb_mdtimm_area_sample_yd_dd", MdtTablesEnum.mdtimm_area_sample_yd),
	mdtimm_area_outgrid_yd("mdtimmAreaOutgridYd","tb_mdtimm_area_outgrid_yd_dd", MdtTablesEnum.mdtimm_area_outgrid_yd),
	mdtimm_area_outgrid_cell("mdtimmAreaOutgridCell","tb_mdtimm_area_outgrid_cell_dd", MdtTablesEnum.mdtimm_area_outgrid_cell),
	mdtimm_area_cell_yd("mdtimmAreaCellYd","tb_mdtimm_area_cell_yd_dd", MdtTablesEnum.mdtimm_area_cell_yd),
	mdtimm_area_yd("mdtimmAreaYd","tb_mdtimm_area_yd_dd", MdtTablesEnum.mdtimm_area_yd),
	mdtimm_outgrid_fullnet_high_ydlt("mdtimmOutgridFullnetHighYdlt","tb_mdtimm_outgrid_fullnet_high_ydlt_dd", MdtTablesEnum.mdtimm_outgrid_fullnet_high_ydlt),
	mdtimm_outgrid_fullnet_mid_ydlt("mdtimmOutgridFullnetMidYdlt","tb_mdtimm_outgrid_fullnet_mid_ydlt_dd", MdtTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt),
	mdtimm_outgrid_fullnet_low_ydlt("mdtimmOutgridFullnetLowYdlt","tb_mdtimm_outgrid_fullnet_low_ydlt_dd", MdtTablesEnum.mdtimm_outgrid_fullnet_low_ydlt),
	mdtimm_outgrid_fullnet_high_yddx("mdtimmOutgridFullnetHighYddx","tb_mdtimm_outgrid_fullnet_high_yddx_dd", MdtTablesEnum.mdtimm_outgrid_fullnet_high_yddx),
	mdtimm_outgrid_fullnet_mid_yddx("mdtimmOutgridFullnetMidYddx","tb_mdtimm_outgrid_fullnet_mid_yddx_dd", MdtTablesEnum.mdtimm_outgrid_fullnet_mid_yddx),
	mdtimm_outgrid_fullnet_low_yddx("mdtimmOutgridFullnetLowYddx","tb_mdtimm_outgrid_fullnet_low_yddx_dd", MdtTablesEnum.mdtimm_outgrid_fullnet_low_yddx),
	mdtimm_ingrid_fullnet_high_ydlt("mdtimmIngridFullnetHighYdlt","tb_mdtimm_ingrid_fullnet_high_ydlt_dd", MdtTablesEnum.mdtimm_ingrid_fullnet_high_ydlt),
	mdtimm_ingrid_fullnet_mid_ydlt("mdtimmIngridFullnetMidYdlt","tb_mdtimm_ingrid_fullnet_mid_ydlt_dd", MdtTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt),
	mdtimm_ingrid_fullnet_low_ydlt("mdtimmIngridFullnetLowYdlt","tb_mdtimm_ingrid_fullnet_low_ydlt_dd", MdtTablesEnum.mdtimm_ingrid_fullnet_low_ydlt),
	mdtimm_ingrid_fullnet_high_yddx("mdtimmIngridFullnetHighYddx","tb_mdtimm_ingrid_fullnet_high_yddx_dd", MdtTablesEnum.mdtimm_ingrid_fullnet_high_yddx),
	mdtimm_ingrid_fullnet_mid_yddx("mdtimmIngridFullnetMidYddx","tb_mdtimm_ingrid_fullnet_mid_yddx_dd", MdtTablesEnum.mdtimm_ingrid_fullnet_mid_yddx),
	mdtimm_ingrid_fullnet_low_yddx("mdtimmIngridFullnetLowYddx","tb_mdtimm_ingrid_fullnet_low_yddx_dd", MdtTablesEnum.mdtimm_ingrid_fullnet_low_yddx),
	mdtimm_building_fullnet_high_ydlt("mdtimmBuildingFullnetHighYdlt","tb_mdtimm_building_fullnet_high_ydlt_dd", MdtTablesEnum.mdtimm_building_fullnet_high_ydlt),
	mdtimm_building_fullnet_mid_ydlt("mdtimmBuildingFullnetMidYdlt","tb_mdtimm_building_fullnet_mid_ydlt_dd", MdtTablesEnum.mdtimm_building_fullnet_mid_ydlt),
	mdtimm_building_fullnet_low_ydlt("mdtimmBuildingFullnetLowYdlt","tb_mdtimm_building_fullnet_low_ydlt_dd", MdtTablesEnum.mdtimm_building_fullnet_low_ydlt),
	mdtimm_building_fullnet_high_yddx("mdtimmBuildingFullnetHighYddx","tb_mdtimm_building_fullnet_high_yddx_dd", MdtTablesEnum.mdtimm_building_fullnet_high_yddx),
	mdtimm_building_fullnet_mid_yddx("mdtimmBuildingFullnetMidYddx","tb_mdtimm_building_fullnet_mid_yddx_dd", MdtTablesEnum.mdtimm_building_fullnet_mid_yddx),
	mdtimm_building_fullnet_low_yddx("mdtimmBuildingFullnetLowYddx","tb_mdtimm_building_fullnet_low_yddx_dd", MdtTablesEnum.mdtimm_building_fullnet_low_yddx),
	mdtimm_cell_fullnet_ydlt("mdtimmCellFullnetYdlt","tb_mdtimm_cell_fullnet_ydlt_dd", MdtTablesEnum.mdtimm_cell_fullnet_ydlt),
	mdtimm_cell_fullnet_yddx("mdtimmCellFullnetYddx","tb_mdtimm_cell_fullnet_yddx_dd", MdtTablesEnum.mdtimm_cell_fullnet_yddx),
	mdtimm_area_outgrid_fullnet_ydlt("mdtimmAreaOutgridFullnetYdlt","tb_mdtimm_area_outgrid_fullnet_ydlt_dd", MdtTablesEnum.mdtimm_area_outgrid_fullnet_ydlt),
	mdtimm_area_outgrid_fullnet_yddx("mdtimmAreaOutgridFullnetYddx","tb_mdtimm_area_outgrid_fullnet_yddx_dd", MdtTablesEnum.mdtimm_area_outgrid_fullnet_yddx),
	mdtimm_area_cell_fullnet_ydlt("mdtimmAreaCellFullnetYdlt","tb_mdtimm_area_cell_fullnet_ydlt_dd", MdtTablesEnum.mdtimm_area_cell_fullnet_ydlt),
	mdtimm_area_cell_fullnet_yddx("mdtimmAreaCellFullnetYddx","tb_mdtimm_area_cell_fullnet_yddx_dd", MdtTablesEnum.mdtimm_area_cell_fullnet_yddx),
	mdtimm_area_fullnet_ydlt("mdtimmAreaFullnetYdlt","tb_mdtimm_area_fullnet_ydlt_dd", MdtTablesEnum.mdtimm_area_fullnet_ydlt),
	mdtimm_area_fullnet_yddx("mdtimmAreaFullnetYddx","tb_mdtimm_area_fullnet_yddx_dd", MdtTablesEnum.mdtimm_area_fullnet_yddx),
	mdtimm_outgrid_dx_high("mdtimmOutgridDxHigh","tb_mdtimm_outgrid_high_dx_dd", MdtTablesEnum.mdtimm_outgrid_dx_high),
	mdtimm_outgrid_dx_mid("mdtimmOutgridDxMid","tb_mdtimm_outgrid_mid_dx_dd", MdtTablesEnum.mdtimm_outgrid_dx_mid),
	mdtimm_outgrid_dx_low("mdtimmOutgridDxLow","tb_mdtimm_outgrid_low_dx_dd", MdtTablesEnum.mdtimm_outgrid_dx_low),
	mdtimm_ingrid_dx_high("mdtimmIngridDxHigh","tb_mdtimm_ingrid_high_dx_dd", MdtTablesEnum.mdtimm_ingrid_dx_high),
	mdtimm_ingrid_dx_mid("mdtimmIngridDxMid","tb_mdtimm_ingrid_mid_dx_dd", MdtTablesEnum.mdtimm_ingrid_dx_mid),
	mdtimm_ingrid_dx_low("mdtimmIngridDxLow","tb_mdtimm_ingrid_low_dx_dd", MdtTablesEnum.mdtimm_ingrid_dx_low),
	mdtimm_building_dx_high("mdtimmBuildingDxHigh","tb_mdtimm_building_high_dx_dd", MdtTablesEnum.mdtimm_building_dx_high),
	mdtimm_building_dx_mid("mdtimmBuildingDxMid","tb_mdtimm_building_mid_dx_dd", MdtTablesEnum.mdtimm_building_dx_mid),
	mdtimm_building_dx_low("mdtimmBuildingDxLow","tb_mdtimm_building_low_dx_dd", MdtTablesEnum.mdtimm_building_dx_low),
	mdtimm_cell_dx("mdtimmCellDx","tb_mdtimm_cell_dx_dd", MdtTablesEnum.mdtimm_cell_dx),
	mdtimm_area_outgrid_dx("mdtimmAreaOutgridDx","tb_mdtimm_area_outgrid_dx_dd", MdtTablesEnum.mdtimm_area_outgrid_dx),
	mdtimm_area_cell_dx("mdtimmAreaCellDx","tb_mdtimm_area_cell_dx_dd", MdtTablesEnum.mdtimm_area_cell_dx),
	mdtimm_area_dx("mdtimmAreaDx","tb_mdtimm_area_dx_dd", MdtTablesEnum.mdtimm_area_dx),
	mdtimm_outgrid_lt_high("mdtimmOutgridLtHigh","tb_mdtimm_outgrid_high_lt_dd", MdtTablesEnum.mdtimm_outgrid_lt_high),
	mdtimm_outgrid_lt_mid("mdtimmOutgridLtMid","tb_mdtimm_outgrid_mid_lt_dd", MdtTablesEnum.mdtimm_outgrid_lt_mid),
	mdtimm_outgrid_lt_low("mdtimmOutgridLtLow","tb_mdtimm_outgrid_low_lt_dd", MdtTablesEnum.mdtimm_outgrid_lt_low),
	mdtimm_ingrid_lt_high("mdtimmIngridLtHigh","tb_mdtimm_ingrid_high_lt_dd", MdtTablesEnum.mdtimm_ingrid_lt_high),
	mdtimm_ingrid_lt_mid("mdtimmIngridLtMid","tb_mdtimm_ingrid_mid_lt_dd", MdtTablesEnum.mdtimm_ingrid_lt_mid),
	mdtimm_ingrid_lt_low("mdtimmIngridLtLow","tb_mdtimm_ingrid_low_lt_dd", MdtTablesEnum.mdtimm_ingrid_lt_low),
	mdtimm_building_lt_high("mdtimmBuildingLtHigh","tb_mdtimm_building_high_lt_dd", MdtTablesEnum.mdtimm_building_lt_high),
	mdtimm_building_lt_mid("mdtimmBuildingLtMid","tb_mdtimm_building_mid_lt_dd", MdtTablesEnum.mdtimm_building_lt_mid),
	mdtimm_building_lt_low("mdtimmBuildingLtLow","tb_mdtimm_building_low_lt_dd", MdtTablesEnum.mdtimm_building_lt_low),
	mdtimm_cell_lt("mdtimmCellLt","tb_mdtimm_cell_lt_dd", MdtTablesEnum.mdtimm_cell_lt),
	mdtimm_area_outgrid_lt("mdtimmAreaOutgridLt","tb_mdtimm_area_outgrid_lt_dd", MdtTablesEnum.mdtimm_area_outgrid_lt),
	mdtimm_area_cell_lt("mdtimmAreaCellLt","tb_mdtimm_area_cell_lt_dd", MdtTablesEnum.mdtimm_area_cell_lt),
	mdtimm_area_lt("mdtimmAreaLt","tb_mdtimm_area_lt_dd", MdtTablesEnum.mdtimm_area_lt),
/*	mdtlog_insample_high("mdtlogInsampleHigh","tb_mdtlog_insample_high_dd", MdtTablesEnum.mdtlog_insample_high),
	mdtlog_insample_mid("mdtlogInsampleMid","tb_mdtlog_insample_mid_dd", MdtTablesEnum.mdtlog_insample_mid),
	mdtlog_insample_low("mdtlogInsampleLow","tb_mdtlog_insample_low_dd", MdtTablesEnum.mdtlog_insample_low),
	mdtlog_outsample_high("mdtlogOutsampleHigh","tb_mdtlog_outsample_high_dd", MdtTablesEnum.mdtlog_outsample_high),
	mdtlog_outsample_mid("mdtlogOutsampleMid","tb_mdtlog_outsample_mid_dd", MdtTablesEnum.mdtlog_outsample_mid),
	mdtlog_outsample_low("mdtlogOutsampleLow","tb_mdtlog_outsample_low_dd", MdtTablesEnum.mdtlog_outsample_low),
	mdtlog_noLocSample("mdtlogNoLocSample","tb_mdtlog_noLocSample_dd", MdtTablesEnum.mdtlog_noLocSample),*/
	mdtlog_outgrid_yd_high("mdtlogOutgridYdHigh","tb_mdtlog_outgrid_high_yd_dd", MdtTablesEnum.mdtlog_outgrid_yd_high),
	mdtlog_outgrid_yd_mid("mdtlogOutgridYdMid","tb_mdtlog_outgrid_mid_yd_dd", MdtTablesEnum.mdtlog_outgrid_yd_mid),
	mdtlog_outgrid_yd_low("mdtlogOutgridYdLow","tb_mdtlog_outgrid_low_yd_dd", MdtTablesEnum.mdtlog_outgrid_yd_low),
	mdtlog_outgrid_cell_high("mdtlogOutgridCellHigh","tb_mdtlog_outgrid_cell_high_dd", MdtTablesEnum.mdtlog_outgrid_cell_high),
	mdtlog_outgrid_cell_mid("mdtlogOutgridCellMid","tb_mdtlog_outgrid_cell_mid_dd", MdtTablesEnum.mdtlog_outgrid_cell_mid),
	mdtlog_outgrid_cell_low("mdtlogOutgridCellLow","tb_mdtlog_outgrid_cell_low_dd", MdtTablesEnum.mdtlog_outgrid_cell_low),
	mdtlog_ingrid_yd_high("mdtlogIngridYdHigh","tb_mdtlog_ingrid_high_yd_dd", MdtTablesEnum.mdtlog_ingrid_yd_high),
	mdtlog_ingrid_yd_mid("mdtlogIngridYdMid","tb_mdtlog_ingrid_mid_yd_dd", MdtTablesEnum.mdtlog_ingrid_yd_mid),
	mdtlog_ingrid_yd_low("mdtlogIngridYdLow","tb_mdtlog_ingrid_low_yd_dd", MdtTablesEnum.mdtlog_ingrid_yd_low),
	mdtlog_ingrid_cell_high("mdtlogIngridCellHigh","tb_mdtlog_ingrid_cell_high_dd", MdtTablesEnum.mdtlog_ingrid_cell_high),
	mdtlog_ingrid_cell_mid("mdtlogIngridCellMid","tb_mdtlog_ingrid_cell_mid_dd", MdtTablesEnum.mdtlog_ingrid_cell_mid),
	mdtlog_ingrid_cell_low("mdtlogIngridCellLow","tb_mdtlog_ingrid_cell_low_dd", MdtTablesEnum.mdtlog_ingrid_cell_low),
	mdtlog_building_yd_high("mdtlogBuildingYdHigh","tb_mdtlog_building_high_yd_dd", MdtTablesEnum.mdtlog_building_yd_high),
	mdtlog_building_yd_mid("mdtlogBuildingYdMid","tb_mdtlog_building_mid_yd_dd", MdtTablesEnum.mdtlog_building_yd_mid),
	mdtlog_building_yd_low("mdtlogBuildingYdLow","tb_mdtlog_building_low_yd_dd", MdtTablesEnum.mdtlog_building_yd_low),
	mdtlog_building_cell_high("mdtlogBuildingCellHigh","tb_mdtlog_building_cell_high_dd", MdtTablesEnum.mdtlog_building_cell_high),
	mdtlog_building_cell_mid("mdtlogBuildingCellMid","tb_mdtlog_building_cell_mid_dd", MdtTablesEnum.mdtlog_building_cell_mid),
	mdtlog_building_cell_low("mdtlogBuildingCellLow","tb_mdtlog_building_cell_low_dd", MdtTablesEnum.mdtlog_building_cell_low),
	mdtlog_cell_yd("mdtlogCellYd","tb_mdtlog_cell_yd_dd", MdtTablesEnum.mdtlog_cell_yd),
	mdtlog_tac("mdtlogTac","tb_mdtlog_imei_dd", MdtTablesEnum.mdtlog_tac),
	mdtlog_area_sample_yd("mdtlogAreaSampleYd","tb_mdtlog_area_sample_yd_dd", MdtTablesEnum.mdtlog_area_sample_yd),
	mdtlog_area_outgrid_yd("mdtlogAreaOutgridYd","tb_mdtlog_area_outgrid_yd_dd", MdtTablesEnum.mdtlog_area_outgrid_yd),
	mdtlog_area_outgrid_cell("mdtlogAreaOutgridCell","tb_mdtlog_area_outgrid_cell_dd", MdtTablesEnum.mdtlog_area_outgrid_cell),
	mdtlog_area_cell_yd("mdtlogAreaCellYd","tb_mdtlog_area_cell_yd_dd", MdtTablesEnum.mdtlog_area_cell_yd),
	mdtlog_area_yd("mdtlogAreaYd","tb_mdtlog_area_yd_dd", MdtTablesEnum.mdtlog_area_yd),
	mdtlog_outgrid_fullnet_high_ydlt("mdtlogOutgridFullnetHighYdlt","tb_mdtlog_outgrid_fullnet_high_ydlt_dd", MdtTablesEnum.mdtlog_outgrid_fullnet_high_ydlt),
	mdtlog_outgrid_fullnet_mid_ydlt("mdtlogOutgridFullnetMidYdlt","tb_mdtlog_outgrid_fullnet_mid_ydlt_dd", MdtTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt),
	mdtlog_outgrid_fullnet_low_ydlt("mdtlogOutgridFullnetLowYdlt","tb_mdtlog_outgrid_fullnet_low_ydlt_dd", MdtTablesEnum.mdtlog_outgrid_fullnet_low_ydlt),
	mdtlog_outgrid_fullnet_high_yddx("mdtlogOutgridFullnetHighYddx","tb_mdtlog_outgrid_fullnet_high_yddx_dd", MdtTablesEnum.mdtlog_outgrid_fullnet_high_yddx),
	mdtlog_outgrid_fullnet_mid_yddx("mdtlogOutgridFullnetMidYddx","tb_mdtlog_outgrid_fullnet_mid_yddx_dd", MdtTablesEnum.mdtlog_outgrid_fullnet_mid_yddx),
	mdtlog_outgrid_fullnet_low_yddx("mdtlogOutgridFullnetLowYddx","tb_mdtlog_outgrid_fullnet_low_yddx_dd", MdtTablesEnum.mdtlog_outgrid_fullnet_low_yddx),
	mdtlog_ingrid_fullnet_high_ydlt("mdtlogIngridFullnetHighYdlt","tb_mdtlog_ingrid_fullnet_high_ydlt_dd", MdtTablesEnum.mdtlog_ingrid_fullnet_high_ydlt),
	mdtlog_ingrid_fullnet_mid_ydlt("mdtlogIngridFullnetMidYdlt","tb_mdtlog_ingrid_fullnet_mid_ydlt_dd", MdtTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt),
	mdtlog_ingrid_fullnet_low_ydlt("mdtlogIngridFullnetLowYdlt","tb_mdtlog_ingrid_fullnet_low_ydlt_dd", MdtTablesEnum.mdtlog_ingrid_fullnet_low_ydlt),
	mdtlog_ingrid_fullnet_high_yddx("mdtlogIngridFullnetHighYddx","tb_mdtlog_ingrid_fullnet_high_yddx_dd", MdtTablesEnum.mdtlog_ingrid_fullnet_high_yddx),
	mdtlog_ingrid_fullnet_mid_yddx("mdtlogIngridFullnetMidYddx","tb_mdtlog_ingrid_fullnet_mid_yddx_dd", MdtTablesEnum.mdtlog_ingrid_fullnet_mid_yddx),
	mdtlog_ingrid_fullnet_low_yddx("mdtlogIngridFullnetLowYddx","tb_mdtlog_ingrid_fullnet_low_yddx_dd", MdtTablesEnum.mdtlog_ingrid_fullnet_low_yddx),
	mdtlog_building_fullnet_high_ydlt("mdtlogBuildingFullnetHighYdlt","tb_mdtlog_building_fullnet_high_ydlt_dd", MdtTablesEnum.mdtlog_building_fullnet_high_ydlt),
	mdtlog_building_fullnet_mid_ydlt("mdtlogBuildingFullnetMidYdlt","tb_mdtlog_building_fullnet_mid_ydlt_dd", MdtTablesEnum.mdtlog_building_fullnet_mid_ydlt),
	mdtlog_building_fullnet_low_ydlt("mdtlogBuildingFullnetLowYdlt","tb_mdtlog_building_fullnet_low_ydlt_dd", MdtTablesEnum.mdtlog_building_fullnet_low_ydlt),
	mdtlog_building_fullnet_high_yddx("mdtlogBuildingFullnetHighYddx","tb_mdtlog_building_fullnet_high_yddx_dd", MdtTablesEnum.mdtlog_building_fullnet_high_yddx),
	mdtlog_building_fullnet_mid_yddx("mdtlogBuildingFullnetMidYddx","tb_mdtlog_building_fullnet_mid_yddx_dd", MdtTablesEnum.mdtlog_building_fullnet_mid_yddx),
	mdtlog_building_fullnet_low_yddx("mdtlogBuildingFullnetLowYddx","tb_mdtlog_building_fullnet_low_yddx_dd", MdtTablesEnum.mdtlog_building_fullnet_low_yddx),
	mdtlog_cell_fullnet_ydlt("mdtlogCellFullnetYdlt","tb_mdtlog_cell_fullnet_ydlt_dd", MdtTablesEnum.mdtlog_cell_fullnet_ydlt),
	mdtlog_cell_fullnet_yddx("mdtlogCellFullnetYddx","tb_mdtlog_cell_fullnet_yddx_dd", MdtTablesEnum.mdtlog_cell_fullnet_yddx),
	mdtlog_area_outgrid_fullnet_ydlt("mdtlogAreaOutgridFullnetYdlt","tb_mdtlog_area_outgrid_fullnet_ydlt_dd", MdtTablesEnum.mdtlog_area_outgrid_fullnet_ydlt),
	mdtlog_area_outgrid_fullnet_yddx("mdtlogAreaOutgridFullnetYddx","tb_mdtlog_area_outgrid_fullnet_yddx_dd", MdtTablesEnum.mdtlog_area_outgrid_fullnet_yddx),
	mdtlog_area_cell_fullnet_ydlt("mdtlogAreaCellFullnetYdlt","tb_mdtlog_area_cell_fullnet_ydlt_dd", MdtTablesEnum.mdtlog_area_cell_fullnet_ydlt),
	mdtlog_area_cell_fullnet_yddx("mdtlogAreaCellFullnetYddx","tb_mdtlog_area_cell_fullnet_yddx_dd", MdtTablesEnum.mdtlog_area_cell_fullnet_yddx),
	mdtlog_area_fullnet_ydlt("mdtlogAreaFullnetYdlt","tb_mdtlog_area_fullnet_ydlt_dd", MdtTablesEnum.mdtlog_area_fullnet_ydlt),
	mdtlog_area_fullnet_yddx("mdtlogAreaFullnetYddx","tb_mdtlog_area_fullnet_yddx_dd", MdtTablesEnum.mdtlog_area_fullnet_yddx),
	mdtlog_outgrid_dx_high("mdtlogOutgridDxHigh","tb_mdtlog_outgrid_high_dx_dd", MdtTablesEnum.mdtlog_outgrid_dx_high),
	mdtlog_outgrid_dx_mid("mdtlogOutgridDxMid","tb_mdtlog_outgrid_mid_dx_dd", MdtTablesEnum.mdtlog_outgrid_dx_mid),
	mdtlog_outgrid_dx_low("mdtlogOutgridDxLow","tb_mdtlog_outgrid_low_dx_dd", MdtTablesEnum.mdtlog_outgrid_dx_low),
	mdtlog_ingrid_dx_high("mdtlogIngridDxHigh","tb_mdtlog_ingrid_high_dx_dd", MdtTablesEnum.mdtlog_ingrid_dx_high),
	mdtlog_ingrid_dx_mid("mdtlogIngridDxMid","tb_mdtlog_ingrid_mid_dx_dd", MdtTablesEnum.mdtlog_ingrid_dx_mid),
	mdtlog_ingrid_dx_low("mdtlogIngridDxLow","tb_mdtlog_ingrid_low_dx_dd", MdtTablesEnum.mdtlog_ingrid_dx_low),
	mdtlog_building_dx_high("mdtlogBuildingDxHigh","tb_mdtlog_building_high_dx_dd", MdtTablesEnum.mdtlog_building_dx_high),
	mdtlog_building_dx_mid("mdtlogBuildingDxMid","tb_mdtlog_building_mid_dx_dd", MdtTablesEnum.mdtlog_building_dx_mid),
	mdtlog_building_dx_low("mdtlogBuildingDxLow","tb_mdtlog_building_low_dx_dd", MdtTablesEnum.mdtlog_building_dx_low),
	mdtlog_cell_dx("mdtlogCellDx","tb_mdtlog_cell_dx_dd", MdtTablesEnum.mdtlog_cell_dx),
	mdtlog_area_outgrid_dx("mdtlogAreaOutgridDx","tb_mdtlog_area_outgrid_dx_dd", MdtTablesEnum.mdtlog_area_outgrid_dx),
	mdtlog_area_cell_dx("mdtlogAreaCellDx","tb_mdtlog_area_cell_dx_dd", MdtTablesEnum.mdtlog_area_cell_dx),
	mdtlog_area_dx("mdtlogAreaDx","tb_mdtlog_area_dx_dd", MdtTablesEnum.mdtlog_area_dx),
	mdtlog_outgrid_lt_high("mdtlogOutgridLtHigh","tb_mdtlog_outgrid_high_lt_dd", MdtTablesEnum.mdtlog_outgrid_lt_high),
	mdtlog_outgrid_lt_mid("mdtlogOutgridLtMid","tb_mdtlog_outgrid_mid_lt_dd", MdtTablesEnum.mdtlog_outgrid_lt_mid),
	mdtlog_outgrid_lt_low("mdtlogOutgridLtLow","tb_mdtlog_outgrid_low_lt_dd", MdtTablesEnum.mdtlog_outgrid_lt_low),
	mdtlog_ingrid_lt_high("mdtlogIngridLtHigh","tb_mdtlog_ingrid_high_lt_dd", MdtTablesEnum.mdtlog_ingrid_lt_high),
	mdtlog_ingrid_lt_mid("mdtlogIngridLtMid","tb_mdtlog_ingrid_mid_lt_dd", MdtTablesEnum.mdtlog_ingrid_lt_mid),
	mdtlog_ingrid_lt_low("mdtlogIngridLtLow","tb_mdtlog_ingrid_low_lt_dd", MdtTablesEnum.mdtlog_ingrid_lt_low),
	mdtlog_building_lt_high("mdtlogBuildingLtHigh","tb_mdtlog_building_high_lt_dd", MdtTablesEnum.mdtlog_building_lt_high),
	mdtlog_building_lt_mid("mdtlogBuildingLtMid","tb_mdtlog_building_mid_lt_dd", MdtTablesEnum.mdtlog_building_lt_mid),
	mdtlog_building_lt_low("mdtlogBuildingLtLow","tb_mdtlog_building_low_lt_dd", MdtTablesEnum.mdtlog_building_lt_low),
	mdtlog_cell_lt("mdtlogCellLt","tb_mdtlog_cell_lt_dd", MdtTablesEnum.mdtlog_cell_lt),
	mdtlog_area_outgrid_lt("mdtlogAreaOutgridLt","tb_mdtlog_area_outgrid_lt_dd", MdtTablesEnum.mdtlog_area_outgrid_lt),
	mdtlog_area_cell_lt("mdtlogAreaCellLt","tb_mdtlog_area_cell_lt_dd", MdtTablesEnum.mdtlog_area_cell_lt),
	mdtlog_area_lt("mdtlogAreaLt","tb_mdtlog_area_lt_dd", MdtTablesEnum.mdtlog_area_lt),

	/*基于高铁进行统计的表格*/
	//高铁栅格统计
	mroHsrYdGrid("hsrYdGrid","tb_hsr_data_grid_yd_dd", MroBsTablesEnum.mroHsrYdGrid),
	//高铁小区栅格统计
	mroHsrYdGridCell("hsrYdGridCell","tb_hsr_data_cellgrid_yd_dd", MroBsTablesEnum.mroHsrYdGridCell),
	//高铁小区统计
	mroHsrYdCell("hsrYdCell","tb_hsr_data_cell_yd_dd", MroBsTablesEnum.mroHsrYdCell),
    // 高铁车次路段
	mroHsrYdTrainSeg("hsrYdTrainSeg","tb_hsr_data_trainsegment_yd_dd", MroBsTablesEnum.mroHsrYdTrainSeg),
    //高铁车次路段小区
	mroHsrYdTrainSegCell("hsrYdTrainSegCell","tb_hsr_data_trainsegment_cell_yd_dd", MroBsTablesEnum.mroHsrYdTrainSegCell),
    //联通高铁栅格
	mroHsrLtGrid("hsrLtGrid","tb_hsr_data_grid_lt_dd", MroBsTablesEnum.mroHsrLtGrid),
	// 联通车次路段
	mroHsrLtTrainSeg("hsrLtTrainSeg","tb_hsr_data_trainsegment_lt_dd", MroBsTablesEnum.mroHsrLtTrainSeg),
	//电信栅格统计
	mroHsrDxGrid("hsrDxGrid","tb_hsr_data_grid_dx_dd", MroBsTablesEnum.mroHsrDxGrid),
	//电信车次路段
	mroHsrDxTrainSeg("hsrDxTrainSeg","tb_hsr_data_trainsegment_dx_dd", MroBsTablesEnum.mroHsrDxTrainSeg)

	// 高铁室外栅格
	,HSRGRIDEVT("hsrEvtGrid", "/tb_hsr_evt_grid_yd_dd", TypeIoEvtEnum.HSRGRIDEVT)
    // 高铁室外栅格小区
	,HSRCELLGRIDEVT("hsrEvtCellGrid", "/tb_hsr_evt_cellgrid_yd_dd", TypeIoEvtEnum.HSRCELLGRIDEVT)
    // 高铁小区
	,HSRCELLEVT("hsrEvtCell", "/tb_hsr_evt_cell_yd_dd", TypeIoEvtEnum.HSRCELLEVT)
    // 高铁路段
	,HSRTRAINSEGEVT("hsrEvtTrainseg", "/tb_hsr_evt_trainsegment_yd_dd", TypeIoEvtEnum.HSRTRAINSEGEVT)
    // 高铁路段小区
	,HSRTRAINSEGCELLEVT("hsrEvtTrainsegCell", "/tb_hsr_evt_trainsegment_cell_yd_dd", TypeIoEvtEnum.HSRTRAINSEGCELLEVT)
	// XDR小区统计
	,XDR_CELL("xdrCell", "/tb_xdr_cell_dd", TypeIoEvtEnum.XDR_CELL),
	//用户xdr，mro统计
	xdrCountStat("xdrCount", "tb_xdr_user_count_yd_dd", MroBsTablesEnum.xdrCountStat),
	//用户小区5分钟统计
	mroUserCellBy5Mins("userCellex", "tb_mr_user_cell_yd_5md", MroBsTablesEnum.mroUserCellByMins);
	private String fileName;
	private String dirName;
	public int index;
	
	private  MergeStatTablesEnum(String fileName, String dirName, IOutPutPathEnum ...mergeStatTable) 
	{
		this.fileName = fileName;
		this.dirName = dirName;
		this.index = mergeStatTable[0].getIndex();
	}

	@Override
	public String getPath(String hPath, String outData) 
	{
		return getBasePath(hPath, outData) + "/" + dirName + "_" + outData;
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
		return hPath + "/mergestat/data_01_" + date;
	}
	
	public static String getHourBasePath(String hPath, String date, String hour)
	{
		if (hour == null)
			return getBasePath(hPath, date);
		return hPath + "/mergestat/data_01_" + date + "/" + hour; 
	}
	@Override
	public String getHourPath(String hPath, String outData, String hour) 
	{
		// TODO Auto-generated method stub
		if (hour == null)
			return getBasePath(hPath, outData);
		return hPath + "/mergestat/data_01_" + outData + "/" + hour;
	}

}
