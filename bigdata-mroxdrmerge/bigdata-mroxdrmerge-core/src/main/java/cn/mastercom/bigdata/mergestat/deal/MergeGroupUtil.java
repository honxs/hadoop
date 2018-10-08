package cn.mastercom.bigdata.mergestat.deal;

import java.util.ArrayList;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.mdt.stat.MdtTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class MergeGroupUtil {
	public static ArrayList<MergeInputStruct> addInputPath(String mroXdrMergePath, String date) {

		ArrayList<MergeInputStruct> inputPath = new ArrayList<MergeInputStruct>();
		
		MergeInputStruct inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.griddt.getIndex(),
				MroCsOTTTableEnum.griddt.getPath(mroXdrMergePath, date) + "," + XdrLocTablesEnum.xdrgriddt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.gridcqt.getIndex(),
				MroCsOTTTableEnum.gridcqt.getPath(mroXdrMergePath, date) + "," + XdrLocTablesEnum.xdrgridcqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.mrogrid.getIndex(), MroCsOTTTableEnum.mrogrid.getPath(mroXdrMergePath, date) + "," + XdrLocTablesEnum.xdrgrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.mrocell.getIndex(), MroCsOTTTableEnum.mrocell.getPath(mroXdrMergePath, date) + "," + XdrLocTablesEnum.xdrcell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.mrocellgrid.getIndex(),
				MroCsOTTTableEnum.mrocellgrid.getPath(mroXdrMergePath, date) + "," + XdrLocTablesEnum.xdrcellgrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + XdrLocTablesEnum.xdrgriduserhour.getIndex(), XdrLocTablesEnum.xdrgriduserhour.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.griddtfreq.getIndex(), MroCsOTTTableEnum.griddtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.gridcqtfreq.getIndex(), MroCsOTTTableEnum.gridcqtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.mrocellfreq.getIndex(), MroCsOTTTableEnum.mrocellfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.useractcell.getIndex(), MroCsOTTTableEnum.useractcell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tenmrogrid.getIndex(), MroCsOTTTableEnum.tenmrogrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tengriddt.getIndex(), MroCsOTTTableEnum.tengriddt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tengridcqt.getIndex(), MroCsOTTTableEnum.tengridcqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tenmrocellgrid.getIndex(), MroCsOTTTableEnum.tenmrocellgrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tengriddtfreq.getIndex(), MroCsOTTTableEnum.tengriddtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tengridcqtfreq.getIndex(), MroCsOTTTableEnum.tengridcqtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.LTtenFreqByImeiDt.getIndex(), MroCsOTTTableEnum.LTtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.LTtenFreqByImeiCqt.getIndex(), MroCsOTTTableEnum.LTtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tencellgriddt.getIndex(), MroCsOTTTableEnum.tencellgriddt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.tencellgridcqt.getIndex(), MroCsOTTTableEnum.tencellgridcqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.DXtenFreqByImeiDt.getIndex(), MroCsOTTTableEnum.DXtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.DXtenFreqByImeiCqt.getIndex(), MroCsOTTTableEnum.DXtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.LTfreqcellByImei.getIndex(), MroCsOTTTableEnum.LTfreqcellByImei.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsOTTTableEnum.DXfreqcellByImei.getIndex(), MroCsOTTTableEnum.DXfreqcellByImei.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpmrogrid.getIndex(), MroCsFgTableEnum.fpmrogrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpgriddt.getIndex(), MroCsFgTableEnum.fpgriddt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpgridcqt.getIndex(), MroCsFgTableEnum.fpgridcqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpmrocellgrid.getIndex(), MroCsFgTableEnum.fpmrocellgrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpgriddtfreq.getIndex(), MroCsFgTableEnum.fpgriddtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpgridcqtfreq.getIndex(), MroCsFgTableEnum.fpgridcqtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptenmrogrid.getIndex(), MroCsFgTableEnum.fptenmrogrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptengriddt.getIndex(), MroCsFgTableEnum.fptengriddt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptengridcqt.getIndex(), MroCsFgTableEnum.fptengridcqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptenmrocellgrid.getIndex(), MroCsFgTableEnum.fptenmrocellgrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptengriddtfreq.getIndex(), MroCsFgTableEnum.fptengriddtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptengridcqtfreq.getIndex(), MroCsFgTableEnum.fptengridcqtfreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpLTtenFreqByImeiDt.getIndex(), MroCsFgTableEnum.fpLTtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpLTtenFreqByImeiCqt.getIndex(), MroCsFgTableEnum.fpLTtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		// yzx update at 2017/12/27 -->
		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptencellgriddt.getIndex(), MroCsFgTableEnum.fptencellgriddt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fptencellgridcqt.getIndex(), MroCsFgTableEnum.fptencellgridcqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpLTfreqcellByImei.getIndex(), MroCsFgTableEnum.fpLTfreqcellByImei.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpDXfreqcellByImei.getIndex(), MroCsFgTableEnum.fpDXfreqcellByImei.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpDXtenFreqByImeiDt.getIndex(), MroCsFgTableEnum.fpDXtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroCsFgTableEnum.fpDXtenFreqByImeiCqt.getIndex(), MroCsFgTableEnum.fpDXtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdOutgridHigh.getIndex(), MroBsTablesEnum.mroYdOutgridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdOutgridMid.getIndex(), MroBsTablesEnum.mroYdOutgridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdOutgridLow.getIndex(), MroBsTablesEnum.mroYdOutgridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroOutgridCellHigh.getIndex(), MroBsTablesEnum.mroOutgridCellHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroOutgridCellMid.getIndex(), MroBsTablesEnum.mroOutgridCellMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroOutgridCellLow.getIndex(), MroBsTablesEnum.mroOutgridCellLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdIngridHigh.getIndex(), MroBsTablesEnum.mroYdIngridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdIngridMid.getIndex(), MroBsTablesEnum.mroYdIngridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdIngridLow.getIndex(), MroBsTablesEnum.mroYdIngridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroIngridCellHigh.getIndex(), MroBsTablesEnum.mroIngridCellHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroIngridCellMid.getIndex(), MroBsTablesEnum.mroIngridCellMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroIngridCellLow.getIndex(), MroBsTablesEnum.mroIngridCellLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdBuildHigh.getIndex(), MroBsTablesEnum.mroYdBuildHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdBuildMid.getIndex(), MroBsTablesEnum.mroYdBuildMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdBuildLow.getIndex(), MroBsTablesEnum.mroYdBuildLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		//yzx add BuildPos at 20180313
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdBuildPosHigh.getIndex(), MroBsTablesEnum.mroYdBuildPosHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdBuildPosMid.getIndex(), MroBsTablesEnum.mroYdBuildPosMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdBuildPosLow.getIndex(), MroBsTablesEnum.mroYdBuildPosLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdCell.getIndex(), MroBsTablesEnum.mroYdCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxOutgridHigh.getIndex(), MroBsTablesEnum.mroDxOutgridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxOutgridMid.getIndex(), MroBsTablesEnum.mroDxOutgridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxOutgridLow.getIndex(), MroBsTablesEnum.mroDxOutgridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxIngridHigh.getIndex(), MroBsTablesEnum.mroDxIngridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxIngridMid.getIndex(), MroBsTablesEnum.mroDxIngridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxIngridLow.getIndex(), MroBsTablesEnum.mroDxIngridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxBuildHigh.getIndex(), MroBsTablesEnum.mroDxBuildHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxBuildMid.getIndex(), MroBsTablesEnum.mroDxBuildMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxBuildLow.getIndex(), MroBsTablesEnum.mroDxBuildLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxCell.getIndex(), MroBsTablesEnum.mroDxCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtOutgridHigh.getIndex(), MroBsTablesEnum.mroLtOutgridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtOutgridMid.getIndex(), MroBsTablesEnum.mroLtOutgridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtOutgridLow.getIndex(), MroBsTablesEnum.mroLtOutgridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtIngridHigh.getIndex(), MroBsTablesEnum.mroLtIngridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtIngridMid.getIndex(), MroBsTablesEnum.mroLtIngridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtIngridLow.getIndex(), MroBsTablesEnum.mroLtIngridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtBuildHigh.getIndex(), MroBsTablesEnum.mroLtBuildHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtBuildMid.getIndex(), MroBsTablesEnum.mroLtBuildMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtBuildLow.getIndex(), MroBsTablesEnum.mroLtBuildLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtCell.getIndex(), MroBsTablesEnum.mroLtCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildCellHigh.getIndex(), MroBsTablesEnum.mroBuildCellHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildCellMid.getIndex(), MroBsTablesEnum.mroBuildCellMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildCellLow.getIndex(), MroBsTablesEnum.mroBuildCellLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		//yzx add BuildCellPos at 20180313
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildCellPosHigh.getIndex(), MroBsTablesEnum.mroBuildCellPosHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildCellPosMid.getIndex(), MroBsTablesEnum.mroBuildCellPosMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildCellPosLow.getIndex(), MroBsTablesEnum.mroBuildCellPosLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdAreaGridCell.getIndex(), MroBsTablesEnum.mroYdAreaGridCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdAreaCell.getIndex(), MroBsTablesEnum.mroYdAreaCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdAreaGrid.getIndex(), MroBsTablesEnum.mroYdAreaGrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdArea.getIndex(), MroBsTablesEnum.mroYdArea.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		// yzx add LT and DX AREA 2017.9.18
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtAreaCell.getIndex(), MroBsTablesEnum.mroLtAreaCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtAreaGrid.getIndex(), MroBsTablesEnum.mroLtAreaGrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtArea.getIndex(), MroBsTablesEnum.mroLtArea.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxAreaCell.getIndex(), MroBsTablesEnum.mroDxAreaCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxAreaGrid.getIndex(), MroBsTablesEnum.mroDxAreaGrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxArea.getIndex(), MroBsTablesEnum.mroDxArea.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		// yzx add EVENT 2017.9.26
		String eventPath = "%1$s/xdr_locall/data_01_%2$s/imsi%3$s%2$s,%1$s/xdr_locall/data_01_%2$s/s1apid%3$s%2$s,%1$s/mro_loc/data_01_%2$s%3$s%2$s";
		if(MainModel.GetInstance().getCompile().Assert(CompileMark.SiChuan)){
			eventPath = "";
			String[] hours = new String[] { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
					"12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23" };
			for (int i = 0; i <24 ; i++) {
				eventPath +="%1$s/xdr_locall/data_01_%2$s/"+hours[i]+"/imsi%3$s%2$s," +
						"%1$s/xdr_locall/data_01_%2$s/"+hours[i]+"/s1apid%3$s%2$s,";
			}
			eventPath = eventPath+"%1$s/mro_loc/data_01_%2$s%3$s%2$s";
		}

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.CELLEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.CELLEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.IHCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.IHCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.IMCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.IMCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.ILCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.ILCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.IHGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.IHGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.IMGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.IMGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.ILGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.ILGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.OHCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.OHCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.OMCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.OMCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.OLCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.OLCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.OHGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.OHGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.OMGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.OMGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.OLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.OLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BHCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BHCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BMCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BMCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BLCELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BLCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);
		
		//yzx add EventBuildCellPos at 20180313
		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BHCELLPOSEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BHCELLPOSEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BMCELLPOSEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BMCELLPOSEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BLCELLPOSEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BLCELLPOSEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BHGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BHGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BMGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BMGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);
		
		//yzx add EventBuildPos at 20180313
		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BHPOSEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BHPOSEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BMPOSEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BMPOSEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.BLPOSEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.BLPOSEVT.getDirName()));
		inputPath.add(inputInfo);

		// EVENT_AREA
		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.AREAEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.AREAEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.AREAGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.AREAGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.AREACELLGRIDEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.AREACELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + TypeIoEvtEnum.AREACELLEVT.getIndex(), String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.AREACELLEVT.getDirName()));
		inputPath.add(inputInfo);

		// 20170921 add fullnet
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltOutgridHigh.getIndex(), MroBsTablesEnum.mroYdltOutgridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltOutgridMid.getIndex(), MroBsTablesEnum.mroYdltOutgridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltOutgridLow.getIndex(), MroBsTablesEnum.mroYdltOutgridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxOutgridHigh.getIndex(), MroBsTablesEnum.mroYddxOutgridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxOutgridMid.getIndex(), MroBsTablesEnum.mroYddxOutgridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxOutgridLow.getIndex(), MroBsTablesEnum.mroYddxOutgridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltIngridHigh.getIndex(), MroBsTablesEnum.mroYdltIngridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltIngridMid.getIndex(), MroBsTablesEnum.mroYdltIngridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltIngridLow.getIndex(), MroBsTablesEnum.mroYdltIngridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxIngridHigh.getIndex(), MroBsTablesEnum.mroYddxIngridHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxIngridMid.getIndex(), MroBsTablesEnum.mroYddxIngridMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxIngridLow.getIndex(), MroBsTablesEnum.mroYddxIngridLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltBuildingHigh.getIndex(), MroBsTablesEnum.mroYdltBuildingHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltBuildingMid.getIndex(), MroBsTablesEnum.mroYdltBuildingMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltBuildingLow.getIndex(), MroBsTablesEnum.mroYdltBuildingLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxBuildingHigh.getIndex(), MroBsTablesEnum.mroYddxBuildingHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxBuildingMid.getIndex(), MroBsTablesEnum.mroYddxBuildingMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxBuildingLow.getIndex(), MroBsTablesEnum.mroYddxBuildingLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltCell.getIndex(), MroBsTablesEnum.mroYdltCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxCell.getIndex(), MroBsTablesEnum.mroYddxCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltAreaGrid.getIndex(), MroBsTablesEnum.mroYdltAreaGrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxAreaGrid.getIndex(), MroBsTablesEnum.mroYddxAreaGrid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltAreaCell.getIndex(), MroBsTablesEnum.mroYdltAreaCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxAreaCell.getIndex(), MroBsTablesEnum.mroYddxAreaCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdltArea.getIndex(), MroBsTablesEnum.mroYdltArea.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYddxArea.getIndex(), MroBsTablesEnum.mroYddxArea.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroUserCell.getIndex(), MroBsTablesEnum.mroUserCell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		// imeiYd/imeiLt/imeiDx
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mrImeiYd.getIndex(), MroBsTablesEnum.mrImeiYd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mrImeiLt.getIndex(), MroBsTablesEnum.mrImeiLt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mrImeiDx.getIndex(), MroBsTablesEnum.mrImeiDx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		// mdtmerge
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_yd_high.getIndex(), MdtTablesEnum.mdtimm_outgrid_yd_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_yd_mid.getIndex(), MdtTablesEnum.mdtimm_outgrid_yd_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_yd_low.getIndex(), MdtTablesEnum.mdtimm_outgrid_yd_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_cell_high.getIndex(), MdtTablesEnum.mdtimm_outgrid_cell_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_cell_mid.getIndex(), MdtTablesEnum.mdtimm_outgrid_cell_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_cell_low.getIndex(), MdtTablesEnum.mdtimm_outgrid_cell_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_yd_high.getIndex(), MdtTablesEnum.mdtimm_ingrid_yd_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_yd_mid.getIndex(), MdtTablesEnum.mdtimm_ingrid_yd_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_yd_low.getIndex(), MdtTablesEnum.mdtimm_ingrid_yd_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_cell_high.getIndex(), MdtTablesEnum.mdtimm_ingrid_cell_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_cell_mid.getIndex(), MdtTablesEnum.mdtimm_ingrid_cell_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_cell_low.getIndex(), MdtTablesEnum.mdtimm_ingrid_cell_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_yd_high.getIndex(), MdtTablesEnum.mdtimm_building_yd_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_yd_mid.getIndex(), MdtTablesEnum.mdtimm_building_yd_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_yd_low.getIndex(), MdtTablesEnum.mdtimm_building_yd_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_cell_high.getIndex(), MdtTablesEnum.mdtimm_building_cell_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_cell_mid.getIndex(), MdtTablesEnum.mdtimm_building_cell_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_cell_low.getIndex(), MdtTablesEnum.mdtimm_building_cell_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_cell_yd.getIndex(), MdtTablesEnum.mdtimm_cell_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_outgrid_yd.getIndex(), MdtTablesEnum.mdtimm_area_outgrid_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_outgrid_cell.getIndex(), MdtTablesEnum.mdtimm_area_outgrid_cell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_cell_yd.getIndex(), MdtTablesEnum.mdtimm_area_cell_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_yd.getIndex(), MdtTablesEnum.mdtimm_area_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getIndex(), MdtTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getIndex(), MdtTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getIndex(), MdtTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getIndex(), MdtTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getIndex(), MdtTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getIndex(), MdtTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getIndex(), MdtTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getIndex(), MdtTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getIndex(), MdtTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getIndex(), MdtTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getIndex(), MdtTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getIndex(), MdtTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_fullnet_high_ydlt.getIndex(), MdtTablesEnum.mdtimm_building_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_fullnet_mid_ydlt.getIndex(), MdtTablesEnum.mdtimm_building_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_fullnet_low_ydlt.getIndex(), MdtTablesEnum.mdtimm_building_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_fullnet_high_yddx.getIndex(), MdtTablesEnum.mdtimm_building_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_fullnet_mid_yddx.getIndex(), MdtTablesEnum.mdtimm_building_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_fullnet_low_yddx.getIndex(), MdtTablesEnum.mdtimm_building_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_cell_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtimm_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_cell_fullnet_yddx.getIndex(), MdtTablesEnum.mdtimm_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getIndex(), MdtTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_cell_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtimm_area_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_cell_fullnet_yddx.getIndex(), MdtTablesEnum.mdtimm_area_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtimm_area_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_fullnet_yddx.getIndex(), MdtTablesEnum.mdtimm_area_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_dx_high.getIndex(), MdtTablesEnum.mdtimm_outgrid_dx_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_dx_mid.getIndex(), MdtTablesEnum.mdtimm_outgrid_dx_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_dx_low.getIndex(), MdtTablesEnum.mdtimm_outgrid_dx_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_dx_high.getIndex(), MdtTablesEnum.mdtimm_ingrid_dx_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_dx_mid.getIndex(), MdtTablesEnum.mdtimm_ingrid_dx_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_dx_low.getIndex(), MdtTablesEnum.mdtimm_ingrid_dx_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_dx_high.getIndex(), MdtTablesEnum.mdtimm_building_dx_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_dx_mid.getIndex(), MdtTablesEnum.mdtimm_building_dx_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_dx_low.getIndex(), MdtTablesEnum.mdtimm_building_dx_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_cell_dx.getIndex(), MdtTablesEnum.mdtimm_cell_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_outgrid_dx.getIndex(), MdtTablesEnum.mdtimm_area_outgrid_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_cell_dx.getIndex(), MdtTablesEnum.mdtimm_area_cell_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_dx.getIndex(), MdtTablesEnum.mdtimm_area_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_lt_high.getIndex(), MdtTablesEnum.mdtimm_outgrid_lt_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_lt_mid.getIndex(), MdtTablesEnum.mdtimm_outgrid_lt_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_outgrid_lt_low.getIndex(), MdtTablesEnum.mdtimm_outgrid_lt_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_lt_high.getIndex(), MdtTablesEnum.mdtimm_ingrid_lt_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_lt_mid.getIndex(), MdtTablesEnum.mdtimm_ingrid_lt_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_ingrid_lt_low.getIndex(), MdtTablesEnum.mdtimm_ingrid_lt_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_lt_high.getIndex(), MdtTablesEnum.mdtimm_building_lt_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_lt_mid.getIndex(), MdtTablesEnum.mdtimm_building_lt_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_building_lt_low.getIndex(), MdtTablesEnum.mdtimm_building_lt_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_cell_lt.getIndex(), MdtTablesEnum.mdtimm_cell_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_outgrid_lt.getIndex(), MdtTablesEnum.mdtimm_area_outgrid_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_cell_lt.getIndex(), MdtTablesEnum.mdtimm_area_cell_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_area_lt.getIndex(), MdtTablesEnum.mdtimm_area_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_yd_high.getIndex(), MdtTablesEnum.mdtlog_outgrid_yd_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_yd_mid.getIndex(), MdtTablesEnum.mdtlog_outgrid_yd_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_yd_low.getIndex(), MdtTablesEnum.mdtlog_outgrid_yd_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_cell_high.getIndex(), MdtTablesEnum.mdtlog_outgrid_cell_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_cell_mid.getIndex(), MdtTablesEnum.mdtlog_outgrid_cell_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_cell_low.getIndex(), MdtTablesEnum.mdtlog_outgrid_cell_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_yd_high.getIndex(), MdtTablesEnum.mdtlog_ingrid_yd_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_yd_mid.getIndex(), MdtTablesEnum.mdtlog_ingrid_yd_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_yd_low.getIndex(), MdtTablesEnum.mdtlog_ingrid_yd_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_cell_high.getIndex(), MdtTablesEnum.mdtlog_ingrid_cell_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_cell_mid.getIndex(), MdtTablesEnum.mdtlog_ingrid_cell_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_cell_low.getIndex(), MdtTablesEnum.mdtlog_ingrid_cell_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_yd_high.getIndex(), MdtTablesEnum.mdtlog_building_yd_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_yd_mid.getIndex(), MdtTablesEnum.mdtlog_building_yd_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_yd_low.getIndex(), MdtTablesEnum.mdtlog_building_yd_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_cell_high.getIndex(), MdtTablesEnum.mdtlog_building_cell_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_cell_mid.getIndex(), MdtTablesEnum.mdtlog_building_cell_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_cell_low.getIndex(), MdtTablesEnum.mdtlog_building_cell_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_cell_yd.getIndex(), MdtTablesEnum.mdtlog_cell_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_outgrid_yd.getIndex(), MdtTablesEnum.mdtlog_area_outgrid_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_outgrid_cell.getIndex(), MdtTablesEnum.mdtlog_area_outgrid_cell.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_cell_yd.getIndex(), MdtTablesEnum.mdtlog_area_cell_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_yd.getIndex(), MdtTablesEnum.mdtlog_area_yd.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_fullnet_high_ydlt.getIndex(), MdtTablesEnum.mdtlog_outgrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt.getIndex(), MdtTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_fullnet_low_ydlt.getIndex(), MdtTablesEnum.mdtlog_outgrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_fullnet_high_yddx.getIndex(), MdtTablesEnum.mdtlog_outgrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_fullnet_mid_yddx.getIndex(), MdtTablesEnum.mdtlog_outgrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_fullnet_low_yddx.getIndex(), MdtTablesEnum.mdtlog_outgrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_fullnet_high_ydlt.getIndex(), MdtTablesEnum.mdtlog_ingrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt.getIndex(), MdtTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_fullnet_low_ydlt.getIndex(), MdtTablesEnum.mdtlog_ingrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_fullnet_high_yddx.getIndex(), MdtTablesEnum.mdtlog_ingrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_fullnet_mid_yddx.getIndex(), MdtTablesEnum.mdtlog_ingrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_fullnet_low_yddx.getIndex(), MdtTablesEnum.mdtlog_ingrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_fullnet_high_ydlt.getIndex(), MdtTablesEnum.mdtlog_building_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_fullnet_mid_ydlt.getIndex(), MdtTablesEnum.mdtlog_building_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_fullnet_low_ydlt.getIndex(), MdtTablesEnum.mdtlog_building_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_fullnet_high_yddx.getIndex(), MdtTablesEnum.mdtlog_building_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_fullnet_mid_yddx.getIndex(), MdtTablesEnum.mdtlog_building_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_fullnet_low_yddx.getIndex(), MdtTablesEnum.mdtlog_building_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_cell_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtlog_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_cell_fullnet_yddx.getIndex(), MdtTablesEnum.mdtlog_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_outgrid_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtlog_area_outgrid_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_outgrid_fullnet_yddx.getIndex(), MdtTablesEnum.mdtlog_area_outgrid_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_cell_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtlog_area_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_cell_fullnet_yddx.getIndex(), MdtTablesEnum.mdtlog_area_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_fullnet_ydlt.getIndex(), MdtTablesEnum.mdtlog_area_fullnet_ydlt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_fullnet_yddx.getIndex(), MdtTablesEnum.mdtlog_area_fullnet_yddx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_dx_high.getIndex(), MdtTablesEnum.mdtlog_outgrid_dx_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_dx_mid.getIndex(), MdtTablesEnum.mdtlog_outgrid_dx_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_dx_low.getIndex(), MdtTablesEnum.mdtlog_outgrid_dx_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_dx_high.getIndex(), MdtTablesEnum.mdtlog_ingrid_dx_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_dx_mid.getIndex(), MdtTablesEnum.mdtlog_ingrid_dx_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_dx_low.getIndex(), MdtTablesEnum.mdtlog_ingrid_dx_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_dx_high.getIndex(), MdtTablesEnum.mdtlog_building_dx_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_dx_mid.getIndex(), MdtTablesEnum.mdtlog_building_dx_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_dx_low.getIndex(), MdtTablesEnum.mdtlog_building_dx_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_cell_dx.getIndex(), MdtTablesEnum.mdtlog_cell_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_outgrid_dx.getIndex(), MdtTablesEnum.mdtlog_area_outgrid_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_cell_dx.getIndex(), MdtTablesEnum.mdtlog_area_cell_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_dx.getIndex(), MdtTablesEnum.mdtlog_area_dx.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_lt_high.getIndex(), MdtTablesEnum.mdtlog_outgrid_lt_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_lt_mid.getIndex(), MdtTablesEnum.mdtlog_outgrid_lt_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_outgrid_lt_low.getIndex(), MdtTablesEnum.mdtlog_outgrid_lt_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_lt_high.getIndex(), MdtTablesEnum.mdtlog_ingrid_lt_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_lt_mid.getIndex(), MdtTablesEnum.mdtlog_ingrid_lt_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_ingrid_lt_low.getIndex(), MdtTablesEnum.mdtlog_ingrid_lt_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_lt_high.getIndex(), MdtTablesEnum.mdtlog_building_lt_high.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_lt_mid.getIndex(), MdtTablesEnum.mdtlog_building_lt_mid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_building_lt_low.getIndex(), MdtTablesEnum.mdtlog_building_lt_low.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_cell_lt.getIndex(), MdtTablesEnum.mdtlog_cell_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_outgrid_lt.getIndex(), MdtTablesEnum.mdtlog_area_outgrid_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_cell_lt.getIndex(), MdtTablesEnum.mdtlog_area_cell_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_area_lt.getIndex(), MdtTablesEnum.mdtlog_area_lt.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtimm_tac.getIndex(), MdtTablesEnum.mdtimm_tac.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		inputInfo = new MergeInputStruct("" + MdtTablesEnum.mdtlog_tac.getIndex(), MdtTablesEnum.mdtlog_tac.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdOutgridHighAllFreq.getIndex(), MroBsTablesEnum.mroYdOutgridHighAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdOutgridMidAllFreq.getIndex(), MroBsTablesEnum.mroYdOutgridMidAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdOutgridLowAllFreq.getIndex(), MroBsTablesEnum.mroYdOutgridLowAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxOutgridHighAllFreq.getIndex(), MroBsTablesEnum.mroDxOutgridHighAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxOutgridMidAllFreq.getIndex(), MroBsTablesEnum.mroDxOutgridMidAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxOutgridLowAllFreq.getIndex(), MroBsTablesEnum.mroDxOutgridLowAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtOutgridHighAllFreq.getIndex(), MroBsTablesEnum.mroLtOutgridHighAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtOutgridMidAllFreq.getIndex(), MroBsTablesEnum.mroLtOutgridMidAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtOutgridLowAllFreq.getIndex(), MroBsTablesEnum.mroLtOutgridLowAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdIngridHighAllFreq.getIndex(), MroBsTablesEnum.mroYdIngridHighAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdIngridMidAllFreq.getIndex(), MroBsTablesEnum.mroYdIngridMidAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroYdIngridLowAllFreq.getIndex(), MroBsTablesEnum.mroYdIngridLowAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxIngridHighAllFreq.getIndex(), MroBsTablesEnum.mroDxIngridHighAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxIngridMidAllFreq.getIndex(), MroBsTablesEnum.mroDxIngridMidAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroDxIngridLowAllFreq.getIndex(), MroBsTablesEnum.mroDxIngridLowAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtIngridHighAllFreq.getIndex(), MroBsTablesEnum.mroLtIngridHighAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtIngridMidAllFreq.getIndex(), MroBsTablesEnum.mroLtIngridMidAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroLtIngridLowAllFreq.getIndex(), MroBsTablesEnum.mroLtIngridLowAllFreq.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroCellResident.getIndex(), MroBsTablesEnum.mroCellResident.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildPosUserHigh.getIndex(), MroBsTablesEnum.mroBuildPosUserHigh.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildPosUserMid.getIndex(), MroBsTablesEnum.mroBuildPosUserMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		
		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroBuildPosUserLow.getIndex(), MroBsTablesEnum.mroBuildPosUserLow.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

//		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroResidentUserCell.getIndex(), MroBsTablesEnum.mroResidentUserCell.getPath(mroXdrMergePath, date));
//		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroruYdBuildMid.getIndex(), MroBsTablesEnum.mroruYdBuildMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroruYdBuildPosMid.getIndex(), MroBsTablesEnum.mroruYdBuildPosMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroruBuildCellMid.getIndex(), MroBsTablesEnum.mroruBuildCellMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroruBuildCellPosMid.getIndex(), MroBsTablesEnum.mroruBuildCellPosMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		inputInfo = new MergeInputStruct("" + MroBsTablesEnum.mroruBuildPosUserMid.getIndex(),
				MroBsTablesEnum.mroruBuildPosUserMid.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);

		/* 新加高铁的事件汇聚 20180712 zhaikaishun */
		//高铁栅格汇聚
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrYdGrid.getIndex(),
				MroBsTablesEnum.mroHsrYdGrid.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		//高铁栅格小区汇聚
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrYdGridCell.getIndex(),
				MroBsTablesEnum.mroHsrYdGridCell.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		//高铁栅格
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrYdCell.getIndex(),
				MroBsTablesEnum.mroHsrYdCell.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		//高铁路段
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrYdTrainSeg.getIndex(),
				MroBsTablesEnum.mroHsrYdTrainSeg.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		//高铁路段小区
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrYdTrainSegCell.getIndex(),
				MroBsTablesEnum.mroHsrYdTrainSegCell.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		// 高铁联通栅格
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrLtGrid.getIndex(),
				MroBsTablesEnum.mroHsrLtGrid.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		// 联通高铁路段
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrLtTrainSeg.getIndex(),
				MroBsTablesEnum.mroHsrLtTrainSeg.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		// 电信高铁栅格
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrDxGrid.getIndex(),
				MroBsTablesEnum.mroHsrDxGrid.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		//电信高铁路段
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroHsrDxTrainSeg.getIndex(),
				MroBsTablesEnum.mroHsrDxTrainSeg.getPath(mroXdrMergePath,date));
		inputPath.add(inputInfo);

		/* 事件的高铁汇聚 */

		// 高铁室外栅格
		inputInfo = new MergeInputStruct(""+TypeIoEvtEnum.HSRGRIDEVT.getIndex(),
				String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.HSRGRIDEVT.getDirName()));
		inputPath.add(inputInfo);
		//高铁室外栅格小区
		inputInfo = new MergeInputStruct(""+TypeIoEvtEnum.HSRCELLGRIDEVT.getIndex(),
				String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.HSRCELLGRIDEVT.getDirName()));
		inputPath.add(inputInfo);
		//高铁小区
		inputInfo = new MergeInputStruct(""+TypeIoEvtEnum.HSRCELLEVT.getIndex(),
				String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.HSRCELLEVT.getDirName()));
		inputPath.add(inputInfo);
		//高铁路段
		inputInfo = new MergeInputStruct(""+TypeIoEvtEnum.HSRTRAINSEGEVT.getIndex(),
				String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.HSRTRAINSEGEVT.getDirName()));
		inputPath.add(inputInfo);
		// 高铁路段小区
		inputInfo = new MergeInputStruct(""+TypeIoEvtEnum.HSRTRAINSEGCELLEVT.getIndex(),
				String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.HSRTRAINSEGCELLEVT.getDirName()));
		inputPath.add(inputInfo);

		//xdr 统计
		inputInfo = new MergeInputStruct(""+TypeIoEvtEnum.XDR_CELL.getIndex(),
				String.format(eventPath, mroXdrMergePath, date, TypeIoEvtEnum.XDR_CELL.getDirName()));
		inputPath.add(inputInfo);

		//用户xdr，mro 统计
		String xdrStatPath = "%1$s/xdr_loc/data_01_%2$s/%3$s_%2$s,%1$s/mro_loc/data_01_%2$s/%3$s_%2$s";

		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.xdrCountStat.getIndex(),
				String.format(xdrStatPath, mroXdrMergePath, date, MroBsTablesEnum.xdrCountStat.getDirName()));
		inputPath.add(inputInfo);

		//用户小区5分钟粒度指标统计
		inputInfo = new MergeInputStruct(""+MroBsTablesEnum.mroUserCellByMins.getIndex(), MroBsTablesEnum.mroUserCellByMins.getPath(mroXdrMergePath, date));
		inputPath.add(inputInfo);
		return inputPath;
	}

	public static ArrayList<MergeOutPutStruct> addOutputPath(String mroXdrMergePath, String date) {
		ArrayList<MergeOutPutStruct> outputList = new ArrayList<MergeOutPutStruct>();
		MergeOutPutStruct outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.griddt.getIndex(), MergeStatTablesEnum.griddt.getFileName(),
				MergeStatTablesEnum.griddt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.gridcqt.getIndex(), MergeStatTablesEnum.gridcqt.getFileName(), MergeStatTablesEnum.gridcqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrogrid.getIndex(), MergeStatTablesEnum.mrogrid.getFileName(), MergeStatTablesEnum.mrogrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrocell.getIndex(), MergeStatTablesEnum.mrocell.getFileName(), MergeStatTablesEnum.mrocell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrocellgrid.getIndex(), MergeStatTablesEnum.mrocellgrid.getFileName(),
				MergeStatTablesEnum.mrocellgrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.xdrgriduserhour.getIndex(), MergeStatTablesEnum.xdrgriduserhour.getFileName(),
				MergeStatTablesEnum.xdrgriduserhour.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.griddtfreq.getIndex(), MergeStatTablesEnum.griddtfreq.getFileName(), MergeStatTablesEnum.griddtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.gridcqtfreq.getIndex(), MergeStatTablesEnum.gridcqtfreq.getFileName(),
				MergeStatTablesEnum.gridcqtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrocellfreq.getIndex(), MergeStatTablesEnum.mrocellfreq.getFileName(),
				MergeStatTablesEnum.mrocellfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.useractcell.getIndex(), MergeStatTablesEnum.useractcell.getFileName(),
				MergeStatTablesEnum.useractcell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tenmrogrid.getIndex(), MergeStatTablesEnum.tenmrogrid.getFileName(), MergeStatTablesEnum.tenmrogrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tengriddt.getIndex(), MergeStatTablesEnum.tengriddt.getFileName(), MergeStatTablesEnum.tengriddt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tengridcqt.getIndex(), MergeStatTablesEnum.tengridcqt.getFileName(), MergeStatTablesEnum.tengridcqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tenmrocellgrid.getIndex(), MergeStatTablesEnum.tenmrocellgrid.getFileName(),
				MergeStatTablesEnum.tenmrocellgrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tengriddtfreq.getIndex(), MergeStatTablesEnum.tengriddtfreq.getFileName(),
				MergeStatTablesEnum.tengriddtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tengridcqtfreq.getIndex(), MergeStatTablesEnum.tengridcqtfreq.getFileName(),
				MergeStatTablesEnum.tengridcqtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.LTtenFreqByImeiDt.getIndex(), MergeStatTablesEnum.LTtenFreqByImeiDt.getFileName(),
				MergeStatTablesEnum.LTtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.LTtenFreqByImeiCqt.getIndex(), MergeStatTablesEnum.LTtenFreqByImeiCqt.getFileName(),
				MergeStatTablesEnum.LTtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tencellgriddt.getIndex(), MergeStatTablesEnum.tencellgriddt.getFileName(),
				MergeStatTablesEnum.tencellgriddt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.tencellgridcqt.getIndex(), MergeStatTablesEnum.tencellgridcqt.getFileName(),
				MergeStatTablesEnum.tencellgridcqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.DXtenFreqByImeiDt.getIndex(), MergeStatTablesEnum.DXtenFreqByImeiDt.getFileName(),
				MergeStatTablesEnum.DXtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.DXtenFreqByImeiCqt.getIndex(), MergeStatTablesEnum.DXtenFreqByImeiCqt.getFileName(),
				MergeStatTablesEnum.DXtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.LTfreqcellByImei.getIndex(), MergeStatTablesEnum.LTfreqcellByImei.getFileName(),
				MergeStatTablesEnum.LTfreqcellByImei.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.DXfreqcellByImei.getIndex(), MergeStatTablesEnum.DXfreqcellByImei.getFileName(),
				MergeStatTablesEnum.DXfreqcellByImei.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpmrogrid.getIndex(), MergeStatTablesEnum.fpmrogrid.getFileName(), MergeStatTablesEnum.fpmrogrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpgriddt.getIndex(), MergeStatTablesEnum.fpgriddt.getFileName(), MergeStatTablesEnum.fpgriddt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpgridcqt.getIndex(), MergeStatTablesEnum.fpgridcqt.getFileName(), MergeStatTablesEnum.fpgridcqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpmrocellgrid.getIndex(), MergeStatTablesEnum.fpmrocellgrid.getFileName(),
				MergeStatTablesEnum.fpmrocellgrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpgriddtfreq.getIndex(), MergeStatTablesEnum.fpgriddtfreq.getFileName(),
				MergeStatTablesEnum.fpgriddtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpgridcqtfreq.getIndex(), MergeStatTablesEnum.fpgridcqtfreq.getFileName(),
				MergeStatTablesEnum.fpgridcqtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptenmrogrid.getIndex(), MergeStatTablesEnum.fptenmrogrid.getFileName(),
				MergeStatTablesEnum.fptenmrogrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptengriddt.getIndex(), MergeStatTablesEnum.fptengriddt.getFileName(), MergeStatTablesEnum.fptengriddt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptengridcqt.getIndex(), MergeStatTablesEnum.fptengridcqt.getFileName(),
				MergeStatTablesEnum.fptengridcqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptenmrocellgrid.getIndex(), MergeStatTablesEnum.fptenmrocellgrid.getFileName(),
				MergeStatTablesEnum.fptenmrocellgrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptengriddtfreq.getIndex(), MergeStatTablesEnum.fptengriddtfreq.getFileName(),
				MergeStatTablesEnum.fptengriddtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptengridcqtfreq.getIndex(), MergeStatTablesEnum.fptengridcqtfreq.getFileName(),
				MergeStatTablesEnum.fptengridcqtfreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpLTtenFreqByImeiDt.getIndex(), MergeStatTablesEnum.fpLTtenFreqByImeiDt.getFileName(),
				MergeStatTablesEnum.fpLTtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpLTtenFreqByImeiCqt.getIndex(), MergeStatTablesEnum.fpLTtenFreqByImeiCqt.getFileName(),
				MergeStatTablesEnum.fpLTtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptencellgriddt.getIndex(), MergeStatTablesEnum.fptencellgriddt.getFileName(),
				MergeStatTablesEnum.fptencellgriddt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fptencellgridcqt.getIndex(), MergeStatTablesEnum.fptencellgridcqt.getFileName(),
				MergeStatTablesEnum.fptencellgridcqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpLTfreqcellByImei.getIndex(), MergeStatTablesEnum.fpLTfreqcellByImei.getFileName(),
				MergeStatTablesEnum.fpLTfreqcellByImei.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpDXfreqcellByImei.getIndex(), MergeStatTablesEnum.fpDXfreqcellByImei.getFileName(),
				MergeStatTablesEnum.fpDXfreqcellByImei.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpDXtenFreqByImeiDt.getIndex(), MergeStatTablesEnum.fpDXtenFreqByImeiDt.getFileName(),
				MergeStatTablesEnum.fpDXtenFreqByImeiDt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.fpDXtenFreqByImeiCqt.getIndex(), MergeStatTablesEnum.fpDXtenFreqByImeiCqt.getFileName(),
				MergeStatTablesEnum.fpDXtenFreqByImeiCqt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdOutgridHigh.getIndex(), MergeStatTablesEnum.mroYdOutgridHigh.getFileName(),
				MergeStatTablesEnum.mroYdOutgridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdOutgridMid.getIndex(), MergeStatTablesEnum.mroYdOutgridMid.getFileName(),
				MergeStatTablesEnum.mroYdOutgridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdOutgridLow.getIndex(), MergeStatTablesEnum.mroYdOutgridLow.getFileName(),
				MergeStatTablesEnum.mroYdOutgridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroOutgridCellHigh.getIndex(), MergeStatTablesEnum.mroOutgridCellHigh.getFileName(),
				MergeStatTablesEnum.mroOutgridCellHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroOutgridCellMid.getIndex(), MergeStatTablesEnum.mroOutgridCellMid.getFileName(),
				MergeStatTablesEnum.mroOutgridCellMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroOutgridCellLow.getIndex(), MergeStatTablesEnum.mroOutgridCellLow.getFileName(),
				MergeStatTablesEnum.mroOutgridCellLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdIngridHigh.getIndex(), MergeStatTablesEnum.mroYdIngridHigh.getFileName(),
				MergeStatTablesEnum.mroYdIngridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdIngridMid.getIndex(), MergeStatTablesEnum.mroYdIngridMid.getFileName(),
				MergeStatTablesEnum.mroYdIngridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdIngridLow.getIndex(), MergeStatTablesEnum.mroYdIngridLow.getFileName(),
				MergeStatTablesEnum.mroYdIngridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroIngridCellHigh.getIndex(), MergeStatTablesEnum.mroIngridCellHigh.getFileName(),
				MergeStatTablesEnum.mroIngridCellHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroIngridCellMid.getIndex(), MergeStatTablesEnum.mroIngridCellMid.getFileName(),
				MergeStatTablesEnum.mroIngridCellMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroIngridCellLow.getIndex(), MergeStatTablesEnum.mroIngridCellLow.getFileName(),
				MergeStatTablesEnum.mroIngridCellLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdBuildHigh.getIndex(), MergeStatTablesEnum.mroYdBuildHigh.getFileName(),MergeStatTablesEnum.mroYdBuildHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdBuildMid.getIndex(), MergeStatTablesEnum.mroYdBuildMid.getFileName(), MergeStatTablesEnum.mroYdBuildMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdBuildLow.getIndex(), MergeStatTablesEnum.mroYdBuildLow.getFileName(), MergeStatTablesEnum.mroYdBuildLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdBuildPosHigh.getIndex(), MergeStatTablesEnum.mroYdBuildPosHigh.getFileName(),MergeStatTablesEnum.mroYdBuildPosHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdBuildPosMid.getIndex(), MergeStatTablesEnum.mroYdBuildPosMid.getFileName(), MergeStatTablesEnum.mroYdBuildPosMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdBuildPosLow.getIndex(), MergeStatTablesEnum.mroYdBuildPosLow.getFileName(), MergeStatTablesEnum.mroYdBuildPosLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdCell.getIndex(), MergeStatTablesEnum.mroYdCell.getFileName(), MergeStatTablesEnum.mroYdCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxOutgridHigh.getIndex(), MergeStatTablesEnum.mroDxOutgridHigh.getFileName(),
				MergeStatTablesEnum.mroDxOutgridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxOutgridMid.getIndex(), MergeStatTablesEnum.mroDxOutgridMid.getFileName(),
				MergeStatTablesEnum.mroDxOutgridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxOutgridLow.getIndex(), MergeStatTablesEnum.mroDxOutgridLow.getFileName(),
				MergeStatTablesEnum.mroDxOutgridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxIngridHigh.getIndex(), MergeStatTablesEnum.mroDxIngridHigh.getFileName(),
				MergeStatTablesEnum.mroDxIngridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxIngridMid.getIndex(), MergeStatTablesEnum.mroDxIngridMid.getFileName(),
				MergeStatTablesEnum.mroDxIngridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxIngridLow.getIndex(), MergeStatTablesEnum.mroDxIngridLow.getFileName(),
				MergeStatTablesEnum.mroDxIngridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxBuildHigh.getIndex(), MergeStatTablesEnum.mroDxBuildHigh.getFileName(),
				MergeStatTablesEnum.mroDxBuildHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxBuildMid.getIndex(), MergeStatTablesEnum.mroDxBuildMid.getFileName(), MergeStatTablesEnum.mroDxBuildMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxBuildLow.getIndex(), MergeStatTablesEnum.mroDxBuildLow.getFileName(), MergeStatTablesEnum.mroDxBuildLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxCell.getIndex(), MergeStatTablesEnum.mroDxCell.getFileName(), MergeStatTablesEnum.mroDxCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtOutgridHigh.getIndex(), MergeStatTablesEnum.mroLtOutgridHigh.getFileName(),
				MergeStatTablesEnum.mroLtOutgridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtOutgridMid.getIndex(), MergeStatTablesEnum.mroLtOutgridMid.getFileName(),
				MergeStatTablesEnum.mroLtOutgridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtOutgridLow.getIndex(), MergeStatTablesEnum.mroLtOutgridLow.getFileName(),
				MergeStatTablesEnum.mroLtOutgridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtIngridHigh.getIndex(), MergeStatTablesEnum.mroLtIngridHigh.getFileName(),
				MergeStatTablesEnum.mroLtIngridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtIngridMid.getIndex(), MergeStatTablesEnum.mroLtIngridMid.getFileName(),
				MergeStatTablesEnum.mroLtIngridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtIngridLow.getIndex(), MergeStatTablesEnum.mroLtIngridLow.getFileName(),
				MergeStatTablesEnum.mroLtIngridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtBuildHigh.getIndex(), MergeStatTablesEnum.mroLtBuildHigh.getFileName(),
				MergeStatTablesEnum.mroLtBuildHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtBuildMid.getIndex(), MergeStatTablesEnum.mroLtBuildMid.getFileName(), MergeStatTablesEnum.mroLtBuildMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtBuildLow.getIndex(), MergeStatTablesEnum.mroLtBuildLow.getFileName(), MergeStatTablesEnum.mroLtBuildLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtCell.getIndex(), MergeStatTablesEnum.mroLtCell.getFileName(), MergeStatTablesEnum.mroLtCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildCellHigh.getIndex(), MergeStatTablesEnum.mroBuildCellHigh.getFileName(),
				MergeStatTablesEnum.mroBuildCellHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildCellMid.getIndex(), MergeStatTablesEnum.mroBuildCellMid.getFileName(),
				MergeStatTablesEnum.mroBuildCellMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildCellLow.getIndex(), MergeStatTablesEnum.mroBuildCellLow.getFileName(),
				MergeStatTablesEnum.mroBuildCellLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildCellPosHigh.getIndex(), MergeStatTablesEnum.mroBuildCellPosHigh.getFileName(),
				MergeStatTablesEnum.mroBuildCellPosHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildCellPosMid.getIndex(), MergeStatTablesEnum.mroBuildCellPosMid.getFileName(),
				MergeStatTablesEnum.mroBuildCellPosMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildCellPosLow.getIndex(), MergeStatTablesEnum.mroBuildCellPosLow.getFileName(),
				MergeStatTablesEnum.mroBuildCellPosLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdAreaGridCell.getIndex(), MergeStatTablesEnum.mroYdAreaGridCell.getFileName(),
				MergeStatTablesEnum.mroYdAreaGridCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdAreaCell.getIndex(), MergeStatTablesEnum.mroYdAreaCell.getFileName(), MergeStatTablesEnum.mroYdAreaCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdAreaGrid.getIndex(), MergeStatTablesEnum.mroYdAreaGrid.getFileName(), MergeStatTablesEnum.mroYdAreaGrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdArea.getIndex(), MergeStatTablesEnum.mroYdArea.getFileName(), MergeStatTablesEnum.mroYdArea.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		// yzx add LT and DX AREA 2017.9.18
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtAreaCell.getIndex(), MergeStatTablesEnum.mroLtAreaCell.getFileName(), MergeStatTablesEnum.mroLtAreaCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtAreaGrid.getIndex(), MergeStatTablesEnum.mroLtAreaGrid.getFileName(), MergeStatTablesEnum.mroLtAreaGrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtArea.getIndex(), MergeStatTablesEnum.mroLtArea.getFileName(), MergeStatTablesEnum.mroLtArea.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxAreaCell.getIndex(), MergeStatTablesEnum.mroDxAreaCell.getFileName(), MergeStatTablesEnum.mroDxAreaCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxAreaGrid.getIndex(), MergeStatTablesEnum.mroDxAreaGrid.getFileName(), MergeStatTablesEnum.mroDxAreaGrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxArea.getIndex(), MergeStatTablesEnum.mroDxArea.getFileName(), MergeStatTablesEnum.mroDxArea.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		// yzx add EVENT 2017.9.26
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.CELLEVT.getIndex(), MergeStatTablesEnum.CELLEVT.getFileName(), MergeStatTablesEnum.CELLEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.IHCELLGRIDEVT.getIndex(), MergeStatTablesEnum.IHCELLGRIDEVT.getFileName(), MergeStatTablesEnum.IHCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.IMCELLGRIDEVT.getIndex(), MergeStatTablesEnum.IMCELLGRIDEVT.getFileName(), MergeStatTablesEnum.IMCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.ILCELLGRIDEVT.getIndex(), MergeStatTablesEnum.ILCELLGRIDEVT.getFileName(), MergeStatTablesEnum.ILCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.IHGRIDEVT.getIndex(), MergeStatTablesEnum.IHGRIDEVT.getFileName(), MergeStatTablesEnum.IHGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.IMGRIDEVT.getIndex(), MergeStatTablesEnum.IMGRIDEVT.getFileName(), MergeStatTablesEnum.IMGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.ILGRIDEVT.getIndex(), MergeStatTablesEnum.ILGRIDEVT.getFileName(), MergeStatTablesEnum.ILGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.OHCELLGRIDEVT.getIndex(), MergeStatTablesEnum.OHCELLGRIDEVT.getFileName(), MergeStatTablesEnum.OHCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.OMCELLGRIDEVT.getIndex(), MergeStatTablesEnum.OMCELLGRIDEVT.getFileName(), MergeStatTablesEnum.OMCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.OLCELLGRIDEVT.getIndex(), MergeStatTablesEnum.OLCELLGRIDEVT.getFileName(), MergeStatTablesEnum.OLCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.OHGRIDEVT.getIndex(), MergeStatTablesEnum.OHGRIDEVT.getFileName(), MergeStatTablesEnum.OHGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.OMGRIDEVT.getIndex(), MergeStatTablesEnum.OMGRIDEVT.getFileName(), MergeStatTablesEnum.OMGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.OLGRIDEVT.getIndex(), MergeStatTablesEnum.OLGRIDEVT.getFileName(), MergeStatTablesEnum.OLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BHCELLGRIDEVT.getIndex(), MergeStatTablesEnum.BHCELLGRIDEVT.getFileName(), MergeStatTablesEnum.BHCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BMCELLGRIDEVT.getIndex(), MergeStatTablesEnum.BMCELLGRIDEVT.getFileName(), MergeStatTablesEnum.BMCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BLCELLGRIDEVT.getIndex(), MergeStatTablesEnum.BLCELLGRIDEVT.getFileName(), MergeStatTablesEnum.BLCELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BHCELLPOSEVT.getIndex(), MergeStatTablesEnum.BHCELLPOSEVT.getFileName(), MergeStatTablesEnum.BHCELLPOSEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BMCELLPOSEVT.getIndex(), MergeStatTablesEnum.BMCELLPOSEVT.getFileName(), MergeStatTablesEnum.BMCELLPOSEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BLCELLPOSEVT.getIndex(), MergeStatTablesEnum.BLCELLPOSEVT.getFileName(), MergeStatTablesEnum.BLCELLPOSEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BHGRIDEVT.getIndex(), MergeStatTablesEnum.BHGRIDEVT.getFileName(), MergeStatTablesEnum.BHGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BMGRIDEVT.getIndex(), MergeStatTablesEnum.BMGRIDEVT.getFileName(), MergeStatTablesEnum.BMGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BLGRIDEVT.getIndex(), MergeStatTablesEnum.BLGRIDEVT.getFileName(), MergeStatTablesEnum.BLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BHPOSEVT.getIndex(), MergeStatTablesEnum.BHPOSEVT.getFileName(), MergeStatTablesEnum.BHPOSEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BMPOSEVT.getIndex(), MergeStatTablesEnum.BMPOSEVT.getFileName(), MergeStatTablesEnum.BMPOSEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.BLPOSEVT.getIndex(), MergeStatTablesEnum.BLPOSEVT.getFileName(), MergeStatTablesEnum.BLPOSEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		// EVENT_AREA
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.AREAEVT.getIndex(), MergeStatTablesEnum.AREAEVT.getFileName(), MergeStatTablesEnum.AREAEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.AREAGRIDEVT.getIndex(), MergeStatTablesEnum.AREAGRIDEVT.getFileName(), MergeStatTablesEnum.AREAGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.AREACELLGRIDEVT.getIndex(), MergeStatTablesEnum.AREACELLGRIDEVT.getFileName(),
				MergeStatTablesEnum.AREACELLGRIDEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.AREACELLEVT.getIndex(), MergeStatTablesEnum.AREACELLEVT.getFileName(), MergeStatTablesEnum.AREACELLEVT.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		// 20170921 add fullnet
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltOutgridHigh.getIndex(), MergeStatTablesEnum.mroYdltOutgridHigh.getFileName(),
				MergeStatTablesEnum.mroYdltOutgridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltOutgridMid.getIndex(), MergeStatTablesEnum.mroYdltOutgridMid.getFileName(),
				MergeStatTablesEnum.mroYdltOutgridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltOutgridLow.getIndex(), MergeStatTablesEnum.mroYdltOutgridLow.getFileName(),
				MergeStatTablesEnum.mroYdltOutgridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxOutgridHigh.getIndex(), MergeStatTablesEnum.mroYddxOutgridHigh.getFileName(),
				MergeStatTablesEnum.mroYddxOutgridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxOutgridMid.getIndex(), MergeStatTablesEnum.mroYddxOutgridMid.getFileName(),
				MergeStatTablesEnum.mroYddxOutgridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxOutgridLow.getIndex(), MergeStatTablesEnum.mroYddxOutgridLow.getFileName(),
				MergeStatTablesEnum.mroYddxOutgridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltIngridHigh.getIndex(), MergeStatTablesEnum.mroYdltIngridHigh.getFileName(),
				MergeStatTablesEnum.mroYdltIngridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltIngridMid.getIndex(), MergeStatTablesEnum.mroYdltIngridMid.getFileName(),
				MergeStatTablesEnum.mroYdltIngridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltIngridLow.getIndex(), MergeStatTablesEnum.mroYdltIngridLow.getFileName(),
				MergeStatTablesEnum.mroYdltIngridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxIngridHigh.getIndex(), MergeStatTablesEnum.mroYddxIngridHigh.getFileName(),
				MergeStatTablesEnum.mroYddxIngridHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxIngridMid.getIndex(), MergeStatTablesEnum.mroYddxIngridMid.getFileName(),
				MergeStatTablesEnum.mroYddxIngridMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxIngridLow.getIndex(), MergeStatTablesEnum.mroYddxIngridLow.getFileName(),
				MergeStatTablesEnum.mroYddxIngridLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltBuildingHigh.getIndex(), MergeStatTablesEnum.mroYdltBuildingHigh.getFileName(),
				MergeStatTablesEnum.mroYdltBuildingHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltBuildingMid.getIndex(), MergeStatTablesEnum.mroYdltBuildingMid.getFileName(),
				MergeStatTablesEnum.mroYdltBuildingMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltBuildingLow.getIndex(), MergeStatTablesEnum.mroYdltBuildingLow.getFileName(),
				MergeStatTablesEnum.mroYdltBuildingLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxBuildingHigh.getIndex(), MergeStatTablesEnum.mroYddxBuildingHigh.getFileName(),
				MergeStatTablesEnum.mroYddxBuildingHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxBuildingMid.getIndex(), MergeStatTablesEnum.mroYddxBuildingMid.getFileName(),
				MergeStatTablesEnum.mroYddxBuildingMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxBuildingLow.getIndex(), MergeStatTablesEnum.mroYddxBuildingLow.getFileName(),
				MergeStatTablesEnum.mroYddxBuildingLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltCell.getIndex(), MergeStatTablesEnum.mroYdltCell.getFileName(), MergeStatTablesEnum.mroYdltCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxCell.getIndex(), MergeStatTablesEnum.mroYddxCell.getFileName(), MergeStatTablesEnum.mroYddxCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltAreaGrid.getIndex(), MergeStatTablesEnum.mroYdltAreaGrid.getFileName(),
				MergeStatTablesEnum.mroYdltAreaGrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxAreaGrid.getIndex(), MergeStatTablesEnum.mroYddxAreaGrid.getFileName(),
				MergeStatTablesEnum.mroYddxAreaGrid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltAreaCell.getIndex(), MergeStatTablesEnum.mroYdltAreaCell.getFileName(),
				MergeStatTablesEnum.mroYdltAreaCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxAreaCell.getIndex(), MergeStatTablesEnum.mroYddxAreaCell.getFileName(),
				MergeStatTablesEnum.mroYddxAreaCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdltArea.getIndex(), MergeStatTablesEnum.mroYdltArea.getFileName(), MergeStatTablesEnum.mroYdltArea.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYddxArea.getIndex(), MergeStatTablesEnum.mroYddxArea.getFileName(), MergeStatTablesEnum.mroYddxArea.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroUserCell.getIndex(), MergeStatTablesEnum.mroUserCell.getFileName(), MergeStatTablesEnum.mroUserCell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		// imei_yd/imei_lt/imei_dx
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrImeiYd.getIndex(), MergeStatTablesEnum.mrImeiYd.getFileName(), MergeStatTablesEnum.mrImeiYd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrImeiLt.getIndex(), MergeStatTablesEnum.mrImeiLt.getFileName(), MergeStatTablesEnum.mrImeiLt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mrImeiDx.getIndex(), MergeStatTablesEnum.mrImeiDx.getFileName(), MergeStatTablesEnum.mrImeiDx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		// ------------
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_yd_high.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_yd_high.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_yd_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_yd_mid.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_yd_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_yd_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_yd_low.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_yd_low.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_yd_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_cell_high.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_cell_high.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_cell_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_cell_mid.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_cell_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_cell_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_cell_low.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_cell_low.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_cell_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_yd_high.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_yd_high.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_yd_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_yd_mid.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_yd_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_yd_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_yd_low.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_yd_low.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_yd_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_cell_high.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_cell_high.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_cell_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_cell_mid.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_cell_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_cell_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_cell_low.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_cell_low.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_cell_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_yd_high.getIndex(), MergeStatTablesEnum.mdtimm_building_yd_high.getFileName(),
				MergeStatTablesEnum.mdtimm_building_yd_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_yd_mid.getIndex(), MergeStatTablesEnum.mdtimm_building_yd_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_building_yd_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_yd_low.getIndex(), MergeStatTablesEnum.mdtimm_building_yd_low.getFileName(),
				MergeStatTablesEnum.mdtimm_building_yd_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_cell_high.getIndex(), MergeStatTablesEnum.mdtimm_building_cell_high.getFileName(),
				MergeStatTablesEnum.mdtimm_building_cell_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_cell_mid.getIndex(), MergeStatTablesEnum.mdtimm_building_cell_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_building_cell_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_cell_low.getIndex(), MergeStatTablesEnum.mdtimm_building_cell_low.getFileName(),
				MergeStatTablesEnum.mdtimm_building_cell_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_cell_yd.getIndex(), MergeStatTablesEnum.mdtimm_cell_yd.getFileName(), MergeStatTablesEnum.mdtimm_cell_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_outgrid_yd.getIndex(), MergeStatTablesEnum.mdtimm_area_outgrid_yd.getFileName(),
				MergeStatTablesEnum.mdtimm_area_outgrid_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_outgrid_cell.getIndex(), MergeStatTablesEnum.mdtimm_area_outgrid_cell.getFileName(),
				MergeStatTablesEnum.mdtimm_area_outgrid_cell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_cell_yd.getIndex(), MergeStatTablesEnum.mdtimm_area_cell_yd.getFileName(),
				MergeStatTablesEnum.mdtimm_area_cell_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_yd.getIndex(), MergeStatTablesEnum.mdtimm_area_yd.getFileName(), MergeStatTablesEnum.mdtimm_area_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_fullnet_high_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_building_fullnet_high_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_building_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_fullnet_mid_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_building_fullnet_mid_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_building_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_fullnet_low_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_building_fullnet_low_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_building_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_fullnet_high_yddx.getIndex(), MergeStatTablesEnum.mdtimm_building_fullnet_high_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_building_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_fullnet_mid_yddx.getIndex(), MergeStatTablesEnum.mdtimm_building_fullnet_mid_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_building_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_fullnet_low_yddx.getIndex(), MergeStatTablesEnum.mdtimm_building_fullnet_low_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_building_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_cell_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_cell_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_cell_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtimm_cell_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_cell_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_area_cell_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_area_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_cell_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtimm_area_cell_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_area_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtimm_area_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtimm_area_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtimm_area_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtimm_area_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_dx_high.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_dx_high.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_dx_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_dx_mid.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_dx_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_dx_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_dx_low.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_dx_low.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_dx_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_dx_high.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_dx_high.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_dx_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_dx_mid.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_dx_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_dx_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_dx_low.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_dx_low.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_dx_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_dx_high.getIndex(), MergeStatTablesEnum.mdtimm_building_dx_high.getFileName(),
				MergeStatTablesEnum.mdtimm_building_dx_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_dx_mid.getIndex(), MergeStatTablesEnum.mdtimm_building_dx_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_building_dx_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_dx_low.getIndex(), MergeStatTablesEnum.mdtimm_building_dx_low.getFileName(),
				MergeStatTablesEnum.mdtimm_building_dx_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_cell_dx.getIndex(), MergeStatTablesEnum.mdtimm_cell_dx.getFileName(), MergeStatTablesEnum.mdtimm_cell_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_outgrid_dx.getIndex(), MergeStatTablesEnum.mdtimm_area_outgrid_dx.getFileName(),
				MergeStatTablesEnum.mdtimm_area_outgrid_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_cell_dx.getIndex(), MergeStatTablesEnum.mdtimm_area_cell_dx.getFileName(),
				MergeStatTablesEnum.mdtimm_area_cell_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_dx.getIndex(), MergeStatTablesEnum.mdtimm_area_dx.getFileName(), MergeStatTablesEnum.mdtimm_area_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_lt_high.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_lt_high.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_lt_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_lt_mid.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_lt_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_lt_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_outgrid_lt_low.getIndex(), MergeStatTablesEnum.mdtimm_outgrid_lt_low.getFileName(),
				MergeStatTablesEnum.mdtimm_outgrid_lt_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_lt_high.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_lt_high.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_lt_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_lt_mid.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_lt_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_lt_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_ingrid_lt_low.getIndex(), MergeStatTablesEnum.mdtimm_ingrid_lt_low.getFileName(),
				MergeStatTablesEnum.mdtimm_ingrid_lt_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_lt_high.getIndex(), MergeStatTablesEnum.mdtimm_building_lt_high.getFileName(),
				MergeStatTablesEnum.mdtimm_building_lt_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_lt_mid.getIndex(), MergeStatTablesEnum.mdtimm_building_lt_mid.getFileName(),
				MergeStatTablesEnum.mdtimm_building_lt_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_building_lt_low.getIndex(), MergeStatTablesEnum.mdtimm_building_lt_low.getFileName(),
				MergeStatTablesEnum.mdtimm_building_lt_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_cell_lt.getIndex(), MergeStatTablesEnum.mdtimm_cell_lt.getFileName(), MergeStatTablesEnum.mdtimm_cell_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_outgrid_lt.getIndex(), MergeStatTablesEnum.mdtimm_area_outgrid_lt.getFileName(),
				MergeStatTablesEnum.mdtimm_area_outgrid_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_cell_lt.getIndex(), MergeStatTablesEnum.mdtimm_area_cell_lt.getFileName(),
				MergeStatTablesEnum.mdtimm_area_cell_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_area_lt.getIndex(), MergeStatTablesEnum.mdtimm_area_lt.getFileName(), MergeStatTablesEnum.mdtimm_area_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_yd_high.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_yd_high.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_yd_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_yd_mid.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_yd_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_yd_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_yd_low.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_yd_low.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_yd_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_cell_high.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_cell_high.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_cell_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_cell_mid.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_cell_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_cell_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_cell_low.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_cell_low.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_cell_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_yd_high.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_yd_high.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_yd_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_yd_mid.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_yd_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_yd_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_yd_low.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_yd_low.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_yd_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_cell_high.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_cell_high.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_cell_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_cell_mid.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_cell_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_cell_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_cell_low.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_cell_low.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_cell_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_yd_high.getIndex(), MergeStatTablesEnum.mdtlog_building_yd_high.getFileName(),
				MergeStatTablesEnum.mdtlog_building_yd_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_yd_mid.getIndex(), MergeStatTablesEnum.mdtlog_building_yd_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_building_yd_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_yd_low.getIndex(), MergeStatTablesEnum.mdtlog_building_yd_low.getFileName(),
				MergeStatTablesEnum.mdtlog_building_yd_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_cell_high.getIndex(), MergeStatTablesEnum.mdtlog_building_cell_high.getFileName(),
				MergeStatTablesEnum.mdtlog_building_cell_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_cell_mid.getIndex(), MergeStatTablesEnum.mdtlog_building_cell_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_building_cell_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_cell_low.getIndex(), MergeStatTablesEnum.mdtlog_building_cell_low.getFileName(),
				MergeStatTablesEnum.mdtlog_building_cell_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_cell_yd.getIndex(), MergeStatTablesEnum.mdtlog_cell_yd.getFileName(), MergeStatTablesEnum.mdtlog_cell_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_outgrid_yd.getIndex(), MergeStatTablesEnum.mdtlog_area_outgrid_yd.getFileName(),
				MergeStatTablesEnum.mdtlog_area_outgrid_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_outgrid_cell.getIndex(), MergeStatTablesEnum.mdtlog_area_outgrid_cell.getFileName(),
				MergeStatTablesEnum.mdtlog_area_outgrid_cell.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_cell_yd.getIndex(), MergeStatTablesEnum.mdtlog_area_cell_yd.getFileName(),
				MergeStatTablesEnum.mdtlog_area_cell_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_yd.getIndex(), MergeStatTablesEnum.mdtlog_area_yd.getFileName(), MergeStatTablesEnum.mdtlog_area_yd.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_fullnet_high_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_fullnet_high_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_fullnet_low_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_fullnet_low_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_fullnet_high_yddx.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_fullnet_high_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_fullnet_mid_yddx.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_fullnet_mid_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_fullnet_low_yddx.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_fullnet_low_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_fullnet_high_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_fullnet_high_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_fullnet_low_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_fullnet_low_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_fullnet_high_yddx.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_fullnet_high_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_fullnet_mid_yddx.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_fullnet_mid_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_fullnet_low_yddx.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_fullnet_low_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_fullnet_high_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_building_fullnet_high_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_building_fullnet_high_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_fullnet_mid_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_building_fullnet_mid_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_building_fullnet_mid_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_fullnet_low_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_building_fullnet_low_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_building_fullnet_low_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_fullnet_high_yddx.getIndex(), MergeStatTablesEnum.mdtlog_building_fullnet_high_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_building_fullnet_high_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_fullnet_mid_yddx.getIndex(), MergeStatTablesEnum.mdtlog_building_fullnet_mid_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_building_fullnet_mid_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_fullnet_low_yddx.getIndex(), MergeStatTablesEnum.mdtlog_building_fullnet_low_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_building_fullnet_low_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_cell_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_cell_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_cell_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtlog_cell_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_outgrid_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_area_outgrid_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_area_outgrid_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_outgrid_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtlog_area_outgrid_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_area_outgrid_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_cell_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_area_cell_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_area_cell_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_cell_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtlog_area_cell_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_area_cell_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_fullnet_ydlt.getIndex(), MergeStatTablesEnum.mdtlog_area_fullnet_ydlt.getFileName(),
				MergeStatTablesEnum.mdtlog_area_fullnet_ydlt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_fullnet_yddx.getIndex(), MergeStatTablesEnum.mdtlog_area_fullnet_yddx.getFileName(),
				MergeStatTablesEnum.mdtlog_area_fullnet_yddx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_dx_high.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_dx_high.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_dx_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_dx_mid.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_dx_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_dx_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_dx_low.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_dx_low.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_dx_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_dx_high.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_dx_high.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_dx_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_dx_mid.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_dx_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_dx_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_dx_low.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_dx_low.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_dx_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_dx_high.getIndex(), MergeStatTablesEnum.mdtlog_building_dx_high.getFileName(),
				MergeStatTablesEnum.mdtlog_building_dx_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_dx_mid.getIndex(), MergeStatTablesEnum.mdtlog_building_dx_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_building_dx_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_dx_low.getIndex(), MergeStatTablesEnum.mdtlog_building_dx_low.getFileName(),
				MergeStatTablesEnum.mdtlog_building_dx_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_cell_dx.getIndex(), MergeStatTablesEnum.mdtlog_cell_dx.getFileName(), MergeStatTablesEnum.mdtlog_cell_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_outgrid_dx.getIndex(), MergeStatTablesEnum.mdtlog_area_outgrid_dx.getFileName(),
				MergeStatTablesEnum.mdtlog_area_outgrid_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_cell_dx.getIndex(), MergeStatTablesEnum.mdtlog_area_cell_dx.getFileName(),
				MergeStatTablesEnum.mdtlog_area_cell_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_dx.getIndex(), MergeStatTablesEnum.mdtlog_area_dx.getFileName(), MergeStatTablesEnum.mdtlog_area_dx.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_lt_high.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_lt_high.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_lt_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_lt_mid.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_lt_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_lt_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_outgrid_lt_low.getIndex(), MergeStatTablesEnum.mdtlog_outgrid_lt_low.getFileName(),
				MergeStatTablesEnum.mdtlog_outgrid_lt_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_lt_high.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_lt_high.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_lt_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_lt_mid.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_lt_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_lt_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_ingrid_lt_low.getIndex(), MergeStatTablesEnum.mdtlog_ingrid_lt_low.getFileName(),
				MergeStatTablesEnum.mdtlog_ingrid_lt_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_lt_high.getIndex(), MergeStatTablesEnum.mdtlog_building_lt_high.getFileName(),
				MergeStatTablesEnum.mdtlog_building_lt_high.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_lt_mid.getIndex(), MergeStatTablesEnum.mdtlog_building_lt_mid.getFileName(),
				MergeStatTablesEnum.mdtlog_building_lt_mid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_building_lt_low.getIndex(), MergeStatTablesEnum.mdtlog_building_lt_low.getFileName(),
				MergeStatTablesEnum.mdtlog_building_lt_low.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_cell_lt.getIndex(), MergeStatTablesEnum.mdtlog_cell_lt.getFileName(), MergeStatTablesEnum.mdtlog_cell_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_outgrid_lt.getIndex(), MergeStatTablesEnum.mdtlog_area_outgrid_lt.getFileName(),
				MergeStatTablesEnum.mdtlog_area_outgrid_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_cell_lt.getIndex(), MergeStatTablesEnum.mdtlog_area_cell_lt.getFileName(),
				MergeStatTablesEnum.mdtlog_area_cell_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_area_lt.getIndex(), MergeStatTablesEnum.mdtlog_area_lt.getFileName(), MergeStatTablesEnum.mdtlog_area_lt.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtimm_tac.getIndex(), MergeStatTablesEnum.mdtimm_tac.getFileName(), MergeStatTablesEnum.mdtimm_tac.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mdtlog_tac.getIndex(), MergeStatTablesEnum.mdtlog_tac.getFileName(), MergeStatTablesEnum.mdtlog_tac.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdOutgridHighAllFreq.getIndex(), MergeStatTablesEnum.mroYdOutgridHighAllFreq.getFileName(),
				MergeStatTablesEnum.mroYdOutgridHighAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdOutgridMidAllFreq.getIndex(), MergeStatTablesEnum.mroYdOutgridMidAllFreq.getFileName(),
				MergeStatTablesEnum.mroYdOutgridMidAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdOutgridLowAllFreq.getIndex(), MergeStatTablesEnum.mroYdOutgridLowAllFreq.getFileName(),
				MergeStatTablesEnum.mroYdOutgridLowAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxOutgridHighAllFreq.getIndex(), MergeStatTablesEnum.mroDxOutgridHighAllFreq.getFileName(),
				MergeStatTablesEnum.mroDxOutgridHighAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxOutgridMidAllFreq.getIndex(), MergeStatTablesEnum.mroDxOutgridMidAllFreq.getFileName(),
				MergeStatTablesEnum.mroDxOutgridMidAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxOutgridLowAllFreq.getIndex(), MergeStatTablesEnum.mroDxOutgridLowAllFreq.getFileName(),
				MergeStatTablesEnum.mroDxOutgridLowAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtOutgridHighAllFreq.getIndex(), MergeStatTablesEnum.mroLtOutgridHighAllFreq.getFileName(),
				MergeStatTablesEnum.mroLtOutgridHighAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtOutgridMidAllFreq.getIndex(), MergeStatTablesEnum.mroLtOutgridMidAllFreq.getFileName(),
				MergeStatTablesEnum.mroLtOutgridMidAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtOutgridLowAllFreq.getIndex(), MergeStatTablesEnum.mroLtOutgridLowAllFreq.getFileName(),
				MergeStatTablesEnum.mroLtOutgridLowAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdIngridHighAllFreq.getIndex(), MergeStatTablesEnum.mroYdIngridHighAllFreq.getFileName(),
				MergeStatTablesEnum.mroYdIngridHighAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdIngridMidAllFreq.getIndex(), MergeStatTablesEnum.mroYdIngridMidAllFreq.getFileName(),
				MergeStatTablesEnum.mroYdIngridMidAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroYdIngridLowAllFreq.getIndex(), MergeStatTablesEnum.mroYdIngridLowAllFreq.getFileName(),
				MergeStatTablesEnum.mroYdIngridLowAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxIngridHighAllFreq.getIndex(), MergeStatTablesEnum.mroDxIngridHighAllFreq.getFileName(),
				MergeStatTablesEnum.mroDxIngridHighAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxIngridMidAllFreq.getIndex(), MergeStatTablesEnum.mroDxIngridMidAllFreq.getFileName(),
				MergeStatTablesEnum.mroDxIngridMidAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroDxIngridLowAllFreq.getIndex(), MergeStatTablesEnum.mroDxIngridLowAllFreq.getFileName(),
				MergeStatTablesEnum.mroDxIngridLowAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtIngridHighAllFreq.getIndex(), MergeStatTablesEnum.mroLtIngridHighAllFreq.getFileName(),
				MergeStatTablesEnum.mroLtIngridHighAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtIngridMidAllFreq.getIndex(), MergeStatTablesEnum.mroLtIngridMidAllFreq.getFileName(),
				MergeStatTablesEnum.mroLtIngridMidAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroLtIngridLowAllFreq.getIndex(), MergeStatTablesEnum.mroLtIngridLowAllFreq.getFileName(),
				MergeStatTablesEnum.mroLtIngridLowAllFreq.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroCellResident.getIndex(), MergeStatTablesEnum.mroCellResident.getFileName(),
				MergeStatTablesEnum.mroCellResident.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildingDirectionHigh.getIndex(), MergeStatTablesEnum.mroBuildingDirectionHigh.getFileName(),
				MergeStatTablesEnum.mroBuildingDirectionHigh.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildingDirectionMid.getIndex(), MergeStatTablesEnum.mroBuildingDirectionMid.getFileName(),
				MergeStatTablesEnum.mroBuildingDirectionMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroBuildingDirectionLow.getIndex(), MergeStatTablesEnum.mroBuildingDirectionLow.getFileName(),
				MergeStatTablesEnum.mroBuildingDirectionLow.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroruYdBuildMid.getIndex(), MergeStatTablesEnum.mroruYdBuildMid.getFileName(),
				MergeStatTablesEnum.mroruYdBuildMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroruYdBuildPosMid.getIndex(), MergeStatTablesEnum.mroruYdBuildPosMid.getFileName(),
				MergeStatTablesEnum.mroruYdBuildPosMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroruBuildCellMid.getIndex(), MergeStatTablesEnum.mroruBuildCellMid.getFileName(),
				MergeStatTablesEnum.mroruBuildCellMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroruBuildCellPosMid.getIndex(), MergeStatTablesEnum.mroruBuildCellPosMid.getFileName(),
				MergeStatTablesEnum.mroruBuildCellPosMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroruBuildPosUserMid.getIndex(), MergeStatTablesEnum.mroruBuildPosUserMid.getFileName(),
				MergeStatTablesEnum.mroruBuildPosUserMid.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

        // 这里必须要包含HSR， 以后要汇聚的不能包括hsr
		for (MergeStatTablesEnum value : MergeStatTablesEnum.values()) {
			if (value.getFileName().toLowerCase().contains("hsr")) {
				outputStruct = new MergeOutPutStruct("" + value.getIndex(), value.getFileName(),
						value.getPath(mroXdrMergePath, date));
				outputList.add(outputStruct);
			}
		}

		//xdr统计
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.XDR_CELL.getIndex(), MergeStatTablesEnum.XDR_CELL.getFileName(),
				MergeStatTablesEnum.XDR_CELL.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		//用户xdr，mro统计
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.xdrCountStat.getIndex(), MergeStatTablesEnum.xdrCountStat.getFileName(),
				MergeStatTablesEnum.xdrCountStat.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		//用户小区5分钟粒度统计
		outputStruct = new MergeOutPutStruct("" + MergeStatTablesEnum.mroUserCellBy5Mins.getIndex(), MergeStatTablesEnum.mroUserCellBy5Mins.getFileName(),
				MergeStatTablesEnum.mroUserCellBy5Mins.getPath(mroXdrMergePath, date));
		outputList.add(outputStruct);

		return outputList;
	}
	
}
