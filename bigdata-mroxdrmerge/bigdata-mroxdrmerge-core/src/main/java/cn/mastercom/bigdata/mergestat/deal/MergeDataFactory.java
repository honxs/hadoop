package cn.mastercom.bigdata.mergestat.deal;

import java.util.HashMap;
import java.util.Map;

import cn.mastercom.bigdata.evt.locall.stat.TypeIoEvtEnum;
import cn.mastercom.bigdata.evt.locall.struct.*;
import cn.mastercom.bigdata.mdt.stat.MdtImeiMergeDo;
import cn.mastercom.bigdata.mdt.stat.MdtTablesEnum;
import cn.mastercom.bigdata.mro.loc.UserActCellMergeDataDo;
import cn.mastercom.bigdata.mro.loc.hsr.mergestat.HsrCellStatMergeDo;
import cn.mastercom.bigdata.mro.loc.hsr.mergestat.TrainSegCellStatMergeDo;
import cn.mastercom.bigdata.mro.loc.hsr.mergestat.TrainSegStatMergeDo;
import cn.mastercom.bigdata.mro.stat.struct.*;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroBsTablesEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsOTTTableEnum;
import cn.mastercom.bigdata.mro.stat.tableEnum.XdrLocTablesEnum;
import cn.mastercom.bigdata.xdr.loc.CellGridMergeDataDo_4G;
import cn.mastercom.bigdata.xdr.loc.CellMergeDataDo_4G;
import cn.mastercom.bigdata.xdr.loc.CellMergeDataDo_Freq;
import cn.mastercom.bigdata.xdr.loc.FreqCellByImeiDataMergeDo;
import cn.mastercom.bigdata.xdr.loc.GridMergeDataDo_4G;
import cn.mastercom.bigdata.xdr.loc.GridMergeFreqByImeiDataDo_4G;
import cn.mastercom.bigdata.xdr.loc.GridMergeFreqDataDo_4G;
import cn.mastercom.bigdata.xdr.loc.UserGridMergeDataDo_4G;

public class MergeDataFactory
{

	private Map<Integer, IMergeDataDo> mergeDataMap;

	private static MergeDataFactory instance;

	public static MergeDataFactory GetInstance()
	{
		if (instance == null)
		{
			instance = new MergeDataFactory();
		}
		return instance;
	}

	private MergeDataFactory()
	{
		init();
	}

	public boolean init()
	{
		mergeDataMap = new HashMap<Integer, IMergeDataDo>();
		{
			BuildCellMergeDo item = new BuildCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildCellHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildCellMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildCellLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			//
			item = new BuildCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_cell_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_cell_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_cell_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_cell_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_cell_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_cell_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new BuildCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroruBuildCellMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeDataDo_4G item = new GridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.griddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.gridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.mrogrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tengriddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tengridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tenmrogrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellMergeDataDo_4G item = new CellMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.mrocell.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellGridMergeDataDo_4G item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.mrocellgrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tenmrocellgrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}

		{
			UserGridMergeDataDo_4G item = new UserGridMergeDataDo_4G();
			item.setDataType(XdrLocTablesEnum.xdrgriduserhour.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeFreqDataDo_4G item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.griddtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.gridcqtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tengriddtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tengridcqtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellMergeDataDo_Freq item = new CellMergeDataDo_Freq();
			item.setDataType(MroCsOTTTableEnum.mrocellfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			UserActCellMergeDataDo item = new UserActCellMergeDataDo();
			item.setDataType(MroCsOTTTableEnum.useractcell.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeDataDo_4G item = new GridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpmrogrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpgriddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpgridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptenmrogrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptengriddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptengridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			CellGridMergeDataDo_4G item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpmrocellgrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptenmrocellgrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			// dt/cqt cellgrid 20170524

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tencellgriddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.tencellgridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptencellgriddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptencellgridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptencellgriddt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellGridMergeDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptencellgridcqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeFreqDataDo_4G item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpgriddtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpgridcqtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptengriddtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fptengridcqtfreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			GridMergeFreqByImeiDataDo_4G item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.LTtenFreqByImeiDt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.LTtenFreqByImeiDt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.LTtenFreqByImeiCqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpLTtenFreqByImeiDt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpLTtenFreqByImeiCqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.DXtenFreqByImeiDt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsOTTTableEnum.DXtenFreqByImeiCqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpDXtenFreqByImeiDt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new GridMergeFreqByImeiDataDo_4G();
			item.setDataType(MroCsFgTableEnum.fpDXtenFreqByImeiCqt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}
		{
			FreqCellByImeiDataMergeDo item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MroCsOTTTableEnum.LTfreqcellByImei.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MroCsOTTTableEnum.DXfreqcellByImei.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MroCsFgTableEnum.fpLTfreqcellByImei.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new FreqCellByImeiDataMergeDo();
			item.setDataType(MroCsFgTableEnum.fpDXfreqcellByImei.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}
		{
			OutGridMerge item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdOutgridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdOutgridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdOutgridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdOutgridHighAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdOutgridMidAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdOutgridLowAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroDxOutgridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroDxOutgridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroDxOutgridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroDxOutgridHighAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroDxOutgridMidAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroDxOutgridLowAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroLtOutgridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroLtOutgridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroLtOutgridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroLtOutgridHighAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroLtOutgridMidAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroLtOutgridLowAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdltOutgridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdltOutgridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYdltOutgridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYddxOutgridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYddxOutgridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new OutGridMerge();
			item.setDataType(MroBsTablesEnum.mroYddxOutgridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_yd_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_yd_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_yd_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_lt_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_lt_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_lt_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_dx_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_dx_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_dx_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_yd_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_yd_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_yd_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_lt_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_lt_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_lt_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_dx_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_dx_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_dx_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			//
			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_fullnet_high_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_fullnet_mid_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_fullnet_low_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_fullnet_high_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_fullnet_mid_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutGridMerge();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_fullnet_low_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}
		{
			OutCellGridMergeDo item = new OutCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroOutgridCellHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroOutgridCellMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroOutgridCellLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_cell_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_cell_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_outgrid_cell_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_cell_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_cell_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new OutCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_outgrid_cell_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
		}
		{
			InGridMergeDo item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdIngridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdIngridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdIngridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdIngridHighAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdIngridMidAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdIngridLowAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxIngridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxIngridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxIngridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxIngridHighAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxIngridMidAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxIngridLowAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtIngridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtIngridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtIngridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtIngridHighAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtIngridMidAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtIngridLowAllFreq.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltIngridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltIngridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltIngridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxIngridHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxIngridMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxIngridLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_yd_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_yd_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_yd_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_lt_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_lt_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_lt_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_dx_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_dx_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_dx_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			//

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_yd_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_yd_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_yd_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_lt_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_lt_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_lt_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_dx_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_dx_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_dx_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_fullnet_high_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_fullnet_mid_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_fullnet_low_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_fullnet_high_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_fullnet_mid_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new InGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_fullnet_low_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}
		{
			InCellGridMergeDo item = new InCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroIngridCellHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroIngridCellMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroIngridCellLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_cell_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_cell_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_ingrid_cell_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_cell_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_cell_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new InCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_ingrid_cell_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			BuildMergeDo item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdBuildHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdBuildMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdBuildLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxBuildHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxBuildMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxBuildLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtBuildHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtBuildMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtBuildLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltBuildingHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltBuildingMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltBuildingLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxBuildingHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxBuildingMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxBuildingLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			//
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_yd_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_yd_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_yd_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_lt_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_lt_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_lt_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_dx_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_dx_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_dx_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_fullnet_high_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_fullnet_mid_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_fullnet_low_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_fullnet_high_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_fullnet_mid_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_building_fullnet_low_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			//
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_yd_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_yd_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_yd_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_lt_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_lt_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_lt_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_dx_high.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_dx_mid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_dx_low.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_fullnet_high_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_fullnet_mid_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_fullnet_low_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_fullnet_high_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_fullnet_mid_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			item = new BuildMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_building_fullnet_low_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new BuildMergeDo();
			item.setDataType(MroBsTablesEnum.mroruYdBuildMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}

		{
			CellMergeDo item = new CellMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_cell_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_cell_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_cell_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_cell_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_cell_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_cell_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_cell_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_cell_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_cell_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new CellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_cell_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			SceneCellGridMergeDo item = new SceneCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdAreaGridCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_outgrid_cell.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new SceneCellGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_outgrid_cell.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			SceneCellMergeDo item = new SceneCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdAreaCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtAreaCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxAreaCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltAreaCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxAreaCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_cell_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_cell_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_cell_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_cell_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_cell_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			//
			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_cell_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_cell_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_cell_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_cell_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneCellMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_cell_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			SceneGridMergeDo item = new SceneGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdAreaGrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtAreaGrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxAreaGrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltAreaGrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxAreaGrid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_outgrid_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_outgrid_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_outgrid_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_outgrid_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_outgrid_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_outgrid_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_outgrid_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneGridMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_outgrid_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

		}

		{
			SceneMergeDo item = new SceneMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdArea.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MroBsTablesEnum.mroLtArea.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MroBsTablesEnum.mroDxArea.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdltArea.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MroBsTablesEnum.mroYddxArea.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_area_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_yd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_lt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_dx.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_fullnet_ydlt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new SceneMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_area_fullnet_yddx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		// // mro round grid bigger 9.12
		// {
		// OutGridMergeBySize item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_YD_OUTGRID_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_YD_OUTGRID_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_YD_OUTGRID_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_DX_OUTGRID_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_DX_OUTGRID_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_DX_OUTGRID_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_LT_OUTGRID_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_LT_OUTGRID_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutGridMergeBySize();
		// item.setDataType(MRO_ROUND_LT_OUTGRID_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		// }
		//
		// {
		// InGridMergeBySize item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_YD_INGRID_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_YD_INGRID_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_YD_INGRID_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_DX_INGRID_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_DX_INGRID_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_DX_INGRID_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_LT_INGRID_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_LT_INGRID_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InGridMergeBySize();
		// item.setDataType(MRO_ROUND_LT_INGRID_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		// }
		//
		// {
		// OutCellGridMergeBySize item = new OutCellGridMergeBySize();
		// item.setDataType(MRO_ROUND_OUTGRID_CELL_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutCellGridMergeBySize();
		// item.setDataType(MRO_ROUND_OUTGRID_CELL_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new OutCellGridMergeBySize();
		// item.setDataType(MRO_ROUND_OUTGRID_CELL_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		// }
		//
		// {
		// InCellGridMergeBySize item = new InCellGridMergeBySize();
		// item.setDataType(MRO_ROUND_INGRID_CELL_HIGH);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InCellGridMergeBySize();
		// item.setDataType(MRO_ROUND_INGRID_CELL_MID);
		// mergeDataMap.put(item.getDataType(), item);
		//
		// item = new InCellGridMergeBySize();
		// item.setDataType(MRO_ROUND_INGRID_CELL_LOW);
		// mergeDataMap.put(item.getDataType(), item);
		// }

		// yzx add EVENT 2017.9.26
		{
			EventCell_mergeDo item = new EventCell_mergeDo();
			item.setDataType(TypeIoEvtEnum.CELLEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventInCellGrid_mergeDo item = new EventInCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.IHCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventInCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.IMCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventInCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.ILCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventInGrid_mergeDo item = new EventInGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.IHGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventInGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.IMGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventInGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.ILGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventOutCellGrid_mergeDo item = new EventOutCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.OHCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventOutCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.OMCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventOutCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.OLCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventOutGrid_mergeDo item = new EventOutGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.OHGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventOutGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.OMGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventOutGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.OLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventBuildCellGrid_mergeDo item = new EventBuildCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.BHCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.BMCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.BLCELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventBuildGrid_mergeDo item = new EventBuildGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.BHGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.BMGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.BLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventArea_mergeDo item = new EventArea_mergeDo();
			item.setDataType(TypeIoEvtEnum.AREAEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventAreaGrid_mergeDo item = new EventAreaGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.AREAGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventAreaCellGrid_mergeDo item = new EventAreaCellGrid_mergeDo();
			item.setDataType(TypeIoEvtEnum.AREACELLGRIDEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			EventAreaCell_mergeDo item = new EventAreaCell_mergeDo();
			item.setDataType(TypeIoEvtEnum.AREACELLEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			UserCellMergeDo item = new UserCellMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserCell.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			ImeiMergeDo item = new ImeiMergeDo();
			item.setDataType(MroBsTablesEnum.mrImeiYd.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new ImeiMergeDo();
			item.setDataType(MroBsTablesEnum.mrImeiLt.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new ImeiMergeDo();
			item.setDataType(MroBsTablesEnum.mrImeiDx.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			MdtImeiMergeDo item = new MdtImeiMergeDo();
			item.setDataType(MdtTablesEnum.mdtimm_tac.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new MdtImeiMergeDo();
			item.setDataType(MdtTablesEnum.mdtlog_tac.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		
		//yzx add BuildPos/BuildCellPos/EventBuildPos/EventBuildCellPos at 20180313
		{
			BuildPosMergeDo item = new BuildPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdBuildPosHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdBuildPosMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroYdBuildPosLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new BuildPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroruYdBuildPosMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			BuildCellPosMergeDo item = new BuildCellPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildCellPosHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildCellPosMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildCellPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildCellPosLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new BuildCellPosMergeDo();
			item.setDataType(MroBsTablesEnum.mroruBuildCellPosMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventBuildPos_mergeDo item = new EventBuildPos_mergeDo();
			item.setDataType(TypeIoEvtEnum.BHPOSEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildPos_mergeDo();
			item.setDataType(TypeIoEvtEnum.BMPOSEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildPos_mergeDo();
			item.setDataType(TypeIoEvtEnum.BLPOSEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		
		{
			EventBuildCellPos_mergeDo item = new EventBuildCellPos_mergeDo();
			item.setDataType(TypeIoEvtEnum.BHCELLPOSEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildCellPos_mergeDo();
			item.setDataType(TypeIoEvtEnum.BMCELLPOSEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new EventBuildCellPos_mergeDo();
			item.setDataType(TypeIoEvtEnum.BLCELLPOSEVT.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		// ch add 180416
		{
			UserOutCellGridMergeDo item = new UserOutCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserOutgridCellHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new UserOutCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserOutgridCellMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new UserOutCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserOutgridCellLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		{
			UserInCellGridMergeDo item = new UserInCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserIngridCellHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new UserInCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserIngridCellMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new UserInCellGridMergeDo();
			item.setDataType(MroBsTablesEnum.mroUserIngridCellLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}

		{
			ResidentMergeDo item = new ResidentMergeDo();
			item.setDataType(MroBsTablesEnum.mroCellResident.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
//		{
//			ResidentUserCellMergeDo item = new ResidentUserCellMergeDo();
//			item.setDataType(MroBsTablesEnum.mroResidentUserCell.getIndex());
//			mergeDataMap.put(item.getDataType(), item);
//		}

		{

			BuildPosUserMergeDo item = new BuildPosUserMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildPosUserHigh.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildPosUserMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildPosUserMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);

			item = new BuildPosUserMergeDo();
			item.setDataType(MroBsTablesEnum.mroBuildPosUserLow.getIndex());
			mergeDataMap.put(item.getDataType(), item);
			
			item = new BuildPosUserMergeDo();
			item.setDataType(MroBsTablesEnum.mroruBuildPosUserMid.getIndex());
			mergeDataMap.put(item.getDataType(), item);
		}
		/*
		 *  20180711 zhaikaishun
		 *  高铁的mro的新事件
		 */
		{
		    //高铁栅格统计
            OutGridMerge outGridMergeHsr = new OutGridMerge();
            outGridMergeHsr.setDataType(MroBsTablesEnum.mroHsrYdGrid.getIndex());
		    mergeDataMap.put(outGridMergeHsr.getDataType(),outGridMergeHsr);

		    //高铁小区栅格统计
            OutCellGridMergeDo outCellGridMergeDo = new OutCellGridMergeDo();
            outCellGridMergeDo.setDataType(MroBsTablesEnum.mroHsrYdGridCell.getIndex());
            mergeDataMap.put(outCellGridMergeDo.getDataType(),outCellGridMergeDo);

            // 高铁小区统计
			HsrCellStatMergeDo hsrCellStatMergeDo = new HsrCellStatMergeDo();
			hsrCellStatMergeDo.setDataType(MroBsTablesEnum.mroHsrYdCell.getIndex());
			mergeDataMap.put(hsrCellStatMergeDo.getDataType(),hsrCellStatMergeDo);

			//高铁车次路段
            TrainSegStatMergeDo trainSegStatMergeDo = new TrainSegStatMergeDo();
            trainSegStatMergeDo.setDataType(MroBsTablesEnum.mroHsrYdTrainSeg.getIndex());
            mergeDataMap.put(trainSegStatMergeDo.getDataType(),trainSegStatMergeDo);

            //高铁车次路段小区
            TrainSegCellStatMergeDo trainSegCellStatMergeDo = new TrainSegCellStatMergeDo();
            trainSegCellStatMergeDo.setDataType(MroBsTablesEnum.mroHsrYdTrainSegCell.getIndex());
            mergeDataMap.put(trainSegCellStatMergeDo.getDataType(),trainSegCellStatMergeDo);

            //联通高铁栅格统计
            OutGridMerge outGridMergeLtHsr = new OutGridMerge();
            outGridMergeLtHsr.setDataType(MroBsTablesEnum.mroHsrLtGrid.getIndex());
            mergeDataMap.put(outGridMergeLtHsr.getDataType(),outGridMergeLtHsr);

            //联通车次路段
            TrainSegStatMergeDo trainSegStatMergeDoLT = new TrainSegStatMergeDo();
            trainSegStatMergeDoLT.setDataType(MroBsTablesEnum.mroHsrLtTrainSeg.getIndex());
            mergeDataMap.put(trainSegStatMergeDoLT.getDataType(),trainSegStatMergeDoLT);

            //电信栅格统计
            OutGridMerge outGridMergeDxHsr = new OutGridMerge();
            outGridMergeDxHsr.setDataType(MroBsTablesEnum.mroHsrDxGrid.getIndex());
            mergeDataMap.put(outGridMergeDxHsr.getDataType(),outGridMergeDxHsr);

            //电信车次路段
            TrainSegStatMergeDo trainSegStatMergeDoDX = new TrainSegStatMergeDo();
            trainSegStatMergeDoDX.setDataType(MroBsTablesEnum.mroHsrDxTrainSeg.getIndex());
            mergeDataMap.put(trainSegStatMergeDoDX.getDataType(),trainSegStatMergeDoDX);
		}
		/*
		 *  20180711 zhaikaishun
		 *  高铁的xdr_locall的事件
		 */
        {
            // 高铁室外栅格
            EventOutGrid_mergeDo eventOutGridMergeDoHsr = new EventOutGrid_mergeDo();
            eventOutGridMergeDoHsr.setDataType(TypeIoEvtEnum.HSRGRIDEVT.getIndex());
            mergeDataMap.put(eventOutGridMergeDoHsr.getDataType(),eventOutGridMergeDoHsr);

            //高铁室外栅格小区
            EventOutCellGrid_mergeDo eventOutCellGridMergeDoHsr = new EventOutCellGrid_mergeDo();
            eventOutCellGridMergeDoHsr.setDataType(TypeIoEvtEnum.HSRCELLGRIDEVT.getIndex());
            mergeDataMap.put(eventOutCellGridMergeDoHsr.getDataType(),eventOutCellGridMergeDoHsr);

            //高铁小区
            EventCell_mergeDo eventCell_mergeDo = new EventCell_mergeDo();
            eventCell_mergeDo.setDataType(TypeIoEvtEnum.HSRCELLEVT.getIndex());
            mergeDataMap.put(eventCell_mergeDo.getDataType(),eventCell_mergeDo);

            //高铁路段
            EventHsrTrainSegMergeDo eventHsrTrainSegMergeDo = new EventHsrTrainSegMergeDo();
            eventHsrTrainSegMergeDo.setDataType(TypeIoEvtEnum.HSRTRAINSEGEVT.getIndex());
            mergeDataMap.put(eventHsrTrainSegMergeDo.getDataType(),eventHsrTrainSegMergeDo);

            // 高铁路段小区
            EventHsrTrainSegCellMergeDo eventHsrTrainSegMergeDoCell = new EventHsrTrainSegCellMergeDo();
            eventHsrTrainSegMergeDoCell.setDataType(TypeIoEvtEnum.HSRTRAINSEGCELLEVT.getIndex());
            mergeDataMap.put(eventHsrTrainSegMergeDoCell.getDataType(),eventHsrTrainSegMergeDoCell);


        }

		{
			//XDR小区统计
			XdrCellMergeDo xdrCellMergeDo = new XdrCellMergeDo();
			xdrCellMergeDo.setDataType(TypeIoEvtEnum.XDR_CELL.getIndex());
			mergeDataMap.put(xdrCellMergeDo.getDataType(), xdrCellMergeDo);
		}

		{
			//用户xdr，mr统计
			UserStatMergeDo userStatMergeDo = new UserStatMergeDo();
			userStatMergeDo.setDataType(MroBsTablesEnum.xdrCountStat.getIndex());
			mergeDataMap.put(userStatMergeDo.getDataType(), userStatMergeDo);
		}

		{
			//用户小区5分钟统计指标
			UserCellByMinMergeDo userCellByMinMergeDo = new UserCellByMinMergeDo();
			userCellByMinMergeDo.setDataType(MroBsTablesEnum.mroUserCellByMins.getIndex());
			mergeDataMap.put(userCellByMinMergeDo.getDataType(), userCellByMinMergeDo);
		}
		
		return true;
	}

	public IMergeDataDo getMergeDataObject(int dataType) throws InstantiationException, IllegalAccessException
	{
		IMergeDataDo mergeDataDo = mergeDataMap.get(dataType);
		IMergeDataDo newItem = mergeDataDo.getClass().newInstance();
		newItem.setDataType(mergeDataDo.getDataType());
		return newItem;
	}

}
