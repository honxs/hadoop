package cn.mastercom.bigdata.mdt.stat;

import java.util.ArrayList;
import java.util.List;

import cn.mastercom.bigdata.StructData.DT_Sample_23G;
import cn.mastercom.bigdata.StructData.DT_Sample_4G;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.mro.stat.AreaCellGridStatDo_4G;
import cn.mastercom.bigdata.mro.stat.AreaCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.AreaGridStatDo_4G;
import cn.mastercom.bigdata.mro.stat.AreaStatDo_4G;
import cn.mastercom.bigdata.mro.stat.BuildCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.BuildStatDo_4G;
import cn.mastercom.bigdata.mro.stat.CellStatDO_4G;
import cn.mastercom.bigdata.mro.stat.CompositeStatDo;
import cn.mastercom.bigdata.mro.stat.DayDataDeal_4G;
import cn.mastercom.bigdata.mro.stat.IStatDo;
import cn.mastercom.bigdata.mro.stat.InGridCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.InGridStatDo_4G;
import cn.mastercom.bigdata.mro.stat.MrAreaSampleStatDo_4G;
import cn.mastercom.bigdata.mro.stat.MrSampleStatDo_4G;
import cn.mastercom.bigdata.mro.stat.OutGridCellStatDo_4G;
import cn.mastercom.bigdata.mro.stat.OutGridStatDo_4G;
import cn.mastercom.bigdata.mroxdrmerge.CompileMark;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;
import cn.mastercom.bigdata.util.ResultOutputer;

public class StatDeal_MDT_IMM
{
	private List<DayDataDeal_4G> dealList;

	public StatDeal_MDT_IMM(ResultOutputer resultOutput)
	{
		// yzx add mdtimm at 2017/12/07
		ArrayList<IStatDo> YDList = new ArrayList<IStatDo>();
		ArrayList<IStatDo> LTList = new ArrayList<IStatDo>();
		ArrayList<IStatDo> DXList = new ArrayList<IStatDo>();
		ArrayList<IStatDo> YDLTList = new ArrayList<IStatDo>();
		ArrayList<IStatDo> YDDXList = new ArrayList<IStatDo>();
		dealList = new ArrayList<DayDataDeal_4G>();
		// YD
		if (!MainModel.GetInstance().getCompile().Assert(CompileMark.NoLowSample))
		{
			YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IL, MdtTablesEnum.mdtimm_insample_low.getIndex()));
			YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OL, MdtTablesEnum.mdtimm_outsample_low.getIndex()));
			YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.NLoc, MdtTablesEnum.mdtimm_noLocSample.getIndex()));
		}
		YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IH, MdtTablesEnum.mdtimm_insample_high.getIndex()));
		YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IM, MdtTablesEnum.mdtimm_insample_mid.getIndex()));
		YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OH, MdtTablesEnum.mdtimm_outsample_high.getIndex()));
		YDList.add(new MrSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OM, MdtTablesEnum.mdtimm_outsample_mid.getIndex()));
		YDList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OH, MdtTablesEnum.mdtimm_outgrid_yd_high.getIndex()));
		YDList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OM, MdtTablesEnum.mdtimm_outgrid_yd_mid.getIndex()));
		YDList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OL, MdtTablesEnum.mdtimm_outgrid_yd_low.getIndex()));
		YDList.add(new OutGridCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OH, MdtTablesEnum.mdtimm_outgrid_cell_high.getIndex()));
		YDList.add(new OutGridCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OM, MdtTablesEnum.mdtimm_outgrid_cell_mid.getIndex()));
		YDList.add(new OutGridCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.OL, MdtTablesEnum.mdtimm_outgrid_cell_low.getIndex()));
		YDList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IH, MdtTablesEnum.mdtimm_ingrid_yd_high.getIndex()));
		YDList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IM, MdtTablesEnum.mdtimm_ingrid_yd_mid.getIndex()));
		YDList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IL, MdtTablesEnum.mdtimm_ingrid_yd_low.getIndex()));
		YDList.add(new InGridCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IH, MdtTablesEnum.mdtimm_ingrid_cell_high.getIndex()));
		YDList.add(new InGridCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IM, MdtTablesEnum.mdtimm_ingrid_cell_mid.getIndex()));
		YDList.add(new InGridCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IL, MdtTablesEnum.mdtimm_ingrid_cell_low.getIndex()));
		YDList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IH, MdtTablesEnum.mdtimm_building_yd_high.getIndex()));
		YDList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IM, MdtTablesEnum.mdtimm_building_yd_mid.getIndex()));
		YDList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IL, MdtTablesEnum.mdtimm_building_yd_low.getIndex()));
		YDList.add(new BuildCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IH, MdtTablesEnum.mdtimm_building_cell_high.getIndex()));
		YDList.add(new BuildCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IM, MdtTablesEnum.mdtimm_building_cell_mid.getIndex()));
		YDList.add(new BuildCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, StaticConfig.IL, MdtTablesEnum.mdtimm_building_cell_low.getIndex()));
		YDList.add(new CellStatDO_4G(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_cell_yd.getIndex()));
		YDList.add(new MdtImeiStatDo(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_tac.getIndex()));
		YDList.add(new MrAreaSampleStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_area_sample_yd.getIndex()));
		YDList.add(new AreaGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_area_outgrid_yd.getIndex()));
		YDList.add(new AreaCellGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_area_outgrid_cell.getIndex()));
		YDList.add(new AreaCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_area_cell_yd.getIndex()));
		YDList.add(new AreaStatDo_4G(resultOutput, StaticConfig.SOURCE_YD, MdtTablesEnum.mdtimm_area_yd.getIndex()));
		LTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.OH, MdtTablesEnum.mdtimm_outgrid_fullnet_high_ydlt.getIndex()));
		LTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.OM, MdtTablesEnum.mdtimm_outgrid_fullnet_mid_ydlt.getIndex()));
		LTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.OL, MdtTablesEnum.mdtimm_outgrid_fullnet_low_ydlt.getIndex()));
		LTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.OH, MdtTablesEnum.mdtimm_outgrid_fullnet_high_yddx.getIndex()));
		LTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.OM, MdtTablesEnum.mdtimm_outgrid_fullnet_mid_yddx.getIndex()));
		LTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.OL, MdtTablesEnum.mdtimm_outgrid_fullnet_low_yddx.getIndex()));
		LTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IH, MdtTablesEnum.mdtimm_ingrid_fullnet_high_ydlt.getIndex()));
		LTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IM, MdtTablesEnum.mdtimm_ingrid_fullnet_mid_ydlt.getIndex()));
		LTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IL, MdtTablesEnum.mdtimm_ingrid_fullnet_low_ydlt.getIndex()));
		LTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IH, MdtTablesEnum.mdtimm_ingrid_fullnet_high_yddx.getIndex()));
		LTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IM, MdtTablesEnum.mdtimm_ingrid_fullnet_mid_yddx.getIndex()));
		LTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IL, MdtTablesEnum.mdtimm_ingrid_fullnet_low_yddx.getIndex()));
		LTList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_LT, StaticConfig.IH, MdtTablesEnum.mdtimm_building_fullnet_high_ydlt.getIndex()));
		DXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, StaticConfig.IM, MdtTablesEnum.mdtimm_building_fullnet_mid_ydlt.getIndex()));
		DXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, StaticConfig.IL, MdtTablesEnum.mdtimm_building_fullnet_low_ydlt.getIndex()));
		DXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, StaticConfig.IH, MdtTablesEnum.mdtimm_building_fullnet_high_yddx.getIndex()));
		DXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, StaticConfig.IM, MdtTablesEnum.mdtimm_building_fullnet_mid_yddx.getIndex()));
		DXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, StaticConfig.IL, MdtTablesEnum.mdtimm_building_fullnet_low_yddx.getIndex()));
		DXList.add(new CellStatDO_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_cell_fullnet_ydlt.getIndex()));
		DXList.add(new CellStatDO_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_cell_fullnet_yddx.getIndex()));
		DXList.add(new AreaGridStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_area_outgrid_fullnet_ydlt.getIndex()));
		DXList.add(new AreaGridStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_area_outgrid_fullnet_yddx.getIndex()));
		DXList.add(new AreaCellStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_area_cell_fullnet_ydlt.getIndex()));
		DXList.add(new AreaCellStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_area_cell_fullnet_yddx.getIndex()));
		DXList.add(new AreaStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_area_fullnet_ydlt.getIndex()));
		DXList.add(new AreaStatDo_4G(resultOutput, StaticConfig.SOURCE_DX, MdtTablesEnum.mdtimm_area_fullnet_yddx.getIndex()));
		YDLTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.OH, MdtTablesEnum.mdtimm_outgrid_dx_high.getIndex()));
		YDLTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.OM, MdtTablesEnum.mdtimm_outgrid_dx_mid.getIndex()));
		YDLTList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.OL, MdtTablesEnum.mdtimm_outgrid_dx_low.getIndex()));
		YDLTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.IH, MdtTablesEnum.mdtimm_ingrid_dx_high.getIndex()));
		YDLTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.IM, MdtTablesEnum.mdtimm_ingrid_dx_mid.getIndex()));
		YDLTList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.IL, MdtTablesEnum.mdtimm_ingrid_dx_low.getIndex()));
		YDLTList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.IH, MdtTablesEnum.mdtimm_building_dx_high.getIndex()));
		YDLTList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.IM, MdtTablesEnum.mdtimm_building_dx_mid.getIndex()));
		YDLTList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, StaticConfig.IL, MdtTablesEnum.mdtimm_building_dx_low.getIndex()));
		YDLTList.add(new CellStatDO_4G(resultOutput, StaticConfig.SOURCE_YDLT, MdtTablesEnum.mdtimm_cell_dx.getIndex()));
		YDLTList.add(new AreaGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, MdtTablesEnum.mdtimm_area_outgrid_dx.getIndex()));
		YDLTList.add(new AreaCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, MdtTablesEnum.mdtimm_area_cell_dx.getIndex()));
		YDLTList.add(new AreaStatDo_4G(resultOutput, StaticConfig.SOURCE_YDLT, MdtTablesEnum.mdtimm_area_dx.getIndex()));
		YDDXList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.OH, MdtTablesEnum.mdtimm_outgrid_lt_high.getIndex()));
		YDDXList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.OM, MdtTablesEnum.mdtimm_outgrid_lt_mid.getIndex()));
		YDDXList.add(new OutGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.OL, MdtTablesEnum.mdtimm_outgrid_lt_low.getIndex()));
		YDDXList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.IH, MdtTablesEnum.mdtimm_ingrid_lt_high.getIndex()));
		YDDXList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.IM, MdtTablesEnum.mdtimm_ingrid_lt_mid.getIndex()));
		YDDXList.add(new InGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.IL, MdtTablesEnum.mdtimm_ingrid_lt_low.getIndex()));
		YDDXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.IH, MdtTablesEnum.mdtimm_building_lt_high.getIndex()));
		YDDXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.IM, MdtTablesEnum.mdtimm_building_lt_mid.getIndex()));
		YDDXList.add(new BuildStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, StaticConfig.IL, MdtTablesEnum.mdtimm_building_lt_low.getIndex()));
		YDDXList.add(new CellStatDO_4G(resultOutput, StaticConfig.SOURCE_YDDX, MdtTablesEnum.mdtimm_cell_lt.getIndex()));
		YDDXList.add(new AreaGridStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, MdtTablesEnum.mdtimm_area_outgrid_lt.getIndex()));
		YDDXList.add(new AreaCellStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, MdtTablesEnum.mdtimm_area_cell_lt.getIndex()));
		YDDXList.add(new AreaStatDo_4G(resultOutput, StaticConfig.SOURCE_YDDX, MdtTablesEnum.mdtimm_area_lt.getIndex()));
		// 默认添加 YD,LT,DX
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(YDList)));
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(LTList)));
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(DXList)));
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(YDLTList))
		{
			@Override
			public void dealSample(DT_Sample_4G sample)
			{
				// fullNetType说明： 0=LT/DX都不支持；1=DX支持；2=LT支持；3=LT/DX都支持
				if ((sample.fullNetType & 2) > 0)
				{
					super.dealSample(sample);
				}
			}
		});
		dealList.add(new DayDataDeal_4G(new CompositeStatDo(YDDXList))
		{
			@Override
			public void dealSample(DT_Sample_4G sample)
			{
				// fullNetType说明： 0=LT/DX都不支持；1=DX支持；2=LT支持；3=LT/DX都支持
				if ((sample.fullNetType & 1) > 0)
				{
					super.dealSample(sample);
				}
			}
		});
	}

	public void dealSample(DT_Sample_4G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
		for (DayDataDeal_4G deal : dealList)
		{
			deal.dealSample(sample);
		}
	}

	public void dealSample(DT_Sample_23G sample)
	{
		if (sample.itime == 0)
		{
			return;
		}
	}

	public int outResult()
	{
		for (DayDataDeal_4G deal : dealList)
		{
			deal.outResult();
		}

		return 0;
	}

}
