package cn.mastercom.bigdata.uep.stat;

import cn.mastercom.bigdata.project.enums.IStandAloneEnum;

public enum UepEnum implements IStandAloneEnum
{
	DataType_UEP_INSAMPLE_MID("tb_uep_insample_mid"),
	DataType_UEP_CELL_YD("tb_uep_cell_yd"),
	DataType_UEP_INGRID_MID_YD("tb_uep_ingrid_mid_yd"),
	DataType_UEP_INGRID_CELL_MID("tb_uep_ingrid_cell_mid"),
	DataType_UEP_BUILDING_MID_YD("tb_uep_building_mid_yd"),
	DataType_UEP_BUILDING_CELL_MID("tb_uep_building_cell_mid"),
	
	DataType_UEP_OUTSAMPLE_MID("tb_uep_outsample_mid"),
	DataType_UEP_OUTGRID_MID_YD("tb_uep_outgrid_mid_yd"),
	DataType_UEP_OUTGRID_CELL_MID("tb_uep_outgrid_cell_mid");

	
	private int mark = 0;
	private String name = null;
	
	UepEnum(String tableName)
	{
		mark = this.hashCode();
		this.name = tableName;
	}

	public int getMark()
	{
		
		return mark;
	}

	public String getName()
	{
		return name;
	}
	
}
