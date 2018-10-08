package cn.mastercom.bigdata.pha.stat;

import cn.mastercom.bigdata.project.enums.IStandAloneEnum;

public enum PhaEnum implements IStandAloneEnum
{
	DataType_PHA_INSAMPLE_MID("tb_pha_insample_mid"),
	DataType_PHA_CELL_YD("tb_pha_cell_yd"),
	DataType_PHA_INGRID_MID_YD("tb_pha_ingrid_mid_yd"),
	DataType_PHA_INGRID_CELL_MID("tb_pha_ingrid_cell_mid"),
	DataType_PHA_BUILDING_MID_YD("tb_pha_building_mid_yd"),
	DataType_PHA_BUILDING_CELL_MID("tb_pha_building_cell_mid"),
	
	DataType_PHA_OUTSAMPLE_MID("tb_pha_outsample_mid"),
	DataType_PHA_OUTGRID_MID_YD("tb_pha_outgrid_mid_yd"),
	DataType_PHA_OUTGRID_CELL_MID("tb_pha_outgrid_cell_mid");
	
	private int mark = 0;
	private String name = null;
	
	PhaEnum(String tableName)
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
