package cn.mastercom.bigdata.mrloc.stat;

import cn.mastercom.bigdata.project.enums.IStandAloneEnum;

public enum MrLocEnum implements IStandAloneEnum
{
	DataType_MR_INSAMPLE_MID("tb_mr_insample_mid"),
	DataType_MR_CELL_YD("tb_mr_cell_yd"),
	DataType_MR_INGRID_MID_YD("tb_mr_ingrid_mid_yd"),
	DataType_MR_INGRID_CELL_MID("tb_mr_ingrid_cell_mid"),
	DataType_MR_BUILDING_MID_YD("tb_mr_building_mid_yd"),
	DataType_MR_BUILDING_CELL_MID("tb_mr_building_cell_mid"),
	
	DataType_MR_OUTSAMPLE_MID("tb_mr_outsample_mid"),
	DataType_MR_OUTGRID_MID_YD("tb_mr_outgrid_mid_yd"),
	DataType_MR_OUTGRID_CELL_MID("tb_mr_outgrid_cell_mid");
	
	private int mark = 0;
	private String name = null;
	
	MrLocEnum(String tableName)
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
