package cn.mastercom.bigdata.evt.locall.struct;

import cn.mastercom.bigdata.mergestat.deal.IMergeDataDo;

import java.io.Serializable;

public class XdrCellMergeDo implements IMergeDataDo,Serializable
{
	private int dataType = 0;
	public StatXdrCell cell = new StatXdrCell();
	private StringBuffer sbTemp = new StringBuffer();
	
	@Override
	public String getMapKey()
	{
		// TODO Auto-generated method stub
		sbTemp.delete(0, sbTemp.length());
		sbTemp.append(cell.iCityID);
		sbTemp.append("_");
		sbTemp.append(cell.iInterface);
		sbTemp.append("_");
		sbTemp.append(cell.lEci);
		sbTemp.append("_");
		sbTemp.append(cell.iTime);
		return sbTemp.toString();
	}

	@Override
	public int getDataType()
	{
		return dataType;
	}

	@Override
	public int setDataType(int dataType)
	{
		this.dataType = dataType;
		return 0;
	}

	@Override
	public boolean mergeData(Object o)
	{
		XdrCellMergeDo data = (XdrCellMergeDo) o;
		for (int i = 0; i < data.cell.fvalue.length; i++)
		{
			if(data.cell.fvalue[i] > 0)
			{
				if(cell.fvalue[i] < 0){
					cell.fvalue[i] = data.cell.fvalue[i];
				}else{
					cell.fvalue[i] += data.cell.fvalue[i];
				}
			}
		}
		return true;
	}

	@Override
	public boolean fillData(String[] vals, int sPos)
	{
		cell = StatXdrCell.fillData(vals, 0);
		return true;
	}

	@Override
	public String getData()
	{
		return cell.toString();
	}

}
