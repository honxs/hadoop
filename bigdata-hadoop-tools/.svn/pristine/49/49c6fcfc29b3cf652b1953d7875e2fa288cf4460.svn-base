package cn.mastercom.bigdata.util.spark;

import java.io.Serializable;

import cn.mastercom.bigdata.util.AAdapterReader;
import cn.mastercom.bigdata.util.DataAdapterConf.ColumnInfo;
import cn.mastercom.bigdata.util.DataAdapterConf.ParseItem;

import org.apache.spark.sql.Row;

public class RowAdapterReader extends AAdapterReader implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private Row row;

	public RowAdapterReader(ParseItem parseItem)
	{
         super(parseItem);
	}
    
    public boolean readData(Row row)
    {
    	this.row = row;
    	return true;
    }
    
    public int GetDataLenth()
    {
    	return row.length();
    }

	@Override
	public String getValue(ColumnInfo columnInfo)
	{
		if(columnInfo.pos < row.length())
		{
			return row.getString(columnInfo.pos);
		}
		return null;
	}
    
	
	
	
	
}
