package cn.mastercom.bigdata.StructData;

/**
 * Qci 数据，
 * 作用 1、隐藏数组长度细节 2、隐藏具体百分比转化规则 
 * @author Kwong
 */
public class LteScPlrQciData
{
	//mr qci数组长度
	public static final int MR_QCI_ARRAY_LENGTH = 9;
	public final double[] LteScPlrULQci;
	public final double[] LteScPlrDLQci;
	public final double[] formatedULQci;// 9
	public final double[] formatedDLQci;// 9
	
	public LteScPlrQciData(final double[] LteScPlrULQci, final double[] LteScPlrDLQci){
		this.LteScPlrULQci = LteScPlrULQci;
		this.LteScPlrDLQci = LteScPlrDLQci;
		
		this.formatedULQci = new double[MR_QCI_ARRAY_LENGTH];
		this.formatedDLQci = new double[MR_QCI_ARRAY_LENGTH];
		for(int i = 0; i < MR_QCI_ARRAY_LENGTH; i++){
			formatedULQci[i] = valueOfQCI(LteScPlrULQci[i]);
			formatedDLQci[i] = valueOfQCI(LteScPlrDLQci[i]);
		}	
	}
	
	private double valueOfQCI(double value)
	{
		if(value >= 0 && value <= 1000)
		{
			return value / 1000D;
		}
		else
		{
			return -1D;
		}
	}
	
	public void toString(StringBuffer bf, String spliter){
		
	}
	
	public void clear(){
		for(int i = 0; i < MR_QCI_ARRAY_LENGTH; i++)
		{
			formatedULQci[i] = 0D;
			formatedDLQci[i] = 0D;
		}
	}
	
}
