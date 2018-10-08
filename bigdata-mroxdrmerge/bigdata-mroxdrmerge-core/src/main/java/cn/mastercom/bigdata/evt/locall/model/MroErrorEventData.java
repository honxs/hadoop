package cn.mastercom.bigdata.evt.locall.model;

import java.util.Arrays;

import cn.mastercom.bigdata.evt.locall.stat.EventData;

/**
 * 故障事件
 * @author Kwong
 */
public class MroErrorEventData extends EventData
{
	/**
	 * 故障 枚举
	 * 将故障 名字 & 判断标准 封装
	 */
	public enum ErrorType
	{
		SINGLE_PASS("单通"), INTERRUPT("断续"), NORMAL("正常");

		private String name;

		ErrorType(String name)
		{
			this.name = name;
		}

		public static ErrorType fromQCI_1(double ulQci1, double dlQci1)
		{
			if (ulQci1 >= 0.8D || dlQci1 >= 0.8D)
			{
				return SINGLE_PASS;
			}
			else if ((ulQci1 >= 0.2D && ulQci1 < 0.8D) || (dlQci1 >= 0.2D && dlQci1 < 0.8D))
			{
				return INTERRUPT;
			}
			else 
			{
				return NORMAL;
			}
		}
		
		public static boolean isErr(double ulQci1, double dlQci1){
			if (ulQci1 < 0.2D && dlQci1 < 0.2D){
				return false;
			}		
			else {return true;}
		}
		
		public String getName(){
			return name;
		}
	}

	public MroErrorEventData()
	{
		super();
		Interface = 0;
		iKpiSet = 1;
		iProcedureType = 1;
		//默认 qci值为0
		Arrays.fill(eventDetial.fvalue, 0D);
	}

}
