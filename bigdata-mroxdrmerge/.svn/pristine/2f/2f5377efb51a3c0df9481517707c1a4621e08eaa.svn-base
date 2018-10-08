package cn.mastercom.bigdata.stat.imsifill.deal;

import cn.mastercom.bigdata.xdr.prepare.stat.XdrPrepareTablesEnum;
import org.apache.hadoop.io.Text;

import cn.mastercom.bigdata.util.DataGeter;
import cn.mastercom.bigdata.util.LOGHelper;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;

public class LocImsiFillDeal
{
	// public static final int Location = 1;
	
	private ResultOutputer resultOutputer;
	private String[] strs;
	private ImsiIPMng imsiIPMng;
	private StringBuffer sb = new StringBuffer();

	public LocImsiFillDeal(ResultOutputer resultOutputer)
	{
		this.resultOutputer = resultOutputer;
	}

	public void deal(ImsiIPKey key, Iterable<Text> values)
	{
		// imsi ip data
		if (key.getDataType() == 1)
		{
			imsiIPMng = new ImsiIPMng();

			for (Text value : values)
			{
				//LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value);
				strs = value.toString().split("\t", -1);
				ImsiIPItem item = new ImsiIPItem();
				try
				{
					item.imsi = Long.parseLong(strs[1]);
					item.userIP = strs[2];
					item.userPort = Integer.parseInt(strs[3]);
					item.serverIP = strs[4];
					item.time = Integer.parseInt(strs[5]);
					item.eci = Integer.parseInt(strs[6]);
				}
				catch (NumberFormatException e)
				{
				}
				imsiIPMng.addImsiIPItem(item);
			}

			imsiIPMng.finInit();
		}
		// location data
		else if (key.getDataType() == 2)
		{
			long imsi = 0;
			int itime = 0;
			int httptime = 0;
			int locTime = 0;
			int eci = 0;
			String userIP = "";
			int port = 0;
			String serverIP = "";
			int location = 0;
			String loctp = "";
			double radius = 0;
			double longitude = 0;
			double latitude = 0;

			long myImsi = 0;

			for (Text value : values)
			{
				// LOGHelper.GetLogger().writeLog(LogType.info, key.toString() +
				// "\t" + value);
				String company = "";
				String wifis = "";
				String host = "";
				try
				{
					strs = value.toString().split("\\|", -1);

					itime = (int) (DataGeter.GetLong(strs[1]) / 1000L);
					locTime = (int) (DataGeter.GetLong(strs[2]) / 1000L);
					eci = DataGeter.GetInt(strs[3]);
					userIP = strs[4];
					port = DataGeter.GetInt(strs[5]);
					serverIP = strs[6];
					location = DataGeter.GetInt(strs[7]);
					loctp = strs[8];
					radius = DataGeter.GetDouble(strs[9]);
					longitude = DataGeter.GetDouble(strs[10]);
					latitude = DataGeter.GetDouble(strs[11]);
					if (strs.length >= 15)
					{
						company = strs[12];
						wifis = strs[13];
						host = strs[14];
					}

					if (strs[0].indexOf('\t') > 0)
					{
						strs[0] = strs[0].substring(strs[0].indexOf('\t') + 1);
					}
					try
					{
						imsi = DataGeter.GetLong(strs[0]);
					}
					catch (Exception e)
					{
					}

					myImsi = 0;
					ImsiIPItem imsiIPItem = null;

					if (imsiIPMng != null)
					{
						imsiIPItem = imsiIPMng.getNearestImsiIP(itime);
						if (imsiIPItem != null)
						{
							myImsi = imsiIPItem.imsi;
							httptime = imsiIPItem.time;
							eci = imsiIPItem.eci;
							if (itime == locTime)
							{// 说明locTime等于采集时间，则采集时间要改成http XDR的时间。
								locTime = httptime;
							}
						}
					}

					if (imsi <= 0)
					{
						imsi = myImsi;
					}
					if (imsi > 0)
					{
						sb.delete(0, sb.length());
						sb.append(imsi);
						sb.append("|");
						sb.append(httptime * 1000L);
						sb.append("|");
						sb.append(locTime * 1000L);
						sb.append("|");
						sb.append(eci);
						sb.append("|");
						sb.append(userIP);
						sb.append("|");
						sb.append(port);
						sb.append("|");
						sb.append(serverIP);
						sb.append("|");
						sb.append(location);
						sb.append("|");
						sb.append(loctp);
						sb.append("|");
						sb.append(radius);
						sb.append("|");
						sb.append(longitude);
						sb.append("|");
						sb.append(latitude);
						sb.append("|");
						sb.append(company);
						sb.append("|");
						sb.append(wifis);
						sb.append("|");
						sb.append(host);
						sb.append("|");
						sb.append(myImsi);
						resultOutputer.pushData(XdrPrepareTablesEnum.xdrLocation.getIndex(), sb.toString());
						//LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value + "|ok");
					}
					else
					{
						LOGHelper.GetLogger().writeLog(LogType.info, key.toString() + "\t" + value + "|fail");
					}
				}
				catch (Exception e)
				{
					LOGHelper.GetLogger().writeLog(LogType.error,"locImsiFillDeal.filldata.error ", "", e);
				}
			}
			imsiIPMng = null;
		}
	}
}
