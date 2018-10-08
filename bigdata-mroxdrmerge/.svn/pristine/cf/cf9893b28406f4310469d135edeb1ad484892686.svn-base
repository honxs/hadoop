package cn.mastercom.bigdata.locuser_v3;

import java.util.List;
import java.util.Random;

import cn.mastercom.bigdata.StructData.SIGNAL_MR_All;
import cn.mastercom.bigdata.StructData.StaticConfig;
import cn.mastercom.bigdata.util.ResultOutputer;
import cn.mastercom.bigdata.mro.stat.tableEnum.MroCsFgTableEnum;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;

public class UserLocer
{
	public ReportProgress rptProgress = new ReportProgress();

	public CfgInfo cInfo = new CfgInfo();
	public CellPorp cPorp = new CellPorp();
	public SamScsp sScsp = new SamScsp();
	public BuildAna buildAna = new BuildAna();
	public SamLocs sLocs = new SamLocs();

	public DataUnit dataUnit = new DataUnit();

	public UserLocer()
	{
	}

	public void DoWork(int nType, List<SIGNAL_MR_All> lSams, ResultOutputer resultOutputer)
	{
		// 1.读取配置
		if (!cInfo.Init(rptProgress))
		{
			rptProgress.writeLog(0, "配置参数错误。");
			return;
		}

		dataUnit.Clear();

		// 2.读取采样点
		SamFiles sf = new SamFiles(rptProgress);

		if (sf.GetNext(dataUnit, lSams))
		{
			if (dataUnit.siCount() == 0)
			{
				return;
			}

			// 3.识别相关服务小区和邻区属性
			SetCell();
			// 4.构造session和切片
			GetScsp();
			// 5.定位
			SetLocs();
			// 6.输出
			OutPut(nType);
			// 用于做室内分析 by lanjiancai
			//DoBuildAna(resultOutputer);

			dataUnit.Clear();
		}
	}

	private void SetCell()
	{
		rptProgress.writeLog(0, "小区属性...");

		cPorp.SetCell(cInfo, dataUnit);
	}

	private void GetScsp()
	{
		rptProgress.writeLog(0, "段落分析...");

		sScsp.GetScsp(dataUnit, rptProgress);
	}

	private void DoBuildAna(ResultOutputer resultOutputer)
	{
		rptProgress.writeLog(0, "室分问题分析...");
		try
		{
			for (IndoorErrResult temp : buildAna.AnaBuild(dataUnit, rptProgress).values())
			{
				resultOutputer.pushData(MroCsFgTableEnum.indoorErr.getIndex(), temp.toString());
			}
		}
		catch (Exception e)
		{
		}
	}

	private void SetLocs()
	{
		rptProgress.writeLog(0, "开始定位...");

		try
		{
			sLocs.SetLocs(dataUnit, cInfo, rptProgress);
		}
		catch (Exception ee)
		{
		}
	}

	private void OutPut(int nType)
	{
		rptProgress.writeLog(0, "开始生成...");

		long spCount = dataUnit.spCount();
		if (spCount == 0)
		{
			return;
		}

		int OutSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuOutSize());
		int InSize = Integer.parseInt(MainModel.GetInstance().getAppConfig().getSimuInSize());
		int OutTimes = OutSize / 5;
		int InTimes = InSize / 5;

		int ilong = 0;
		int ilati = 0;
		int buildingid = -1;
		int ilevel = -1;
		try
		{
			for (EciUnit eunit : dataUnit.eciUnits.values())
			{
				for (MrUser mu : eunit.muser.values())
				{
					for (MrSec msc : mu.sections)
					{
						for (MrSplice sl : msc.splices)
						{
							Random outrd = new Random();
							Random inrd = new Random();

							List<MrSam> msl = mu.samples;

							for (MrSam ms : msl)
							{
								if (ms.itime > sl.splice_etime || ms.itime < sl.splice_btime)
								{
									continue;
								}

								ilong = 0;
								ilati = 0;
								buildingid = -1;
								ilevel = -1;
								if (ms.longitude > 0)
								{
									ilong = ms.longitude;
									ilati = ms.latitude;
									buildingid = ms.buildingid;
									ilevel = ms.ilevel;
								}
								else
								{
									if (sl.longitude > 0)
									{
										if (sl.buildingid > 0)
										{
											int rd = inrd.nextInt(InTimes * InTimes);
											ilong = RandPos(sl.longitude, 100, InSize, 5, rd % InTimes, InTimes);
											ilati = RandPos(sl.latitude, 90, InSize, 5, rd / InTimes, InTimes);
										}
										else
										{
											int rd = outrd.nextInt(OutTimes * OutTimes);
											ilong = RandPos(sl.longitude, 100, OutSize, 5, rd % OutTimes, OutTimes);
											ilati = RandPos(sl.latitude, 90, OutSize, 5, rd / OutTimes, OutTimes);
										}
										buildingid = sl.buildingid;
										ilevel = sl.ilevel;
									}
								}

								// 没有的才赋值
								if (ms.mall.tsc.longitude <= 0 && ilong > 0)
								{
									ms.mall.ibuildingID = buildingid;
									ms.mall.iheight = (short) ilevel;
									ms.mall.tsc.longitude = ilong;
									ms.mall.tsc.latitude = ilati;
									ms.mall.locSource = StaticConfig.LOCTYPE_LOW;
									if ((sl.isroad & FingerGrid.HITYPE) > 0)
									{
										if (sl.buildingid > 0)
										{
											ms.mall.locType = "fg7";
										}
										else
										{
											ms.mall.locType = "fg6";
										}
									}
									else
									{
										ms.mall.locType = "fg8";
									}
									ms.mall.indoor = sl.doortype; // 1室内2室外
									ms.mall.locType = "fp" + String.valueOf(sl.loctype); // 1小区
																						// 2simu
									if (nType == 1)
									{
										if (sl.doortype == 1)
										{
											ms.mall.testType = StaticConfig.TestType_CQT;
										}
										else if (sl.doortype == 2)
										{
											ms.mall.testType = StaticConfig.TestType_DT;
										}
									}
								}
								else if (ms.mall.tsc.longitude > 0 && ilong > 0)
								{
									ms.mall.simuLongitude = ilong;
									ms.mall.simuLatitude = ilati; 
									ms.mall.serviceType = (int)(sl.grate*10000L);
									ms.mall.subServiceType = sl.gcount;
								}
								if ((sl.doortype == 1 || sl.doortype == 3) && ms.mall.ibuildingID > 0)
								{
									ms.mall.samState = StaticConfig.ACTTYPE_IN;
								}
								else if (ms.mall.tsc.longitude > 0)
								{
									ms.mall.samState = StaticConfig.ACTTYPE_OUT;
								}
							}
						}
					}
				}
			}
		}
		catch (Exception ee)
		{
		}
		rptProgress.writeLog(0, "生成完毕。");
	}

	private int RandPos(int ipos, int nspan, int nsize, int osize, int rd, int ntimes)
	{
		int ilola = ipos / (nsize * nspan) * (nsize * nspan) + rd * (osize * nspan) + (osize * nspan) / 2;

		return ilola;
	}
}
