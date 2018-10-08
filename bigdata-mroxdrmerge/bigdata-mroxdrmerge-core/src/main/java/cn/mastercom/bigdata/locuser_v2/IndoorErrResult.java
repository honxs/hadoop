package cn.mastercom.bigdata.locuser_v2;

public class IndoorErrResult
{
	public int cityid;
	public long eci;
	public String eciName;
	public int suspectedPoin;// 疑似外泄点//疑似外泄_采样点数
	public long totalUserNum_seq;// 样本用户总数（连续）//样本用户数_连续采样点数
	public int suspectedPoin2;// 疑似外泄点2//疑似外泄_连续采样点数
	public int total_s_rsrp_cut;// RSRP样本采样总数//采样点数
	public int mo3DisCut_seq;// 模三干扰连续记录数//模三干扰_连续采样点数
	public int mo3DisCut_rsrp;// 模三干扰采样点数 //模三干扰_采样点数
	public long totalUserNum;// 样本用户总数//样本用户数
	public double avg_rsrp_up;// 场强均值分母
	public long avg_rsrp_down;// 场强均值分子
	public double avg_rsrp;// 场强均值 //采样点RSRP均值
	public long totalUserNum_weak;// 样本用户数-弱覆盖 //样本用户数_弱覆盖
	public long totalRsrpNum_weak;// 样本采样总数-弱覆盖 //采样点数_弱覆盖
	public long totalUserNum_seq_weak; // 样本用户数-弱覆盖（连续）//样本用户数_弱覆盖_连续采样点
	public long totalUserNum_seq_weak_noNC;// 样本用户数-弱覆盖（连续无邻区）//样本用户数_弱覆盖_连续无邻区
	public long totalUserNum_seq_weak_NC;// 样本用户数-弱覆盖（连续弱邻区）//样本用户数_弱覆盖_连续弱邻区
	public long totalUserNum_seq_Strong_NC;// 样本用户数-弱覆盖（连续强邻区）//样本用户数_弱覆盖_连续强邻区

	public String errConfidence;// 覆盖分析结果//覆盖分析结果
	public double suspectedPoinRate_seq;// 疑似外泄比例(连续) //疑似外泄_比例_连续采样点
	public double suspectedPoinRate_rsrp;// 疑似外泄比例(采样点)//疑似外泄_比例_采样点
	public double mo3DisCut_seq_rate;// 模三干扰比例(连续)//模三干扰_比例_连续采样点
	public double mo3DisCut_rsrp_rate;// 模三干扰比例(采样点)//模三干扰_比例_采样点
	public short ifweakQuestion;// 是否弱覆盖问题 //是否弱覆盖问题
	public short noDeepQuestion;// 深度覆盖不足（连续弱覆盖样本较多）//是否深度覆盖不足
	public String coverType;// 覆盖类型
	public double weakCoverUserRate;// 弱覆盖样本用户比例_5//样本用户比例_弱覆盖
	public double weakCoverUser_seq_Rate;// 弱覆盖样本用户比例(连续)_5//样本用户比例_弱覆盖_连续采样点
	public double UserNum_seq_weak_noNC_Rate;// 弱覆盖样本用户比例(无邻区部分)//样本用户比例_弱覆盖_无邻区部分
	public double UserNum_seq_weak_NC_Rate;// 弱覆盖样本用户比例(弱邻区部分)//样本用户比例_弱覆盖_弱邻区部分
	public double UserNum_seq_Strong_NC_Rate;// 弱覆盖样本用户比例(强邻区部分)//样本用户比例_弱覆盖_强邻区部分

	public static final float rsrp_weak = -100;
	public static final int span = 4;
	public static final float rsrp_strong = -90;

	public void dealSpliter(MrSplice ms)
	{
		if (ms.scell.cell.rsrp_avg >= -90 && ms.scell.cell.rsrp_cnt >= span && ms.nocell.cell.rsrp_cnt > 0 && ms.nocell.cell.rsrp_avg > -85 && ms.nout95 >= 1)
		{
			suspectedPoin++;
		}
		if (ms.scell.cell.rsrp_cnt >= span)
		{
			totalUserNum_seq++;
		}
		if (ms.scell.cell.rsrp_avg >= -90 && ms.nout95 > 2)
		{
			suspectedPoin2 += ms.scell.cell.rsrp_cnt;
		}
		total_s_rsrp_cut += ms.scell.cell.rsrp_cnt;
		if ((ms.scell.cell.pciarfcn % 65536 == ms.nicell.cell.pciarfcn % 65536) && ((ms.scell.cell.pciarfcn / 65536) % 3 == (ms.nicell.cell.pciarfcn / 65536) % 3)
				&& (ms.nicell.cell.rsrp_avg - ms.scell.cell.rsrp_avg > -3) && ms.scell.cell.rsrp_avg >= -95)
		{
			mo3DisCut_rsrp += ms.scell.cell.rsrp_cnt;
			if (ms.scell.cell.rsrp_cnt >= 3)
			{
				mo3DisCut_seq++;
			}
		}

		totalUserNum++;
		avg_rsrp_up += (ms.scell.cell.rsrp_avg * ms.scell.cell.rsrp_cnt);
		avg_rsrp_down += ms.scell.cell.rsrp_cnt;

		if (ms.scell.cell.rsrp_avg < rsrp_weak)
		{
			totalUserNum_weak++;
			totalRsrpNum_weak += ms.scell.cell.rsrp_cnt;
			if (ms.scell.cell.rsrp_cnt >= span)
			{
				totalUserNum_seq_weak++;
				if (ms.nocell.cell.rsrp_cnt == 0)
				{
					totalUserNum_seq_weak_noNC++;
				}
				else if (ms.nocell.cell.rsrp_cnt > 0)
				{
					if (ms.nocell.cell.rsrp_avg < -95)
					{
						totalUserNum_seq_weak_NC++;
					}
					if (ms.nocell.cell.rsrp_avg > rsrp_strong)
					{
						totalUserNum_seq_Strong_NC++;
					}
				}
			}
		}
	}

	public void statRate()
	{
		weakCoverUserRate = (totalUserNum_weak * 1.0) / totalUserNum;
		if (totalUserNum_seq == 0)
		{
			weakCoverUser_seq_Rate = totalUserNum_seq_weak / -0.0001;
			UserNum_seq_weak_noNC_Rate = totalUserNum_seq_weak_noNC / -0.001;
			UserNum_seq_weak_NC_Rate = totalUserNum_seq_weak_NC / -0.001;
			UserNum_seq_Strong_NC_Rate = totalUserNum_seq_Strong_NC / -0.001;
		}
		else
		{
			weakCoverUser_seq_Rate = totalUserNum_seq_weak * 1.0 / totalUserNum_seq;
			UserNum_seq_weak_noNC_Rate = totalUserNum_seq_weak_noNC * 1.0 / totalUserNum_seq;
			UserNum_seq_weak_NC_Rate = totalUserNum_seq_weak_NC * 1.0 / totalUserNum_seq;
			UserNum_seq_Strong_NC_Rate = totalUserNum_seq_Strong_NC * 1.0 / totalUserNum_seq;
		}

		if (weakCoverUser_seq_Rate > 0.05)
		{
			ifweakQuestion = 1;
			errConfidence = "可能覆盖问题";
			if (totalUserNum_seq_weak > 2000)
			{
				errConfidence = "肯定覆盖问题";
			}
		}
		else if (totalUserNum_seq_weak > 2000)
		{
			errConfidence = "可能覆盖问题";
		}
		else if (weakCoverUser_seq_Rate <= 0.05 && totalUserNum_seq_weak <= 2000)
		{
			errConfidence = "无覆盖问题";
		}
		else
		{
			errConfidence = "其它";
		}

		if (totalUserNum_seq != 0)
		{
			suspectedPoinRate_seq = (suspectedPoin * 1.0) / totalUserNum_seq;
			mo3DisCut_seq_rate = (mo3DisCut_seq * 1.0) / totalUserNum_seq;
		}
		if (total_s_rsrp_cut != 0)
		{
			suspectedPoinRate_rsrp = (suspectedPoin2 * 1.0) / total_s_rsrp_cut;
			mo3DisCut_rsrp_rate = mo3DisCut_rsrp * 1.0 / total_s_rsrp_cut;
		}
		if (totalUserNum_seq_weak > 2000)
		{
			noDeepQuestion = 1;
		}
	}

	public void setAvg_rsrp()
	{
		if (avg_rsrp_down > 0)
		{
			this.avg_rsrp = avg_rsrp_up / avg_rsrp_down;
		}
		else
		{
			avg_rsrp = 0;
		}
	}

	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		String spliter = "\t";
		sb.append(cityid);
		sb.append(spliter);
		sb.append(eci);
		sb.append(spliter);
		sb.append(eciName);
		sb.append(spliter);
		sb.append(errConfidence);
		sb.append(spliter);
		sb.append(noDeepQuestion);
		sb.append(spliter);
		sb.append(ifweakQuestion);
		sb.append(spliter);
		sb.append(total_s_rsrp_cut);
		sb.append(spliter);
		sb.append(totalRsrpNum_weak);
		sb.append(spliter);
		sb.append(avg_rsrp);
		sb.append(spliter);
		sb.append(totalUserNum);
		sb.append(spliter);
		sb.append(totalUserNum_seq);
		sb.append(spliter);
		sb.append(totalUserNum_weak);
		sb.append(spliter);
		sb.append(totalUserNum_seq_weak);
		sb.append(spliter);
		sb.append(totalUserNum_seq_weak_noNC);
		sb.append(spliter);
		sb.append(totalUserNum_seq_weak_NC);
		sb.append(spliter);
		sb.append(totalUserNum_seq_Strong_NC);
		sb.append(spliter);
		sb.append(weakCoverUserRate);
		sb.append(spliter);
		sb.append(weakCoverUser_seq_Rate);
		sb.append(spliter);
		sb.append(UserNum_seq_weak_noNC_Rate);
		sb.append(spliter);
		sb.append(UserNum_seq_weak_NC_Rate);
		sb.append(spliter);
		sb.append(UserNum_seq_Strong_NC_Rate);
		sb.append(spliter);
		sb.append(mo3DisCut_rsrp);
		sb.append(spliter);
		sb.append(mo3DisCut_seq);
		sb.append(spliter);
		sb.append(mo3DisCut_rsrp_rate);
		sb.append(spliter);
		sb.append(mo3DisCut_seq_rate);
		sb.append(spliter);
		sb.append(suspectedPoin);
		sb.append(spliter);
		sb.append(suspectedPoin2);
		sb.append(spliter);
		sb.append(suspectedPoinRate_rsrp);
		sb.append(spliter);
		sb.append(suspectedPoinRate_seq);
		return sb.toString();
	}

}
