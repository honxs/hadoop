package cn.mastercom.bigdata.locuser_v3;

public class DoorType
{
    public static void SetDoorType(MrSplice splice, EciUnit eunit)
    {
        splice.doortype = 0;

        int drtype2 = GetDrtype2(splice); //是否运动，不足以判断
        int drtype3 = GetDrtype3(splice); //邻区场强特性，如果是2，则室外情况较明显
        int drtype4 = GetDrtype4(splice); //服务小区及邻区差值情况，室内外条件均较明显
        // 0未知 1室内 2室外
        if (splice.scell.cell.isindoor == 1)
        {
            // 满足 1，2，3中的所有均为室外特征；则为室外；其余均为室内；
            if (drtype2 == 2 && drtype3 == 2 && drtype4 == 2)
            {
                splice.doortype = 2;
            }
            else
            {
                splice.doortype = 1;
            }
        }
        else
        {
            int drtype1 = GetDrtype1(splice, eunit); //准确性概率较大

            if (drtype1 == 2) //室外明显特征
            {
                if (drtype2 != 1 || drtype3 != 1 || drtype4 != 1) //只要不全是室内特征
                {
                    splice.doortype = 2;
                }
                else
                {
                    splice.doortype = 1;
                }
            }
            else if (drtype1 == 1) //室内特征明显
            {
                if (drtype2 != 2 || drtype3 != 2 || drtype4 != 2) //只要不全是室外特征
                {
                    splice.doortype = 1;
                }
                else
                {
                    splice.doortype = 2;
                }
            }
            else
            {
                if ((drtype3 == 2) || (drtype4 == 2))  //邻区场强特性，如果是2，则室外情况较明显
                {
                    splice.doortype = 2;
                }
                else
                {
                    splice.doortype = 1;
                }
            }
            
			////室内：
			//// a 1 室内 and （3 or 4 室内）
			//// b 2 and 3 and 4 室内）
			//if ((drtype1 == 1 && ((drtype3 == 1 && drtype4 != 2) || (drtype4 == 1 && drtype3 != 2))) ||
			//    (drtype2 == 1 && drtype3 == 1 && drtype4 != 2))
			//{
			//    splice.doortype = 1;
			//}
			////室外：
			////a 1 室外 and （2 or 3 or 4 室外特征）
			////b （2 and 3 and 4 室外特征）
			//else if ((drtype1 == 2 && (drtype2 == 2 || drtype3 == 2 || drtype4 == 2)) ||
			//    (drtype2 == 2 && drtype3 == 2 && drtype4 != 1))
			//{
			//    splice.doortype = 2;
			//}
			////如果还未知，则增加如下室内外判断：
			////室内：
			////a 3 and 4 室内
			////室外：
			////a 3 and 4 室外
			//else if (drtype3 == 1 && drtype4 != 2)
			//{
			//    splice.doortype = 1;
			//}
			//else if (drtype3 == 2 && drtype4 != 1)
			//{
			//    splice.doortype = 2;
			//}
            
			////室外：
			////a 满足1 and 2，3，4任意一个室外； == 1 and (2 or 3 or 4) 
			////b 满足1非室内 2 and 3 and 4； == 1notin and 2 and 3 and 4
			//if (((drtype1 == 2) && (drtype2 == 2 || drtype3 == 2 || drtype4 == 2))
			//    || (drtype1 != 1 && drtype2 == 2 && drtype3 == 2 && drtype4 == 2))
			//{
			//    splice.doortype = 2;
			//}
			//else
			//{
			//    // 无室分
			//    if (splice.nicell == null)
			//    {
			//        //室内：
			//        //a 满足 1 and 2，3，4任意一个室内； == 1 and (2 or 3 or 4) 
			//        //b 满足1非室外、2 and 3 and 4 ；== 1notout and 2 and 3 and 4
			//        if (((drtype1 == 1) && (drtype2 == 1 || drtype3 == 1 || drtype4 == 1))
			//            || (drtype1 != 2 && drtype2 == 1 && drtype3 == 1 && drtype4 == 1))
			//        {
			//            splice.doortype = 1;
			//        }
			//    }
			//    else
			//    {
			//        //室内：
			//        //a 满足 1 and 2，3，4任意一个室内； == 1 and (2 or 3 or 4) 
			//        //b 满足1非室外、2 and 3 and 4 ；== 1notout and 2 and 3 and 4
			//        if (((drtype1 == 1) && (drtype2 == 1 || drtype3 == 1 || drtype4 == 1))
			//            || (drtype1 != 2 && drtype2 == 1 && drtype3 == 1 && drtype4 == 1)
			//            || (drtype1 != 2 && GetDrtype5(splice) == 1))
			//        {
			//            splice.doortype = 1;
			//        }
			//    }
			//}
        }
    }
    private static int GetDrtype1(MrSplice splice, EciUnit eunit)
    {
        if (splice.scell.cell.rsrp_avg > -130 && splice.ta >= 0 && splice.ta < EciUnit.TACNT)
        {
            if (eunit.ta_peak[splice.ta] != null)
            {
                if (eunit.ta_peak[splice.ta][0] != -1000000 && eunit.ta_peak[splice.ta][1] != -1000000)
                {
                    if (splice.scell.cell.rsrp_avg <= eunit.ta_peak[splice.ta][0])
                    {
                        return 1;
                    }
                    else if (splice.scell.cell.rsrp_avg >= eunit.ta_peak[splice.ta][1])
                    {
                        return 2;
                    }
                }
            }
        }

        return 0;
    }
    private static int GetDrtype2(MrSplice splice)
    {
        //2.1 session有移动特征，且结束时邻区室外信号强于服务小区，则为室外特性；
        //2.2 session没有移动特性，则为室内特性；
        //2.3 其余为未知特性；
        if (splice.moving == 1 && splice.scell.cell.rsrp_cnt >= 5) //有连续的5个点以上没有移动
        {
            return 1;
        }
        else if (splice.moving == 3) //移动特性，且结束时邻区室外信号强于服务小区
        {
            return 2;
        }
        else
        {
            return 0;
        }

        //return splice.moving;
    }
    private static int GetDrtype3(MrSplice splice)
    {
        //3.1 去除室内邻区的个数，切片邻区场强高于-95dB的小区数小于等于2个，则为室内特性；
        //3.2 去除室内邻区的个数，切片邻区场强高于-95dB的小区数大于等于3个，则为室外特性；
        //3.3 其余为未知特性；
        if (splice.nout95 <= 1)
        {
            return 1;
        }
        else if (splice.nout95 >= 2)
        {
            return 2;
        }

        return 0;
    }
    private static int GetDrtype4(MrSplice splice)
    {
        //4.1 主服和邻区差值在10dB及以上，且邻区场强低于-95dB为明显室内特征；
        //4.2 主服和邻区差值在6dB以内，且邻区强度高于-90dB，明显室外特征；
        //4.3 其余为未知特性；

        if (splice.nocell != null)
        {
            if ((splice.nocell.cell.rsrp_avg < -95) && (Math.abs(splice.nocell.cell.rsrp_avg - splice.scell.cell.rsrp_avg) >= 10))
            {
                return 1;
            }
            else if ((splice.nocell.cell.rsrp_avg > -90) && (Math.abs(splice.nocell.cell.rsrp_avg - splice.scell.cell.rsrp_avg) <= 6))
            {
                return 2;
            }
        }

        return 0;
    }
	//private static int GetDrtype5(MrSplice splice)
	//{
	//    //5  如果最强邻区为室分，且室分信号强度+5dB>=服务小区信号强度,且室外邻区最强低于-100dB，判别为室内，且楼宇为室分楼宇。
	//    if (splice.nocell != null)
	//    {
	//        if (splice.nicell.cell.rsrp_avg > splice.nocell.cell.rsrp_avg
	//            || splice.nicell.cell.rsrp_avg + 5 >= splice.scell.cell.rsrp_avg
	//            || splice.nocell.cell.rsrp_avg < -100)
	//        {
	//            return 1;
	//        }
	//    }
	//
	//    return 0;
	//}
}
