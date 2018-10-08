package cn.mastercom.bigdata.locuser;

public class Property
{
    // 是否有室分（1有室分，2无室分） + 是否室内（0未知,1室内，2室外,3 未知转室内 ,4 未知转室外） + 运动静止（1运动，2静止）
    public static int static_indoor_ccell = 112;//室分室内静止用户
    public static int move_indoor_ccell = 111;//室分室内运动用户

    // out表 非室分eci->100米以内的栅格  
    public static int static_outdoor_ccell = 122;//室分室外静止用户
    public static int move_outdoor_ccell = 121;//室分室外运动用户

    // build表
    public static int static_indoor_ccell_no = 212;//无室分室内静止用户
    public static int move_indoor_ccell_no = 211;//无室分室内运动用户

    // out表
    public static int static_outdoor_ccell_no = 222; //无室分室外静止用户
    public static int move_outdoor_ccell_no = 221;//无室分室外运动用户

    // build+out (室分判断buildid，无室分判断100m)
    public static int static_unknown_ccell = 102;//室分未知静止
    public static int move_unknown_ccell = 101;//室分未知运动

    public static int static_unknown_ccell_no = 202;//未知无室分静止        
    public static int move_unknown_ccell_no = 201;//未知无室分运动
}
