package cn.mastercom.bigdata.util.ftp;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class FtpRuleTime
{
    public List<String> ReplaceTime(List<String> strs, Date date)
    {
        List<String> result = new ArrayList<String>();
        for (String str : strs)
        {
            result.add(ReplaceTime(str, date));
        }
        
        return result;
    }

    public String ReplaceTime(String str, Date date)
    {
        String[] arrs = str.split("\\$TIME\\{");
        StringBuffer sb = new StringBuffer();
        sb.append(arrs[0]);
        for (int i = 1; i < arrs.length; i++)
        {
            int index = arrs[i].indexOf("}");
            String strTime = arrs[i].substring(0, index);
            String strValue = arrs[i].substring(index + 1);
            arrs[i] = getTimeString(strTime, date) + strValue;
            sb.append(arrs[i]);
        }
        return sb.toString();
    }

    /// <summary>
    /// $TIME{HH,8,1+1}
    /// </summary>
    public String getTimeString(String strTime, Date date)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        String[] arrs = strTime.split(",|\\+");
        String timeFormat = arrs[0];
        // 时间加法
        arrs = strTime.split(",");
        if (arrs.length >= 2) cal.add(Calendar.HOUR, strToInt(arrs[1]));
        if (arrs.length >= 3) cal.add(Calendar.MINUTE, strToInt(arrs[2]));
        //
        String result = getTimeStringDetail(timeFormat, cal.getTime());
        //
        arrs = strTime.split("\\+");
        if (arrs.length >= 2)
        {
            result = String.format("%02d", strToInt(result) + strToInt(arrs[1]));
        }
        return result;
    }

    private String getTimeStringDetail(String strTime, Date date)
    {
        SimpleDateFormat sdf = new SimpleDateFormat(strTime);

        return sdf.format(date);
    }

    private int strToInt(String strValue)
    {
        return Integer.parseInt(strValue.split(",|\\+")[0].trim());
    }

    public static void main(String[] args) throws Exception
    {
        String line = "ab/$TIME{yyMMddHHmm}/mm/$TIME{yyMMddHHmm,1,2}/$TIME{dd+2, 24}";

        FtpRuleTime lrl = new FtpRuleTime();
        String str = lrl.ReplaceTime(line, new Date());
        @SuppressWarnings("unused")
		int x = str.length();
    }
}
