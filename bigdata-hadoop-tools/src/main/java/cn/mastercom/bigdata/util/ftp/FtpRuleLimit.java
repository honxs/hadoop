package cn.mastercom.bigdata.util.ftp;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class FtpRuleLimit
{
    public final String TimeLimitFlag = "$LIMIT{";

    public String Path = null;
    public Date Min = null;
    public Date Max = null;

    public void getTimeLimit(String line, Date date) throws Exception
    {
        int index = line.lastIndexOf(TimeLimitFlag);
        if (index < 0)
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);            
            Path = line;
            return;
        }

        Path = line.substring(0, index);
        String str = line.substring(index + TimeLimitFlag.length(), line.length() - 1);

        getTimeLimit(str);
    }

    private void getTimeLimit(String str) throws Exception
    {
        SimpleDateFormat sdf = new SimpleDateFormat();
        int index = str.indexOf(",");
    }

    public static void main(String[] args) throws Exception
    {
        String line = "abc$LIMIT{1000-01-01 00:00:00,1000-01-01 00:00:00}";

        FtpRuleLimit lrl = new FtpRuleLimit();
        lrl.getTimeLimit(line, new Date());
    }

}
