package cn.mastercom.bigdata.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringHelper
{

	public static String SideTrim(String srcstr, String trimstr )
	{
		if (srcstr == null || srcstr.length() == 0 || trimstr == null || trimstr.length() == 0)
		{
			return srcstr;
		}

		int epos = 0;

		String regpattern = "[" + trimstr + "]*+";
		Pattern pattern = Pattern.compile(regpattern, Pattern.CASE_INSENSITIVE);

		StringBuffer buffer = new StringBuffer(srcstr).reverse();
		Matcher matcher = pattern.matcher(buffer);
		if (matcher.lookingAt())
		{
			epos = matcher.end();
			srcstr = new StringBuffer(buffer.substring(epos)).reverse().toString();
		}

		matcher = pattern.matcher(srcstr);
		if (matcher.lookingAt())
		{
			epos = matcher.end();
			srcstr = srcstr.substring(epos);
		}

		return srcstr;
	}

	public static boolean isBlank(CharSequence cs) {
        int strLen;
        if(cs != null && (strLen = cs.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if(!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }
	
	public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }
}
