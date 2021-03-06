package cn.mastercom.bigdata.mroxdrmerge;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.log4j.Logger;
import cn.mastercom.bigdata.protocol.URLStreamHandlerFactory;
import cn.mastercom.bigdata.util.IWriteLogCallBack.LogType;
import cn.mastercom.bigdata.util.LOGHelper;

import com.chinamobile.xdr.LocationInfo;

public class LocModel
{
	protected static Logger LOG = Logger.getLogger(LocModel.class);

	private static LocModel instance = null;

	public static void main(String[] args)
	{
		LocModel.GetInstance(args[0]);
	}

	public static LocModel GetInstance(String pass)
	{
		if (instance == null || setUrlConfig == null)
		{
			strPass = pass;
			if (strPass.length() > 5)
			{
				LOG.info("create LocModel instance:" + strPass.substring(0, 5));
			}
			instance = new LocModel();
		}
		else
		{
			// System.out.println("GetInstance thread:" +
			// Thread.currentThread().getId());
		}
		return instance;
	}

	private LocModel()
	{
		// File file = new
		// File(this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		LoadCTLib();
	}

	public static List<LocationInfo> DecryptLoc(String requestType, String host, String url, String downlinkContent,
			String uplinkContent, boolean bHex, boolean bHexDownlink)
	{
		if (mtdDecryptLoc != null)
		{
			try
			{
				return (List<LocationInfo>) mtdDecryptLoc.invoke(null, requestType, host, url, downlinkContent,
						uplinkContent, bHex, bHexDownlink);
			}
			catch (Exception e)
			{
				System.out.println("");
			}
		}
		return null;
	}

	public static List<String> DecryptLocString(String requestType, String host, String url, String downlinkContent,
			String uplinkContent, boolean bHex, boolean bHexDownlink, String get_post_time, String ECI, String IMSI)
	{
		if (mtdDecryptLocString != null)
		{
			try
			{
				return (List<String>) mtdDecryptLocString.invoke(null, requestType, host, url, downlinkContent,
						uplinkContent, bHex, bHexDownlink, get_post_time, ECI, IMSI);
			}
			catch (Exception e)
			{
				LOGHelper.GetLogger().writeDetailLog(LogType.info,"DecryptLocString error", "DecryptLocString error.", e);
				e.printStackTrace();
				//System.out.println("");
			}
		}
		return null;
	}

	static Method setUrlConfig = null;
	static Method mtdDecryptLoc = null;
	static Method mtdDecryptLocString = null;
	public static String strPass = "";

	private static void LoadCTLib()
	{
		if (mtdDecryptLoc != null)
		{
			// System.out.println("LoadCTLib thread:" +
			// Thread.currentThread().getId());
			return;
		}
		// System.out.println("LoadCTLib thread:" +
		// Thread.currentThread().getId());

		URL[] urls = new URL[1];
		try
		{
			URL.setURLStreamHandlerFactory(new URLStreamHandlerFactory());
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
		}

		try
		{
			if (!strPass.equals(""))
			{
				urls[0] = new URL("me:jarfile:/LocParthLib_new.dat" + "@DES$" + strPass);
			}
			else
			{
				urls[0] = new URL("me:jarfile:/LocParthLib.jar");
			}
		}
		catch (Exception e1)
		{
			e1.printStackTrace();
			LOGHelper.GetLogger().writeLog(LogType.error, "LocModel pass error","LocModel pass error! : " + e1.getMessage(), e1);
		}

		try
		{
			URLClassLoader loader = new URLClassLoader(urls);
			Thread.currentThread().setContextClassLoader(loader);
		}
		catch (Exception e)
		{
			System.out.println("LoadCTLib fail setContextClassLoader: " + e.getMessage());
			LOG.info("LoadCTLib fail setContextClassLoader: " + e.getMessage());
			LOGHelper.GetLogger().writeLog(LogType.error,"LocModel init error", "LocModel init error! : " + e.getMessage(), e);
		}

		try
		{
			Class<?> clazz = Class.forName("com.chinamobile.xdr.LocDecryptor", true, Thread.currentThread()
					.getContextClassLoader());

			setUrlConfig = clazz.getDeclaredMethod("setUrlConfig", InputStream.class);
			mtdDecryptLoc = clazz.getDeclaredMethod("DecryptLoc", String.class, String.class, String.class,
					String.class, String.class, boolean.class, boolean.class);
			mtdDecryptLocString = clazz.getDeclaredMethod("DecryptLocStr", String.class, String.class, String.class,
					String.class, String.class, boolean.class, boolean.class, String.class, String.class, String.class);

//			List<String> locinfo = DecryptLocString(
//					"post",
//					"loc.map.baidu.com",
//					"http://loc.map.baidu.com/sdk.php",
//					"",
//					"626c6f633d764b6a763875373472664469386571387037666c39664b6d7550793973394b6a3750506838726938694b324831347a5733496e51316433576a345746334d3648335a615a33496d5a6b4d44447a4d66467a7048437937757836725337343772787475573335374c74377262343876756970616a37712d327170504369396143717a38584e6c356e416c707555334d4c466b704c4e6b393365696f2d476a3961486834584d67597653303478386379783666336c31646e68344c573177663342336257396f597a70714f5452715a6d4a7666536378426c526243556b66585434654731304842513966576c55574742424a536b5965566759594c78455054555a4e597a4a71506a426f4f446b374d47423349573158636a70326543346e4b3338684b5330704944316e63555966534555654468644e45423454526c39464842675158686464444241485841514844685a4842674543364b4c6a5f66583472766a732d2d58372d7369363950612d72755378674c4b783750617876597979377676566973754933735f556a4c3261336f61436a747a62797065626b63375a6d5f4f4c674d444231596d586e627675712d7575364c7a6b384c506975377278362d6230766657765f7657735f726e7a364b79716f7154566c4d6a67752d79597a7562686e4f433273732d52375037647264474b6a346d74686f6945684d2d4b6c6441784a6e4133656d39304e48395f5a5870304b6734384e324d6f5352494d44437777506e3063414334794a455a34414630614146745452427747524639545656684a435564625144354e476c6b444330784856454a594e33316d4e7a553661695a796647777a6177524d51696c6865484a2d6348466962476c6f503335375958314444316c643056763174762e2e2537437470253344332675703d76666135393775687071626f6f752d347075726c6f366631765f2d3574345f35365f376a704f48743276574769647254696444626739614b6749474f67383752694d5765333432647a5a7245776357546c3843516b377276764c48757572476e764c617934374f77377543746f6679686f61796d3865716e7071623770504c797a7075626b73795a6e633265747232777062487874504c6e334a654f5f4d7a656774654c6839585330494975494835784b6e52785a33416a635845696348306d626d397362574673614445714e5759364d574a6859414a635731355a5756494a586856455532685956514e644430676d4351394e47304a4b5268415447525959614777334f587431624152674a32356b5932383859436f67665341754c7942384d475230485451316548425051526f5954426f5a5168465642424d6f45305a50436834435651634b41316f5941415653566c455a71717931395f796f395f75797436326a70376973372d6e6e36726e6d39653333366275506f62697573364c533175534230495f486839693732492d66685954486e4d7a6477354b6b6a4d795577386e486c354b616c2d66705f656a5371504732354b4b6a74726e72344b665f765069366f364b67706f616c6f396148685f5079754c6e4a374f79346d5a4b7834634b33793848466e4b79446739614c783469586738434e68737652784968714a797070494835384457456f66444a545755595a4c5455314c784d494c44423364784d32595846725a77306353516f6556515657425659575855454c644667625841746445523946496d4538516c30564755464c4d33516f646e687962476f6d627a556d4c72694c4c366f31253743747025334433266578743d75504f6838666e39704e2d76684a33517261614c5f3554363565445a6b64374c736f62446c76656c7437694a694e4c5a727447456a64696b32343246384e5f337970486f783544496e5a657874707a4c794d7a4c344f536175726136756566707437724f354f5f74732d6d6f387654562d4b62356a4b725467366a792d6654726a5948646773664b6a35584831457a3850774e67253743747025334433267472746d3d31343934323138333934343235",
//					true, true, "12345", "54321", "460001234512345");
//			for (int i = 0; i < locinfo.size(); i++)
//			{
//				System.out.println("check loclib right: " + locinfo.get(i));
//				LOG.info(locinfo.get(i));
//			}
		}
		catch (Exception e)
		{
			System.out.println("LoadCTLib forName fail: " + e.getMessage());
			LOG.info("LoadCTLib forName fail: " + e.getMessage());
			LOGHelper.GetLogger().writeLog(LogType.error,"LocModel loc error", "LocModel loc error3! : " + e.getMessage(), e);
		}
	}

}
