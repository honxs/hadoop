package cn.mastercom.bigdata.protocol.me;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

public class MEURLConnection extends URLConnection {
	protected MEURLConnection(URL url) {
		super(url);
		parseKeyAndURL2(url);
	}

	protected MEURLConnection(URL url, Proxy proxy) {
		super(url);
		this.proxy = proxy;
		parseKeyAndURL2(url);
	}

	void SaveFile(InputStream is, String outfile)
	{
		try {
			if(new File(outfile).exists())
				new File(outfile).delete();
			
			FileOutputStream fos = new FileOutputStream(outfile);

			byte[] b = new byte[1024];
			int bytesRead = 0;
			while((bytesRead = is.read(b)) != -1)
			{
				fos.write(b, 0, bytesRead);
			}
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static final String aaa = "@";

	private static final String bbb = "$";

	private void parseKeyAndURL2(URL url) {
		try {
			//System.out.println(url);
			String urlString = url.toString();
			// url2
			int indexParam = urlString.lastIndexOf(aaa);
			urlFile = urlString.substring(urlString.indexOf(':') + 1, indexParam >= 0 ? indexParam : urlString.length());
			
			if(!urlFile.startsWith("jarfile:"))
			{
				URL url2 = new URL(
						urlString.substring(urlString.indexOf(':') + 1, indexParam >= 0 ? indexParam : urlString.length()));
				if (proxy != null) {
					urlConnection2 = url2.openConnection(proxy);
				} else {
					urlConnection2 = url2.openConnection();
				}
			}
			
			// paramString
			String paramString = indexParam >= 0 ? urlString.substring(indexParam + aaa.length()) : null;
			int index2 = paramString != null ? paramString.lastIndexOf(bbb) : -1;
			int index1 = paramString != null && index2 >= 0 ? paramString.lastIndexOf(bbb, index2 - 1) : -1;
			String provider = paramString != null && index1 >= 0 ? paramString.substring(0, index1) : null;
			String cipherAlgorithm = paramString != null && index2 >= 0
					? paramString.substring(index1 >= 0 ? index1 + bbb.length() : 0, index2) : null;
			String keyString = paramString != null && index2 >= 0
					? paramString.substring(index2 + bbb.length(), paramString.length()) : null;

			String keyAlgorithm = cipherAlgorithm;
			if (cipherAlgorithm != null && !cipherAlgorithm.trim().equals("")) 
			{
				if (provider != null && !provider.trim().equals("")) {
					cipher = Cipher.getInstance(cipherAlgorithm, provider);
				} else {
					cipher = Cipher.getInstance(cipherAlgorithm);
				}
				if (cipherAlgorithm.indexOf('/') >= 0) {
					keyAlgorithm = cipherAlgorithm.substring(0, cipherAlgorithm.indexOf('/'));
				}
			}

			SecretKeyFactory keyFactory = null;
			if (keyAlgorithm != null && !keyAlgorithm.trim().equals("")) 
			{
				if (provider != null && !provider.trim().equals("")) {
					keyFactory = SecretKeyFactory.getInstance(keyAlgorithm, provider);
				} else {
					keyFactory = SecretKeyFactory.getInstance(keyAlgorithm);
				}
			}
			
			if (keyString != null && keyFactory != null) 
			{
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] keyBytes = keyString.getBytes();
				byte[] digestBytes = new byte[32];
				md.update(keyBytes);
				md.digest(digestBytes, 0, 16);
				md.update(keyBytes);
				md.update(keyBytes);
				md.digest(digestBytes, 16, 16);
				
				DESKeySpec desKeySpec = new DESKeySpec(digestBytes);
				//SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");		
		    	key = keyFactory.generateSecret(desKeySpec);

				//SecretKeySpec keySpec = new SecretKeySpec(digestBytes, keyAlgorithm);
				//key = keyFactory.generateSecret(keySpec);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void connect() throws IOException 
	{
		if (urlConnection2 == null) {
			throw new IOException("urlConnection2 is null.");
		}
		urlConnection2.connect();
	}

	@Override
	public InputStream getInputStream() throws IOException {
		if (urlConnection2 == null && !urlFile.startsWith("jarfile:")) {
			throw new IOException("urlConnection2 is null.");
		}
		//System.out.println(urlFile);
		
		InputStream is = null;
		if(urlConnection2!=null)
		{//Ulr File
			is = urlConnection2.getInputStream();
		}
		else if(urlFile.startsWith("jarfile:"))
		{
			try {
				String exePath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
				//String path = this.getClass().getResource("").getPath();
				//System.out.println(exePath);
				//File file = new File(path);
				//String exePath = "";
				if (!exePath.toLowerCase().endsWith(".jar"))
				{//local file
					//exePath = file.getPath();
					System.out.println(exePath + urlFile.replace("jarfile:", ""));					 
					is = new FileInputStream(exePath + urlFile.replace("jarfile:", ""));
				}
				else
				{//file in jar
					System.out.println("Load jarfile:" + urlFile.replace("jarfile:", ""));
					try {
						is = this.getClass().getResourceAsStream(urlFile.replace("jarfile:", ""));
						/*InputStream is2 = this.getClass().getResourceAsStream(urlFile.replace("jarfile:", ""));
						String file =  new File(exePath).getParent() + "/tmp.jar";
						if(is2 == null)
						{
							System.out.println("getResourceAsStream fail");
							return null;
						}
						SaveFile(is2,file);
						is2.close();
						is = new FileInputStream(file);*/										
					} catch (Exception e) {
						System.out.println("Load jarfile Fail:" + exePath);
						//e.printStackTrace();
					}
				}
			} catch (Exception e) {		
				e.printStackTrace();
			}
		}
		
		try {
			if ((cipher == null || key == null))
			{
				//System.out.println("not cipher");
				return is;
			} 			 
			else 
			{
				//System.out.println("cipher");
				cipher.init(Cipher.DECRYPT_MODE, key, new SecureRandom());
				CipherInputStream ci = new CipherInputStream(is, cipher);
				//System.out.println("getInputStream Success");
				return ci;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		finally
		{
		}
		//return null;
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		if (urlConnection2 == null) {
			throw new IOException("urlConnection2 is null.");
		}
		try {
			if (cipher == null || key == null) {
				return urlConnection2.getOutputStream();
			} else {
				cipher.init(Cipher.ENCRYPT_MODE, key, new SecureRandom());
				return new CipherOutputStream(urlConnection2.getOutputStream(), cipher);
			}
		} catch (InvalidKeyException e) {
			e.printStackTrace();
			throw new IOException(e);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	private Proxy proxy;

	private URLConnection urlConnection2;

	private Cipher cipher;

	private SecretKey key;
	
	private String urlFile = "";
}
