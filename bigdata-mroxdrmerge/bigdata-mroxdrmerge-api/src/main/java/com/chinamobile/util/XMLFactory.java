package com.chinamobile.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotProguard
public class XMLFactory
{
	private static Logger log = LoggerFactory.getLogger(XMLFactory.class);
	private Marshaller marshaller;
	private Unmarshaller unmarshaller;

	/**
	 * 参数types为所有需要序列化的Root对象的类�?
	 */
	public XMLFactory(Class<?>... types)
	{
		try
		{
			JAXBContext jaxbContext = JAXBContext.newInstance(types);
			marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
			unmarshaller = jaxbContext.createUnmarshaller();
		} catch (JAXBException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Java->Xml
	 */
	public String marshal(Object root)
	{
		try
		{
			StringWriter writer = new StringWriter();
			marshaller.marshal(root, writer);
			return writer.toString();
		} catch (JAXBException e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Xml->Java
	 */
	@SuppressWarnings("unchecked")
	public <T> T unmarshal(String xml)
	{
		try
		{
			StringReader reader = new StringReader(xml);
			return (T) unmarshaller.unmarshal(reader);
		} catch (Exception e)
		{
			e.printStackTrace();
			log.error(e.getMessage());
			return null;
		}
	}

	/**
	 * Xml->Java
	 */
	@SuppressWarnings("unchecked")
	public <T> T unmarshal(InputStream in)
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new InputStreamReader(in, "utf-8"));
		} catch (UnsupportedEncodingException ex)
		{
			ex.printStackTrace();
			log.error(ex.getMessage());
		}

		try
		{
			return (T) unmarshaller.unmarshal(br);
		} catch (Exception e)
		{
			e.printStackTrace();
			log.error(e.getMessage());
			return null;
		}
	}
}
