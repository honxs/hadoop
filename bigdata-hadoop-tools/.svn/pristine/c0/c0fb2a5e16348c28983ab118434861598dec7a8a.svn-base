package cn.mastercom.bigdata.util.spark;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

public class RDDResultMultipleTextOutputFormat<K, V> extends MultipleTextOutputFormat<TypeInfo, V>
{
	public static final String ValueRowSpliter = ", ";

	private boolean[] typeFlags;
	private String tmStr;

	@Override
	public RecordWriter<TypeInfo, V> getRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3)
			throws IOException
	{
		final FileSystem myFS = fs;
		final String myName = generateLeafFileName(name);
		final JobConf myJob = job;
		final Progressable myProgressable = arg3;

		return new RecordWriter<TypeInfo, V>()
		{
			TreeMap<String, RecordWriter<TypeInfo, V>> recordWriters = new TreeMap<String, RecordWriter<TypeInfo, V>>();

			@Override
			public void write(TypeInfo key, V value) throws IOException
			{
				if (checkToSave(key.getType()))
				{
					String finalPath = key.getTypePath() + "/" + key.getTypeName() + "-" + myName;

					V actualValue = generateActualValue(key, value);

					RecordWriter<TypeInfo, V> rw = this.recordWriters.get(finalPath);
					if (rw == null)
					{
						rw = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
						this.recordWriters.put(finalPath, rw);
					}

					rw.write(null, actualValue);
				}
			};

			@Override
			public void close(Reporter reporter) throws IOException
			{
				Iterator<String> keys = this.recordWriters.keySet().iterator();
				while (keys.hasNext())
				{
					RecordWriter<TypeInfo, V> rw = this.recordWriters.get(keys.next());
					rw.close(reporter);
				}
				this.recordWriters.clear();
			};

			private boolean checkToSave(int dataType)
			{
				if (typeFlags == null)
				{
					typeFlags = new boolean[1000];
					for (boolean item : typeFlags)
					{
						item = false;
					}

					String saveList = myJob.get(RDDResultOutputer.OutPutSaveList);
					String types[] = saveList.split(",");
					for (String type : types)
					{
						int itype = Integer.parseInt(type);
						if (itype > typeFlags.length - 1 || itype < 0)
						{
							continue;
						}
						typeFlags[itype] = true;
					}
				}

				if (dataType < 0 || dataType > typeFlags.length - 1)
				{
					return false;
				}
				return typeFlags[dataType];
			};
		};

	}

	@Override
	protected String generateFileNameForKeyValue(TypeInfo key, V value, String name)
	{
		return key.getTypeName().toString();
	};

	@Override
	protected TypeInfo generateActualKey(TypeInfo key, V value)
	{
		return key;
	}

	@Override
	protected V generateActualValue(TypeInfo key, V value)
	{
		tmStr = value.toString();
		if (tmStr.length() > 2)
		{
			tmStr = tmStr.substring(1, tmStr.length() - 1);
		}
		return (V) tmStr.replace(ValueRowSpliter, "\n");
	}

}
