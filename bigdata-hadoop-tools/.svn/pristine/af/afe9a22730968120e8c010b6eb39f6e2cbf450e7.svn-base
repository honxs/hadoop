package cn.mastercom.bigdata.util.spark;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import cn.mastercom.bigdata.util.IDataOutputer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progress;
import org.apache.spark.TaskContext;

public class MultiOutputMngSpark<KEYOUT, VALUEOUT> implements IDataOutputer
{

	MultipleOutputs<NullWritable, Text> mos;
	String jobtrackerID;
	Text outValue;

	TypeInfoMng typeInfoMng;

	public MultiOutputMngSpark(TypeInfoMng typeInfoMng)
	{
		this.typeInfoMng = typeInfoMng;

		outValue = new MyText();
		jobtrackerID = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
	}

	@SuppressWarnings("unchecked")
	public void init(String fsUri, String baseOutPath)
	{
		// Step 1： 构造 mos
		try
		{

			Job job = Job.getInstance();
			FileOutputFormat.setOutputPath(job, new Path(baseOutPath));
			Configuration conf_tmp = job.getConfiguration();
			if (fsUri != null && fsUri.length() > 0)
			{
				conf_tmp.set("fs.defaultFS", fsUri);
			}

			TaskContext context = TaskContext.get();

			mos = new MultipleOutputs<NullWritable, Text>(new ReduceContextImpl(conf_tmp, new TaskAttemptID(
					jobtrackerID, /* stageId */0, false, context.partitionId(), context.attemptNumber()),
					new DummyIterator(), new GenericCounter(), new GenericCounter(), new DummyRecordWriter(),
					new DummyOutputCommitter(), new TaskAttemptContextImpl.DummyReporter(), null, NullWritable.class,
					Text.class));
		}
		catch (Exception e)
		{
			e.printStackTrace();
			throw new RuntimeException();
		}

	}

	@Override
	public int pushData(int dataType, String value)
	{
		TypeInfo typeInfo = typeInfoMng.getTypeInfo(dataType);
		if (typeInfo == null)
			return -1;
		
		outValue.set(value);
		try
		{
			mos.write(NullWritable.get(), outValue, typeInfo.getTypePath() + "/" + typeInfo.getTypeName());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int pushData(int dataType, List<String> valueList)
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void clear()
	{
		try
		{
			mos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	private class DummyOutputCommitter extends OutputCommitter
	{

		@Override
		public void setupJob(JobContext jobContext) throws IOException
		{

		}

		@Override
		public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException
		{

		}

		@Override
		public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException
		{
			return false;
		}

		@Override
		public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException
		{

		}

		@Override
		public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException
		{

		}
	}

	private class DummyRecordWriter<K, V> extends RecordWriter<K, V>
	{

		@Override
		public void write(K k, V v) throws IOException, InterruptedException
		{
		}

		@Override
		public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
		{

		}
	}

	private class DummyIterator implements RawKeyValueIterator
	{

		@Override
		public DataInputBuffer getKey() throws IOException
		{
			return null;
		}

		@Override
		public DataInputBuffer getValue() throws IOException
		{
			return null;
		}

		@Override
		public boolean next() throws IOException
		{
			return false;
		}

		@Override
		public void close() throws IOException
		{

		}

		@Override
		public Progress getProgress()
		{
			return null;
		}
	}

	private class MyText extends Text implements Serializable
	{
	}
}
