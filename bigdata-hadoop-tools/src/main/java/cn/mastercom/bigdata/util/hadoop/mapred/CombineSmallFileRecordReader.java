package cn.mastercom.bigdata.util.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class CombineSmallFileRecordReader extends RecordReader<LongWritable, Text>
{
	public static final String CombineSmallFileName = "mapreduce.combinefile.input.file.name";
	public static final String CombineSmallFilePath = "mapreduce.combinefile.input.file.path";

	private CombineFileSplit combineFileSplit;
	private LineRecordReader lineRecordReader = new LineRecordReader();
	private Path[] paths;
	private int totalLength;
	private int currentIndex;
	private float currentProgress = 0;
	private LongWritable currentKey;
	private Text currentValue = new Text();;

	public CombineSmallFileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index)
			throws IOException
	{
		super();
		this.combineFileSplit = combineFileSplit;
		this.currentIndex = index; // 当前要处理的小文件Block在CombineFileSplit中的索引
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		this.combineFileSplit = (CombineFileSplit) split;
		// 处理CombineFileSplit中的一个小文件Block，因为使用LineRecordReader，需要构造一个FileSplit对象，然后才能够读取数据
		FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(currentIndex),
				combineFileSplit.getOffset(currentIndex), combineFileSplit.getLength(currentIndex),
				combineFileSplit.getLocations());
		lineRecordReader.initialize(fileSplit, context);

		this.paths = combineFileSplit.getPaths();
		totalLength = paths.length;
		context.getConfiguration().set(CombineSmallFileName, combineFileSplit.getPath(currentIndex).getName());
		context.getConfiguration().set(CombineSmallFilePath, combineFileSplit.getPath(currentIndex).getParent().toString());
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException
	{
		currentKey = lineRecordReader.getCurrentKey();
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException
	{
		currentValue.set(lineRecordReader.getCurrentValue());
		return currentValue;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (currentIndex >= 0 && currentIndex < totalLength)
		{
			return lineRecordReader.nextKeyValue();
		}
		else
		{
			return false;
		}
	}

	@Override
	public float getProgress() throws IOException
	{
		if (currentIndex >= 0 && currentIndex < totalLength)
		{
			currentProgress = (float) currentIndex / totalLength;
			return currentProgress;
		}
		return currentProgress;
	}

	@Override
	public void close() throws IOException
	{
		lineRecordReader.close();
	}
}
