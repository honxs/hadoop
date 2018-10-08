package cn.mastercom.bigdata.util.hadoop.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

/**
 * 封装好任务的控制任务和状态信息
 */
public class DownloadAndUploadTask implements Runnable {

	private InputStream is;
	private OutputStream os;
	private String fileName;
	/**
	 * <tt>-1</tt>表示没有任务
	 */
	private long fileSize;
	/**
	 * <tt>-1</tt>表示没有任务
	 */
	private long progress;
	private TaskType taskType;
	private Date startDate;
	private Date endDate;
	private byte[] buffer;

	private volatile boolean isComplete;
	private volatile boolean hasError;
	private volatile boolean isStart;
	private volatile boolean isSuspend;

	public DownloadAndUploadTask() {
		this.is = null;
		this.os = null;
		this.fileName = "";
		this.fileSize = -1;
		this.progress = -1;
		this.taskType = TaskType.UnknownType;
		this.startDate = new Date();
		this.endDate = null;
		this.buffer = new byte[1024 * 8];
		this.isComplete = false;
		this.hasError = false;
		this.isStart = true;
		this.isSuspend = false;
	}

	public DownloadAndUploadTask(InputStream is, OutputStream os, String fileName, long fileSize, TaskType taskType) {
		this();
		this.is = is;
		this.os = os;
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.taskType = taskType;
	}

	public String getFileName() {
		return fileName;
	}

	public int getProgress() {
		return progress==-1?0:fileSize==0?100:(int) (progress / (double) fileSize * 100);
	}

	public TaskType getTaskType() {
		return taskType;
	}

	public Date getStartDate() {
		return startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public boolean isComplete() {
		return isComplete;
	}

	public boolean hasError() {
		return hasError;
	}

	public boolean isStart() {
		return isStart;
	}

	public boolean isSuspend() {
		return isSuspend;
	}

	public String getStatus() {
		if (isComplete)
			return "任务完成";
		if (hasError)
			return "出现异常";
		if (isSuspend)
			return "任务中止";
		if (isStart)
			return "任务正在执行中...";
		else
			return "任务已暂停";
	}

	public void suspend() {
		if (isComplete) return;
		isSuspend = true;
	}

	public void start() {
		if (isComplete) return;
		isStart = true;
	}

	public void pause() {
		if (isComplete) return;
		isStart = false;
	}

	public void setParameter(InputStream is, OutputStream os, String fileName, long fileSize, TaskType taskType) {
		this.is = is;
		this.os = os;
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.taskType = taskType;
	}
	
	protected void before() {}
	protected void exception() {}
	protected void finish() {}
	
	@Override
	public void run() {
		before();
		if (is == null || os == null)
			return;
		try {
			while (!this.isSuspend()) {
				if (this.isStart()) {
					if (progress >= fileSize) {
						progress = fileSize;
						isComplete = true;
						return;
					}
					int len = is.read(buffer);
					if (len != -1) {
						os.write(buffer, 0, len);
						progress += buffer.length;
						continue;
					} else {
						progress = fileSize;
						isComplete = true;
						return;
					}
				}
			}
		} catch (Exception e) {
			exception();
			e.printStackTrace();
			isComplete = false;
			hasError = true;
		} finally {
			try {
				is.close();
				os.close();
				endDate = new Date();
			} catch (IOException ignored) {}
		}
		finish();
	}

	public enum TaskType {
		UnknownType(-1, "未知类型"),
		DownloadFile(0, "下载文件"), UploadFile(1, "上传文件"),
		DownloadDir(2, "下载文件夹"), UploadDir(3, "上传文件夹"),
		Download5M(4, "下载5M文件"), MergeDownload(5, "归并压缩下载"), 
		UncompressUpload(6, "解压上传文件");

		private int value;
		private String description;

		TaskType(int value, String description) {
			this.value = value;
			this.description = description;
		}

		public int getValue() {
			return value;
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
