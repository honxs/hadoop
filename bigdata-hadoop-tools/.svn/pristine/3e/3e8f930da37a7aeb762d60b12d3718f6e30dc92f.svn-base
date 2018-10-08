package cn.mastercom.bigdata.util.ftp;

import org.apache.log4j.Logger;

import com.jcraft.jsch.SftpProgressMonitor;

/**
 * 进度监控器-JSch每次传输一个数据块，就会调用count方法来实现主动进度通知
 * @author ch007
 *
 */
public class SFTPProgressMonitor implements SftpProgressMonitor
{
	private long count = 0;     //当前接收的总字节数
    private long max = 0;       //最终文件大小
    private long percent = -1;  //进度
    private final Logger  log  = Logger.getLogger(getClass());
    /**
     * 当文件开始传输时，调用init方法
     */
	@Override
	public void init(int op, String src, String dest, long max)
	{
		if (op==SftpProgressMonitor.PUT) {
            log.info("Upload file begin.");
        }else {
        	log.info("Download file begin.");
        }
        this.max = max;
        this.count = 0;
        this.percent = -1;
	}
	
	/**
	 * 当每次传输了一个数据块后，调用count方法，count方法的参数为这一次传输的数据块大小
	 */
	@Override
	public boolean count(long count)
	{
		this.count += count;
		if(max != 0) {
	        if (percent >= this.count * 100 / max) {
	            return true;
	        }
	        percent = this.count * 100 / max;
//	        log.info("Completed " + this.count + "(" + percent+ "%) out of " + max + ".");
		}       
        return true;
	}

	@Override
	public void end()
	{
		log.info("Transferring done.");		
	}
	
}
