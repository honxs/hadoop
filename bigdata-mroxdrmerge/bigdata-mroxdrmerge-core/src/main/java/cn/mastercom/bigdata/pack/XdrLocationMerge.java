package cn.mastercom.bigdata.pack;
import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import cn.mastercom.bigdata.util.IWriteLogCallBack;
import cn.mastercom.bigdata.util.SSHHelper;
import cn.mastercom.bigdata.util.hadoop.hdfs.HDFSOper;
import cn.mastercom.bigdata.mroxdrmerge.MainModel;


public class XdrLocationMerge implements IWriteLogCallBack
{
	private Date statDate = null;
	private String queneName = "";
   
	public XdrLocationMerge(Date statDate, String queneName)
	{
	   this.statDate = statDate;	
	   this.queneName = queneName;
	}
	
	public boolean run()
	{
		//构造执行脚本
        String path= "";
        
		try
		{	
			path= MainModel.GetInstance().getExePath() + "/xdrlocation.pig";
	        writeLog(LogType.info, "execute file: " + path);
			
	        File file=new File(path);
	        if(file.exists())
	        {
	        	file.delete();
	        }
	        file.createNewFile();
	        FileOutputStream out=new FileOutputStream(file,false); //如果追加方式用true        
	        StringBuffer sb=new StringBuffer();
	        sb.append(makeCmd());
	        out.write(sb.toString().getBytes("utf-8"));//注意需要转换对应的字符集
	        out.close();
	        
	        //删除老数据的目录
	        HDFSOper hdfsOper = new HDFSOper(MainModel.GetInstance().getAppConfig().getHadoopHost(), MainModel.GetInstance().getAppConfig().getHadoopHdfsPort());
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	        String xdrFilePath = String.format("%s/%s", MainModel.GetInstance().getAppConfig().getXdrDataPath(), format.format(statDate));
	        if(hdfsOper.checkDirExist(xdrFilePath))
	        {
	        	writeLog(LogType.info, "delete exists dir : " + xdrFilePath);
	        	hdfsOper.delete(xdrFilePath);
	        }
	        
	        //执行脚本
	        SSHHelper sshHelper = null;
//			SSHHelper sshHelper = new SSHHelper(MainModel.GetInstance().getAppConfig().getSSHHost(),
//					MainModel.GetInstance().getAppConfig().getSSHPort(),
//					MainModel.GetInstance().getAppConfig().getSSHUser(),
//					MainModel.GetInstance().getAppConfig().getSSHPwd(), 
//					this);
			
			System.out.println("开始执行二表关联...");
			System.out.println("file 目录：" + file.getPath());
			sshHelper.excuteCmd(MainModel.GetInstance().getAppConfig().getPigBinPath() + "/pig -x mapreduce " + path, 2);
			System.out.println("执行二表关联完毕！");
			
			//删除文件
			file.delete();
		}
		catch (Exception e)
		{
			writeLog(LogType.error, e.getMessage());
			return false;
		}
		finally
		{
	        File file=new File(path);
	        if(file.exists())
	        {
	        	file.delete();
	        }
		}
		return true;
	}
	
	private String makeCmd() throws Exception
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String strDate = format.format(statDate);
		
//		String strCmd = String.format("SET mapreduce.job.queuename network;\n");
//	    strCmd += String.format("xdr = load '/flume/xdr/%s' as (online_id:chararray,session_id:chararray,start_date_time:chararray,end_date_time:chararray,user_interface:chararray,interface:chararray,machine_ip_type:chararray,rat_type:chararray,mme_sgw_ip:chararray,mme_sgw_port:chararray,mme_sgw:chararray,mme_group_id:chararray,mme_code:chararray,mme_ue_s1ap_id:chararray,enb_ue_s1ap_id:chararray,enb_ip:chararray,enb_port:chararray,enb:chararray,up_ul_teid:chararray,up_dl_teid:chararray,sgw_consult_ip:chararray,enb_consult_ip:chararray,tac:chararray,ci:chararray,mtmsi:chararray,imsi:chararray,imei:chararray,prefix_imei:chararray,brand:chararray,type:chararray,apn:chararray,event_type:chararray,event_direction:chararray,protocol:chararray,ip_len_ul:chararray,ip_len_dl:chararray,ip_len:chararray,ip_data_len_ul:chararray,ip_data_len_dl:chararray,count_packet_ul:chararray,count_packet_dl:chararray,user_ip_len_ul:chararray,user_ip_len_dl:chararray,weight_rate_ul:chararray,weight_rate_dl:chararray,weight_rate_total:chararray,duration:chararray,source_ip:chararray,dest_ip:chararray,source_port:chararray,dest_port:chararray,uri:chararray,uri_main:chararray,content_type_part1:chararray,content_type_part2:chararray,user_agent_main:chararray,service_type:chararray,sub_service_type:chararray,sed_service:chararray,isclient:chararray,isprivate_service:chararray,isreassemble:chararray,request:chararray,repeat:chararray,reject:chararray,reject_cause:chararray,accept:chararray,accept_delayfirst:chararray,accept_repeat:chararray,complete:chararray,complete_delayfirst:chararray,rrc_est_cause:chararray,req_type:chararray,tau_attr:chararray,opp_tac:chararray,opp_ci:chararray,erab_id:chararray,auth_req:chararray,auth_delayfirst:chararray,auth_repeat:chararray,auth_resp:chararray,auth_failure:chararray,auth_failure_cause:chararray,auth_resp_delay:chararray,security_req:chararray,security_delayfirst:chararray,security_repeat:chararray,security_rejiect:chararray,security_rejiect_cause:chararray,security_resp_delay:chararray,identity_req:chararray,identity_delayfirst:chararray,identity_repeat:chararray,identity_resp:chararray,identity_resp_delay:chararray,aka_rand:chararray,aka_autn:chararray,aka_res:chararray,de_bear_acc:chararray,de_bear_acc_direction:chararray,pdn_disc:chararray,pdn_disc_direction:chararray,detach_acc:chararray,detach_acc_direction:chararray,de_cause:chararray,ue_context_rel:chararray,ue_context_rel_direction:chararray,ue_context_rel_cause:chararray,abnormal_reason:chararray,abnormal_type:chararray,resp:chararray,resp_delayfirst:chararray,resp_cause:chararray,mms_cause:chararray,result:chararray,result_delayfirst:chararray,ack:chararray,ack_delay:chararray,last_ack_delay:chararray,referer:chararray,fin:chararray,abort:chararray,abort_reason_user:chararray,abort_reason_provider:chararray,disconnect:chararray,reset:chararray,reset_direction:chararray,from:chararray,to:chararray,subject:chararray,query_name:chararray,query_type:chararray,query_op:chararray,icmp_type:chararray,icmp_code:chararray,gob_src_ip:chararray,gob_dst_ip:chararray,gob_ip_len:chararray,gob_src_port:chararray,gob_dst_port:chararray,gob_udp_len:chararray,retran_packet_dl:chararray,total_packet_dl:chararray,retran_packet_ul:chararray,total_packet_ul:chararray,ftp_data_ip:chararray,ftp_data_port:chararray,ftp_file_name:chararray,ftp_file_size:chararray,ftp_user:chararray,ftp_status:chararray,ftp_direction:chararray,ftp_current_path:chararray,ftp_type:chararray,ftp_local_data_port:chararray,security_resp:chararray,security_failure:chararray,security_failure_cause:chararray,reject_delayfirst:chararray,failure:chararray,segment_ip_len_ul:chararray,segment_ip_len_dl:chararray,sed_service_type:chararray,out_of_order_packet_dl:chararray,out_of_order_packet_ul:chararray,ip_len_dl_rate:chararray,host:chararray,x_online_host:chararray,cookie:chararray,content_length:chararray,mmse_to_count:chararray,mmse_cc_bcc_count:chararray,mmse_cc_bcc:chararray,mmse_transaction_id:chararray,mmse_message_id:chararray,mmse_data_length:chararray,url_search_string:chararray,fin_delay:chararray,syn_synack_delay:chararray,synack_ack_delay:chararray,ack_getpost_delay:chararray,answer_count:chararray,answer_address:chararray,auth_reject:chararray,mcc:chararray,mnc:chararray,pdn_ip:chararray,pdn_ip_type:chararray,ue_ip_type:chararray,extend_reject:chararray,extend_reject_cause:chararray,paging_id:chararray,additional_count:chararray,old_tmsi:chararray,old_mme_code:chararray,old_mme_group_id:chararray,syn_count:chararray,syn_ack_count:chararray,syn_ack_ack_count:chararray,syn_ack_delay:chararray,http_version:chararray,http_title:chararray,authority_count:chararray,sgw_consult_teid:chararray,enb_consult_teid:chararray,reject_cause_type:chararray,ue_context_rel_cause_type:chararray,msisdn:chararray,pci:chararray,opp_pci:chararray,guti_type:chararray,extend_reject_cause_type:chararray,window_size:chararray,mss:chararray,qq_version:chararray,qq_number:chararray,sms_opp_number:chararray,sms_smc_addr:chararray,sms_length:chararray,sms_tp_para_id:chararray,email_size:chararray,email_user_name:chararray,email_domain:chararray,email_subject:chararray,email_to:chararray,email_from:chararray,rtsp_req_url:chararray,rtsp_user_agent:chararray,rtsp_server_ip:chararray,rtsp_client_sport:chararray,rtsp_client_fport:chararray,rtsp_server_sport:chararray,rtsp_server_fport:chararray,rtsp_video_num:chararray,rtsp_audio_num:chararray,rtsp_describe_delay:chararray,rtsp_session_id:chararray,rtsp_track_type:chararray,rtsp_track_id:chararray,rtsp_video_rate:chararray,rtsp_audio_rate:chararray,rtsp_video_width:chararray,rtsp_audio_width:chararray,rtsp_interrupt_num:chararray,rtsp_interrupt_duration:chararray,rtsp_firstbuffer_duration:chararray,voip_direction:chararray,voip_mo_num:chararray,voip_mt_num:chararray,voip_type:chararray,voip_data_value:chararray,voip_disconect_type:chararray,voip_disconect_cause:chararray,voip_protocl_type:chararray,voip_call_id:chararray,p2p_file_lenght:chararray,p2p_file_ind:chararray,p2p_tracker:chararray,qci:chararray,longitude:float,latitude:float,eutra_fdd:chararray,eutra_tdd:chararray,eci:chararray);\n", strDate);
//	    strCmd += String.format("xdr1 = foreach xdr generate online_id,session_id,start_date_time,end_date_time,mme_group_id,mme_code,mme_ue_s1ap_id,enb_ue_s1ap_id,enb,tac,eci,imsi,imei,brand,type,event_type,ip_len_ul,ip_len_dl,ip_len,ip_data_len_ul,ip_data_len_dl,count_packet_ul,count_packet_dl,user_ip_len_ul,user_ip_len_dl,weight_rate_ul,weight_rate_dl,weight_rate_total,duration,content_type_part1,content_type_part2,service_type,sub_service_type,result,result_delayfirst,referer,retran_packet_dl,retran_packet_ul,reject_cause_type,msisdn;\n");
//	    strCmd += String.format("loc = load '/flume/location/%s' as (session_id1:chararray,online_id1:chararray,event_type1:chararray,imsi1:chararray,bd_lon:float,bd_lat:float,location:int,dist:chararray,radius:chararray,loctp:chararray,indoor:chararray);\n", strDate);
//	    strCmd += String.format("xdr_loc = join xdr1 by (online_id,session_id,event_type,imsi) left outer, loc by (online_id1,session_id1,event_type1,imsi1);\n");
//	    strCmd += String.format("xdr_loc1 = foreach xdr_loc generate online_id,session_id,start_date_time,end_date_time,mme_group_id,mme_code,mme_ue_s1ap_id,enb_ue_s1ap_id,enb,tac,eci,imsi,imei,brand,type,event_type,ip_len_ul,ip_len_dl,ip_len,ip_data_len_ul,ip_data_len_dl,count_packet_ul,count_packet_dl,user_ip_len_ul,user_ip_len_dl,weight_rate_ul,weight_rate_dl,weight_rate_total,duration,content_type_part1,content_type_part2,service_type,sub_service_type,result,result_delayfirst,referer,retran_packet_dl,retran_packet_ul,reject_cause_type,msisdn,bd_lon,bd_lat,location,dist,radius,loctp,indoor;\n");
//	    strCmd += String.format("store xdr_loc1 into '/result/test/%s' using PigStorage('\\t');\n", strDate);
//	    
		
		String strCmd = String.format("SET mapreduce.job.queuename %s;\n", queneName);
	    strCmd += String.format("xdr = load '%s/%s' as (online_id:chararray,session_id:chararray,start_date_time:chararray,end_date_time:chararray,mme_group_id:chararray,mme_code:chararray,mme_ue_s1ap_id:chararray,enb_ue_s1ap_id:chararray,enb:chararray,tac:chararray,eci:chararray,imsi:chararray,imei:chararray,msisdn:chararray,brand:chararray,type:chararray,event_type:chararray,duration:chararray,ip_len_ul:chararray,ip_len_dl:chararray,ip_len:chararray,ip_data_len_ul:chararray,ip_data_len_dl:chararray,count_packet_ul:chararray,count_packet_dl:chararray,user_ip_len_ul:chararray,user_ip_len_dl:chararray,weight_rate_ul:chararray,weight_rate_dl:chararray,weight_rate_total:chararray,content_type_part1:chararray,content_type_part2:chararray,service_type:chararray,sub_service_type:chararray,result:chararray,result_delayfirst:chararray,referer:chararray,retran_packet_dl:chararray,retran_packet_ul:chararray,latitude:chararray,longitude:chararray,latlng_time:chararray,apn:chararray);\n", MainModel.GetInstance().getAppConfig().getOrigLocationPath(), strDate);
	    strCmd += String.format("xdr1 = foreach xdr generate online_id,session_id,start_date_time,end_date_time,mme_group_id,mme_code,mme_ue_s1ap_id,enb_ue_s1ap_id,enb,tac,eci,imsi,imei,brand,type,event_type,ip_len_ul,ip_len_dl,ip_len,ip_data_len_ul,ip_data_len_dl,count_packet_ul,count_packet_dl,user_ip_len_ul,user_ip_len_dl,weight_rate_ul,weight_rate_dl,weight_rate_total,duration,content_type_part1,content_type_part2,service_type,sub_service_type,result,result_delayfirst,referer,retran_packet_dl,retran_packet_ul,msisdn,latlng_time,apn;\n");
	    strCmd += String.format("loc = load '%s/%s' as (session_id1:chararray,online_id1:chararray,event_type1:chararray,imsi1:chararray,bd_lon:float,bd_lat:float,location:int,dist:chararray,radius:chararray,loctp:chararray,indoor:chararray);\n", MainModel.GetInstance().getAppConfig().getOrigLocationPath(), strDate);
	    strCmd += String.format("xdr_loc = join xdr1 by (online_id,session_id,event_type,imsi) left outer, loc by (online_id1,session_id1,event_type1,imsi1);\n");
	    strCmd += String.format("xdr_loc1 = foreach xdr_loc generate online_id,session_id,start_date_time,end_date_time,mme_group_id,mme_code,mme_ue_s1ap_id,enb_ue_s1ap_id,enb,tac,eci,imsi,imei,brand,type,event_type,ip_len_ul,ip_len_dl,ip_len,ip_data_len_ul,ip_data_len_dl,count_packet_ul,count_packet_dl,user_ip_len_ul,user_ip_len_dl,weight_rate_ul,weight_rate_dl,weight_rate_total,duration,content_type_part1,content_type_part2,service_type,sub_service_type,result,result_delayfirst,referer,retran_packet_dl,retran_packet_ul,msisdn,latlng_time,bd_lon,bd_lat,location,dist,radius,loctp,indoor,apn;\n");
	    strCmd += String.format("store xdr_loc1 into '%s/%s' using PigStorage('\\t');\n", MainModel.GetInstance().getAppConfig().getXdrDataPath(), strDate);

		return strCmd;
	}

	@Override
	public void writeLog(LogType type, String strlog)
	{
		System.out.println(strlog);
	}
	
	
	
	
	
}
