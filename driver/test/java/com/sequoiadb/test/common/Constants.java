package com.sequoiadb.test.common;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;

public class Constants {

/* use in ci */
	public final static int SERVER                         = 11810;
  	public final static String HOST                        = "localhost"; // change this for you own mechine (rhel ubutu-dev1)
  	public final static String BACKUPPATH                  = "/opt/sequoiadb/database/test/backup";
	public final static String DATAPATH4                   = "/opt/sequoiadb/database/test/data4";

	public final static String _HOST                       = HOST;
	public final static int _SERVER                        = SERVER;
	public final static String COOR_NODE_CONN              = HOST+":"+ SERVER;
	
	public final static String TEST_CS_NAME_1              = "testfoo";
	public final static String TEST_CS_NAME_2              = "testCS2";
	
	public final static String TEST_CL_NAME_1              = "testbar";
	public final static String TEST_CL_NAME_2              = "testCL2";
	public final static String TEST_CL_FULL_NAME1          = "testfoo.testbar";
	public final static int SDB_PAGESIZE_4K                = 4*1024;
	
	public final static String GROUPNAME                   = "group1";
	public final static String TEST_RG_NAME_SRC            = "testRGSrc";
	public final static String TEST_RG_NAME_DEST           = "testRGDest";
	public final static String CATALOGRGNAME               = "SYSCatalogGroup";
	public final static int GROUPID                        = 1000;
	
	public final static String TEST_RN_HOSTNAME_SPLIT      = "vmsvr2-ubun-x64-2";
	public final static String TEST_RN_PORT_SPLIT          = "50100";
	public final static String TEST_INDEX_NAME             = "testIndexName";
	
	public final static String BACKUPNAME                  = "backup_in_java_test";
	public final static String BACKUPGROUPNAME             = GROUPNAME;

	public final static String TEST_DOMAIN_NAME            = "domain_java";
	
	
	private static Sequoiadb sdb;
	
	public static boolean isCluster(){
	 sdb = new Sequoiadb(HOST+":"+SERVER, "", "");
	 try{
		 BSONObject empty = new BasicBSONObject();
		 sdb.getList(7, empty, empty, empty);
	 }catch (BaseException e){
		 if(e.getErrorType().equals("SDB_RTN_COORD_ONLY"));
		 return false;
	 }
	 return true;
	}
	

}
