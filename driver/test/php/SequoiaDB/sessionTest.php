<?php
class SequoiaDB_Session_Test extends PHPUnit_Framework_TestCase
{
   protected $hostname ;
   protected $port ;
   protected $address ;

   protected function setUp()
   {
      $this -> hostname = empty( $_POST['hostname'] ) ? '127.0.0.1' : $_POST['hostname'] ;
      $this -> port = empty( $_POST['port'] ) ? '11810' : $_POST['port'] ;
      $this -> address = $this -> hostname.':'.$this -> port ;
   }

   public function test_connect()
   {
      $db = new SequoiaDB();
      $err = $db -> connect( $this->address ) ;
      $this -> assertEquals( 0, $err['errno'], '数据库连接错误( 数组参数: '.$this->address.' )' ) ;
      return $db ;
   }
   
   /**
    * @depends test_connect
    */
   public function test_setSessionAttr( $db )
   {
      $err = $db -> setSessionAttr( array( 'PreferedInstance' => 'm' ) ) ;
      $this -> assertEquals( 0, $err['errno'], 'setSessionAttr错误' ) ;
   }
   
   /**
    * @depends test_connect
    */
   public function test_forceSession( $db )
   {
      $sessionID = -1 ;
      $cursor = $db -> list( SDB_LIST_SESSIONS_CURRENT ) ;
      while( $record = $cursor -> next() ) {
         $sessionID = $record['SessionID'] ;
      }
      if( $sessionID > 0 )
      {
         //有session
         $err = $db -> forceSession( $sessionID ) ;
         if( $err['errno'] == -16 )
         {
            //把自己断开连接了, 说明forceSession正常
            return ;
         }
         $this -> assertEquals( 0, $err['errno'], 'forceSession错误' ) ;
      }
      
      $cursor = $db -> execSQL( 'select * from $SNAPSHOT_SESSION where Status="Running"' ) ;
      $sessionID = -1 ;
      $nodename = '' ;
      while( $record = $cursor -> next() )
      {
         if( $record['Type'] == 'ShardAgent' )
         {
            $nodename  = $record['NodeName'] ;
            $sessionID = $record['SessionID'] ;
            break ;
         }
      }
      if( $sessionID > 0 )
      {
         //有session
         $hostname = explode( ':', $nodename ) ;
         $svcname  = $hostname[1] ;
         $hostname = $hostname[0] ;
         $err = $db -> forceSession( $sessionID, array( 'HostName' => $hostname, 'svcname' => $svcname ) ) ;
         if( $err['errno'] == -16 )
         {
            //把自己断开连接了, 说明forceSession正常
            return ;
         }
         $this -> assertEquals( 0, $err['errno'], 'forceSession错误' ) ;
      }
   }
}

?>