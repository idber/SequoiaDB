<?php
class SequoiaDB_Task_Test extends PHPUnit_Framework_TestCase
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
   public function test_isStandlone( $db )
   {
      $cursor = $db -> list( SDB_LIST_SHARDS ) ;
      $err = $db -> getError() ;
      if( $err['errno'] == -159 )
      {
         return true ;
      }
      $this -> assertEquals( 0, $err['errno'], 'list错误' ) ;
      $this -> assertNotEmpty( $cursor, 'list错误' ) ;
      return false ;
   }
   
   /**
    * @depends test_connect
    * @depends test_isStandlone
    */
   public function test_listTask( $db, $isStandlone )
   {
      if( $isStandlone == false )
      {
         $cursor = $db -> listTask() ;
         $err = $db -> getError() ;
         $this -> assertEquals( 0, $err['errno'], 'listTask错误' ) ;
         $this -> assertNotEmpty( $cursor, 'listTask错误' ) ;
         while( $record = $cursor -> next() ) {
         }
      }
   }
   
   /**
    * @depends test_connect
    * @depends test_isStandlone
    */
   public function test_waitTask( $db, $isStandlone )
   {
      if( $isStandlone == false )
      {
         $err = $db -> waitTask( array( 1, new SequoiaInt64( '2' ) ) ) ;
         $this -> assertEquals( 0, $err['errno'], 'waitTask错误' ) ;
         
         $err = $db -> waitTask( 1 ) ;
         $this -> assertEquals( 0, $err['errno'], 'waitTask错误' ) ;
      }
   }
   
   /**
    * @depends test_connect
    * @depends test_isStandlone
    */
   public function test_cancelTask( $db, $isStandlone )
   {
      if( $isStandlone == false )
      {
         $err = $db -> cancelTask( 1, false ) ;
         if( $err['errno'] == -173 )
         {
            return ;
         }
         $this -> assertEquals( 0, $err['errno'], 'cancelTask错误' ) ;
      }
   }
}

?>