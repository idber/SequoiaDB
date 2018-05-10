<?php
class cl_LOB_Test extends PHPUnit_Framework_TestCase
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

   public function test_Connect()
   {
      $db = new SequoiaDB();
      $err = $db -> connect( $this->address ) ;
      $this -> assertEquals( 0, $err['errno'], '数据库连接错误( 参数: '.$this->address.' )' ) ;
      return $db ;
   }

   /**
    * @depends test_Connect
    */
   public function test_select_cs( $db )
   {
      $cs = $db -> selectCS( 'test_foo' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cs错误' ) ;
      $this -> assertNotEmpty( $cs, '创建cs错误' ) ;
      return $cs ;
   }

   /**
    * @depends test_Connect
    * @depends test_select_cs
    */
   public function test_select_cl( $db, $cs )
   {
      $cl = $cs -> selectCL( 'test_bar', array( 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cs, '创建cl错误' ) ;
      return $cl ;
   }
   
   /**
    * @depends test_Connect
    * @depends test_select_cs
    * @depends test_select_cl
    */
   public function test_openLob( $db, $cs, $cl )
   {
      $cl -> removeLob( "123456789012345678901234" ) ;
      $lobObj = $cl -> openLob( "123456789012345678901234", SDB_LOB_CREATEONLY ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], 'openLob错误' ) ;
      $this -> assertNotEmpty( $lobObj, 'openLob错误' ) ;
      $err = $lobObj -> close() ;
      $this -> assertEquals( 0, $err['errno'], 'Lob close错误' ) ;
   }
   
   /**
    * @depends test_Connect
    * @depends test_select_cs
    * @depends test_select_cl
    */
   public function test_listLob( $db, $cs, $cl )
   {
      $cursor = $cl -> listLob() ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], 'listLob错误' ) ;
      $this -> assertNotEmpty( $cursor, 'listLob错误' ) ;
      $num = 0 ;
      while( $record = $cursor -> next() ){
         ++$num ;
      }
      $this -> assertEquals( 1, $num, 'listLob错误' ) ;
   }
   
   /**
    * @depends test_Connect
    * @depends test_select_cs
    * @depends test_select_cl
    */
   public function test_listLobPieces( $db, $cs, $cl )
   {
      $cursor = $cl -> listLobPieces() ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], 'listLobPieces错误' ) ;
      $this -> assertNotEmpty( $cursor, 'listLobPieces错误' ) ;
      $num = 0 ;
      while( $record = $cursor -> next() ){
         ++$num ;
      }
      $this -> assertEquals( 1, $num, 'listLobPieces错误' ) ;
   }
   
   /**
    * @depends test_Connect
    * @depends test_select_cs
    * @depends test_select_cl
    */
   public function test_removeLob( $db, $cs, $cl )
   {
      $err = $cl -> removeLob( "123456789012345678901234" ) ;
      $this -> assertEquals( 0, $err['errno'], 'removeLob错误' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], 'removeLob错误' ) ;
   }
}
?>