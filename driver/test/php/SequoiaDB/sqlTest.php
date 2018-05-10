<?php
class SequoiaDB_SQL_Test extends PHPUnit_Framework_TestCase
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
   public function test_execSQL( $db )
   {
      $cs = $db -> selectCS( 'foo' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cs错误' ) ;
      $this -> assertNotEmpty( $cs, '创建cs错误' ) ;

      $cl = $cs -> selectCL( 'bar', array( 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cl, '创建cl错误' ) ;

      $cursor = $db -> execSQL( 'select * from foo.bar' ) ;
      $this -> assertEquals( 0, $err['errno'], 'listGroup错误' ) ;
      $this -> assertNotEmpty( $cursor, 'listGroup错误' ) ;
      while( $record = $cursor -> next() ) {
      }
      
      $err = $db -> dropCS( 'foo' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cs错误' ) ;
   }
   
   /**
    * @depends test_connect
    */
   public function test_execUpdateSQL( $db )
   {
      $err = $db -> execUpdateSQL( 'create collectionspace foo1' ) ;
      $this -> assertEquals( 0, $err['errno'], 'execUpdateSQL错误' ) ;
      $err = $db -> dropCS( 'foo1' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cs错误' ) ;
   }
}

?>