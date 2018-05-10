<?php
class cs_cl_Test extends PHPUnit_Framework_TestCase
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
      $err = sdbInitClient( array( 'enableCacheStrategy' => false ) ) ;
      $this -> assertEquals( 0, $err, '设置驱动缓存失败' ) ;
      $db = new SequoiaDB();
      $err = $db -> connect( $this->address ) ;
      $this -> assertEquals( 0, $err['errno'], '数据库连接错误( 数组参数: '.$this->address.' )' ) ;
      return $db ;
   }

   /**
    * @depends test_Connect
    */
   public function test_select_cs( $db )
   {
      $cs = $db -> selectCS( 'test_cs' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cs错误' ) ;
      $this -> assertNotEmpty( $cs, '创建cs错误' ) ;
      return $cs ;
   }

   /**
    * @depends test_Connect
    * @depends test_select_cs
    */
   public function test_selectCL( $db, $cs )
   {     
      $cl = $cs -> selectCL( 'bar_compressed', array( 'Compressed' => true, 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cl, '创建cl错误' ) ;

      $err = $cs -> dropCL( 'bar_compressed' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cl错误' ) ;

      $cl = $cs -> selectCollection( 'bar_compressed' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '获取cl错误' ) ;
      $this -> assertNotEmpty( $cl, '获取cl错误' ) ;

      $err = $cs -> dropCL( 'bar_compressed' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cl错误' ) ;
   }

   /**
    * @depends test_Connect
    * @depends test_select_cs
    */
   public function test_createCL( $db, $cs )
   {     
      $cl = $cs -> createCL( 'create_bar', array( 'Compressed' => true, 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cl, '创建cl错误' ) ;

      $cl = $cs -> getCL( 'create_bar' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '获取cl错误' ) ;
      $this -> assertNotEmpty( $cl, '获取cl错误' ) ;

      $err = $cs -> dropCL( 'create_bar' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cl错误' ) ;
   }

   /**
    * @depends test_Connect
    * @depends test_select_cs
    */
   public function test_getCL( $db, $cs )
   {
      $cl = $cs -> selectCL( 'bar', array( 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cl, '创建cl错误' ) ;

      $cl = $cs -> getCL( 'bar' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '获取cl错误' ) ;
      $this -> assertNotEmpty( $cl, '获取cl错误' ) ;
   }

   /**
    * @depends test_Connect
    * @depends test_select_cs
    */
   public function test_dropCL( $db, $cs )
   {
      $cl = $cs -> selectCL( 'bar', array( 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cl, '创建cl错误' ) ;

      $err = $cs -> dropCL( 'bar' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cl错误' ) ;

      $cl = $cs -> getCL( 'bar' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( -23, $err['errno'], '创建cl错误' ) ;
      $this -> assertEmpty( $cl, '创建cl错误' ) ;

      $cl = $cs -> selectCL( 'bar', array( 'ReplSize' => -1 ) ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( 0, $err['errno'], '创建cl错误' ) ;
      $this -> assertNotEmpty( $cl, '创建cl错误' ) ;

      $err = $cs -> dropCollection( 'bar' ) ;
      $this -> assertEquals( 0, $err['errno'], '删除cl错误' ) ;

      $cl = $cs -> getCL( 'bar' ) ;
      $err = $db -> getError() ;
      $this -> assertEquals( -23, $err['errno'], '创建cl错误' ) ;
      $this -> assertEmpty( $cl, '创建cl错误' ) ;

   }

   /**
    * @depends test_Connect
    * @depends test_select_cs
    */
   public function test_clear( $db, $cs )
   {
      $err = $cs -> drop() ;
      $this -> assertEquals( 0, $err['errno'], '删除cs错误' ) ;
   }

}
?>
