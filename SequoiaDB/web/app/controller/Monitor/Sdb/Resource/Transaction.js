(function(){
   var sacApp = window.SdbSacManagerModule ;
   //控制器
   sacApp.controllerProvider.register( 'Monitor.SdbResource.Transaction.Ctrl', function( $scope, $compile, SdbRest, $location, SdbFunction ){
      
      _IndexPublic.checkMonitorEdition( $location ) ; //检测是不是企业版

      var clusterName = SdbFunction.LocalData( 'SdbClusterName' ) ;
      var moduleType = SdbFunction.LocalData( 'SdbModuleType' ) ;
      var moduleMode = SdbFunction.LocalData( 'SdbModuleMode' ) ;
      var moduleName = SdbFunction.LocalData( 'SdbModuleName' ) ;
      if( clusterName == null || moduleType != 'sequoiadb' || moduleMode == null || moduleName == null )
      {
         $location.path( '/Transfer' ).search( { 'r': new Date().getTime() } ) ;
         return;
      }

      //初始化
      $scope.ModuleMode = moduleMode ;
      //会话详细信息 弹窗
      $scope.SessionInfo = {
         'config': {},
         'callback': {}
      } ;
      //事务详细信息 弹窗
      $scope.TransactionInfo = {
         'config': {},
         'callback': {}
      } ;
      //事务列表
      var transactionInfoList = [] ;
      //上下文列表的表格
      $scope.TransactionTable = {
         'title': {
            'TransactionID':        $scope.autoLanguage( '事务ID' ),
            'NodeName':             $scope.autoLanguage( '节点' ),
            'SessionID':            $scope.autoLanguage( '会话ID' ),
            'GroupName':            $scope.autoLanguage( '分区组' ),
            'CurrentTransLSN':      $scope.autoLanguage( '当前事务LSN' ),
            'IsRollback':           $scope.autoLanguage( '是否回滚' ),
            'WaitLock':             $scope.autoLanguage( '等待锁' ),
            'TransactionLocksNum':  $scope.autoLanguage( '锁总数' ),
            'CsLock':               $scope.autoLanguage( '集合空间锁' ),
            'ClLock':               $scope.autoLanguage( '集合锁' ),
            'RecordLock':           $scope.autoLanguage( '记录锁' )
         },
         'body': [],
         'options': {
            'width': {},
            'sort': {
               'TransactionID':        true,
               'NodeName':             true,
               'SessionID':            true,
               'GroupName':            true,
               'CurrentTransLSN':      true,
               'IsRollback':           true,
               'WaitLock':             true,
               'TransactionLocksNum':  true,
               'CsLock':               true,
               'ClLock':               true,
               'RecordLock':           true
            },
            'max': 50,
            'filter': {
               'TransactionID':        'indexof',
               'NodeName':             'indexof',
               'SessionID':            'number',
               'GroupName':            'indexof',
               'CurrentTransLSN':      'number',
               'IsRollback': [
                  { 'key': $scope.autoLanguage( '全部' ), 'value': '' },
                  { 'key': $scope.autoLanguage( '是' ), 'value': true },
                  { 'key': $scope.autoLanguage( '否' ), 'value': false }
               ],
               'WaitLock':             'indexof',
               'TransactionLocksNum':  'number',
               'CsLock':               'number',
               'ClLock':               'number',
               'RecordLock':           'number'
            }
         },
         'callback': {}
      } ;

      //获取DB快照
      var getDbList = function( transactionList ){
         var sql  = 'SELECT NodeName, GroupName, NodeID, ServiceName FROM $SNAPSHOT_DB WHERE Role="data"' ;
         SdbRest.Exec( sql, {
            'success': function( dbList ){

               $.each( transactionList, function( index, transactionInfo ){
                  var csLock = 0 ;
                  var clLock = 0 ;
                  var recordLock = 0 ;
                  var waitLock = 0 ;

                  transactionInfo['CsLock'] = 0 ;
                  transactionInfo['ClLock'] = 0 ;
                  transactionInfo['RecordLock'] = 0 ;
                  transactionInfo['i'] = index ;

                  $.each( dbList, function( dbIndex, dbInfo ){
                     if( transactionInfo['NodeName'] == dbInfo['NodeName'] )
                     {
                        transactionList[index]['GroupName'] = dbInfo['GroupName'] ;
                        return false ;
                     }
                  } ) ;

                  $.each( transactionInfo['GotLocks'], function( Index2, lockInfo ){
                     if( lockInfo['recordID'] > -1  ) //记录锁
                     {
                        ++recordLock ;
                     }
                     else if( lockInfo['CLID'] > -1 && lockInfo['CLID'] < 65535 )
                     {
                        ++clLock ;
                     }
                     else if( lockInfo['CSID'] > -1 )
                     {
                        ++csLock ;
                     }
                  } ) ;

                  if( transactionInfo['WaitLock']['CSID'] > -1 )
                  {
                     ++waitLock ;
                  }
                  if( transactionInfo['WaitLock']['ClID'] > -1 && transactionInfo['WaitLock']['ClID'] < 65535 )
                  {
                     ++waitLock ;
                  }
                  if( transactionInfo['WaitLock']['recordID'] > -1 )
                  {
                     ++waitLock ;
                  }

                  transactionList[index]['CsLock'] = csLock ;
                  transactionList[index]['ClLock'] = clLock ;
                  transactionList[index]['RecordLock'] = clLock ;
                  transactionList[index]['WaitLock'] = waitLock ;

               } ) ;
               $scope.TransactionTable['body'] = transactionList ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getDbList( transactionList ) ;
                  return true ;
               } ) ;
            }
         }, { 'showLoading': false } ) ;
      }

      //获取事务列表
      var getTransaction = function(){
         var sql = 'SELECT * FROM $SNAPSHOT_TRANS' ;
         SdbRest.Exec( sql, {
            'success': function( data ){
               var transactionList = [] ;
               $.each( data, function( index, transactionInfo ){
                  if( isArray( transactionInfo['ErrNodes'] ) == false )
                  {
                     transactionList.push( transactionInfo ) ;
                  }
               } ) ;
               //如果有事务，执行获取db快照
               if( transactionList.length > 0 )
               {
                  $scope.TransactionTable['body'] = transactionList ;
                  transactionInfoList = $.extend( true, transactionList ) ;
                  getDbList( $scope.TransactionTable['body'] ) ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getTransaction() ;
                  return true ;
               } ) ;
            }
         }, { 'showLoading': false } ) ;
      }
      
      getTransaction() ;

      //显示会话详细
      $scope.ShowSession = function( index ){
         var sessionInfo = {} ;
         var sessionID = transactionInfoList[index]['SessionID'] ;
         var nodeName = transactionInfoList[index]['NodeName'] ;
         var sql = 'SELECT * FROM $SNAPSHOT_SESSION WHERE SessionID = ' + sessionID + ' AND NodeName = "' + nodeName + '"' ;
         SdbRest.Exec( sql, {
            'success': function( sessionList ){
               if( sessionList.length > 0 )
               {
                  sessionInfo = sessionList[0] ;
                  //将上下文ID数组转成字符串
                  sessionInfo['Contexts'] = sessionInfo['Contexts'].join() ;
                  $scope.SessionInfo['config'] = sessionInfo ;
                  //设置标题
                  $scope.SessionInfo['callback']['SetTitle']( $scope.autoLanguage( '会话信息' ) ) ;
                  //设置图标
                  $scope.SessionInfo['callback']['SetIcon']( '' ) ;
                  //打开窗口
                  $scope.SessionInfo['callback']['Open']() ;
                  //异步打开弹窗，需要马上刷新$watch
                  $scope.$digest() ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  $scope.SessionInfo['callback']['Close']() ;
                  $scope.ShowSession( nodeName, sessionID ) ;
                  return true ;
               } ) ;
            }
         }, { 'showLoading': false } ) ;
      }

      //显示事务详细
      $scope.ShowTrans = function( index ){
         $scope.TransactionInfo['config'] = transactionInfoList[index] ;
         $scope.TransactionInfo['callback']['SetTitle']( $scope.autoLanguage( '事务信息' ) ) ;
         $scope.TransactionInfo['callback']['SetIcon']('') ;
         $scope.TransactionInfo['callback']['Open']() ;
      }

      //跳转至分区组
      $scope.GotoGroup = function( groupName ){
         SdbFunction.LocalData( 'SdbGroupName', groupName ) ;
         $location.path( '/Monitor/SDB-Nodes/Group/Index' ).search( { 'r': new Date().getTime() } ) ;
      }
      
       //跳转至资源
      $scope.GotoResources = function(){
         $location.path( '/Monitor/SDB-Resources/Session' ).search( { 'r': new Date().getTime() } ) ;
      }

      //跳转至主机列表
      $scope.GotoHostList = function(){
         $location.path( '/Monitor/SDB-Host/List/Index' ).search( { 'r': new Date().getTime() } ) ;
      }
      
      //跳转至节点列表
      $scope.GotoNodeList = function(){
         if( moduleMode == 'distribution' )
         {
            $location.path( '/Monitor/SDB-Nodes/Nodes' ).search( { 'r': new Date().getTime() } ) ;
         }
         else
         {
            $location.path( '/Monitor/SDB-Nodes/Node/Index' ).search( { 'r': new Date().getTime() } ) ;
         }
      }
   } ) ;
}());