(function(){
   var sacApp = window.SdbSacManagerModule ;
   //控制器
   sacApp.controllerProvider.register( 'Monitor.SdbOverview.GroupsNature.Ctrl', function( $scope, $compile, $location, SdbRest, SdbFunction ){
      
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

      if( moduleMode != 'distribution' )  //只支持集群模式
      {
         $location.path( '/Monitor/SDB/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }

      //初始化
      var isFirst = true ; //是否第一次初始化
      //上一次的值
      $scope.LastValue = [] ;
      //分区组列表的表格
      $scope.GroupTable = {
         'title': {
            'Condition':      'Status',
            'GroupName':      'GroupName',
            'Group.length':   'Node number',
            'TotalRecords':   'TotalRecords',
            'TotalInsert':    'TotalInsert',
            'TotalDelete':    'TotalDelete',
            'TotalUpdate':    'TotalUpdate',
            'TotalRead':      'TotalRead'
         },
         'body': [],
         'options': {
            'width': {},
            'sort': {
               'Condition':      true,
               'GroupName':      true,
               'Group.length':   true,
               'TotalRecords':   true,
               'TotalInsert':    true,
               'TotalDelete':    true,
               'TotalUpdate':    true,
               'TotalRead':      true
            },
            'max': 50,
            'filter': {
               'Condition': [
                  { 'key': $scope.autoLanguage( '全部' ),     'value': '' },
                  { 'key': $scope.autoLanguage( '正常' ),     'value': 'Normal' },
                  { 'key': $scope.autoLanguage( '未激活' ),   'value': 'Inactivity' },
                  { 'key': $scope.autoLanguage( '节点异常' ), 'value': 'Warning' },
                  { 'key': $scope.autoLanguage( '错误' ),     'value': 'Error' }
               ],
               'GroupName':      'indexof',
               'Group.length':   'number',
               'TotalRecords':   'number',
               'TotalInsert':    'number',
               'TotalDelete':    'number',
               'TotalUpdate':    'number',
               'TotalRead':      'number'
            }
         },
         'callback': {}
      } ;
      //定时器
      $scope.Timer = {
         'config': {
            interval: 5
         },
         'callback': {}
      } ;
      //实时刷新设置 弹窗
      $scope.CreateBrush = {
         'config': {},
         'callback': {}
      } ;
      //显示列 下拉菜单
      $scope.FieldDropdown = {
         'config': [
            { 'key': 'Condition',      'field': 'Status',         'show': true },
            { 'key': 'GroupName',      'field': 'GroupName',      'show': true },
            { 'key': 'Group.length',   'field': 'Node number',    'show': true },
            { 'key': 'TotalRecords',   'field': 'TotalRecords',   'show': true },
            { 'key': 'TotalInsert',    'field': 'TotalInsert',    'show': true },
            { 'key': 'TotalDelete',    'field': 'TotalDelete',    'show': true },
            { 'key': 'TotalUpdate',    'field': 'TotalUpdate',    'show': true },
            { 'key': 'TotalRead',      'field': 'TotalRead',      'show': true }
         ],
         'callback': {}
      } ;

      //打开 显示列 的下拉菜单
      $scope.OpenShowFieldDropdown = function( event ){
         $.each( $scope.FieldDropdown['config'], function( index, fieldInfo ){
            $scope.FieldDropdown['config'][index]['show'] = typeof( $scope.GroupTable['title'][fieldInfo['key']] ) == 'string' ;
         } ) ;
         $scope.FieldDropdown['callback']['Open']( event.currentTarget ) ;
      }

      //保存 显示列
      $scope.SaveField = function(){
         $.each( $scope.FieldDropdown['config'], function( index, fieldInfo ){
            $scope.GroupTable['title'][fieldInfo['key']] = fieldInfo['show'] ? fieldInfo['field'] : false ;
         } ) ;
         $scope.FieldDropdown['callback']['Close']() ;
         $scope.GroupTable['callback']['ShowCurrentPage']() ;
      }

      //获取coord节点状态
      var getCoordStatus = function( groupInfo, nodesInfo, hostname, svcname ){
         var sql = 'SELECT NodeName FROM $SNAPSHOT_DB WHERE GLOBAL=false' ;
         SdbRest.Exec( sql, {
            'before': function( jqXHR ){
               jqXHR.setRequestHeader( 'SdbHostName', hostname ) ;
               jqXHR.setRequestHeader( 'SdbServiceName', svcname ) ;
            },
            'success': function( dbInfo ){
               if( dbInfo.length > 0 )
               {
                  dbInfo = dbInfo[0] ;
                  if( dbInfo['NodeName'] != hostname + ':' + svcname ) //om虽然连接成功，但是已经切换到其他coord节点了，所以还是失败的
                  {
                     groupInfo['ErrNodes'].push( { 'NodeName': hostname + ':' + svcname, 'Flag': -15 } ) ;
                     groupInfo['Condition'] = ( groupInfo['ErrNodes'].length == groupInfo['Group'].length ) ? 'Error' : 'Warning' ;
                  }
               }
            }, 
            'failed':function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getCoordStatus( groupInfo, nodesInfo, hostname, svcname ) ;
                  return true ;
               } ) ;
            }
         }, { 'showLoading': false } ) ;
      }

      //获取CL快照
      var getClList = function( groupList, dbList ){
         var sql  = 'SELECT * FROM $SNAPSHOT_CL WHERE NodeSelect = "master"' ;
         SdbRest.Exec( sql, {
            'success': function( clList ){
               $.each( groupList, function( groupIndex, groupInfo ){
                  groupList[groupIndex]['i'] = groupIndex ;
                  groupList[groupIndex]['ErrNodes'] = [] ;
                  //初始化分区组状态，Inactivity为未激活分区组
                  groupList[groupIndex]['Condition'] = groupInfo['Status'] == 0 ? 'Inactivity' : 'Normal' ;
                  if( groupInfo['Role'] == 1 || groupInfo['Status'] == 0 )
                  {
                     groupList[groupIndex]['TotalRecords'] = '-' ;
                     groupList[groupIndex]['TotalInsert'] = '-' ;
                     groupList[groupIndex]['TotalDelete'] = '-' ;
                     groupList[groupIndex]['TotalUpdate'] = '-' ;
                     groupList[groupIndex]['TotalRead'] = '-' ;
                     if( groupInfo['Role'] == 1 )
                     {
                        //检查协调节点状态
                        $.each( groupInfo['Group'], function( nodeIndex, nodeInfo ){
                           getCoordStatus( groupInfo, nodeInfo, nodeInfo['HostName'], nodeInfo['Service'][0]['Name'] ) ;
                        } ) ;
                     }
                  }
                  else
                  {
                     groupList[groupIndex]['TotalInsert'] = 0 ;
                     groupList[groupIndex]['TotalDelete'] = 0 ;
                     groupList[groupIndex]['TotalUpdate'] = 0 ;
                     groupList[groupIndex]['TotalRead'] = 0 ;
                     groupList[groupIndex]['TotalRecords'] = 0 ;
                     $.each( dbList, function( dbIndex, dbInfo ){
                        //节点错误时
                        if( isArray( dbInfo['ErrNodes'] ) == true )
                        {
                           $.each( dbInfo['ErrNodes'], function( errIndex, errInfo ){
                              if( groupInfo['GroupName'] == errInfo['GroupName'] )
                              {
                                 groupList[groupIndex]['ErrNodes'].push( { 'NodeName': errInfo['NodeName'], 'Flag': errInfo['Flag'] } ) ;
                              }
                           } ) ;
                        }
                        else
                        {
                           //计算分区组的增删改查数
                           if( dbInfo['GroupName'] == groupInfo['GroupName'] && dbInfo['IsPrimary'] == true )
                           {
                              groupList[groupIndex]['TotalInsert'] = dbInfo['TotalInsert'] ;
                              groupList[groupIndex]['TotalDelete'] = dbInfo['TotalDelete'] ;
                              groupList[groupIndex]['TotalUpdate'] = dbInfo['TotalUpdate'] ;
                              groupList[groupIndex]['TotalRead']   = dbInfo['TotalRead'] ;
                           }
                        }
                     } ) ;

                     //分析分区组错误
                     if( groupList[groupIndex]['ErrNodes'].length > 0 )
                     {
                        groupList[groupIndex]['Condition'] = ( groupList[groupIndex]['ErrNodes'].length == groupList[groupIndex]['Group'].length ) ? 'Error' : 'Warning' ;
                        if( groupList[groupIndex]['ErrNodes'].length == groupList[groupIndex]['Group'].length )
                        {
                           groupList[groupIndex]['TotalRecords'] = '-' ;
                           groupList[groupIndex]['TotalInsert'] = '-' ;
                           groupList[groupIndex]['TotalDelete'] = '-' ;
                           groupList[groupIndex]['TotalUpdate'] = '-' ;
                           groupList[groupIndex]['TotalRead'] = '-' ;
                        }
                     }

                     //计算分区组的记录数
                     $.each( clList, function( clIndex, clInfo ){
                        //节点错误时不计算
                        if( typeof( clInfo['ErrNodes'] ) == 'undefined' )
                        {
                           $.each( clInfo['Details'], function( index, value ){
                              if( value['GroupName'] == groupInfo['GroupName'] )
                              {
                                 groupList[groupIndex]['TotalRecords'] += value['TotalRecords'] ;                              
                              }
                           } ) ;
                        }
                     } ) ;
                  }
               } ) ;
               $scope.LastValue = $scope.GroupTable['body'] ;
               $scope.GroupTable['body'] = groupList ;
               if( isFirst == true )
               {
                  $scope.LastValue = groupList ;
                  isFirst = false ;
               }
               if( $scope.Timer['callback']['GetStatus']() == 'start' || $scope.Timer['callback']['GetStatus']() == 'complete' ) //如果开了定时器，就开始
               {
                  $scope.Timer['callback']['Complete']() ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getClList( groupList, dbList ) ;
                  return true ;
               } ) ;
            }
         }, { 'showLoading': false } ) ;
      }

      //获取DB快照
      var getDbList = function( groupList ){
         var sql  = 'SELECT * FROM $SNAPSHOT_DB' ;
         SdbRest.Exec( sql, {
            'success': function( dbList ){
               getClList( groupList, dbList ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getDbList( groupList ) ;
                  return true ;
               } ) ;
            }
         }, { 'showLoading': false } ) ;
      }

      //获取分区组列表
      var getGroupList = function(){
         var data = { 'cmd': 'list groups' } ;
         SdbRest.DataOperation( data, function( groupList ){
            getDbList( groupList ) ;
         }, function( errorInfo ){
            _IndexPublic.createRetryModel( $scope, errorInfo, function(){
               getGroupList() ;
               return true ;
            } ) ;
         }, null, null, null, false ) ;
      }

      getGroupList() ;

      //打开 实时刷新 弹窗
      $scope.OpenBrushWindows = function(){
         //表单参数
         var brushForm = {
            'inputList': [
               {
                  "name": "play",
                  "webName": $scope.autoLanguage( '自动刷新' ),
                  "type": "select",
                  "value": $scope.Timer['callback']['GetStatus']() != 'stop',
                  "valid": [
                     { 'key': $scope.autoLanguage( '开启' ), 'value': true },
                     { 'key': $scope.autoLanguage( '停止' ), 'value': false }
                  ]
               },
               {
                  "name": "interval",
                  "webName": $scope.autoLanguage( '刷新间距(秒)' ),
                  "type": "int",
                  "value": $scope.Timer['callback']['GetInterval'](),
                  "valid": {
                     'min': 1
                  }
               }
            ]
         } ;
         $scope.CreateBrush['config'] = brushForm ;
         //设置确定按钮
         $scope.CreateBrush['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = brushForm.check() ;
            if( isAllClear )
            {
               var formVal = brushForm.getValue() ;
               $scope.Timer['callback']['SetInterval']( formVal['interval'] ) ;
               if( formVal['play'] == true )
               {
                  $scope.Timer['callback']['Start']( getGroupList ) ;
               }
               else
               {
                  $scope.Timer['callback']['Stop']() ;
               }
            }
            return isAllClear ;
         } ) ;
         //设置标题
         $scope.CreateBrush['callback']['SetTitle']( $scope.autoLanguage( '实时刷新设置' ) ) ;
         //设置图标
         $scope.CreateBrush['callback']['SetIcon']( '' ) ;
         //打开窗口
         $scope.CreateBrush['callback']['Open']() ;
      }

      //跳转至分区组信息
      $scope.GotoGroup = function( GroupName ){
         SdbFunction.LocalData( 'SdbGroupName', GroupName ) ;
         $location.path( '/Monitor/SDB-Nodes/Group/Index' ).search( { 'r': new Date().getTime() } ) ;
      } ;

      //跳转至资源
      $scope.GotoResources = function(){
         $location.path( '/Monitor/SDB-Resources/Session' ).search( { 'r': new Date().getTime() } ) ;
      } ;

      //跳转至主机列表
      $scope.GotoHostList = function(){
         $location.path( '/Monitor/SDB-Host/List/Index' ).search( { 'r': new Date().getTime() } ) ;
      } ;
      
      
      //跳转至节点列表
      $scope.GotoNodeList = function(){
         $location.path( '/Monitor/SDB-Nodes/Nodes' ).search( { 'r': new Date().getTime() } ) ;
      } ;
   } ) ;
}());

