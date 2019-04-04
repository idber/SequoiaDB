//@ sourceURL=Index.js
//"use strict" ;
(function(){
   var sacApp = window.SdbSacManagerModule ;
   //主控制器
   sacApp.controllerProvider.register( 'Deploy.Index.Ctrl', function( $scope, $location, $rootScope, SdbFunction, SdbRest, SdbSignal, SdbSwap, Loading ){
      var defaultShow = $rootScope.tempData( 'Deploy', 'Index' ) ;
      //初始化
      $scope.ShowType  = 1 ;
      //集群列表
      $scope.ClusterList = [] ;
      //默认选的cluster
      $scope.CurrentCluster = 0 ;
      //默认显示业务还是主机列表
      $scope.CurrentPage = 'module' ;
      //主机列表数量
      $scope.HostNum = 0 ;
      //主机列表
      SdbSwap.hostList = [] ;
      //业务列表数量
      $scope.ModuleNum = 0 ;
      //业务列表
      $scope.ModuleList = [] ;
      //业务集群模式数量
      SdbSwap.distributionNum = 0 ;
      //SDB业务数量
      SdbSwap.sdbModuleNum = 0 ;
      //业务类型列表
      SdbSwap.moduleType = [] ;

      //右侧高度偏移量
      $scope.BoxHeight = { 'offsetY': -119 } ;
      //判断如果是Firefox浏览器的话，调整右侧高度偏移量
      var browser = SdbFunction.getBrowserInfo() ;
      if( browser[0] == 'firefox' )
      {
         $scope.BoxHeight = { 'offsetY': -131 } ;
      }

      //关联关系列表
      SdbSwap.relationshipList = [] ;

      //主机和业务的关联表(也就是有安装业务的主机列表)
      var host_module_table = [] ;
      //循环查询的业务
      var autoQueryModuleIndex = [] ;
      //清空Deploy域的数据
      $rootScope.tempData( 'Deploy' ) ;

      //获取业务关联关系
      SdbSwap.getRelationship = function(){
         var data = { 'cmd': 'list relationship' } ;
         SdbRest.OmOperation( data, {
            'success': function( result ){
               SdbSwap.relationshipList = result ;
               $.each( $scope.ModuleList, function( index, moduleInfo ){
                  moduleInfo['Relationship'] = [] ;
                  $.each( SdbSwap.relationshipList, function( index2, relationInfo ){
                     if( relationInfo['From'] == moduleInfo['BusinessName'] || relationInfo['To'] == moduleInfo['BusinessName'] )
                     {
                        moduleInfo['Relationship'].push( relationInfo ) ;
                     }
                  } ) ;
               } ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  SdbSwap.getRelationship() ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': false
         } ) ;
      }

      //计算每个服务的资源
      var countModule_Host = function(){
         $.each( $scope.ModuleList, function( index, moduleInfo ){
            if( isArray( moduleInfo['Location'] ) )
            {
               var cpu = 0 ;
               var memory = 0 ;
               var disk = 0 ;
               var length = 0 ;
               $.each( moduleInfo['Location'], function( index2, hostInfo ){
                  var index3 = hostModuleTableIsExist( hostInfo['HostName'] ) ;
                  if( index3 >= 0 )
                  {
                     ++length ;
                     cpu += host_module_table[index3]['Info']['CPU'] ;
                     memory += host_module_table[index3]['Info']['Memory'] ;
                     disk += host_module_table[index3]['Info']['Disk'] ;
                     if( $scope.ModuleList[index]['Error']['Type'] == 'Host' || $scope.ModuleList[index]['Error']['Flag'] == 0 )
                     {
                        if( host_module_table[index3]['Error']['Flag'] == 0 )
                        {
                           $scope.ModuleList[index]['Error']['Flag'] = 0 ;
                        }
                        else
                        {
                           $scope.ModuleList[index]['Error']['Flag'] = host_module_table[index3]['Error']['Flag'] ;
                           $scope.ModuleList[index]['Error']['Type'] = 'Host' ;
                           $scope.ModuleList[index]['Error']['Message'] = sprintf( $scope.autoLanguage( '主机 ? 状态异常: ?。' ), 
                                                                                   host_module_table[index3]['HostName'],
                                                                                   host_module_table[index3]['Error']['Message'] ) ;
                        }
                     }
                  }
               } ) ;
               $scope.ModuleList[index]['Chart']['Host']['CPU'] = { 'percent': fixedNumber( cpu / length, 2 ), 'style': { 'progress': { 'background': '#87CEFA' } } } ;
               $scope.ModuleList[index]['Chart']['Host']['Memory'] = { 'percent': fixedNumber( memory / length, 2 ), 'style': { 'progress': { 'background': '#DDA0DD' } } } ;
               $scope.ModuleList[index]['Chart']['Host']['Disk'] = { 'percent':  fixedNumber( disk / length, 2 ), 'style': { 'progress': { 'background': '#FFA07A' } } } ;
            }
         } ) ;
      }
      
      //host_module_table是否已经存在该主机
      var hostModuleTableIsExist = function( hostName ){
         var flag = -1 ;
         $.each( host_module_table, function( index, hostInfo ){
            if( hostInfo['HostName'] == hostName )
            {
               flag = index ;
               return false ;
            }
         } ) ;
         return flag ;
      }

      //SdbSwap.hostList是否存在该主机
      var hostListIsExist = function( hostName ){
         var flag = -1 ;
         $.each( SdbSwap.hostList, function( index, hostInfo ){
            if( hostInfo['HostName'] == hostName )
            {
               flag = index ;
               return false ;
            }
         } ) ;
         return flag ;
      }

      //查询主机状态
      var queryHostStatus = function(){
         var isFirstQueryHostStatus = true ;
         var queryHostList ;
         SdbRest.OmOperation( null, {
            'init': function(){
               queryHostList = { 'HostInfo': [] } ;
               if( typeof( SdbSwap.hostList ) == 'undefined' )
               {
                  SdbSwap.hostList = [] ;
               }
               $.each( SdbSwap.hostList, function( index, hostInfo ){
                  if( isFirstQueryHostStatus || hostInfo['ClusterName'] == $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] )
                  {
                     if( typeof( hostInfo['HostName'] ) == 'undefined' )
                     {
                        return ;
                     }
                     queryHostList['HostInfo'].push( { 'HostName': hostInfo['HostName'] } ) ;
                  }
               } ) ;
               isFirstQueryHostStatus = false ;
               return { 'cmd': 'query host status', 'HostInfo': JSON.stringify( queryHostList ) } ;
            },
            'success': function( hostStatusList ){
               $.each( hostStatusList[0]['HostInfo'], function( index, statusInfo ){
                  var index2 = hostModuleTableIsExist( statusInfo['HostName'] ) ;
                  if( index2 >= 0 )
                  {
                     if( isNaN( statusInfo['errno'] ) || statusInfo['errno'] == 0  )
                     {
                        if( typeof( host_module_table[index2]['CPU'] ) == 'object' )
                        {
                           var resource = host_module_table[index2] ;
                           var old_idle1   = resource['CPU']['Idle']['Megabit'] ;
                           var old_idle2   = resource['CPU']['Idle']['Unit'] ;
                           var old_cpuSum1 = resource['CPU']['Idle']['Megabit'] +
                                             resource['CPU']['Other']['Megabit'] +
                                             resource['CPU']['Sys']['Megabit'] +
                                             resource['CPU']['User']['Megabit'] ;
                           var old_cpuSum2 = resource['CPU']['Idle']['Unit'] +
                                             resource['CPU']['Other']['Unit'] +
                                             resource['CPU']['Sys']['Unit'] +
                                             resource['CPU']['User']['Unit'] ;
                           var idle1   = statusInfo['CPU']['Idle']['Megabit'] ;
                           var idle2   = statusInfo['CPU']['Idle']['Unit'] ;
                           var cpuSum1 = statusInfo['CPU']['Idle']['Megabit'] +
                                         statusInfo['CPU']['Other']['Megabit'] +
                                         statusInfo['CPU']['Sys']['Megabit'] +
                                         statusInfo['CPU']['User']['Megabit'] ;
                           var cpuSum2 = statusInfo['CPU']['Idle']['Unit'] +
                                         statusInfo['CPU']['Other']['Unit'] +
                                         statusInfo['CPU']['Sys']['Unit'] +
                                         statusInfo['CPU']['User']['Unit'] ;
                           host_module_table[index2]['Info']['CPU'] = ( ( 1 - ( ( idle1 - old_idle1 ) * 1024 + ( idle2 - old_idle2 ) / 1024 ) / ( ( cpuSum1 - old_cpuSum1 ) * 1024 + ( cpuSum2 - old_cpuSum2 ) / 1024 ) ) * 100 ) ;
                        }
                        else
                        {
                           host_module_table[index2]['Info']['CPU'] = 0 ;
                        }
                        host_module_table[index2]['CPU'] = statusInfo['CPU'] ;
                        var diskFree = 0 ;
                        var diskSize = 0 ;
                        $.each( statusInfo['Disk'], function( index2, diskInfo ){
                           diskFree += diskInfo['Free'] ;
                           diskSize += diskInfo['Size'] ;
                        } ) ;
                        if( diskSize == 0 )
                        {
                           host_module_table[index2]['Info']['Disk'] = 0 ;
                        }
                        else
                        {
                           host_module_table[index2]['Info']['Disk'] = ( 1 - diskFree / diskSize ) * 100 ;
                        }
                        host_module_table[index2]['Info']['Memory'] = statusInfo['Memory']['Used'] / statusInfo['Memory']['Size'] * 100 ;
                        host_module_table[index2]['Error']['Flag'] = 0 ;
                        var index3 = hostListIsExist( statusInfo['HostName'] ) ;
                        if( index3 >= 0 )
                        {
                           SdbSwap.hostList[index3]['Error']['Flag'] = 0 ;
                        }
                     }
                     else
                     {
                        host_module_table[index2]['Info']['CPU'] = 0 ;
                        host_module_table[index2]['Info']['Disk'] = 0 ;
                        host_module_table[index2]['Info']['Memory'] = 0 ;
                        host_module_table[index2]['Error']['Flag'] = statusInfo['errno'] ;
                        host_module_table[index2]['Error']['Message'] = statusInfo['detail'] ;
                        var index3 = hostListIsExist( statusInfo['HostName'] ) ;
                        if( index3 >= 0 )
                        {
                           SdbSwap.hostList[index3]['Error']['Flag'] = statusInfo['errno'] ;
                           SdbSwap.hostList[index3]['Error']['Message'] = statusInfo['detail'] ;
                        }
                     }
                  }
                  else
                  {
                     if( statusInfo['errno'] == 0 || typeof( statusInfo['errno'] ) == 'undefined' )
                     {
                        var index3 = hostListIsExist( statusInfo['HostName'] ) ;
                        if( index3 >= 0 )
                        {
                           SdbSwap.hostList[index3]['Error']['Flag'] = 0 ;
                        }
                     }
                     else
                     {
                        var index3 = hostListIsExist( statusInfo['HostName'] ) ;
                        if( index3 >= 0 )
                        {
                           SdbSwap.hostList[index3]['Error']['Flag'] = statusInfo['errno'] ;
                           SdbSwap.hostList[index3]['Error']['Message'] = statusInfo['detail'] ;
                        }
                     }
                  }
               } ) ;
               countModule_Host() ;
            }
         }, {
            'showLoading': false,
            'delay': 5000,
            'loop': true
         } ) ;
      }

      //获取sequoiadb的节点信息
      var getNodesList = function( moduleIndex ){
         if( $.inArray( moduleIndex, autoQueryModuleIndex ) == -1 )
         {
            return ;
         }
         $scope.ModuleList[moduleIndex]['BusinessInfo'] = {} ;
         var moduleName = $scope.ModuleList[moduleIndex]['BusinessName'] ;
         var data = { 'cmd': 'list nodes', 'BusinessName': moduleName } ;
         SdbRest.OmOperation( data, {
            'success': function( nodeList ){
               $scope.ModuleList[moduleIndex]['BusinessInfo']['NodeList'] = nodeList ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getNodesList( moduleIndex ) ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': false
         } ) ;
      } ;

      //获取sequoiadb服务信息
      var getCollectionInfo = function( moduleIndex, clusterIndex ){
         if( $.inArray( moduleIndex, autoQueryModuleIndex ) == -1 )
         {
            return ;
         }

         var clusterName = $scope.ModuleList[moduleIndex]['ClusterName'] ;
         var moduleName = $scope.ModuleList[moduleIndex]['BusinessName'] ;
         var moduleMode = $scope.ModuleList[moduleIndex]['DeployMod'] ;
         var sql ;
         if( moduleMode == 'standalone' )
         {
            sql = 'SELECT t1.Name, t1.Details.TotalIndexPages, t1.Details.PageSize, t1.Details.TotalDataPages, t1.Details.LobPageSize, t1.Details.TotalLobPages FROM (SELECT * FROM $SNAPSHOT_CL split BY Details) AS t1' ;
         }
         else
         {
            sql = 'SELECT t1.Name, t1.Details.TotalIndexPages, t1.Details.PageSize, t1.Details.TotalDataPages, t1.Details.LobPageSize, t1.Details.TotalLobPages FROM (SELECT * FROM $SNAPSHOT_CL WHERE NodeSelect="master" split BY Details) AS t1' ;
         }
         SdbRest.Exec2( clusterName, moduleName, sql, {
            'before': function(){
               if( $.inArray( moduleIndex, autoQueryModuleIndex ) == -1 || clusterIndex != $scope.CurrentCluster )
               {
                  return false ;
               }
            },
            'success': function( clList ){
               var index = 0 ;
               var data = 0 ;
               var lob = 0 ;
               var indexPercent = 0 ;
               var dataPercent = 0 ;
               var lobPercent = 0 ;
               $.each( clList, function( clIndex, clInfo ){
                  index += clInfo['PageSize'] * clInfo['TotalIndexPages'] ;
                  data += clInfo['PageSize'] * clInfo['TotalDataPages'] ;
                  lob += clInfo['LobPageSize'] * clInfo['TotalLobPages'] ;
               } ) ;
               var sum = index + data + lob ;
               var indexPercent = fixedNumber( index / sum * 100, 2 ) ;
               var dataPercent  = fixedNumber( data / sum * 100, 2 ) ;
               var lobPercent   = 100 - indexPercent - dataPercent ;
               if( isNaN( indexPercent ) || index == 0 )
               {
                  indexPercent = 0 ;
               }
               if( isNaN( dataPercent ) || data == 0 )
               {
                  dataPercent = 0 ;
               }
               if( isNaN( lobPercent ) || lob == 0 )
               {
                  lobPercent = 0 ;
               }
               $scope.ModuleList[ moduleIndex ]['Chart']['Module']['value'] = [
                  [ 0, indexPercent, true, false ],
                  [ 1, dataPercent, true, false ],
                  [ 2, lobPercent, true, false ]
               ] ;

               if( $scope.ModuleList[moduleIndex]['Error']['Type'] == 'Module cl' )
               {
                  $scope.ModuleList[moduleIndex]['Error']['Flag'] = 0 ;
               }
            },
            'failed': function( errorInfo ){
               //if( moduleMode == 'standalone' )
               {
                  if( $scope.ModuleList[moduleIndex]['Error']['Type'] == 'Module cl' || $scope.ModuleList[moduleIndex]['Error']['Flag'] == 0 )
                  {
                     $scope.ModuleList[moduleIndex]['Error']['Flag'] = errorInfo['errno'] ;
                     $scope.ModuleList[moduleIndex]['Error']['Type'] = 'Module cl' ;
                     $scope.ModuleList[moduleIndex]['Error']['Message'] = sprintf( $scope.autoLanguage( '节点错误: ?，错误码 ?。' ),
                                                                                   errorInfo['description'],
                                                                                   errorInfo['errno'] ) ;
                  }
               }
            }
         }, {
            'showLoading':false,
            'delay': 5000,
            'loop': true
         } ) ;
      }

      //获取sequoiadb的错误节点信息
      var getErrNodes = function( moduleIndex, clusterIndex ){
         if( $.inArray( moduleIndex, autoQueryModuleIndex ) == -1 )
         {
            return ;
         }
         var moduleMode = $scope.ModuleList[moduleIndex]['DeployMod'] ;
         if( moduleMode == 'standalone' )
         {
            return ;
         }
         var clusterName = $scope.ModuleList[moduleIndex]['ClusterName'] ;
         var moduleName = $scope.ModuleList[moduleIndex]['BusinessName'] ;
         var data = { 'cmd': 'snapshot system', 'selector': JSON.stringify( { 'ErrNodes': 1 } ) } ;
         SdbRest.DataOperation2( clusterName, moduleName, data, {
            'before': function(){
               if( $.inArray( moduleIndex, autoQueryModuleIndex ) == -1 || clusterIndex != $scope.CurrentCluster )
               {
                  return false ;
               }
            },
            'success': function( errNodes ){
               errNodes = errNodes[0]['ErrNodes'] ;
               if( $scope.ModuleList[moduleIndex]['Error']['Type'] == 'Module node' || $scope.ModuleList[moduleIndex]['Error']['Flag'] == 0 )
               {
                  if( errNodes.length > 0 )
                  {
                     $scope.ModuleList[moduleIndex]['Error']['Flag'] = errNodes[0]['Flag'] ;
                     $scope.ModuleList[moduleIndex]['Error']['Type'] = 'Module node' ;
                     $scope.ModuleList[moduleIndex]['Error']['Message'] = sprintf( $scope.autoLanguage( '节点错误: ?，错误码 ?。' ),
                                                                                   errNodes[0]['NodeName'],
                                                                                   errNodes[0]['Flag'] ) ;
                  }
                  else if( errNodes.length == 0 )
                  {
                     $scope.ModuleList[moduleIndex]['Error']['Flag'] = 0 ;
                  }
               }
            }
         }, {
            'showLoading': false,
            'delay': 5000,
            'loop': true
         } ) ;
      }

      //查询服务
      SdbSwap.queryModule = function(){
         var ClusterList = [] ;
         var clusterIsExist = function( clusterName ){
            var flag = 0 ;
            var isFind = false ;
            $.each( ClusterList, function( index, clusterInfo ){
               if( clusterInfo['ClusterName'] == clusterName )
               {
                  isFind = true ;
                  flag = clusterInfo['index'] ;
                  ++ClusterList[index]['index'] ;
                  return false ;
               }
            } ) ;
            if( isFind == false )
            {
               ClusterList.push( { 'ClusterName': clusterName, 'index': 1 } ) ;
            }
            return flag ;
         }
         var data = { 'cmd': 'query business' } ;
         SdbRest.OmOperation( data, {
            'success': function( moduleList ){
               $scope.ModuleList = moduleList ;
               host_module_table = [] ;
               autoQueryModuleIndex = [] ;
               //获取服务关联信息
               SdbSwap.getRelationship() ;
               $.each( $scope.ModuleList, function( index, moduleInfo ){

                  var colorId = clusterIsExist( moduleInfo['ClusterName'] ) ;

                  $scope.ModuleList[index]['Color'] = colorId ;

                  $scope.ModuleList[index]['Error'] = {} ;
                  $scope.ModuleList[index]['Error']['Flag'] = 0 ;
                  $scope.ModuleList[index]['Error']['Type'] = '' ;
                  $scope.ModuleList[index]['Error']['Message'] = '' ;

                  $scope.ModuleList[index]['Chart'] = {} ;
                  $scope.ModuleList[index]['Chart']['Module'] = {} ;
                  $scope.ModuleList[index]['Chart']['Module']['options'] = $.extend( true, {}, window.SdbSacManagerConf.StorageScaleEchart ) ;
                  $scope.ModuleList[index]['Chart']['Module']['options']['title']['text'] = $scope.autoLanguage( '元数据比例' ) ;

                  $scope.ModuleList[index]['Chart']['Host'] = {} ;
                  $scope.ModuleList[index]['Chart']['Host']['CPU'] = { 'percent': 0 } ;
                  $scope.ModuleList[index]['Chart']['Memory'] = { 'percent': 0 } ;
                  $scope.ModuleList[index]['Chart']['Disk'] = { 'percent': 0 } ;
                  if( isArray( moduleInfo['Location'] ) )
                  {
                     $.each( moduleInfo['Location'], function( index2, hostInfo ){
                        if( hostModuleTableIsExist( hostInfo['HostName'] ) == -1 )
                        {
                           host_module_table.push( { 'HostName': hostInfo['HostName'], 'Info': {}, 'Error': {} } ) ;
                        }
                     } ) ;
                  }
                  if( moduleInfo['BusinessType'] == 'sequoiadb' && moduleInfo['ClusterName'] == $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] )
                  {
                     autoQueryModuleIndex.push( index ) ;
                  }
               } ) ;
               $scope.SwitchCluster( $scope.CurrentCluster ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  SdbSwap.queryModule() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //查询集群
      var queryCluster = function(){
         var data = { 'cmd': 'query cluster' } ;
         SdbRest.OmOperation( data, {
            'success': function( ClusterList ){
               $scope.ClusterList = ClusterList ;
               if( $scope.ClusterList.length > 0 )
               {
                  SdbSignal.commit( 'queryHost' ) ;
                  SdbSwap.queryModule() ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  queryCluster() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //跳转到服务数据
      $scope.GotoDataModule = function( clusterName, moduleType, moduleMode, moduleName ){
         SdbFunction.LocalData( 'SdbClusterName', clusterName ) ;
         SdbFunction.LocalData( 'SdbModuleType', moduleType ) ;
         SdbFunction.LocalData( 'SdbModuleMode', moduleMode ) ;
         SdbFunction.LocalData( 'SdbModuleName', moduleName ) ;
         switch( moduleType )
         {
         case 'sequoiadb':
            $location.path( '/Data/SDB-Database/Index' ).search( { 'r': new Date().getTime() } ) ; break ;
         case 'sequoiasql-postgresql':
            $location.path( '/Data/SequoiaSQL/PostgreSQL/Database/Index' ).search( { 'r': new Date().getTime() } ) ; break ;
         case 'sequoiasql-mysql':
            $location.path( '/Data/SequoiaSQL/MySQL/Database/Index' ).search( { 'r': new Date().getTime() } ) ; break ;
         case 'hdfs':
            $location.path( '/Data/HDFS-web/Index' ).search( { 'r': new Date().getTime() } ) ; break ;
         case 'spark':
            $location.path( '/Data/SPARK-web/Index' ).search( { 'r': new Date().getTime() } ) ; break ;
         case 'yarn':
            $location.path( '/Data/YARN-web/Index' ).search( { 'r': new Date().getTime() } ) ; break ;
         default:
            break ;
         }
      }

      //切换服务和主机
      $scope.SwitchPage = function( page ){
         $scope.CurrentPage = page ;
         $scope.bindResize() ;
      }

      //查询鉴权
      SdbSwap.queryAuth = function( businessName, index ){
         //查询鉴权
         var data = {
            'cmd': 'query business authority',
            'filter': JSON.stringify( { "BusinessName": businessName } ) 
         }
         SdbRest.OmOperation( data, {
            'success': function( authorityResult ){
               if( authorityResult.length > 0 )
               {
                  $scope.ModuleList[index]['authority'] = authorityResult ;
               }
               else
               {
                  $scope.ModuleList[index]['authority'] = [{}] ;
               }
            }
         }, {
            'showLoading': false
         } ) ;
      }

      //切换集群
      $scope.SwitchCluster = function( index ){
         $scope.CurrentCluster = index ;
         if( $scope.ClusterList.length > 0 )
         {
            var clusterName = $scope.ClusterList[ index ]['ClusterName'] ;
            $scope.ModuleNum = 0 ;
            SdbSwap.distributionNum = 0 ;
            SdbSwap.sdbModuleNum = 0 ;
            autoQueryModuleIndex = [] ;
            $.each( $scope.ModuleList, function( index2, moduleInfo ){
               if( moduleInfo['ClusterName'] == clusterName )
               {
                  ++$scope.ModuleNum ;
                  autoQueryModuleIndex.push( index2 ) ;
                  if( moduleInfo['BusinessType'] == 'sequoiadb' )
                  {
                     if( moduleInfo['DeployMod'] == 'distribution' )
                     {
                        ++SdbSwap.distributionNum ;
                     }
                     ++SdbSwap.sdbModuleNum ;
                     getNodesList( index2 ) ;
                     getCollectionInfo( index2, index ) ;
                     getErrNodes( index2, index ) ;
                  }

                  if( moduleInfo['BusinessType'] == 'sequoiadb' || moduleInfo['BusinessType'] == 'sequoiasql-postgresql' || moduleInfo['BusinessType'] == 'sequoiasql-mysql' )
                  {
                     SdbSwap.queryAuth( moduleInfo['BusinessName'], index2 ) ;
                  }
               }
            } ) ;

            $scope.HostNum = 0 ;
            var hostTableContent = [] ;
            $.each( SdbSwap.hostList, function( index2, hostInfo ){
               if( hostInfo['ClusterName'] == clusterName )
               {
                  hostTableContent.push( hostInfo )
                  ++$scope.HostNum ;
               }
            } ) ;
            SdbSignal.commit( 'updateHostTable', hostTableContent ) ;
         }
         $scope.bindResize() ;
      }

      //获取服务类型列表
      var GetModuleType = function(){
         var data = { 'cmd': 'list business type' } ;
         SdbRest.OmOperation( data, {
            'success': function( moduleType ){
               SdbSwap.moduleType = moduleType ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  GetModuleType() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //切换创建集群弹窗的tab
      $scope.SwitchParam = function( type ){
         $scope.ShowType = type ;
      }

      //创建集群
      var createCluster = function( clusterInfo, success ){
         var data = { 'cmd': 'create cluster', 'ClusterInfo': JSON.stringify( clusterInfo ) } ;
         SdbRest.OmOperation( data, {
            'success': function(){
               success() ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  createCluster( clusterInfo, success ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //创建集群 弹窗
      $scope.CreateClusterWindow = {
         'config': [],
         'callback': {}
      } ;

      //打开 创建集群 弹窗
      $scope.ShowCreateCluster = function(){
         $scope.ShowType = 1 ;
         $scope.CreateClusterWindow['config'] = [
            {
               'inputList': [
                  {
                     "name": 'ClusterName',
                     "webName": $scope.autoLanguage( '集群名' ),
                     "type": "string",
                     "required": true,
                     "value": "",
                     "valid": {
                        'min': 1,
                        'max': 127,
                        'regex': '^[0-9a-zA-Z_-]+$'
                     }
                  },
                  {
                     'name': 'Desc',
                     'webName': $scope.autoLanguage( '描述' ),
                     'type': 'string',
                     'value': '',
                     'valid': {
                        'min': 0,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'SdbUser',
                     "webName": $scope.autoLanguage( '用户名' ),
                     "type": "string",
                     "required": true,
                     "value": 'sdbadmin',
                     "valid": {
                        'min': 1,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'SdbPasswd',
                     "webName": $scope.autoLanguage( '密码' ),
                     "type": "string",
                     "required": true,
                     "value": 'sdbadmin',
                     "valid": {
                        'min': 1,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'SdbUserGroup',
                     "webName": $scope.autoLanguage( '用户组' ),
                     "type": "string",
                     "required": true,
                     "value": 'sdbadmin_group',
                     "valid": {
                        'min': 1,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'InstallPath',
                     "webName": $scope.autoLanguage( '安装路径' ),
                     "type": "string",
                     "required": true,
                     "value": '/opt/sequoiadb/',
                     "valid": {
                        'min': 1,
                        'max': 2048
                     }
                  }
               ]
            },
            {
               'inputList': [
                  {
                     "name": 'HostFile',
                     "webName": 'HostFile',
                     "type": "switch",
                     "value": true,
                     "desc": $scope.autoLanguage( '是否授权om对系统hosts文件的修改' ),
                     "onChange": function( name, key ){
                        $scope.CreateClusterWindow['config'][1]['inputList'][0]['value'] = !key ;
                     }
                  },
                  {
                     'name': 'RootUser',
                     'webName': 'RootUser',
                     'type': 'switch',
                     'disabled': true,
                     "value": true,
                     "desc": $scope.autoLanguage( '是否允许om使用系统root用户' )
                  }
               ]
            }
         ]
         var num = 1 ;
         var defaultName = '' ;
         while( true )
         {
            var isFind = false ;
            defaultName = sprintf( 'myCluster?', num ) ;
            $.each( $scope.ClusterList, function( index, clusterInfo ){
               if( defaultName == clusterInfo['ClusterName'] )
               {
                  isFind = true ;
                  return false ;
               }
            } ) ;
            if( isFind == false )
            {
               break ;
            }
            ++num ;
         }
         $scope.CreateClusterWindow['config'][0]['inputList'][0]['value'] = defaultName ;
         $scope.CreateClusterWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = $scope.CreateClusterWindow['config'][0].check( function( formVal ){
               var isFind = false ;
               $.each( $scope.ClusterList, function( index, clusterInfo ){
                  if( formVal['ClusterName'] == clusterInfo['ClusterName'] )
                  {
                     isFind = true ;
                     return false ;
                  }
               } ) ;
               if( isFind == true )
               {
                  return [ { 'name': 'ClusterName', 'error': $scope.autoLanguage( '集群名已经存在' ) } ]
               }
               else
               {
                  return [] ;
               }
            } ) ;
            if( isAllClear )
            {
               var formVal = $scope.CreateClusterWindow['config'][0].getValue() ;
               var formVal2 = $scope.CreateClusterWindow['config'][1].getValue() ;
               formVal['GrantConf'] = [] ;
               formVal['GrantConf'].push( { 'Name': 'HostFile', 'Privilege': formVal2['HostFile'] } ) ;
               formVal['GrantConf'].push( { 'Name': 'RootUser', 'Privilege': formVal2['RootUser'] } ) ;
               createCluster( formVal, function(){
                  $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() }  ) ;
               } ) ;
            }
            return isAllClear ;
         } ) ;
         $scope.CreateClusterWindow['callback']['SetTitle']( $scope.autoLanguage( '创建集群' ) ) ;
         $scope.CreateClusterWindow['callback']['Open']() ;
      }

      //删除集群
      var removeCluster = function( clusterIndex ){
         var clusterName = $scope.ClusterList[clusterIndex]['ClusterName'] ;
         var data = { 'cmd': 'remove cluster', 'ClusterName': clusterName } ;
         SdbRest.OmOperation( data, {
            'success': function(){
               if( clusterIndex == $scope.CurrentCluster )
               {
                  $scope.CurrentCluster = 0 ;
               }
               $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() }  ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  removeCluster( clusterIndex ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //删除集群 弹窗
      $scope.RemoveClusterWindow = {
         'config': {},
         'callback': {}
      } ;

      //打开 删除集群 弹窗
      $scope.ShowRemoveCluster = function(){
         if( $scope.ClusterList.length == 0 )
         {
            return ;
         }
         $scope.RemoveClusterWindow['config'] = {
            'inputList': [
               {
                  "name": 'ClusterName',
                  "webName": $scope.autoLanguage( '集群名' ),
                  "type": "select",
                  "value": $scope.CurrentCluster,
                  "valid": []
               }
            ]
         } ;
         $.each( $scope.ClusterList, function( index ){
            $scope.RemoveClusterWindow['config']['inputList'][0]['valid'].push( { 'key': $scope.ClusterList[index]['ClusterName'], 'value': index } ) ;
         } ) ;
         $scope.RemoveClusterWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = $scope.RemoveClusterWindow['config'].check() ;
            if( isAllClear )
            {
               var formVal = $scope.RemoveClusterWindow['config'].getValue() ;
               removeCluster( formVal['ClusterName'] ) ;
            }
            return isAllClear ;
         } ) ;
         $scope.RemoveClusterWindow['callback']['SetTitle']( $scope.autoLanguage( '删除集群' ) ) ;
         $scope.RemoveClusterWindow['callback']['Open']() ;
      }

      //一键部署 弹窗
      $scope.DeployModuleWindow = {
         'config': {},
         'callback': {}
      } ;

      //打开 一键部署 弹窗
      $scope.ShowDeployModule = function(){
         var num = 1 ;
         var defaultName = '' ;
         $scope.ShowType = 1 ;
         while( true )
         {
            var isFind = false ;
            defaultName = sprintf( 'myCluster?', num ) ;
            $.each( $scope.ClusterList, function( index, clusterInfo ){
               if( defaultName == clusterInfo['ClusterName'] )
               {
                  isFind = true ;
                  return false ;
               }
            } ) ;
            if( isFind == false )
            {
               break ;
            }
            ++num ;
         }
         $scope.DeployModuleWindow['config'] = [
            { 
               'inputList': [
                  {
                     "name": 'ClusterName',
                     "webName": $scope.autoLanguage( '集群名' ),
                     "type": "string",
                     "required": true,
                     "value": defaultName,
                     "valid": {
                        'min': 1,
                        'max': 127,
                        'regex': '^[0-9a-zA-Z_-]+$'
                     }
                  },
                  {
                     'name': 'Desc',
                     'webName': $scope.autoLanguage( '描述' ),
                     'type': 'string',
                     'value': '',
                     'valid': {
                        'min': 0,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'SdbUser',
                     "webName": $scope.autoLanguage( '用户名' ),
                     "type": "string",
                     "required": true,
                        "value": 'sdbadmin',
                     "valid": {
                        'min': 1,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'SdbPasswd',
                     "webName": $scope.autoLanguage( '密码' ),
                     "type": "string",
                     "required": true,
                     "value": 'sdbadmin',
                     "valid": {
                        'min': 1,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'SdbUserGroup',
                     "webName": $scope.autoLanguage( '用户组' ),
                     "type": "string",
                     "required": true,
                     "value": 'sdbadmin_group',
                     "valid": {
                        'min': 1,
                        'max': 1024
                     }
                  },
                  {
                     "name": 'InstallPath',
                     "webName": $scope.autoLanguage( '安装路径' ),
                     "type": "string",
                     "required": true,
                     "value": '/opt/sequoiadb/',
                     "valid": {
                        'min': 1,
                        'max': 2048
                     }
                  }
               ]
            },
            {
               'inputList': [
                  {
                     "name": 'moduleName',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "string",
                     "required": true,
                     "value": "myService",
                     "valid": {
                        "min": 1,
                        "max": 127,
                        'regex': '^[0-9a-zA-Z_-]+$'
                     }
                  },
                  {
                     "name": 'moduleType',
                     "webName": $scope.autoLanguage( '服务类型' ),
                     "type": "select",
                     "value": 0,
                     "valid": []
                  },
               ]  
            },
            {
               'inputList': [
                  {
                     "name": 'HostFile',
                     "webName": 'HostFile',
                     "type": "switch",
                     "value": true,
                     "desc": $scope.autoLanguage( '是否授权om对系统hosts文件的修改' ),
                     "onChange": function( name, key ){
                        $scope.DeployModuleWindow['config'][2]['inputList'][0]['value'] = !key ;
                     }
                  },
                  {
                     'name': 'RootUser',
                     'webName': 'RootUser',
                     'type': 'switch',
                     'disabled': true,
                     "value": true,
                     "desc": $scope.autoLanguage( '是否允许om使用系统root用户' )
                  }
               ]
            }
         ] ;
         num = 1 ;
         defaultName = '' ;
         while( true )
         {
            var isFind = false ;
            defaultName = sprintf( 'myService?', num ) ;
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( defaultName == moduleInfo['BusinessName'] )
               {
                  isFind = true ;
                  return false ;
               }
            } ) ;
            if( isFind == false )
            {
               $.each( $rootScope.OmTaskList, function( index, taskInfo ){
                  if( taskInfo['Status'] != 4 && defaultName == taskInfo['Info']['BusinessName'] )
                  {
                     isFind = true ;
                     return false ;
                  }
               } ) ;
               if( isFind == false )
               {
                  break ;
               }
            }
            ++num ;
         }
         $scope.DeployModuleWindow['config'][1]['inputList'][0]['value'] = defaultName ;
         $.each( SdbSwap.moduleType, function( index, typeInfo ){
            if( typeInfo['BusinessType'] == 'sequoiadb' )
            {
               $scope.DeployModuleWindow['config'][1]['inputList'][1]['valid'].push( { 'key': typeInfo['BusinessDesc'], 'value': index } ) ;
            }
            else
            {
               return ;
            }
         } ) ;
         $scope.DeployModuleWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear1 = $scope.DeployModuleWindow['config'][0].check( function( formVal ){
               var rv = [] ;
               var isFind = false ;
               $.each( $scope.ClusterList, function( index, clusterInfo ){
                  if( formVal['ClusterName'] == clusterInfo['ClusterName'] )
                  {
                     isFind = true ;
                     return false ;
                  }
               } ) ;
               if( isFind == true )
               {
                  rv.push( { 'name': 'ClusterName', 'error': $scope.autoLanguage( '集群名已经存在' ) } ) ;
               }
               return rv ;
            } ) ;
            var isAllClear2 = $scope.DeployModuleWindow['config'][1].check( function( formVal ){
               var rv = [] ;
               isFind = false ;
               $.each( $scope.ModuleList, function( index, moduleInfo ){
                  if( formVal['moduleName'] == moduleInfo['BusinessName'] )
                  {
                     isFind = true ;
                     return false ;
                  }
               } ) ;
               if( isFind == false )
               {
                  $.each( $rootScope.OmTaskList, function( index, taskInfo ){
                     if( taskInfo['Status'] != 4 && formVal['moduleName'] == taskInfo['Info']['BusinessName'] )
                     {
                        isFind = true ;
                        return false ;
                     }
                  } ) ;
               }
               if( isFind == true )
               {
                  rv.push( { 'name': 'moduleName', 'error': $scope.autoLanguage( '服务名已经存在' ) } ) ;
               }
               return rv ;
            } ) ;
            var isAllClear3 = $scope.DeployModuleWindow['config'][2].check() ;
            if( isAllClear1 && isAllClear2 && isAllClear3 )
            {
               var formVal1 = $scope.DeployModuleWindow['config'][0].getValue() ;
               var formVal2 = $scope.DeployModuleWindow['config'][1].getValue() ;
               var formVal3 = $scope.DeployModuleWindow['config'][2].getValue() ;
               $.each( formVal2, function( key, value ){
                  formVal1[key] = value ;
               } ) ;
               $.each( formVal3, function( key, value ){
                  formVal1[key] = value ;
               } ) ;
               createCluster( formVal1, function(){
                  $rootScope.tempData( 'Deploy', 'ClusterName', formVal1['ClusterName'] ) ;
                  $rootScope.tempData( 'Deploy', 'ModuleName', formVal1['moduleName'] ) ;
                  $rootScope.tempData( 'Deploy', 'Model', 'Deploy' ) ;
                  $rootScope.tempData( 'Deploy', 'Module', SdbSwap.moduleType[ formVal1['moduleType'] ]['BusinessType'] ) ;
                  $rootScope.tempData( 'Deploy', 'InstallPath', formVal1['InstallPath'] ) ;
                  $location.path( '/Deploy/ScanHost' ).search( { 'r': new Date().getTime() } ) ;
               } ) ;
            }
            return isAllClear1 && isAllClear2 && isAllClear3 ;
         } ) ;
         $scope.DeployModuleWindow['callback']['SetTitle']( $scope.autoLanguage( '部署' ) ) ;
         $scope.DeployModuleWindow['callback']['Open']() ;
      }

      //设置资源授权
      var setGrant = function( conf ){
         var data = { 'cmd': 'grant sysconf', 'name': conf['name'], 'privilege': conf['privilege'], 'clustername': conf['clustername'] } ;
         SdbRest.OmOperation( data, {
            'success': function(){
               queryCluster();
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  setGrant( conf ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //通过哪个cluster打开下拉菜单
      var chooseCluster = $scope.CurrentCluster ;

      //资源授权 弹窗
      $scope.ResourceGrantWindow = {
         'config': {},
         'callback': {}
      }

      //打开 资源授权
      var showResourceGrant = function(){
         $scope.ResourceGrantWindow['config'] = {
            'inputList': [
               {
                  "name": 'ClusterName',
                  "webName": $scope.autoLanguage( '集群名' ),
                  "type": "normal",
                  "value": $scope.ClusterList[chooseCluster]['ClusterName']
               },
               {
                  "name": 'HostFile',
                  "webName": 'HostFile',
                  "type": "switch",
                  "value": $scope.ClusterList[chooseCluster]['GrantConf'][0]['Privilege'],
                  "desc": $scope.autoLanguage( '是否授权om对系统hosts文件的修改' ),
                  "onChange": function( name, key ){
                     $scope.ResourceGrantWindow['config']['inputList'][1]['value'] = !key ;
                     setGrant( { 'name': 'HostFile', 'privilege': !key, 'clustername': $scope.ClusterList[chooseCluster]['ClusterName'] } ) ;
                  }
               },
               {
                  'name': 'RootUser',
                  'webName': 'RootUser',
                  'type': 'switch',
                  'disabled': true,
                  "value": true,
                  "desc": $scope.autoLanguage( '是否允许om使用系统root用户' )
               }
            ]
         } ;
         $scope.ResourceGrantWindow['callback']['SetCloseButton']( $scope.autoLanguage( '关闭' ), function(){
            $scope.ResourceGrantWindow['callback']['Close']() ;
         } ) ;
         $scope.ResourceGrantWindow['callback']['SetTitle']( $scope.autoLanguage( '资源授权' ) ) ;
         $scope.ResourceGrantWindow['callback']['Open']() ;
      }

      //集群操作下拉菜单
      $scope.ClusterDropdown = {
         'config': [
            { 'key': $scope.autoLanguage( '资源授权' ) },
            { 'key': $scope.autoLanguage( '删除集群' ) }
         ],
         'OnClick': function( index ){
            if( index == 0 )
            {
               showResourceGrant() ;
               $scope.ResourceGrantWindow['config']['inputList'][0]['disabled'] = true ;
               $scope.ResourceGrantWindow['config']['inputList'][1]['value'] = $scope.ClusterList[chooseCluster]['GrantConf'][0]['Privilege'] ;
            }
            else
            {
               $scope.Components.Confirm.type = 3 ;
               $scope.Components.Confirm.context = sprintf( $scope.autoLanguage( '是否确定删除集群：?？' ), $scope.ClusterList[chooseCluster]['ClusterName'] ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
               $scope.Components.Confirm.ok = function(){
                  removeCluster( chooseCluster ) ;
               }
            }
            $scope.ClusterDropdown['callback']['Close']() ;
         },
         'callback': {}
      } ;

      //打开 集群操作下拉菜单
      $scope.OpenClusterDropdown = function( event, index ){
         chooseCluster = index ;
         $scope.ClusterDropdown['callback']['Open']( event.currentTarget ) ;
      }

      //执行
      GetModuleType() ;
      queryCluster() ;
      queryHostStatus() ;

      if( defaultShow == 'host' )
      {
         $scope.SwitchPage( defaultShow ) ;         
      }
   } ) ;

   //服务控制器
   sacApp.controllerProvider.register( 'Deploy.Index.Module.Ctrl', function( $scope, $rootScope, $location, SdbFunction, SdbRest, SdbSignal, SdbSwap ){

      $scope.UninstallTips = '' ;
      //服务扩容 弹窗
      $scope.ExtendWindow = {
         'config': {},
         'callback': {}
      } ;

      //打开 服务扩容 弹窗
      var showExtendWindow = function(){
         if( $scope.ClusterList.length > 0 && SdbSwap.distributionNum != 0 )
         {
            $scope.ExtendWindow['config'] = {
               inputList: [
                  {
                     "name": 'moduleName',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "select",
                     "value": null,
                     "valid": []
                  }
               ]
            } ;
            var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( clusterName == moduleInfo['ClusterName'] && moduleInfo['BusinessType'] == 'sequoiadb' && moduleInfo['DeployMod'] == 'distribution' )
               {
                  if( $scope.ExtendWindow['config']['inputList'][0]['value'] == null )
                  {
                     $scope.ExtendWindow['config']['inputList'][0]['value'] = index ;
                  }
                  $scope.ExtendWindow['config']['inputList'][0]['valid'].push( { 'key': moduleInfo['BusinessName'], 'value': index } )
               }
            } ) ;
            $scope.ExtendWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.ExtendWindow['config'].check() ;
               if( isAllClear )
               {
                  var formVal = $scope.ExtendWindow['config'].getValue() ;
                  $rootScope.tempData( 'Deploy', 'Model',       'Module' ) ;
                  $rootScope.tempData( 'Deploy', 'Module',      'sequoiadb' ) ;
                  $rootScope.tempData( 'Deploy', 'ModuleName',  $scope.ModuleList[ formVal['moduleName'] ]['BusinessName'] ) ;
                  $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
                  $rootScope.tempData( 'Deploy', 'DeployMod',   $scope.ModuleList[ formVal['moduleName'] ]['DeployMod'] ) ;
                  SdbFunction.LocalData( 'SdbClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
                  SdbFunction.LocalData( 'SdbModuleName',  $scope.ModuleList[ formVal['moduleName'] ]['BusinessName'] ) ;
                  $location.path( '/Deploy/SDB-ExtendConf' ).search( { 'r': new Date().getTime() } ) ;
               }
               return isAllClear ;
            } ) ;
            $scope.ExtendWindow['callback']['SetTitle']( $scope.autoLanguage( '服务扩容' ) ) ;
            $scope.ExtendWindow['callback']['SetIcon']( '' ) ;
            $scope.ExtendWindow['callback']['Open']() ;
         }
      }

      //服务减容 弹窗
      $scope.ShrinkWindow = {
         'config': {},
         'callback': {}
      }

      //打开 服务减容 弹窗
      var showShrinkWindow = function(){
         if( $scope.ClusterList.length > 0 && SdbSwap.distributionNum != 0 )
         {
            $scope.ShrinkWindow['config'] = {
               inputList: [
                  {
                     "name": 'moduleName',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "select",
                     "value": null,
                     "valid": []
                  }
               ]
            } ;
            var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( clusterName == moduleInfo['ClusterName'] && moduleInfo['BusinessType'] == 'sequoiadb' && moduleInfo['DeployMod'] == 'distribution' )
               {
                  if( $scope.ShrinkWindow['config']['inputList'][0]['value'] == null )
                  {
                     $scope.ShrinkWindow['config']['inputList'][0]['value'] = index ;
                  }
                  $scope.ShrinkWindow['config']['inputList'][0]['valid'].push( { 'key': moduleInfo['BusinessName'], 'value': index } )
               }
            } ) ;
            $scope.ShrinkWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.ShrinkWindow['config'].check() ;
               if( isAllClear )
               {
                  var formVal = $scope.ShrinkWindow['config'].getValue() ;
                  $rootScope.tempData( 'Deploy', 'Model',       'Module' ) ;
                  $rootScope.tempData( 'Deploy', 'Module',      'sequoiadb' ) ;
                  $rootScope.tempData( 'Deploy', 'ModuleName',  $scope.ModuleList[ formVal['moduleName'] ]['BusinessName'] ) ;
                  $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
                  $rootScope.tempData( 'Deploy', 'Shrink', true ) ;
                  SdbFunction.LocalData( 'SdbModuleName',  $scope.ModuleList[ formVal['moduleName'] ]['BusinessName'] ) ;
                  $location.path( '/Deploy/SDB-ShrinkConf' ).search( { 'r': new Date().getTime() } ) ;
               }
               return isAllClear ;
            } ) ;
            $scope.ShrinkWindow['callback']['SetTitle']( $scope.autoLanguage( '服务减容' ) ) ;
            $scope.ShrinkWindow['callback']['SetIcon']( '' ) ;
            $scope.ShrinkWindow['callback']['Open']() ;
         }
      }

      //同步服务 弹窗
      $scope.SyncWindow = {
         'config': {},
         'callback': {}
      }

      //打开 同步服务 弹窗
      var showSyncWindow = function(){
         if( SdbSwap.sdbModuleNum != 0 )
         {
            $scope.SyncWindow['config'] = {
               inputList: [
                  {
                     "name": 'moduleName',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "select",
                     "value": null,
                     "valid": []
                  }
               ]
            } ;
            var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( clusterName == moduleInfo['ClusterName'] && moduleInfo['BusinessType'] == 'sequoiadb')
               {
                  if( $scope.SyncWindow['config']['inputList'][0]['value'] == null )
                  {
                     $scope.SyncWindow['config']['inputList'][0]['value'] = index ;
                  }
                  $scope.SyncWindow['config']['inputList'][0]['valid'].push( { 'key': moduleInfo['BusinessName'], 'value': index } )
               }
            } ) ;
            $scope.SyncWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.SyncWindow['config'].check() ;
               if( isAllClear )
               {
                  var formVal = $scope.SyncWindow['config'].getValue() ;
                  $rootScope.tempData( 'Deploy', 'ModuleName',  $scope.ModuleList[ formVal['moduleName'] ]['BusinessName'] ) ;
                  $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
                  $rootScope.tempData( 'Deploy', 'InstallPath', $scope.ClusterList[ $scope.CurrentCluster ]['InstallPath'] ) ;
                  $location.path( '/Deploy/SDB-Sync' ).search( { 'r': new Date().getTime() } ) ;
               }
               return isAllClear ;
            } ) ;
            $scope.SyncWindow['callback']['SetTitle']( $scope.autoLanguage( '同步服务' ) ) ;
            $scope.SyncWindow['callback']['SetIcon']( '' ) ;
            $scope.SyncWindow['callback']['Open']() ;
         }
      }

      //重启服务 弹窗
      $scope.RestartWindow = {
         'config': {
            'inputList': [
               {
                  "name": 'moduleName',
                  "webName": $scope.autoLanguage( '服务名' ),
                  "type": "select",
                  "value": null,
                  "valid": []
               }
            ]
         },
         'callback': {}
      }

      function restartModule( clusterName, moduleName )
      {
         var data = { 'cmd': 'restart business', 'ClusterName': clusterName, 'BusinessName': moduleName } ;
         SdbRest.OmOperation( data, {
            'success': function( taskInfo ){
               $rootScope.tempData( 'Deploy', 'Model', 'Task' ) ;
               $rootScope.tempData( 'Deploy', 'Module', 'None' ) ;
               $rootScope.tempData( 'Deploy', 'ModuleTaskID', taskInfo[0]['TaskID'] ) ;
               $location.path( '/Deploy/Task/Restart' ).search( { 'r': new Date().getTime() } ) ;
            }, 
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  restartModule( clusterName, moduleName ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //打开 重启服务 弹窗
      var showRestartWindow = function(){
         if( $scope.ModuleNum > 0 )
         {
            $scope.RestartWindow['config']['inputList'][0]['value'] = null ;
            $scope.RestartWindow['config']['inputList'][0]['valid'] = [] ;

            var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( clusterName == moduleInfo['ClusterName'] &&
                   ( moduleInfo['BusinessType'] == 'sequoiadb' ||
                     moduleInfo['BusinessType'] == 'sequoiasql-postgresql' ||
                     moduleInfo['BusinessType'] == 'sequoiasql-mysql' ) )
               {
                  if( $scope.RestartWindow['config']['inputList'][0]['value'] == null )
                  {
                     $scope.RestartWindow['config']['inputList'][0]['value'] = index ;
                  }
                  $scope.RestartWindow['config']['inputList'][0]['valid'].push( { 'key': moduleInfo['BusinessName'], 'value': index } )
               }
            } ) ;

            $scope.RestartWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.RestartWindow['config'].check() ;
               if( isAllClear )
               {
                  var formVal = $scope.RestartWindow['config'].getValue() ;
                  $scope.Components.Confirm.type = 2 ;
                  $scope.Components.Confirm.context = $scope.autoLanguage( '该操作将重启该服务所有节点，是否确定继续？' ) ;
                  $scope.Components.Confirm.isShow = true ;
                  $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
                  $scope.Components.Confirm.ok = function(){
                     restartModule( $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'], $scope.ModuleList[ formVal['moduleName'] ]['BusinessName'] ) ;
                     $scope.Components.Confirm.isShow = false ;
                  }
               }
               return isAllClear ;
            } ) ;
            $scope.RestartWindow['callback']['SetTitle']( $scope.autoLanguage( '重启服务' ) ) ;
            $scope.RestartWindow['callback']['SetIcon']( '' ) ;
            $scope.RestartWindow['callback']['Open']() ;
         }
      }

      //添加服务 弹窗
      $scope.InstallModule = {
         'config': {},
         'callback': {}
      } ;

      //打开 添加服务 弹窗
      $scope.ShowInstallModule = function(){
         if( $scope.ClusterList.length > 0 )
         {
            if( $scope.HostNum == 0 )
            {
               $scope.Components.Confirm.type = 3 ;
               $scope.Components.Confirm.context = $scope.autoLanguage( '集群还没有安装主机。' ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.okText = $scope.autoLanguage( '安装主机' ) ;
               $scope.Components.Confirm.ok = function(){
                  SdbSignal.commit( 'addHost' ) ;
               }
               return ;
            }
            $scope.InstallModule['config'] = {
               inputList: [
                  {
                     "name": 'moduleName',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "string",
                     "required": true,
                     "value": "",
                     "valid": {
                        "min": 1,
                        "max": 127,
                        'regex': '^[0-9a-zA-Z_-]+$'
                     }
                  },
                  {
                     "name": 'moduleType',
                     "webName": $scope.autoLanguage( '服务类型' ),
                     "type": "select",
                     "value": 0,
                     "valid": []
                  }
               ]
            } ;
            var num = 1 ;
            var defaultName = '' ;
            while( true )
            {
               var isFind = false ;
               defaultName = sprintf( 'myService?', num ) ;
               $.each( $scope.ModuleList, function( index, moduleInfo ){
                  if( defaultName == moduleInfo['BusinessName'] )
                  {
                     isFind = true ;
                     return false ;
                  }
               } ) ;
               if( isFind == false )
               {
                  $.each( $rootScope.OmTaskList, function( index, taskInfo ){
                     if( taskInfo['Status'] != 4 && defaultName == taskInfo['Info']['BusinessName'] )
                     {
                        isFind = true ;
                        return false ;
                     }
                  } ) ;
                  if( isFind == false )
                  {
                     break ;
                  }
               }
               ++num ;
            }
            $scope.InstallModule['config']['inputList'][0]['value'] = defaultName ;
            $.each( SdbSwap.moduleType, function( index, typeInfo ){
               $scope.InstallModule['config']['inputList'][1]['valid'].push( { 'key': typeInfo['BusinessDesc'], 'value': index } ) ;
            } ) ;
            $scope.InstallModule['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.InstallModule['config'].check( function( formVal ){
                  var isFind = false ;
                  $.each( $scope.ModuleList, function( index, moduleInfo ){
                     if( formVal['moduleName'] == moduleInfo['BusinessName'] )
                     {
                        isFind = true ;
                        return false ;
                     }
                  } ) ;
                  if( isFind == false )
                  {
                     $.each( $rootScope.OmTaskList, function( index, taskInfo ){
                        if( taskInfo['Status'] != 4 && formVal['moduleName'] == taskInfo['Info']['BusinessName'] )
                        {
                           isFind = true ;
                           return false ;
                        }
                     } ) ;
                  }
                  if( isFind == true )
                  {
                     return [ { 'name': 'moduleName', 'error': $scope.autoLanguage( '服务名已经存在' ) } ]
                  }
                  else
                  {
                     return [] ;
                  }
               } ) ;
               if( isAllClear )
               {
                  var formVal = $scope.InstallModule['config'].getValue() ;
                  $rootScope.tempData( 'Deploy', 'Model', 'Module' ) ;
                  $rootScope.tempData( 'Deploy', 'Module', SdbSwap.moduleType[ formVal['moduleType'] ]['BusinessType'] ) ;
                  $rootScope.tempData( 'Deploy', 'ModuleName', formVal['moduleName'] ) ;
                  $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
                  if( SdbSwap.moduleType[ formVal['moduleType'] ]['BusinessType'] == 'sequoiadb' )
                  {
                     $location.path( '/Deploy/SDB-Conf' ).search( { 'r': new Date().getTime() } ) ;
                  }
                  //当服务类型是postgresql时
                  else if( SdbSwap.moduleType[ formVal['moduleType'] ]['BusinessType'] == 'sequoiasql-postgresql' )
                  {
                     var checkSqlHost = 0 ;
                     $.each( SdbSwap.hostList, function( index, hostInfo ){
                        if( hostInfo['ClusterName'] == $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] )
                        {
                           $.each( hostInfo['Packages'], function( packIndex, packInfo ){
                              if( packInfo['Name'] == 'sequoiasql-postgresql' )
                              {
                                 ++checkSqlHost ;
                              }
                           } ) ;
                        }
                     } ) ;
                     if( checkSqlHost == 0 )
                     {
                        $scope.Components.Confirm.type = 3 ;
                        $scope.Components.Confirm.context = $scope.autoLanguage( '创建 SequoiaSQL-PostgreSQL 服务需要主机已经部署 SequoiaSQL-PostgreSQL 包。' ) ;
                        $scope.Components.Confirm.isShow = true ;
                        $scope.Components.Confirm.noClose = true ;
                     }
                     else
                     {
                        var businessConf = {} ;
                        businessConf['ClusterName'] = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
                        businessConf['BusinessName'] = formVal['moduleName'] ;
                        businessConf['BusinessType'] = SdbSwap.moduleType[ formVal['moduleType'] ]['BusinessType'] ;
                        businessConf['DeployMod'] = '' ;
                        businessConf['Property'] = [] ;
                        $rootScope.tempData( 'Deploy', 'ModuleConfig', businessConf ) ;
                        $location.path( '/Deploy/PostgreSQL-Mod' ).search( { 'r': new Date().getTime() } ) ;
                     }
                  }
                  else if( SdbSwap.moduleType[ formVal['moduleType'] ]['BusinessType'] == 'sequoiasql-mysql' )
                  {
                     var checkSqlHost = 0 ;
                     $.each( SdbSwap.hostList, function( index, hostInfo ){
                        if( hostInfo['ClusterName'] == $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] )
                        {
                           $.each( hostInfo['Packages'], function( packIndex, packInfo ){
                              if( packInfo['Name'] == 'sequoiasql-mysql' )
                              {
                                 ++checkSqlHost ;
                              }
                           } ) ;
                        }
                     } ) ;
                     if( checkSqlHost == 0 )
                     {
                        $scope.Components.Confirm.type = 3 ;
                        $scope.Components.Confirm.context = $scope.autoLanguage( '创建 SequoiaSQL-MySQL 服务需要主机已经部署 SequoiaSQL-MySQL 包。' ) ;
                        $scope.Components.Confirm.isShow = true ;
                        $scope.Components.Confirm.noClose = true ;
                     }
                     else
                     {
                        var businessConf = {} ;
                        businessConf['ClusterName'] = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
                        businessConf['BusinessName'] = formVal['moduleName'] ;
                        businessConf['BusinessType'] = SdbSwap.moduleType[ formVal['moduleType'] ]['BusinessType'] ;
                        businessConf['DeployMod'] = '' ;
                        businessConf['Property'] = [] ;
                        $rootScope.tempData( 'Deploy', 'ModuleConfig', businessConf ) ;
                        $location.path( '/Deploy/MySQL-Mod' ).search( { 'r': new Date().getTime() } ) ;
                     }
                  }
               }
               return isAllClear ;
            } ) ;
            $scope.InstallModule['callback']['SetTitle']( $scope.autoLanguage( '创建服务' ) ) ;
            $scope.InstallModule['callback']['SetIcon']( '' ) ;
            $scope.InstallModule['callback']['Open']() ;
         }
      }

      //发现服务 弹窗
      $scope.AppendModule = {
         'config': {},
         'callback': {}
      } ;

      //打开 发现服务 弹窗
      $scope.ShowAppendModule = function(){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.AppendModule['config'] = {
               inputList: [
                  {
                     "name": 'moduleName',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "string",
                     "required": true,
                     "value": "",
                     "valid": {
                        "min": 1,
                        "max": 127,
                        'regex': '^[0-9a-zA-Z_-]+$'
                     }
                  },
                  {
                     "name": 'moduleType',
                     "webName": $scope.autoLanguage( '服务类型' ),
                     "type": "select",
                     "value": 'sequoiadb',
                     "valid": [
                        { 'key': 'SequoiaDB', 'value': 'sequoiadb' },
                        { 'key': 'Spark', 'value': 'spark' },
                        { 'key': 'Hdfs', 'value': 'hdfs' },
                        { 'key': 'Yarn', 'value': 'yarn' }
                     ]
                  }
               ]
            } ;
            var num = 1 ;
            var defaultName = '' ;
            while( true )
            {
               var isFind = false ;
               defaultName = sprintf( 'myService?', num ) ;
               $.each( $scope.ModuleList, function( index, moduleInfo ){
                  if( defaultName == moduleInfo['BusinessName'] )
                  {
                     isFind = true ;
                     return false ;
                  }
               } ) ;
               if( isFind == false )
               {
                  $.each( $rootScope.OmTaskList, function( index, taskInfo ){
                     if( taskInfo['Status'] != 4 && defaultName == taskInfo['Info']['BusinessName'] )
                     {
                        isFind = true ;
                        return false ;
                     }
                  } ) ;
                  if( isFind == false )
                  {
                     break ;
                  }
               }
               ++num ;
            }
            $scope.AppendModule['config']['inputList'][0]['value'] = defaultName ;
            $scope.AppendModule['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.AppendModule['config'].check( function( formVal ){
                  var isFind = false ;
                  $.each( $scope.ModuleList, function( index, moduleInfo ){
                     if( formVal['moduleName'] == moduleInfo['BusinessName'] )
                     {
                        isFind = true ;
                        return false ;
                     }
                  } ) ;
                  if( isFind == false )
                  {
                     $.each( $rootScope.OmTaskList, function( index, taskInfo ){
                        if( taskInfo['Status'] != 4 && formVal['moduleName'] == taskInfo['Info']['BusinessName'] )
                        {
                           isFind = true ;
                           return false ;
                        }
                     } ) ;
                  }
                  if( isFind == true )
                  {
                     return [ { 'name': 'moduleName', 'error': $scope.autoLanguage( '服务名已经存在' ) } ]
                  }
                  else
                  {
                     return [] ;
                  }
               } ) ;
               if( isAllClear )
               {
                  $scope.AppendModule['callback']['Close']() ;
                  var formVal = $scope.AppendModule['config'].getValue() ;
                  if( formVal['moduleType'] == 'sequoiadb' )
                  {
                     setTimeout( function(){
                        showAppendSdb( formVal['moduleName'] ) ;
                        $scope.$apply() ;
                     } ) ;
                  }
                  else
                  {
                     setTimeout( function(){
                        showAppendOtherModule( formVal['moduleName'], formVal['moduleType'] ) ;
                        $scope.$apply() ;
                     } ) ;
                  }
               }
               else
               {
                  return false ;
               }
            } ) ;
            $scope.AppendModule['callback']['SetTitle']( $scope.autoLanguage( '发现服务' ) ) ;
            $scope.AppendModule['callback']['SetIcon']( '' ) ;
            $scope.AppendModule['callback']['Open']() ;
         }
      }

      //从发现前往添加主机
      var gotoAddHost = function( configure ){
         $rootScope.tempData( 'Deploy', 'Model', 'Deploy' ) ;
         $rootScope.tempData( 'Deploy', 'Module', 'None' ) ;
         $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
         $rootScope.tempData( 'Deploy', 'InstallPath', $scope.ClusterList[ $scope.CurrentCluster ]['InstallPath'] ) ;
         $rootScope.tempData( 'Deploy', 'DiscoverConf', configure ) ;
         $location.path( '/Deploy/ScanHost' ).search( { 'r': new Date().getTime() } ) ;
      }

      //发现sdb 弹窗
      $scope.AppendSdb = {
         'config': {},
         'callback': {}
      }

      //打开 发现sdb 弹窗
      var showAppendSdb = function( moduleName ){
         $scope.AppendSdb['config'] = {
            inputList: [
               {
                  "name": 'HostName',
                  "webName": $scope.autoLanguage( '地址' ),
                  "type": "string",
                  "required": true,
                  "desc": $scope.autoLanguage( 'coord节点或standalone所在的主机名或者IP地址' ),
                  "value": "",
                  "valid": {
                     "min": 1
                  }
               },
               {
                  "name": 'ServiceName',
                  "webName": $scope.autoLanguage( '服务名' ),
                  "type": "port",
                  "required": true,
                  "desc": $scope.autoLanguage( 'coord节点或standalone端口号' ),
                  "value": '',
                  "valid": {}
               },
               {
                  "name": 'User',
                  "webName": $scope.autoLanguage( '数据库用户名' ),
                  "type": "string",
                  "value": ""
               },
               {
                  "name": 'Passwd',
                  "webName": $scope.autoLanguage( '数据库密码' ),
                  "type": "password",
                  "value": ""
               },
               {
                  "name": 'AgentService',
                  "webName": $scope.autoLanguage( '代理端口' ),
                  "type": "port",
                  "value": '11790',
                  "valid": {}
               }
            ]
         }
         $scope.AppendSdb['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = $scope.AppendSdb['config'].check() ;
            if( isAllClear )
            {
               var formVal = $scope.AppendSdb['config'].getValue() ;
               var configure = {} ;
               configure['ClusterName']  = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
               configure['BusinessType'] = 'sequoiadb' ;
               configure['BusinessName'] = moduleName ;
               configure['BusinessInfo'] = formVal ;
               discoverModule( configure ) ;
            }
            return isAllClear ;
         } ) ;
         $scope.AppendSdb['callback']['SetTitle']( 'SequoiaDB' ) ;
         $scope.AppendSdb['callback']['SetIcon']( '' ) ;
         $scope.AppendSdb['callback']['Open']() ;
      }

      //发现其他服务 弹窗
      $scope.AppendOtherModule = {
         'config': {},
         'callback': {}
      } ;

      //打开 发现其他服务 弹窗
      var showAppendOtherModule = function( moduleName, moduleType ){
         $scope.AppendOtherModule['config'] = {
            inputList: [
               {
                  "name": 'HostName',
                  "webName": $scope.autoLanguage( '主机名' ),
                  "type": "string",
                  "required": true,
                  "value": "",
                  "valid": {
                     "min": 1
                  }
               },
               {
                  "name": 'WebServicePort',
                  "webName": $scope.autoLanguage( '服务名' ),
                  "type": "port",
                  "required": true,
                  "value": '',
                  "valid": {}
               }
            ]
         } ;
         $scope.AppendOtherModule['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = $scope.AppendOtherModule['config'].check() ;
            if( isAllClear )
            {
               var formVal = $scope.AppendOtherModule['config'].getValue() ;
               var configure = {} ;
               configure['ClusterName']  = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
               configure['BusinessType'] = moduleType ;
               configure['BusinessName'] = moduleName ;
               configure['BusinessInfo'] = formVal ;
               discoverModule( configure ) ;
            }
            return isAllClear ;
         } ) ;
         $scope.AppendOtherModule['callback']['SetTitle']( moduleType ) ;
         $scope.AppendOtherModule['callback']['SetIcon']( '' ) ;
         $scope.AppendOtherModule['callback']['Open']() ;
      }

      //发现服务
      var discoverModule = function( configure ){
         var data = { 'cmd': 'discover business', 'ConfigInfo': JSON.stringify( configure ) } ;
         SdbRest.OmOperation( data, {
            'success': function(){
               if( configure['BusinessType'] == 'sequoiadb' )
               {
                  $rootScope.tempData( 'Deploy', 'ModuleName', configure['BusinessName'] ) ;
                  $rootScope.tempData( 'Deploy', 'ClusterName', configure['ClusterName'] ) ;
                  $location.path( '/Deploy/SDB-Discover' ).search( { 'r': new Date().getTime() }  ) ;
               }
               else
               {
                  $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() }  ) ;
               }
            }, 
            'failed': function( errorInfo ){
               if( configure['BusinessType'] == 'sequoiadb' &&
                   isArray( errorInfo['hosts'] ) &&
                   errorInfo['hosts'].length > 0 )
               {
                  $scope.Components.Confirm.type = 3 ;
                  $scope.Components.Confirm.context = $scope.autoLanguage( '发现SequoiaDB需要先在集群中添加该服务的所有主机。是否前往添加主机？' ) ;
                  $scope.Components.Confirm.isShow = true ;
                  $scope.Components.Confirm.okText = $scope.autoLanguage( '是' ) ;
                  $scope.Components.Confirm.ok = function(){
                     $rootScope.tempData( 'Deploy', 'DiscoverHostList', errorInfo['hosts'] ) ;
                     gotoAddHost( configure ) ;
                  }
               }
               else
               {
                  _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                     discoverModule( configure ) ;
                     return true ;
                  } ) ;
               }
               
            }
         } ) ;
      }

      //卸载服务
      var uninstallModule = function( index, isForce ){
         if( typeof( $scope.ModuleList[index]['AddtionType'] ) == 'undefined' || $scope.ModuleList[index]['AddtionType'] != 1 )
         {
            var data = { 'cmd': 'remove business', 'BusinessName': $scope.ModuleList[index]['BusinessName'], 'force': isForce } ;
            SdbRest.OmOperation( data, {
               'success': function( taskInfo ){
                  $rootScope.tempData( 'Deploy', 'Model', 'Task' ) ;
                  $rootScope.tempData( 'Deploy', 'Module', 'None' ) ;
                  $rootScope.tempData( 'Deploy', 'ModuleTaskID', taskInfo[0]['TaskID'] ) ;
                  $location.path( '/Deploy/Task/Module' ).search( { 'r': new Date().getTime() } ) ;
               },
               'failed': function( errorInfo ){
                  _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                     uninstallModule( index ) ;
                     return true ;
                  } ) ;
               }
            } ) ;
         }
         else
         {
            var data = { 'cmd': 'undiscover business', 'ClusterName': $scope.ModuleList[index]['ClusterName'], 'BusinessName': $scope.ModuleList[index]['BusinessName'] } ;
            SdbRest.OmOperation( data, {
               'success': function(){
                  $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() }  ) ;
               },
               'failed': function( errorInfo ){
                  _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                     uninstallModule( index ) ;
                     return true ;
                  } ) ;
               }
            } ) ;
         }
      }

      //卸载服务 弹窗
      $scope.UninstallModuleWindow = {
         'config': {},
         'callback': {}
      } ;

      //打开 卸载服务 弹窗
      var showUninstallModule = function(){
         if( $scope.ClusterList.length > 0 )
         {
            if( $scope.ModuleNum == 0 )
            {
               $scope.Components.Confirm.type = 3 ;
               $scope.Components.Confirm.context = $scope.autoLanguage( '已经没有服务了。' ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
               return ;
            }
            $scope.UninstallTips = '' ;
            var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
            $scope.UninstallModuleWindow['config'] = {
               'inputList': [
                  {
                     "name": 'moduleIndex',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "select",
                     "value": null,
                     "valid": []
                  },
                  {
                     "name": 'force',
                     "webName": $scope.autoLanguage( '删除所有数据' ),
                     "type": "select",
                     "value": false,
                     "valid": [
                        { 'key': $scope.autoLanguage( '否' ),  'value': false },
                        { 'key': $scope.autoLanguage( '是' ),  'value': true  }
                     ],
                     "onChange": function( name, key, value ){
                        if( value == true )
                        {
                           $scope.UninstallTips = $scope.autoLanguage( '提示：选择删除数据将清空该服务所有数据！' ) ;
                        }
                        else
                        {
                           $scope.UninstallTips = '' ;
                        }
                     }
                  }
               ]
            }
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( clusterName == moduleInfo['ClusterName'] )
               {
                  if( $scope.UninstallModuleWindow['config']['inputList'][0]['value'] == null )
                  {
                     $scope.UninstallModuleWindow['config']['inputList'][0]['value'] = index ;
                  }
                  $scope.UninstallModuleWindow['config']['inputList'][0]['valid'].push( { 'key': moduleInfo['BusinessName'], 'value': index } )
               }
            } ) ;
            $scope.UninstallModuleWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.UninstallModuleWindow['config'].check() ;
               if( isAllClear )
               {
                  var formVal = $scope.UninstallModuleWindow['config'].getValue() ;
                  if( formVal['force'] == true )
                  {
                     $scope.Components.Confirm.type = 2 ;
                     $scope.Components.Confirm.context = $scope.autoLanguage( '该操作将删除该服务所有数据，是否确定继续？' ) ;
                     $scope.Components.Confirm.isShow = true ;
                     $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
                     $scope.Components.Confirm.ok = function(){
                        uninstallModule( formVal['moduleIndex'], formVal['force'] ) ;
                        $scope.Components.Confirm.isShow = false ;
                     }
                  }
                  else
                  {
                     uninstallModule( formVal['moduleIndex'], formVal['force'] ) ;
                  }
               }
               return isAllClear ;
            } ) ;
            $scope.UninstallModuleWindow['callback']['SetTitle']( $scope.autoLanguage( '卸载服务' ) ) ;
            $scope.UninstallModuleWindow['callback']['Open']() ;
         }
      }


      //解绑服务
      var unbindModule = function( clusterName, businessName ){
         var data = {
            'cmd': 'unbind business', 'ClusterName': clusterName, 'BusinessName': businessName
         } ;
         SdbRest.OmOperation( data, {
            'success': function(){
               $scope.Components.Confirm.type = 4 ;
               $scope.Components.Confirm.context = sprintf( $scope.autoLanguage( '服务：? 解绑成功。' ), businessName ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
               $scope.Components.Confirm.normalOK = true ;
               $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
               $scope.Components.Confirm.ok = function(){
                  $scope.Components.Confirm.isShow = false ;
                  $scope.Components.Confirm.noClose = false ;
                  $scope.Components.Confirm.normalOK = false ;
                  $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() }  ) ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  unbindModule( clusterName, businessName ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //解绑服务 弹窗
      $scope.UnbindModuleWindow = {
         'config': {},
         'callback': {}
      }

      //打开 解绑服务 弹窗
      var showUnbindModule = function(){
         if( $scope.ClusterList.length > 0 && $scope.ModuleNum != 0 )
         {
            var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
            $scope.UnbindModuleWindow['config'] = {
               'inputList': [
                  {
                     "name": 'moduleIndex',
                     "webName": $scope.autoLanguage( '服务名' ),
                     "type": "select",
                     "value": null,
                     "valid": []
                  }
               ]
            }
            $.each( $scope.ModuleList, function( index, moduleInfo ){
               if( clusterName == moduleInfo['ClusterName'] )
               {
                  if( $scope.UnbindModuleWindow['config']['inputList'][0]['value'] == null )
                  {
                     $scope.UnbindModuleWindow['config']['inputList'][0]['value'] = index ;
                  }
                  $scope.UnbindModuleWindow['config']['inputList'][0]['valid'].push( { 'key': moduleInfo['BusinessName'], 'value': index } )
               }
            } ) ;
            $scope.UnbindModuleWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
               var isAllClear = $scope.UnbindModuleWindow['config'].check() ;
               if( isAllClear )
               {
                  var formVal = $scope.UnbindModuleWindow['config'].getValue() ;
                  var businessName = $scope.ModuleList[ formVal['moduleIndex'] ]['BusinessName'] ;
                  var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
                  var businessType = $scope.ModuleList[ formVal['moduleIndex'] ]['BusinessType'] ;
                  if( businessType == 'sequoiasql-mysql' )
                  {
                     $scope.Components.Confirm.type = 1 ;
                     $scope.Components.Confirm.context = $scope.autoLanguage( '解绑服务将重启MySQL服务，是否继续？' ) ;
                     $scope.Components.Confirm.isShow = true ;
                     $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
                     $scope.Components.Confirm.ok = function(){
                        $scope.Components.Confirm.isShow = false ;
                        unbindModule( clusterName, businessName ) ;
                     }
                  }
                  else
                  {
                     unbindModule( clusterName, businessName ) ;
                  }
               }
               return isAllClear ;
            } ) ;
            $scope.UnbindModuleWindow['callback']['SetTitle']( $scope.autoLanguage( '解绑服务' ) ) ;
            $scope.UnbindModuleWindow['callback']['Open']() ;
         }
      }

      //设置鉴权 弹窗
      $scope.SetAuthority = {
         'config': {},
         'callback': {}
      } ;

      //表单
      var authorityform = {
         'inputList': [
            {
               "name": "BusinessName",
               "webName": $scope.autoLanguage( '服务名' ),
               "type": "string",
               "disabled": true,
               "value": ''
            },
            {
               "name": "User",
               "webName": $scope.autoLanguage( '用户名' ),
               "type": "string",
               "required": true,
               "value": "",
               "valid": {
                  "min": 1,
                  "max": 127,
                  "regex": '^[0-9a-zA-Z]+$'
               }
            },
            {
               "name": "Password",
               "webName": $scope.autoLanguage( '密码' ),
               "type": "password",
               "value": ""
            }
         ]
      } ;

      //保存当前选中的服务名
      var chooseBusinessName = '' ;
      $scope.SaveBsName = function( businessName )
      {
         chooseBusinessName = businessName ;
      }

      //设置鉴权
      var setAuthority = function( data, businessName, index ){
         SdbRest.OmOperation( data, {
            'success': function(){
               SdbSwap.queryAuth( businessName, index ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  setAuthority( data, businessName, index ) ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': true
         } ) ;
      }

      //打开 设置鉴权 弹窗
      $scope.ShowSetAuthority = function( businessName, index ){
         if( typeof( businessName ) == 'undefined' )
         {
            businessName = chooseBusinessName ;
            index = chooseBusinessIndex ;
         }
         
         var user = '' ;
         if( typeof( $scope.ModuleList[index]['authority'][0]['User'] ) != 'undefined' )
         {
            user = $scope.ModuleList[index]['authority'][0]['User'] ;
         }

         authorityform['inputList'][1]['value'] = '' ;
         authorityform['inputList'][2]['value'] = '' ;

         //关闭鉴权下拉菜单
         $scope.AuthorityDropdown['callback']['Close']() ;

         var form = authorityform ;
         form['inputList'][0]['value'] = businessName ;
         form['inputList'][1]['value'] = user ;
         $scope.SetAuthority['config'] = form ;
         $scope.SetAuthority['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = form.check() ;
               if( isAllClear )
               {
                  var formVal = form.getValue() ;
                  var data = {
                     'cmd': 'set business authority',
                     'BusinessName': businessName,
                     'User': formVal['User'],
                     'Passwd': formVal['Password']
                  } ;
                  setAuthority( data, businessName, index ) ;
                  $scope.SetAuthority['callback']['Close']() ;
               }
         } ) ;
         $scope.SetAuthority['callback']['SetTitle']( $scope.autoLanguage( '设置鉴权' ) ) ;
         $scope.SetAuthority['callback']['SetIcon']( '' ) ;
         $scope.SetAuthority['callback']['Open']() ;

      }

      //删除鉴权
      $scope.DropAuthorityModel = function( businessName, index ){
         if( typeof( businessName ) == 'undefined' )
         {
            businessName = chooseBusinessName ;
            index = chooseBusinessIndex ;
         }
         //关闭下拉菜单
         $scope.AuthorityDropdown['callback']['Close']() ;
         $scope.Components.Confirm.isShow = true ;
         $scope.Components.Confirm.type = 1 ;
         $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
         $scope.Components.Confirm.closeText = $scope.autoLanguage( '取消' ) ;
         $scope.Components.Confirm.title = $scope.autoLanguage( '要删除该服务的鉴权吗？' ) ;
         $scope.Components.Confirm.context = $scope.autoLanguage( '服务名' ) + ': ' + businessName ;
         $scope.Components.Confirm.ok = function(){
            var data = {
               'cmd': 'remove business authority',
               'BusinessName': businessName
            }
            SdbRest.OmOperation( data, {
               'success': function(){
                  SdbSwap.queryAuth( businessName, index ) ;
               },
               'failed': function( errorInfo, retryRun ){
                  _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                     retryRun() ;
                     return true ;
                  } ) ;
               }
            }, {
               'showLoading': true
            } ) ;
            return true ;
         }
      }

      //鉴权下拉菜单
      $scope.AuthorityDropdown = {
         'config': [
            { 'field': $scope.autoLanguage( '修改鉴权' ), 'value': "edit" },
            { 'field': $scope.autoLanguage( '删除鉴权' ), 'value': "delete" }
         ],
         'callback': {}
      } ;

      //打开鉴权下拉菜单
      $scope.OpenShowAuthorityDropdown = function( event, businessName, index ){
         chooseBusinessName = businessName ;
         chooseBusinessIndex = index ;
         $scope.AuthorityDropdown['callback']['Open']( event.currentTarget ) ;
      }

      $scope.SetDefaultDbWindow = {
         'config': {},
         'callback': {}
      } ;

      $scope.ShowSetDefaultDb = function( businessName, index ){
         var dbName = '' ;
         if( typeof( $scope.ModuleList[index]['authority'][0]['DbName'] ) != 'undefined' )
         {
            dbName = $scope.ModuleList[index]['authority'][0]['DbName'] ;
         }
         var form = {
            'inputList': [
               {
                  "name": "DbName",
                  "webName": $scope.autoLanguage( '默认数据库' ),
                  "type": "string",
                  "required": true,
                  "value": dbName,
                  "valid": {
                     "min": 1
                  } 
               }
            ]
         } ;
         $scope.SetDefaultDbWindow['config'] = form ;
         $scope.SetDefaultDbWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = $scope.SetDefaultDbWindow['config'].check() ;
            if( isAllClear )
            {
               var formVal = form.getValue() ;
               var data = {
                  'cmd': 'set business authority',
                  'BusinessName': businessName,
                  'DbName' : formVal['DbName']
               } ;
               setAuthority( data, businessName, index ) ;
               $scope.SetDefaultDbWindow['callback']['Close']() ;
            }
         } ) ;
         $scope.SetDefaultDbWindow['callback']['SetTitle']( $scope.autoLanguage( '设置默认数据库' ) ) ;
         $scope.SetDefaultDbWindow['callback']['SetIcon']( '' ) ;
         $scope.SetDefaultDbWindow['callback']['Open']() ;
      }

      //判断类型
      $scope.Typeof = function( value ){
         return typeof( value ) ;
      }

      $scope.GotoExpansion = function(){
         $location.path( '/Deploy/SDB-ExtendConf' ).search( { 'r': new Date().getTime() } ) ;
      }

      //添加服务下拉菜单
      $scope.AddModuleDropdown = {
         'config': [
            { 'key': $scope.autoLanguage( '创建服务' ) },
            { 'key': $scope.autoLanguage( '发现服务' ) }
         ],
         'OnClick': function( index ){
            if( index == 0 )
            {
               $scope.ShowInstallModule() ;
            }
            else
            {
               $scope.ShowAppendModule() ;
            }
            $scope.AddModuleDropdown['callback']['Close']() ;
         },
         'callback': {}
      }

      //打开 添加服务下拉菜单
      $scope.OpenAddModuleDropdown = function( event ){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.AddModuleDropdown['callback']['Open']( event.currentTarget ) ;
         }
      }

      //删除服务下拉菜单
      $scope.DeleteModuleDropdown = {
         'config': [
            { 'key': $scope.autoLanguage( '卸载服务' ) },
            { 'key': $scope.autoLanguage( '解绑服务' ) }
         ],
         'OnClick': function( index ){
            if( index == 0 )
            {
               showUninstallModule() ;
            }
            else
            {
               showUnbindModule() ;
            }
            $scope.DeleteModuleDropdown['callback']['Close']() ;
         },
         'callback': {}
      }

      //打开 删除服务下拉菜单
      $scope.OpenDeleteModuleDropdown = function( event ){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.DeleteModuleDropdown['callback']['Open']( event.currentTarget ) ;
         }
      }

      //修改服务下拉菜单
      $scope.EditModuleDropdown = {
         'config': [],
         'OnClick': function( index ){
            if( index == 0 )
            {
               showExtendWindow() ;
            }
            else if( index == 1 )
            {
               showShrinkWindow() ;
            }
            else if( index == 2 )
            {
               showSyncWindow() ;
            }
            else if( index == 3 )
            {
               showRestartWindow() ;
            }
            $scope.EditModuleDropdown['callback']['Close']() ;
         },
         'callback': {}
      }

      //打开 修改服务下拉菜单
      $scope.OpenEditModuleDropdown = function( event ){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.EditModuleDropdown['config'] = [] ;
            var disabled = false ;
            var syncDisabled = false ;
            if( SdbSwap.distributionNum == 0 )
            {
               disabled = true ;
            }
            if( SdbSwap.sdbModuleNum == 0 )
            {
               syncDisabled = true ;
            }
            $scope.EditModuleDropdown['config'].push( { 'key': $scope.autoLanguage( '服务扩容' ), 'disabled': disabled } ) ;
            $scope.EditModuleDropdown['config'].push( { 'key': $scope.autoLanguage( '服务减容' ), 'disabled': disabled } ) ;
            $scope.EditModuleDropdown['config'].push( { 'key': $scope.autoLanguage( '同步服务' ), 'disabled': syncDisabled } ) ;
            $scope.EditModuleDropdown['config'].push( { 'key': $scope.autoLanguage( '重启服务' ), 'disabled': $scope.ModuleNum == 0 } ) ;
            $scope.EditModuleDropdown['callback']['Open']( event.currentTarget ) ;
         }
      }

      //创建关联
      function createRelation( name, from, to, options )
      {
         var data = {
            'cmd'    : 'create relationship',
            'Name'   : name,
            'From'   : from,
            'To'     : to,
            'Options': JSON.stringify( options )
         } ;

         SdbRest.OmOperation( data, {
            'success': function( result ){
               $scope.Components.Confirm.type = 4 ;
               $scope.Components.Confirm.context = sprintf( $scope.autoLanguage( '创建关联成功：?' ), name ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
               $scope.Components.Confirm.normalOK = true ;
               $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
               $scope.Components.Confirm.ok = function(){
                  $scope.Components.Confirm.isShow = false ;
                  $scope.Components.Confirm.noClose = false ;
                  $scope.Components.Confirm.normalOK = false ;
                  SdbSwap.getRelationship() ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  createRelation( name, from, to, options ) ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': true
         } ) ;
      }

      //解除关联
      function removeRelation( name )
      {
         var data = {
            'cmd' : 'remove relationship',
            'Name'  : name
         } ;

         SdbRest.OmOperation( data, {
            'success': function(){
               $scope.Components.Confirm.type = 4 ;
               $scope.Components.Confirm.context = sprintf( $scope.autoLanguage( '解除关联成功：?' ), name ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
               $scope.Components.Confirm.normalOK = true ;
               $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
               $scope.Components.Confirm.ok = function(){
                  $scope.Components.Confirm.isShow = false ;
                  $scope.Components.Confirm.noClose = false ;
                  $scope.Components.Confirm.normalOK = false ;
                  SdbSwap.getRelationship() ;
               }
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  removeRelation( name ) ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': true
         } ) ;
      }

      //关联操作 下拉菜单
      $scope.RelationDropdown = {
         'config': [],
         'OnClick': function( index ){
            if( index == 0 )
            {
               $scope.OpenCreateRelation() ;
            }
            else if( index == 1 )                
            {
               $scope.OpenRemoveRelation() ;
            }
            $scope.RelationDropdown['callback']['Close']() ;
         },
         'callback': {}
      } ;

      //是否禁用创建关联按钮
      var createRelationDisabled = false ;

      //打开 关联操作 下拉菜单
      $scope.OpenRelationDropdown = function( event ){
         var disabled = false ;
         var pgsqlModule = 0 ;
         var mysqlModule = 0 ;
         var sdbModule = 0 ;
         $.each( $scope.ModuleList, function( index, moduleInfo ){
            if( moduleInfo['BusinessType'] == 'sequoiadb' )
            {
               ++sdbModule ;
            }
            else if( moduleInfo['BusinessType'] == 'sequoiasql-postgresql' )
            {
               ++pgsqlModule ;
            }
            else if( moduleInfo['BusinessType'] == 'sequoiasql-mysql' )
            {
               ++mysqlModule ;
            }
         } ) ;
         if( ( pgsqlModule == 0 && mysqlModule == 0 ) || sdbModule == 0 )
         {
            createRelationDisabled = true ;
         }
         else
         {
            createRelationDisabled = false ;
         }
         $scope.RelationDropdown['config'] = [] ;
         $scope.RelationDropdown['config'].push( { 'key': $scope.autoLanguage( '创建关联' ), 'disabled': createRelationDisabled } ) ;

         if( SdbSwap.relationshipList.length == 0 )
         {
            disabled = true ;
         }
         $scope.RelationDropdown['config'].push( { 'key': $scope.autoLanguage( '解除关联' ), 'disabled': disabled } ) ;
         $scope.RelationDropdown['callback']['Open']( event.currentTarget ) ;
      }

      //创建关联 弹窗
      $scope.CreateRelationWindow = {
         'config': {
            'ShowType': 1,
            'inputErrNum1': 0,
            'inputErrNum2': 0,
            'normal': {
               'inputList': [
                  {
                     "name": 'Name',
                     "webName": $scope.autoLanguage( '关联名' ),
                     "type": "string",
                     "required": true,
                     "value": '',
                     "valid": {
                        "min": 1,
                        "max": 63,
                        "regex": "^[a-zA-Z]+[0-9a-zA-Z_-]*$"
                     }
                  },
                  {
                     "name": "Type",
                     "webName": $scope.autoLanguage( '关联类型' ),
                     "required": true,
                     "type": "select",
                     "value": 'sdb-pg',
                     "valid": []
                  },
                  {
                     "name": "From",
                     "webName": $scope.autoLanguage( '关联服务名' ),
                     "required": true,
                     "type": "select",
                     "value": '',
                     "valid": []
                  },
                  {
                     "name": 'DbName',
                     "webName": $scope.autoLanguage( '关联服务的数据库' ),
                     "type": "select",
                     "required": true,
                     "value": "",
                     "valid": []
                  },
                  {
                     "name": "To",
                     "webName": $scope.autoLanguage( '被关联服务名' ),
                     "required": true,
                     "type": "select",
                     "value": '',
                     "valid":[]
                  }
               ]
            },
            'advance': {
               'inputList': [
                  {
                     "name": "preferedinstance",
                     "webName": "preferedinstance",
                     "type": "select",
                     "required": true,
                     "value": 'a',
                     "valid":[
                        { 'key': 's', 'value': 's' },
                        { 'key': 'm', 'value': 'm' },
                        { 'key': 'a', 'value': 'a' },
                        { 'key': '1', 'value': '1' },
                        { 'key': '2', 'value': '2' },
                        { 'key': '3', 'value': '3' },
                        { 'key': '4', 'value': '4' },
                        { 'key': '5', 'value': '5' },
                        { 'key': '6', 'value': '6' },
                        { 'key': '7', 'value': '7' }
                     ]
                  },
                  {
                     "name": "transaction",
                     "webName": "transaction",
                     "type": "select",
                     "required": true,
                     "value": "OFF",
                     "valid":[
                        { 'key': 'ON', 'value': 'ON' },
                        { 'key': 'OFF', 'value': 'OFF' }
                     ]
                  },
                  {
                     "name": "sequoiadb_use_partition",
                     "webName": $scope.autoLanguage( "自动分区" ),
                     "type": "select",
                     "required": true,
                     "value": "ON",
                     "valid":[
                        { 'key': 'ON', 'value': 'ON' },
                        { 'key': 'OFF', 'value': 'OFF' }
                     ]
                  },
                  {
                     "name": "sequoiadb_use_bulk_insert",
                     "webName": $scope.autoLanguage( "批量插入" ),
                     "type": "select",
                     "required": true,
                     "value": "ON",
                     "valid":[
                        { 'key': 'ON', 'value': 'ON' },
                        { 'key': 'OFF', 'value': 'OFF' }
                     ]
                  },
                  {
                     "name": "sequoiadb_bulk_insert_size",
                     "webName": $scope.autoLanguage( "批量插入的最大记录数" ),
                     "type": "int",
                     "required": true,
                     "value": 100,
                     "valid": {
                        'min': 1,
                        'max': 100000
                     }
                  },
                  {
                     "name": "sequoiadb_use_autocommit",
                     "webName": $scope.autoLanguage( "自动提交模式" ),
                     "type": "select",
                     "required": true,
                     "value": "ON",
                     "valid":[
                        { 'key': 'ON', 'value': 'ON' },
                        { 'key': 'OFF', 'value': 'OFF' }
                     ]
                  },
                  {
                     "name": "sequoiadb_debug_log",
                     "webName": $scope.autoLanguage( "打印debug日志" ),
                     "type": "select",
                     "required": true,
                     "value": "OFF",
                     "valid":[
                        { 'key': 'ON', 'value': 'ON' },
                        { 'key': 'OFF', 'value': 'OFF' }
                     ]
                  },
                  {
                     "name": "address",
                     "webName": $scope.autoLanguage( '选择被关联节点' ),
                     "type": "multiple",
                     "required": true,
                     "value": [],
                     "valid": {}
                  }
               ]
            }
         },
         'callback': {}
      } ;

      //获取postgresql数据库列表
      function getPostgresqlDB( clusterName, serviceName, func )
      {
         var sql = 'SELECT datname FROM pg_database WHERE datname NOT LIKE \'template0\' AND datname NOT LIKE \'template1\'' ;
         var data = { 'Sql': sql, 'IsAll': 'true' } ;

         SdbRest.DataOperationV21( clusterName, serviceName, '/sql', data, {
            'success': func,
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getPostgresqlDB( clusterName, serviceName, func ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //设置关联窗口的节点地址
      function setCreateRelationAddress( serviceName, advanceInput )
      {
         var serviceInfo = getArrayItem( $scope.ModuleList, 'BusinessName', serviceName ) ;

         if( serviceInfo && isObject( serviceInfo['BusinessInfo'] ) && isArray( serviceInfo['BusinessInfo']['NodeList'] ) )
         {
            var nodeInputList = [] ;

            $.each( serviceInfo['BusinessInfo']['NodeList'], function( index, nodeInfo ){
               if( nodeInfo['Role'] == 'coord' || nodeInfo['Role'] == 'standalone' )
               {
                  nodeInputList.push( {
                     'key': nodeInfo['HostName'] + ':' + nodeInfo['ServiceName'],
                     'value': nodeInfo['HostName'] + ':' + nodeInfo['ServiceName'],
                     'checked': true
                  } ) ;
               }
            } ) ;

            setArrayItemValue( advanceInput, 'name', 'address', { 'valid': { 'min': 0, 'list': nodeInputList } } ) ;
         }
      }

      //打开 创建关联 弹窗
      $scope.OpenCreateRelation = function() {
         if( createRelationDisabled == true )
         {
            return ;
         }

         $scope.CreateRelationWindow['config'].ShowType = 1 ;

         var pgsqlBusList = [] ;
         var mysqlBusList = [] ;

         var typeValid = [] ;
         var fromValid = [] ;
         var toValid = [] ;
         var dbValid = [] ;

         var normalInput  = $scope.CreateRelationWindow['config']['normal']['inputList'] ;
         var advanceInput = $scope.CreateRelationWindow['config']['advance']['inputList'] ;

         //修改关联类型的事件
         function switchCreateRelationType( name, key, value )
         {
            var to = $scope.CreateRelationWindow['config']['normal'].getValueByOne( 'To' ) ;

            if( value == 'pg-sdb' )
            {
               //切换配置项
               $scope.CreateRelationWindow['config']['normal'] .EnableItem( 'DbName' ) ;
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'preferedinstance' ) ;
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'transaction' ) ;

               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'sequoiadb_use_partition' ) ;
               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'sequoiadb_use_bulk_insert' ) ;
               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'sequoiadb_bulk_insert_size' ) ;
               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'sequoiadb_use_autocommit' ) ;
               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'sequoiadb_debug_log' ) ;

               fromValid = pgsqlBusList ;
               
               getPostgresqlDB( $scope.ClusterList[$scope.CurrentCluster]['ClusterName'], fromValid[0]['value'], function( dbList ){

                  clearArray( dbValid ) ;

                  $.each( dbList, function( index, dbInfo ){
                     dbValid.push( { 'key': dbInfo['datname'], 'value': dbInfo['datname'] } ) ;
                  } ) ;
                  //pg不支持‘-’，如果名字带有‘-’修改为‘_’
                  to = to.replace( '-', '_' ) ;
                  var relationName = sprintf( '?_?_?', fromValid[0]['value'], to, dbValid[0]['value'] ) ;

                  setArrayItemValue( normalInput, 'name', 'Name',   { 'value': relationName } ) ;
                  setArrayItemValue( normalInput, 'name', 'DbName', { 'value': dbValid[0]['value'], 'valid': dbValid } ) ;
               } ) ;
            }  
            else if( value == 'mysql-sdb' )
            {
               //切换配置项
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'sequoiadb_use_partition' ) ;
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'sequoiadb_use_bulk_insert' ) ;
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'sequoiadb_bulk_insert_size' ) ;
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'sequoiadb_use_autocommit' ) ;
               $scope.CreateRelationWindow['config']['advance'].EnableItem( 'sequoiadb_debug_log' ) ;

               $scope.CreateRelationWindow['config']['normal'] .DisableItem( 'DbName' ) ;
               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'preferedinstance' ) ;
               $scope.CreateRelationWindow['config']['advance'].DisableItem( 'transaction' ) ;

               fromValid = mysqlBusList ;
               
               var relationName = sprintf( '?_?', fromValid[0]['value'], to ) ;

               setArrayItemValue( normalInput, 'name', 'Name', { 'value': relationName } ) ;
            }

            setArrayItemValue( normalInput, 'name', 'From', { 'value': fromValid[0]['value'], 'valid': fromValid } ) ;
         }

         //修改关联服务名的事件
         function switchCreateRelationFrom( name, key, value )
         {
            var current = $scope.CreateRelationWindow['config']['normal'].getValue() ;
            var type = current['Type'] ;
            var to = current['To'] ;

            if( type == 'pg-sdb' )
            {
               getPostgresqlDB( $scope.ClusterList[$scope.CurrentCluster]['ClusterName'], value, function( dbList ){

                  clearArray( dbValid ) ;

                  $.each( dbList, function( index, dbInfo ){
                     dbValid.push( { 'key': dbInfo['datname'], 'value': dbInfo['datname'] } ) ;
                  } ) ;
                  
                  //pg不支持‘-’，如果名字带有‘-’修改为‘_’
                  to = to.replace( '-', '_' ) ;
                  var relationName = sprintf( '?_?_?', value, to, dbValid[0]['value'] ) ;

                  setArrayItemValue( normalInput, 'name', 'Name',   { 'value': relationName } ) ;
                  setArrayItemValue( normalInput, 'name', 'DbName', { 'value': dbValid[0]['value'], 'valid': dbValid } ) ;
               } ) ;
            }  
            else if( type == 'mysql-sdb' )
            {
               var relationName = sprintf( '?_?', value, to ) ;    
               
               setArrayItemValue( normalInput, 'name', 'Name', { 'value': relationName } ) ;
            }
         }

         //修改关联服务名的数据库的事件
         function switchCreateRelationDbName( name, key, value )
         {
            var relationName = '' ;
            var current = $scope.CreateRelationWindow['config']['normal'].getValue() ;
            var type = current['Type'] ;
            var from = current['From'] ;
            var to = current['To'] ;

            if( type == 'pg-sdb' )
            {
               //pg不支持‘-’，如果名字带有‘-’修改为‘_’
               to = to.replace( '-', '_' ) ;
               relationName = sprintf( '?_?_?', from, to, value ) ;
            }  
            else if( type == 'mysql-sdb' )
            {
               relationName = sprintf( '?_?', from, to ) ;             
            }

            setArrayItemValue( normalInput, 'name', 'Name', { 'value': relationName } ) ;
         }

         //修改被关联服务名的事件
         function switchCreateRelationTo( name, key, value )
         {
            var relationName = '' ;
            var current = $scope.CreateRelationWindow['config']['normal'].getValue() ;
            var type = current['Type'] ;
            var from = current['From'] ;

            if( type == 'pg-sdb' )
            {
               var dbName = current['DbName'] ;
               
               //pg不支持‘-’，如果名字带有‘-’修改为‘_’
               value = value.replace( '-', '_' ) ;
               relationName = sprintf( '?_?_?', from, value, dbName ) ;
            }  
            else if( type == 'mysql-sdb' )
            {
               relationName = sprintf( '?_?', from, value ) ;             
            }

            setArrayItemValue( normalInput, 'name', 'Name', { 'value': relationName } ) ;
            setCreateRelationAddress( value, advanceInput ) ;
         }

         //获取业务列表
         $.each( $scope.ModuleList, function( index, moduleInfo ){
            if( moduleInfo['BusinessType'] == 'sequoiasql-postgresql' )
            {
               pgsqlBusList.push( { 'key': moduleInfo['BusinessName'], 'value': moduleInfo['BusinessName'] } ) ;
            }
            else if( moduleInfo['BusinessType'] == 'sequoiasql-mysql' )
            {
               mysqlBusList.push( { 'key': moduleInfo['BusinessName'], 'value': moduleInfo['BusinessName'] } ) ;
            }
            else if( moduleInfo['BusinessType'] == 'sequoiadb' )
            {
               toValid.push( { 'key': moduleInfo['BusinessName'], 'value': moduleInfo['BusinessName'] } ) ;
            }
         } ) ;

         if ( pgsqlBusList.length > 0 )
         {
            typeValid.push( { 'key': $scope.autoLanguage( 'SequoiaSQL-PostgreSQL 关联 SequoiaDB' ), 'value': 'pg-sdb' } ) ;
         }
         if ( mysqlBusList.length > 0 )
         {
            typeValid.push( { 'key': $scope.autoLanguage( 'SequoiaSQL-MySQL 关联 SequoiaDB' ), 'value': 'mysql-sdb' } ) ;
         }

         if ( typeValid[0]['value'] == 'pg-sdb' )
         {
            fromValid = pgsqlBusList ;

            setArrayItemValue( normalInput,  'name', 'DbName',           { 'enable': true } ) ;
            setArrayItemValue( advanceInput, 'name', 'preferedinstance', { 'enable': true } ) ;
            setArrayItemValue( advanceInput, 'name', 'transaction',      { 'enable': true } ) ;

            setArrayItemValue( advanceInput, 'name', 'sequoiadb_use_partition',     { 'enable': false } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_use_bulk_insert',   { 'enable': false } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_bulk_insert_size',  { 'enable': false } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_use_autocommit',    { 'enable': false } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_debug_log',         { 'enable': false } ) ;

            getPostgresqlDB( $scope.ClusterList[$scope.CurrentCluster]['ClusterName'], fromValid[0]['value'], function( dbList ){

               clearArray( dbValid ) ;

               $.each( dbList, function( index, dbInfo ){
                  dbValid.push( { 'key': dbInfo['datname'], 'value': dbInfo['datname'] } ) ;
               } ) ;

               var relationName = sprintf( '?_?_?', fromValid[0]['value'], toValid[0]['value'], dbValid[0]['value'] ) ;

               setArrayItemValue( normalInput, 'name', 'Name',   { 'value': relationName } ) ;
               setArrayItemValue( normalInput, 'name', 'DbName', { 'value': dbValid[0]['value'], 'valid': dbValid } ) ;
            } ) ;
         }
         else if ( typeValid[0]['value'] == 'mysql-sdb' )
         {
            fromValid = mysqlBusList ;

            setArrayItemValue( advanceInput, 'name', 'sequoiadb_use_partition',     { 'enable': true } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_use_bulk_insert',   { 'enable': true } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_bulk_insert_size',  { 'enable': true } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_use_autocommit',    { 'enable': true } ) ;
            setArrayItemValue( advanceInput, 'name', 'sequoiadb_debug_log',         { 'enable': true } ) ;

            setArrayItemValue( normalInput,  'name', 'DbName',           { 'enable': false } ) ;
            setArrayItemValue( advanceInput, 'name', 'preferedinstance', { 'enable': false } ) ;
            setArrayItemValue( advanceInput, 'name', 'transaction',      { 'enable': false } ) ;

            var relationName = sprintf( '?_?', fromValid[0]['value'], toValid[0]['value'] ) ;
            setArrayItemValue( normalInput, 'name', 'Name', { 'value': relationName } ) ;
         }

         //设置表单规则,默认值,事件
         setArrayItemValue( normalInput, 'name', 'Type',   { 'value': typeValid[0]['value'], 'valid': typeValid, 'onChange': switchCreateRelationType } ) ;
         setArrayItemValue( normalInput, 'name', 'From',   { 'value': fromValid[0]['value'], 'valid': fromValid, 'onChange': switchCreateRelationFrom } ) ;
         setArrayItemValue( normalInput, 'name', 'DbName', { 'onChange': switchCreateRelationDbName } ) ;
         setArrayItemValue( normalInput, 'name', 'To',     { 'value': toValid[0]['value'],   'valid': toValid,   'onChange': switchCreateRelationTo   } ) ;

         setCreateRelationAddress( toValid[0]['value'], advanceInput ) ;

         $scope.CreateRelationWindow['config']['normal']['inputList']  = normalInput ;
         $scope.CreateRelationWindow['config']['advance']['inputList'] = advanceInput ;

         $scope.CreateRelationWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var inputErrNum1 = $scope.CreateRelationWindow['config']['normal'].getErrNum() ;
            var inputErrNum2 = $scope.CreateRelationWindow['config']['advance'].getErrNum() ;
            var isAllClear   = ( inputErrNum1 == 0 && inputErrNum2 == 0 ) ;

            $scope.CreateRelationWindow['config'].inputErrNum1 = inputErrNum1 ;
            $scope.CreateRelationWindow['config'].inputErrNum2 = inputErrNum2 ;

            if( isAllClear )
            {
               var options  = {} ;
               var formVal1 = $scope.CreateRelationWindow['config']['normal'].getValue() ;
               var formVal2 = $scope.CreateRelationWindow['config']['advance'].getValue() ;
               var address  = formVal2['address'].join( ',' ) ;

               for ( var key in formVal2 )
               {
                  if ( key == 'address' )
                  {
                     continue ;
                  }
                  options[key] = formVal2[key] ;
               }

               if ( formVal1['Type'] == 'pg-sdb' )
               {
                  options['DbName']  = formVal1['DbName'] ;
                  options['address'] = address ;
               }
               else if ( formVal1['Type'] == 'mysql-sdb' )
               {
                  options['sequoiadb_conn_addr'] = address ;
               }

               createRelation( formVal1['Name'], formVal1['From'], formVal1['To'], options ) ;               
            }
            else
            {
               if ( inputErrNum1 > 0 )
               {
                  $scope.CreateRelationWindow['config'].ShowType = 1 ;
                  $scope.CreateRelationWindow['config']['normal'].scrollToError( null ) ;
               }
               else if ( inputErrNum2 > 0 )
               {
                  $scope.CreateRelationWindow['config'].ShowType = 2 ;
                  $scope.CreateRelationWindow['config']['advance'].scrollToError( null ) ;
               }
            }

            return isAllClear ;
         } ) ;

         $scope.CreateRelationWindow['callback']['SetTitle']( $scope.autoLanguage( '创建关联' ) ) ;
         $scope.CreateRelationWindow['callback']['Open']() ;
      } ;

      //解除关联 弹窗
      $scope.RemoveRelationWindow = {
         'config': {},
         'callback': {}
      } ;

      //打开 解除关联 弹窗
      $scope.OpenRemoveRelation= function(){
         if( SdbSwap.relationshipList.length == 0 )
         {
            return;
         }
         
         var relationInfoList = [] ;

         $.each( SdbSwap.relationshipList, function( index, relationInfo ){
            var from = '' ;
            var to = '' ;
            $.each( $scope.ModuleList, function( index2, moduleInfo ){
               if( relationInfo['To'] == moduleInfo['BusinessName'] )
               {
                  to = relationInfo['To'] + '  ( ' + moduleInfo['BusinessType'] + ' )' ;
               }
               if( relationInfo['From'] == moduleInfo['BusinessName'] )
               {
                  from = relationInfo['From'] + '  ( ' + moduleInfo['BusinessType'] + ' )' ;
               }
            } ) ;
            
            relationInfoList.push(
               { 'key': relationInfo['Name'], 'value': index, 'to': to, 'from': from }
            ) ;
         } ) ;

         $scope.RemoveRelationWindow['config'] = {
            inputList: [
               {
                  "name": "Name",
                  "webName": $scope.autoLanguage( '关联名' ),
                  "type": "select",
                  "required": true,
                  "value": relationInfoList[0]['value'],
                  "valid": relationInfoList,
                  "onChange": function( name, key, value ){
                     $scope.RemoveRelationWindow['config']['inputList'][1]['value'] = relationInfoList[value]['from'] ;
                     $scope.RemoveRelationWindow['config']['inputList'][2]['value'] = relationInfoList[value]['to'] ;
                  }
               },
               {
                  "name": "from",
                  "webName": $scope.autoLanguage( '关联服务名' ),
                  "type": "string",
                  "disabled": true,
                  "value": relationInfoList[0]['from']
               },
               {
                  "name": "to",
                  "webName": $scope.autoLanguage( '被关联服务名' ),
                  "type": "string",
                  "disabled": true,
                  "value": relationInfoList[0]['to']
               }
            ]
         } ;


         $scope.RemoveRelationWindow['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
            var isAllClear = $scope.RemoveRelationWindow['config'].check() ;
            if( isAllClear )
            {
               var formVal = $scope.RemoveRelationWindow['config'].getValue() ;
               removeRelation( relationInfoList[formVal['Name']]['key'] ) ;
            }
            return isAllClear ;
         } ) ;
         $scope.RemoveRelationWindow['callback']['SetTitle']( $scope.autoLanguage( '解除关联' ) ) ;
         $scope.RemoveRelationWindow['callback']['Open']() ;
      }
      
      //关联信息 弹窗
      $scope.RelationshipWindow = {
         'config': [],
         'callback': {}
      } ;

      //打开 关联信息 弹窗
      $scope.ShowRelationship = function( moduleName ){
         $scope.RelationshipWindow['config'] = [] ;
         $.each( SdbSwap.relationshipList, function( index, info ){
            if( moduleName == info['From'] )
            {
               info['where'] = 'From' ;
            }
            else if( moduleName == info['To'] )
            {
               info['where'] = 'To' ;
            }
            else
            {
               return ;
            }
            $scope.RelationshipWindow['config'].push( info ) ;

         } ) ;
         $scope.RelationshipWindow['callback']['SetTitle']( $scope.autoLanguage( '关联信息' ) ) ;
         $scope.RelationshipWindow['callback']['SetCloseButton']( $scope.autoLanguage( '关闭' ), function(){
            $scope.RelationshipWindow['callback']['Close']() ;
         } ) ;
         $scope.RelationshipWindow['callback']['Open']() ;
      }
   } ) ;

   //主机控制器
   sacApp.controllerProvider.register( 'Deploy.Index.Host.Ctrl', function( $scope, $rootScope, $location, SdbRest, SdbSignal, SdbSwap, Loading ){

      //主机表格
      $scope.HostListTable = {
         'title': {
            'Check':            '',
            'Error.Flag':       $scope.autoLanguage( '状态' ),
            'HostName':         $scope.autoLanguage( '主机名' ),
            'IP':               $scope.autoLanguage( 'IP地址' ),
            'BusinessName':     $scope.autoLanguage( '服务' ),
            'Packages':         $scope.autoLanguage( '包' )
         },
         'body': [],
         'options': {
            'width': {
               'Check':          '30px',
               'Error.Flag':     '60px',
               'HostName':       '25%',
               'IP':             '25%',
               'BusinessName':   '25%',
               'Packages':       '25%'
            },
            'sort': {
               'Check':                 false,
               'Error.Flag':            true,
               'HostName':              true,
               'IP':                    true,
               'BusinessName':          true,
               'Packages':              true
            },
            'max': 50,
            'filter': {
               'Check':             null,
               'Error.Flag':        'indexof',
               'HostName':          'indexof',
               'IP':                'indexof',
               'BusinessName':      'indexof',
               'Packages':          'indexof'
            }
         },
         'callback': {}
      } ;

      //查询主机
      SdbSignal.on( 'queryHost', function(){
         var data = { 'cmd': 'query host' } ;
         SdbRest.OmOperation( data, {
            'success': function( hostList ){
               SdbSwap.hostList = hostList ;
               $scope.HostListTable['body'] = SdbSwap.hostList ;
               $.each( SdbSwap.hostList, function( index ){
                  SdbSwap.hostList[index]['Error'] = {} ;
                  SdbSwap.hostList[index]['Error']['Flag'] = 0 ;
               } ) ;
               $scope.SwitchCluster( $scope.CurrentCluster ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  SdbSignal.commit( 'queryHost' ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      } ) ;

      //更新主机表格
      SdbSignal.on( 'updateHostTable', function( result ){
         $scope.HostListTable['body'] = result ;
      } ) ;

      //全选
      $scope.SelectAll = function(){
         $.each( SdbSwap.hostList, function( index ){
            if( $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] == SdbSwap.hostList[index]['ClusterName'] )
            {
               SdbSwap.hostList[index]['checked'] = true ;
            }
         } ) ;
      }

      //反选
      $scope.Unselect = function(){
         $.each( SdbSwap.hostList, function( index ){
            if( $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] == SdbSwap.hostList[index]['ClusterName'] )
            {
               SdbSwap.hostList[index]['checked'] = !SdbSwap.hostList[index]['checked'] ;
            }
         } ) ;
      }

      //添加主机
      $scope.AddHost = function(){
         if( $scope.ClusterList.length > 0 )
         {
            $rootScope.tempData( 'Deploy', 'Model', 'Host' ) ;
            $rootScope.tempData( 'Deploy', 'Module', 'None' ) ;
            $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
            $rootScope.tempData( 'Deploy', 'InstallPath', $scope.ClusterList[ $scope.CurrentCluster ]['InstallPath'] ) ;
            $location.path( '/Deploy/ScanHost' ).search( { 'r': new Date().getTime() } ) ;
         }
      }

      SdbSignal.on( 'addHost', function(){
         $scope.AddHost() ;
      } ) ;

      //删除主机
      $scope.RemoveHost = function(){
         if( $scope.ClusterList.length > 0 )
         {
            var hostList = [] ;
            $.each( SdbSwap.hostList, function( index ){
               if( SdbSwap.hostList[index]['checked'] == true && $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] == SdbSwap.hostList[index]['ClusterName'] )
               {
                  hostList.push( { 'HostName': SdbSwap.hostList[index]['HostName'] } ) ;
               }
            } ) ;
            if( hostList.length > 0 )
            {
               var data = {
                  'cmd': 'remove host',
                  'HostInfo': JSON.stringify( { 'ClusterName': $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'], 'HostInfo': hostList } )
               } ;
               SdbRest.OmOperation( data, {
                  'success': function( taskInfo ){
                     $rootScope.tempData( 'Deploy', 'Model', 'Task' ) ;
                     $rootScope.tempData( 'Deploy', 'Module', 'None' ) ;
                     $rootScope.tempData( 'Deploy', 'HostTaskID', taskInfo[0]['TaskID'] ) ;
                     $location.path( '/Deploy/Task/Host' ).search( { 'r': new Date().getTime() } ) ;
                  },
                  'failed': function( errorInfo ){
                     _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                        $scope.RemoveHost() ;
                        return true ;
                     } ) ;
                  }
               } ) ;
            }
            else
            {
               $scope.Components.Confirm.type = 3 ;
               $scope.Components.Confirm.context = $scope.autoLanguage( '至少选择一台主机。' ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
            }
         }
      }

      //解绑主机
      $scope.UnbindHost = function(){
         if( $scope.ClusterList.length > 0 )
         {
            var hostList = { "HostInfo": [] } ;
            var hostListTips = '' ;
            $.each( SdbSwap.hostList, function( index ){
               if( SdbSwap.hostList[index]['checked'] == true && $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] == SdbSwap.hostList[index]['ClusterName'] )
               {
                  hostList['HostInfo'].push( { 'HostName': SdbSwap.hostList[index]['HostName'] } ) ;
               }
            } ) ;
            if( hostList['HostInfo'].length > 0 )
            {
               var clusterName = $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ;
               var data = { 'cmd': 'unbind host', 'ClusterName': clusterName, 'HostInfo': JSON.stringify( hostList ) } ;
               SdbRest.OmOperation( data, {
                  'success': function( taskInfo ){
                     $scope.Components.Confirm.type = 4 ;
                     $scope.Components.Confirm.context = sprintf( $scope.autoLanguage( '主机解绑成功。' ) ) ;
                     $scope.Components.Confirm.isShow = true ;
                     $scope.Components.Confirm.noClose = true ;
                     $scope.Components.Confirm.normalOK = true ;
                     $scope.Components.Confirm.okText = $scope.autoLanguage( '确定' ) ;
                     $scope.Components.Confirm.ok = function(){
                        $scope.Components.Confirm.normalOK = false ;
                        $scope.Components.Confirm.isShow = false ;
                        $scope.Components.Confirm.noClose = false ;
                        SdbSignal.commit( 'queryHost' ) ;
                     }
                  },
                  'failed': function( errorInfo ){
                     _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                        $scope.UnbindHost() ;
                        return true ;
                     } ) ;
                  }
               } ) ;
            }
            else
            {
               $scope.Components.Confirm.type = 3 ;
               $scope.Components.Confirm.context = $scope.autoLanguage( '至少选择一台主机。' ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
            }
         }
      }
      
      //逐个更新主机信息
      var updateHostsInfo = function( hostList, index, success ) {
         if( index == hostList.length )
         {
            setTimeout( success ) ;
            return ;
         }

         if( hostList[index]['Flag'] != 0 )
         {
            updateHostsInfo( hostList, index + 1, success ) ;
            return ;
         }

         var hostInfo = {
            'HostInfo' : [
               {
                  'HostName': hostList[index]['HostName'],
                  'IP': hostList[index]['IP']
               }   
            ]
         }
         var data = { 'cmd': 'update host info', 'HostInfo': JSON.stringify( hostInfo ) } ;
         SdbRest.OmOperation( data, {
            'success': function( scanInfo ){
               hostList[index]['Status'] = $scope.autoLanguage( '更新主机信息成功。' ) ;
               SdbSwap.hostList[ hostList[index]['SourceIndex'] ]['IP'] = hostList[index]['IP'] ;
               updateHostsInfo( hostList, index + 1, success ) ;
            },
            'failed': function( errorInfo ){
               Loading.close() ;
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  Loading.create() ;
                  updateHostsInfo( hostList, index, success ) ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': false
         } ) ;
      }

      //逐个扫描主机
      var scanHosts = function( hostList, index, success ){
      
         if( index == hostList.length )
         {
            success() ;
            return ;
         }

         if( hostList[index]['IP'] == SdbSwap.hostList[ hostList[index]['SourceIndex'] ]['IP'] )
         {
            hostList[index]['Status'] = $scope.autoLanguage( 'IP地址没有改变，跳过。' ) ;
            scanHosts( hostList, index + 1, success ) ;
            return ;
         }

         hostList[index]['Status'] = $scope.autoLanguage( '正在检测...' ) ;

         var scanHostInfo = [ {
            'IP': hostList[index]['IP'],
            'SshPort': hostList[index]['SshPort'],
            'AgentService': hostList[index]['AgentService']
         } ] ;
         var clusterName = $scope.ClusterList[$scope.CurrentCluster]['ClusterName'] ;
         var clusterUser = $scope.ClusterList[$scope.CurrentCluster]['SdbUser'] ;
         var clusterPwd  = $scope.ClusterList[$scope.CurrentCluster]['SdbPasswd'] ;
         var hostInfo = {
            'ClusterName': clusterName,
            'HostInfo': scanHostInfo,
            'User': clusterUser,
            'Passwd': clusterPwd,
            'SshPort': '-',
            'AgentService': '-'
         } ;
         var data = { 'cmd': 'scan host', 'HostInfo': JSON.stringify( hostInfo ) } ;
         SdbRest.OmOperation( data, {
            'success':function( scanInfo ){
               if( scanInfo[0]['errno'] == -38 || scanInfo[0]['errno'] == 0 )
               {
                  if( scanInfo[0]['HostName'] == hostList[index]['HostName'] )
                  {
                     hostList[index]['Flag'] = 0 ;
                     hostList[index]['Status'] = $scope.autoLanguage( '匹配成功。' ) ;
                  }
                  else
                  {
                     hostList[index]['Status'] = $scope.sprintf( $scope.autoLanguage( '主机名匹配错误，IP地址?的主机名是?。' ), scanInfo[0]['IP'], scanInfo[0]['HostName'] ) ;
                  }
               }
               else
               {
                  hostList[index]['Status'] = $scope.autoLanguage( '错误' ) + ': ' + scanInfo[0]['detail'] ;
               }
               scanHosts( hostList, index + 1, success ) ;
            },
            'failed': function( errorInfo ){
               Loading.close() ;
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  Loading.create() ;
                  scanHosts( hostList, index, success ) ;
                  return true ;
               } ) ;
            }
         }, {
            'showLoading': false
         } ) ;
      }

      //更新主机IP 弹窗
      $scope.UpdateHostIP = {
         'config': {},
         'callback': {}
      } ;

      //打开 更新主机IP 弹窗
      var showUpdateHostIP = function(){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.UpdateHostList = [] ;
            $.each( SdbSwap.hostList, function( index ){
               if( SdbSwap.hostList[index]['checked'] == true && $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] == SdbSwap.hostList[index]['ClusterName'] )
               {
                  $scope.UpdateHostList.push( {
                     'HostName': SdbSwap.hostList[index]['HostName'],
                     'IP': SdbSwap.hostList[index]['IP'],
                     'SshPort': SdbSwap.hostList[index]['SshPort'],
                     'AgentService': SdbSwap.hostList[index]['AgentService'],
                     'Flag': -1,
                     'Status': ( SdbSwap.hostList[index]['Error']['Flag'] == 0 ? '' : $scope.autoLanguage( '错误' ) + ': ' + SdbSwap.hostList[index]['Error']['Message'] ),
                     'SourceIndex': index
                  } ) ;
               }
            } ) ;
            if( $scope.UpdateHostList.length > 0 )
            {
               //设置确定按钮
               $scope.UpdateHostIP['callback']['SetOkButton']( $scope.autoLanguage( '确定' ), function(){
                  Loading.create() ;
                  scanHosts( $scope.UpdateHostList, 0, function(){
                     updateHostsInfo( $scope.UpdateHostList, 0, function(){
                        $scope.$apply() ;
                        Loading.cancel() ;
                     } ) ;
                  } ) ;
                  return false ;
               } ) ;
               //设置标题
               $scope.UpdateHostIP['callback']['SetTitle']( $scope.autoLanguage( '更新主机信息' ) ) ;
               //设置图标
               $scope.UpdateHostIP['callback']['SetIcon']( '' ) ;
               //打开窗口
               $scope.UpdateHostIP['callback']['Open']() ;
            }
            else
            {
               $scope.Components.Confirm.type = 3 ;
               $scope.Components.Confirm.context = $scope.autoLanguage( '至少选择一台主机。' ) ;
               $scope.Components.Confirm.isShow = true ;
               $scope.Components.Confirm.noClose = true ;
            }
         }
      }

      //前往部署安装包
      var gotoDeployPackage = function(){
         var hostList = [] ;
         $.each( SdbSwap.hostList, function( index ){
            if( SdbSwap.hostList[index]['checked'] == true && $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] == SdbSwap.hostList[index]['ClusterName'] )
            {
               hostList.push(
                  {
                     'HostName': SdbSwap.hostList[index]['HostName'],
                     'IP': SdbSwap.hostList[index]['IP'],
                     'User': SdbSwap.hostList[index]['User'],
                     'Packages': SdbSwap.hostList[index]['Packages']
                  } 
               ) ;
            }
         } ) ;
         if( hostList.length > 0 )
         {
            $rootScope.tempData( 'Deploy', 'Model',  'Package' ) ;
            $rootScope.tempData( 'Deploy', 'Module', 'None' ) ;
            $rootScope.tempData( 'Deploy', 'HostList', hostList ) ;
            $rootScope.tempData( 'Deploy', 'ClusterName', $scope.ClusterList[ $scope.CurrentCluster ]['ClusterName'] ) ;
            $location.path( '/Deploy/Package' ).search( { 'r': new Date().getTime() } ) ;
         }
         else
         {
            $scope.Components.Confirm.type = 3 ;
            $scope.Components.Confirm.context = $scope.autoLanguage( '至少选择一台主机。' ) ;
            $scope.Components.Confirm.isShow = true ;
            $scope.Components.Confirm.noClose = true ;
         }
      }

      //删除主机 下拉菜单
      $scope.RemoveHostDropdown = {
         'config': [],
         'OnClick': function( index ){
            if( index == 0 )
            {
               $scope.RemoveHost() ;
            }
            else if( index == 1 )
            {
               $scope.UnbindHost() ;
            }
            $scope.RemoveHostDropdown['callback']['Close']() ;
         },
         'callback': {}
      }

      //打开 删除主机下拉菜单
      $scope.OpenRemoveHost = function( event ){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.RemoveHostDropdown['config'] = [] ;
            $scope.RemoveHostDropdown['config'].push( { 'key': $scope.autoLanguage( '卸载主机' ) } ) ;
            $scope.RemoveHostDropdown['config'].push( { 'key': $scope.autoLanguage( '解绑主机' ) } ) ;
            $scope.RemoveHostDropdown['callback']['Open']( event.currentTarget ) ;
         }
      }

      //编辑主机 下拉菜单
      $scope.EditHostDropdown = {
         'config': [],
         'OnClick': function( index ){
            if( index == 0 )
            {
               gotoDeployPackage() ;
            }
            else if( index == 1 )
            {
               showUpdateHostIP() ;
            }
            $scope.EditHostDropdown['callback']['Close']() ;
         },
         'callback': {}
      }

      //打开 编辑主机下拉菜单
      $scope.OpenEditHostDropdown = function( event ){
         if( $scope.ClusterList.length > 0 )
         {
            $scope.EditHostDropdown['config'] = [] ;
            $scope.EditHostDropdown['config'].push( { 'key': $scope.autoLanguage( '部署包' ) } ) ;
            $scope.EditHostDropdown['config'].push( { 'key': $scope.autoLanguage( '更新主机信息' ) } ) ;
            $scope.EditHostDropdown['callback']['Open']( event.currentTarget ) ;
         }
      }
   } ) ;
}());