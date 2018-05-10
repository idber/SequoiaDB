(function(){
   var sacApp = window.SdbSacManagerModule ;
   //控制器
   sacApp.controllerProvider.register( 'Deploy.Scan.Ctrl', function( $scope, $compile, $location, $rootScope, SdbRest ){

      //初始化
      var scanHostTmp        = $rootScope.tempData( 'Deploy', 'ScanHost' ) ;
      var successNum         = 0 ;
      $scope.HostList        = scanHostTmp == null ? [] : scanHostTmp ;
      $scope.HostGridOptions = { 'titleWidth': [ '30px', 16, 20, 16, 12, 12, 12, 12 ] } ;
      $scope.HostGridTool    = sprintf( $scope.autoLanguage( '已扫描 ? 台主机, 共 ? 台主机连接成功。' ), $scope.HostList.length, successNum ) ;
      $scope.checkedHostNum  = 0 ;
      $scope.ScanForm = {
         'inputList': [
            {
               "name": "address",
               "webName": $scope.autoLanguage( 'IP地址/主机名' ),
               "type": "text",
               "value": "",
               "valid": {
                  "min": 1
               }
            },
            {
               "name": "user",
               "webName": $scope.autoLanguage( '用户名' ),
               "type": "string",
               "value": "root",
               "valid": {
                  "min": 1
               },
               'disabled': true
            },
            {
               "name": "password",
               "webName": $scope.autoLanguage( '密码' ),
               "type": "password",
               "value": "",
               "valid": {
                  "min": 1
               },
               'onKeypress': function( event ){
                  $scope.ScanHost( event ) ;
               }
            },
            {
               "name": "ssh",
               "webName": $scope.autoLanguage( 'SSH端口' ),
               "type": "string",
               "value": "22",
               "valid": {
                  "min": 1
               },
               'onKeypress': function( event ){
                  $scope.ScanHost( event ) ;
               }
            },
            {
               "name": "proxy",
               "webName": $scope.autoLanguage( '代理端口' ),
               "type": "string",
               "value": "11790",
               "valid": {
                  "min": 1
               },
               'onKeypress': function( event ){
                  $scope.ScanHost( event ) ;
               }
            }
         ]
      } ;

      var deployModel  = $rootScope.tempData( 'Deploy', 'Model' ) ;
      var deplpyModule = $rootScope.tempData( 'Deploy', 'Module' ) ;
      var clusterName  = $rootScope.tempData( 'Deploy', 'ClusterName' ) ;
      if( deployModel == null || clusterName == null || deplpyModule == null )
      {
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }

      $scope.stepList = _Deploy.BuildSdbStep( $scope, $location, deployModel, $scope['Url']['Action'], deplpyModule ) ;
      if( $scope.stepList['info'].length == 0 )
      {
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }

      $scope.ClearInput = function(){
         $scope.ScanForm['inputList'][0]['value'] = '' ;
         $scope.ScanForm['inputList'][2]['value'] = '' ;
      }

      $scope.CountCheckedHostNum = function(){
         setTimeout( function(){
            $scope.checkedHostNum = 0 ;
            $.each( $scope.HostList, function( index, hostInfo ){
               if( hostInfo['checked'] == true )
               {
                  ++$scope.checkedHostNum ;
               }
            } ) ;
            $scope.$apply() ;
         } ) ;
      }

      var countSuccessHostNum = function(){
         successNum = 0 ;
         $.each( $scope.HostList, function( index2, hostInfo ){
            if( hostInfo['Errno'] == 0 )
            {
               ++successNum ;
            }
         } ) ;
      }

      countSuccessHostNum() ;
      $scope.CountCheckedHostNum() ;

      $scope.SelectAll = function(){
         $.each( $scope.HostList, function( index ){
            if( $scope.HostList[index]['Errno'] == 0 )
            {
               $scope.HostList[index]['checked'] = true ;
            }
         } ) ;
      }

      $scope.Unselected = function(){
         $.each( $scope.HostList, function( index ){
            if( $scope.HostList[index]['Errno'] == 0 )
            {
               $scope.HostList[index]['checked'] = !$scope.HostList[index]['checked'] ;
            }
         } ) ;
      }

      $scope.ClearErrorHost = function(){
         var newHostList = [] ;
         $.each( $scope.HostList, function( index, hostInfo ){
            if( hostInfo['Errno'] == 0 )
            {
               newHostList.push( hostInfo ) ;
            }
         } ) ;
         $scope.HostList = newHostList ;
         $scope.HostGridTool = sprintf( $scope.autoLanguage( '已扫描 ? 台主机, 共 ? 台主机连接成功。' ), $scope.HostList.length, $scope.HostList.length ) ;
         $rootScope.tempData( 'Deploy', 'ScanHost', $scope.HostList ) ;
      }

      $scope.Helper = function(){
         $scope.Components.Modal.icon = '' ;
         $scope.Components.Modal.title = $scope.autoLanguage( '帮助' ) ;
         $scope.Components.Modal.isShow = true ;
         $scope.Components.Modal.Context = '\
<p>' + $scope.autoLanguage( '请输入主机名或IP地址，然后单击<b>扫描</b>。您还可以指定主机名和IP地址范围：' ) + '</p>\
<table class="table loosen border">\
   <tr>\
      <td><b>' + $scope.autoLanguage( '使用此延展范围' ) + '</b></td>\
      <td><b>' + $scope.autoLanguage( '要指定的主机' ) + '</b></td>\
   </tr>\
   <tr>\
      <td>10.1.1.[1-4]</td>\
      <td>10.1.1.1, 10.1.1.2, 10.1.1.3, 10.1.1.4</td>\
   </tr>\
   <tr>\
      <td>pc[1-4]host</td>\
      <td>pc1host, pc2host, pc3host, pc4host</td>\
   </tr>\
   <tr>\
      <td>pc[098-101]host</td>\
      <td>pc098host, pc099host, pc100host, pc101host</td>\
   </tr>\
</table>\
<p>' + $scope.autoLanguage( '您可以添加多个主机名、IP地址和主机名范围、IP地址范围，但要注意主机名和IP地址是唯一。' ) + '</p>\
<p>' + $scope.autoLanguage( '扫描结果将包括所有扫描的地址，但只有运行SSH服务的主机才会被选择包含在集群中。如果是用户名、密码或SSH填写不正确，可以点击主机名或IP地址修改。' ) + '</p>\
<p><b>' + $scope.autoLanguage( '注意' ) + '</b>:' + $scope.autoLanguage( '如果您不知道主机的HostName和IP，可输入更大的范围进行扫描。但是范围越大，扫描时间越长。' ) + '</p>' ;
         $scope.Components.Modal.ok = function(){
            return true ;
         }
      }

      function ScanAllHost( formVal )
      {
         var hostList = parseHostString( formVal['address'] ) ;
         var index = 0 ;
         var scanHostOnce = function(){
            if( index < hostList.length )
            {
               var hostInfo = { "ClusterName": clusterName, "HostInfo": hostList[index], "User": formVal['user'], "Passwd": formVal['password'], "SshPort": formVal['ssh'],"AgentService": formVal['proxy'] } ;
               var data = { 'cmd': 'scan host', 'HostInfo': JSON.stringify( hostInfo ) } ;
               SdbRest.OmOperation( data, {
                  'success': function( hostList ){
                     $.each( hostList, function( index, hostInfo ){
                        var isExists = checkHostIsExist( $scope.HostList, hostInfo['HostName'], hostInfo['IP'] ) ;
                        var newHostInfo = { 'checked': ( hostInfo['errno'] == 0 ? true : false ), 'Errno': hostInfo['errno'], 'Detail': hostInfo['detail'], 'HostName': hostInfo['HostName'], 'IP': hostInfo['IP'], 'User': formVal['user'], 'Password': formVal['password'], 'SSH': formVal['ssh'], 'Proxy': formVal['proxy']  } ;
                        if( isExists == -1 || isExists == -2 )
                        {
                           $scope.HostList.push( newHostInfo ) ;
                        }
                        else
                        {
                           $scope.HostList[isExists] = newHostInfo ;
                        }
                     } ) ;
                     $rootScope.tempData( 'Deploy', 'ScanHost', $scope.HostList ) ;
                     countSuccessHostNum() ;
                     $scope.CountCheckedHostNum() ;
                     $scope.HostGridTool = sprintf( $scope.autoLanguage( '已扫描 ? 台主机, 共 ? 台主机连接成功。' ), $scope.HostList.length, successNum ) ;
                     $rootScope.bindResize() ;
                     $scope.$apply() ;
                     ++index ;
                     scanHostOnce() ;
                  },
                  'failed': function( errorInfo ){
                     _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                        $scope.ScanHost() ;
                        return true ;
                     } ) ;
                  }
               } ) ;
            }
         }
         scanHostOnce() ;
      }

      $scope.ChangeHostInfo = function( index ){
         var hostInfo = $scope.HostList[index] ;
         $scope.Components.Modal.icon = '' ;
         $scope.Components.Modal.title = $scope.autoLanguage( '修改主机信息' ) ;
         $scope.Components.Modal.isShow = true ;
         $scope.Components.Modal.form = {
            inputList: [
               {
                  "name": "address",
                  "webName": $scope.autoLanguage( 'IP地址/主机名' ),
                  "type": "string",
                  "value": hostInfo['HostName'] ? hostInfo['HostName'] : hostInfo['IP'],
                  "valid": {
                     "min": 1
                  },
                  'disabled': true
               },
               {
                  "name": "user",
                  "webName": $scope.autoLanguage( '用户名' ),
                  "type": "string",
                  "value": hostInfo['User'],
                  "valid": {
                     "min": 1
                  },
                  'disabled': true
               },
               {
                  "name": "password",
                  "webName": $scope.autoLanguage( '密码' ),
                  "type": "password",
                  "value": hostInfo['Password'],
                  "valid": {
                     "min": 1
                  }
               },
               {
                  "name": "ssh",
                  "webName": $scope.autoLanguage( 'SSH端口' ),
                  "type": "string",
                  "value": hostInfo['SSH'],
                  "valid": {
                     "min": 1
                  }
               },
               {
                  "name": "proxy",
                  "webName": $scope.autoLanguage( '代理端口' ),
                  "type": "string",
                  "value": hostInfo['Proxy'],
                  "valid": {
                     "min": 1
                  }
               }
            ]
         } ;
         $scope.Components.Modal.Context = '<div form-create para="data.form"></div>' ;
         $scope.Components.Modal.ok = function(){
            var isAllClear = $scope.Components.Modal.form.check( function( val ){
               var errInfo = [] ;
               if( !checkPort( val['ssh'] ) )
               {
                  errInfo.push( { 'name': 'ssh', 'error': $scope.autoLanguage( 'SSH端口格式错误' ) } ) ;
               }
               if( !checkPort( val['proxy'] ) )
               {
                  errInfo.push( { 'name': 'proxy', 'error': $scope.autoLanguage( '代理端口格式错误' ) } ) ;
               }
               return errInfo ;
            } ) ;
            if( isAllClear )
            {
               var formVal = $scope.Components.Modal.form.getValue() ;
               ScanAllHost( formVal ) ;
            }
            return isAllClear ;
         }
      }

      $scope.ScanHost = function( event ){
         if( typeof( event ) == 'undefined' || event['keyCode'] == 13 )
         {
            var isAllClear = $scope.ScanForm.check( function( val ){
               var errInfo = [] ;
               if( !checkPort( val['ssh'] ) )
               {
                  errInfo.push( { 'name': 'ssh', 'error': $scope.autoLanguage( 'SSH端口格式错误' ) } ) ;
               }
               if( !checkPort( val['proxy'] ) )
               {
                  errInfo.push( { 'name': 'proxy', 'error': $scope.autoLanguage( '代理端口格式错误' ) } ) ;
               }
               return errInfo ;
            } ) ;
            if( isAllClear )
            {
               var formVal = $scope.ScanForm.getValue() ;
               ScanAllHost( formVal ) ;
            }
         }
      }

      //上一步
      $scope.GotoDeploy = function(){
         if( deployModel == 'Host' )
         {
            $rootScope.tempData( 'Deploy', 'Index', 'host' ) ;
         }
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
      }

      //下一步
      $scope.GotoAddHost = function(){
         if( $scope.checkedHostNum > 0 )
         {
            var newHostList = {
               'ClusterName': clusterName,
               'HostInfo': [],
               'User': '-',
               'Passwd': '-',
               'SshPort': '-',
               'AgentService': '-'
            } ;
            $.each( $scope.HostList, function( index, hostInfo ){
               if( hostInfo['checked'] == true )
               {
                  newHostList['HostInfo'].push( {
                     "HostName": hostInfo['HostName'],
                     "IP":       hostInfo['IP'],
                     "User":     hostInfo['User'],
                     "Passwd":   hostInfo['Password'],
                     "SshPort":  hostInfo['SSH'],
                     "AgentService": hostInfo['Proxy']
                  } ) ;
               }
            } ) ;
            $rootScope.tempData( 'Deploy', 'AddHost', newHostList ) ;
            $location.path( '/Deploy/AddHost' ).search( { 'r': new Date().getTime() } ) ;
         }
         else
         {
            $scope.Components.Confirm.type = 3 ;
            $scope.Components.Confirm.context = $scope.autoLanguage( '至少选择一台的主机，才可以进入下一步操作。' ) ;
            $scope.Components.Confirm.isShow = true ;
            $scope.Components.Confirm.okText = $scope.autoLanguage( '好的' ) ;
         }
      } ;
   } ) ;
}());