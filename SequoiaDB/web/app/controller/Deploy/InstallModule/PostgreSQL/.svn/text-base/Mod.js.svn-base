//@ sourceURL=Mod.js
//"use strict" ;
(function(){
   var sacApp = window.SdbSacManagerModule ;
   //控制器
   sacApp.controllerProvider.register( 'Deploy.Ssql.Mod.Ctrl', function( $scope, $compile, $location, $rootScope, $interval, SdbRest, SdbFunction, Loading ){
      
      var configure      = $rootScope.tempData( 'Deploy', 'ModuleConfig' ) ;
      $scope.ModuleName  = $rootScope.tempData( 'Deploy', 'ModuleName' ) ;
      var deployType     = $rootScope.tempData( 'Deploy', 'Model' ) ;
      var clusterName    = $rootScope.tempData( 'Deploy', 'ClusterName' ) ;
      if( deployType == null || clusterName == null || $scope.ModuleName == null || configure == null )
      {
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }
      $scope.stepList = _Deploy.BuildSdbPgsqlStep( $scope, $location, $scope['Url']['Action'] ) ;
      if( $scope.stepList['info'].length == 0 )
      {
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }

      $scope.InputErrNum = {} ;
      $scope.ShowType  = 'normal' ;
      
      var hostSelectList = [] ;
      var installConfig = [] ;
      var buildConf = [] ;
      //切换分类
      $scope.SwitchParam = function( type ){
         $scope.ShowType = type ;
      }

      //生成form表单
      var buildConfForm= function( config ){
         installConfig = config[0] ;
         buildConf = config[0]['Config'] ;
         $scope.Template = config[0]['Property'] ;
         $scope.AllForm = {} ;
         $scope.AllForm['normal'] = {
            'keyWidth': '200px',
            'inputList': _Deploy.ConvertTemplate( $scope.Template, 0 )
         } ;
         $scope.AllForm['normal']['inputList'].splice( 0, 0, {
            "name": "HostName",
            "webName": 'hostname',
            "type": "select",
            "value": hostSelectList[0]['key'],
            "valid": hostSelectList
         } ) ;
         $.each( $scope.AllForm['normal']['inputList'], function( index ){
            var name = $scope.AllForm['normal']['inputList'][index]['name'] ;
            $scope.AllForm['normal']['inputList'][index]['value'] = buildConf[0][name] ;
         } ) ;

         var otherForm = _Deploy.ConvertTemplate( $scope.Template, 1, true, true ) ;
         $.each( otherForm, function( index, formInfo ){
            formInfo['confType'] = formInfo['confType'].toLowerCase() ;
            if( typeof( $scope.AllForm[formInfo['confType']] ) == 'undefined' )
            {
               $scope.AllForm[formInfo['confType']] = {
                  'keyWidth': '200px',
                  'inputList': [ formInfo ]
               } ;
            }
            else
            {
               $scope.AllForm[formInfo['confType']]['inputList'].push( formInfo ) ;
            }
         } ) ;
      }

      //获取配置
      var getModuleConfig = function(){
         var data = { 'cmd': 'get business config', 'TemplateInfo': JSON.stringify( configure ) } ;
         SdbRest.OmOperation( data, {
            'success': function( config ){
               buildConfForm( config ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getModuleConfig() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //获取主机列表
      var getHostList = function(){
         var data = { 'cmd': 'query host' } ;
         SdbRest.OmOperation( data, {
            'success': function( hostList ){
               configure['HostInfo'] = [] ;
               $.each( hostList, function( index, hostInfo ){
                  if( hostInfo['ClusterName'] == clusterName )
                  {
                     $.each( hostInfo['Packages'], function( packageIndex, packageInfo ){
                        if( packageInfo['Name'] == 'sequoiasql-postgresql' )
                        {
                           hostSelectList.push( { 'key': hostInfo['HostName'], 'value': hostInfo['HostName'] } ) ;
                           configure['HostInfo'].push( { 'HostName': hostInfo['HostName'] } ) ; 
                        }
                     } ) ;
                  }
               } ) ;
               getModuleConfig() ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getHostList() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }
      getHostList() ;
      
      $scope.GotoDeploy = function(){
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
      }

      var installPgsql = function( installConfig ){
         var data = { 'cmd': 'add business', 'ConfigInfo': JSON.stringify( installConfig ) } ;
         SdbRest.OmOperation( data, {
            'success': function( taskInfo ){
               $rootScope.tempData( 'Deploy', 'ModuleTaskID', taskInfo[0]['TaskID'] ) ;
               $location.path( '/Deploy/Task/Module' ).search( { 'r': new Date().getTime() } ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  installPgsql( installConfig ) ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      function convertConfig()
      {
         var config = {} ;
         config['ClusterName']  = installConfig['ClusterName'] ;
         config['BusinessType'] = installConfig['BusinessType'] ;
         config['BusinessName'] = installConfig['BusinessName'] ;
         config['DeployMod']    = installConfig['DeployMod'] ;

         var firstErrFormName = null ;
         var allFormVal = {} ;
         var isAllClear = true ;
         $.each( $scope.AllForm, function( key, formInfo ){
            var inputErrNum = 0 ;
            if( key == 'normal' )
            {
               inputErrNum = formInfo.getErrNum( function( formVal ){
                  //检查普通页port、dbpath配置
                  var error = [] ;
                  if( checkPort( formVal['port'] ) == false )
                  {
                     error.push( { 'name': 'port', 'error': sprintf( $scope.autoLanguage( '?格式错误。' ), $scope.autoLanguage( '端口' ) ) } ) ;
                  }
                  if( formVal['dbpath'].length == 0 )
                  {
                     error.push( { 'name': 'dbpath', 'error': sprintf( $scope.autoLanguage( '?长度不能小于?。' ), $scope.autoLanguage( '数据路径' ), 1 ) } ) ;
                  }
                  return error ;
               } ) ;
               if( inputErrNum > 0 )
               {
                  if( isNull( firstErrFormName ) )
                  {
                     firstErrFormName = key ;
                  }
                  isAllClear = false ;
               }
            }
            else
            {
               inputErrNum = formInfo.getErrNum() ;
               if( inputErrNum > 0 )
               {
                  if( isNull( firstErrFormName ) )
                  {
                     firstErrFormName = key ;
                  }
                  isAllClear = false ;
               }
            }
            $scope.InputErrNum[key] = inputErrNum ;
         } ) ;

         if ( isAllClear == false )
         {
            $scope.SwitchParam( firstErrFormName ) ;
            $scope.AllForm[firstErrFormName].scrollToError( null, -10 ) ;
            return false ;
         }

         $.each( $scope.AllForm, function( key, formInfo ){
            var formVal = formInfo.getValue() ;

            $.each( formVal, function( key2, value ){
               if(  value.length > 0 || ( typeof( value ) == 'number' && isNaN( value ) == false ) )
               {
                  allFormVal[key2] = value ;
               }
            } ) ;
         } ) ;

         config['Config'] = [ {} ] ;
         $.each( allFormVal, function( key, value ){
            config['Config'][0][key] = value ;
         } ) ;

         return config ;
      }

      function isFilterDefaultItem( key, value ) {
         var result = false ;
         var unSkipList = [ 'HostName', 'dbpath', 'port' ] ;
         if ( unSkipList.indexOf( key ) >= 0 )
         {
            return false ;
         }
         $.each( $scope.Template, function( index, item ){
            if ( item['Name'] == key )
            {
               result = ( item['Default'] == value ) ;
               return false ;
            }
         } ) ;
         return result ;
      }

      $scope.GotoInstall = function(){
         var oldConfigure = convertConfig() ;
         if( oldConfigure == false )
         {
            return ;
         }
         var configure = {} ;
         $.each( oldConfigure, function( key, value ){
            configure[key] = value ;
         } ) ;
         configure['Config'] = [] ;
         $.each( oldConfigure['Config'], function( nodeIndex, nodeInfo ){
            var nodeConfig = {} ;
            $.each( nodeInfo, function( key, value ){
               if( value.length > 0 && isFilterDefaultItem( key, value ) == false )
               {
                   nodeConfig[key] = value ;
               }
            } ) ;
            configure['Config'].push( nodeConfig ) ;
         } ) ;
         if( configure )
            installPgsql( configure ) ;
      }

   } ) ;
}());