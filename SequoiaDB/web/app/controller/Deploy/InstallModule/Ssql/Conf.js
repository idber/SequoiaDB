(function(){
   var sacApp = window.SdbSacManagerModule ;
   //控制器
   sacApp.controllerProvider.register( 'Deploy.Ssql.Conf.Ctrl', function( $scope, $compile, $location, $rootScope, SdbRest ){

      //初始化
      $scope.IsAllClear = true ;
      $scope.Conf1 = {} ;
      $scope.Conf2 = {} ;
      $scope.HostList = [] ;
      $scope.templateList = [] ;
      $scope.ConfForm1 = { 'keyWidth': '160px', 'inputList': [] } ;
      $scope.ConfForm2 = { 'keyWidth': '160px', 'inputList': [] } ;
      $scope.currentTemplate  = {} ;
      $scope.RedundancyChart = {} ;
      $scope.RedundancyChart['options'] = $.extend( true, {}, window.SdbSacManagerConf.RedundancyChart ) ;
      $scope.HostGridOptions = { 'titleWidth': [ '30px', 50, 50 ] } ;

      $scope.DeployType  = $rootScope.tempData( 'Deploy', 'Model' ) ;
      var clusterName    = $rootScope.tempData( 'Deploy', 'ClusterName' ) ;
      $scope.ModuleName  = $rootScope.tempData( 'Deploy', 'ModuleName' ) ;

      /*
      $scope.DeployType = 'Install_Module' ;
      clusterName = 'myCluster1' ;
      $scope.ModuleName = 'myModule2' ;
      */

      if( $scope.DeployType == null || clusterName == null || $scope.ModuleName == null )
      {
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }

      $scope.stepList = _Deploy.BuildSdbStep( $scope, $location, $scope.DeployType, $scope['Url']['Action'], 'sequoiasql' ) ;
      if( $scope.stepList['info'].length == 0 )
      {
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
         return ;
      }

      //获取主机列表
      var getHostList = function(){
         var filter = { "ClusterName": clusterName } ;
         var data = { 'cmd': 'query host', 'filter': JSON.stringify( filter ) } ;
         SdbRest.OmOperation( data, {
            'success': function( hostList ){
               $scope.HostList = hostList ;
               $scope.Conf1['DiskNum'] = 0 ;
               $.each( $scope.HostList, function( index, hostInfo ){
                  $scope.HostList[index]['checked'] = true ;
                  $.each( hostInfo['Disk'], function( index2, diskInfo ){
                     if( diskInfo['IsLocal'] == true )
                     {
                        ++$scope.Conf1['DiskNum'] ;
                     }
                  } ) ;
               } )  ;
               getBusinessTemplate() ;
               $scope.$apply() ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getHostList() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //获取业务模板
      var getBusinessTemplate = function(){
         var data = { 'cmd': 'get business template', 'BusinessType': 'sequoiasql' } ;
         SdbRest.OmOperation( data, {
            'success': function( templateList ){
               $.each( templateList, function( index ){
                  templateList[index]['Property'] = _Deploy.ConvertTemplate( templateList[index]['Property'] ) ;
               } ) ;
               var confForm = {
                  'keyWidth': '160px',
                  'inputList': [
                     {
                        "name": "type",
                        "webName": $scope.autoLanguage( '部署模式' ),
                        "type": "select",
                        "value": "",
                        "valid": []
                     }
                  ]
               } ;
               $.each( templateList, function( index, template ){
                  if( index == 0 )
                  {
                     confForm['inputList'][0]['value'] = template['DeployMod'] ;
                     $scope.ConfForm2 = { 'keyWidth': '160px', 'inputList': template['Property'] } ;
                  }
                  confForm['inputList'][0]['valid'].push( { 'key': template['WebName'], 'value': template['DeployMod'] } ) ;
               } ) ;
               $scope.templateList = templateList ;
               $scope.ConfForm1 = confForm ;
               $scope.$apply() ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getBusinessTemplate() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //返回
      $scope.GotoDeploy = function(){
         $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
      }

      //上一步
      $scope.GotoScanHost = function(){
         $location.path( '/Deploy/Install' ) ;
      }

      //获取配置
      var getModuleConfig = function( Configure ){
         var data = { 'cmd': 'get business config', 'TemplateInfo': JSON.stringify( Configure ) } ;
         SdbRest.OmOperation( data, {
            'success': function(){
               $rootScope.tempData( 'Deploy', 'ModuleConfig', Configure ) ;
               $location.path( '/Deploy/SSQL-Mod' ).search( { 'r': new Date().getTime() } ) ;
            },
            'failed': function( errorInfo ){
               _IndexPublic.createRetryModel( $scope, errorInfo, function(){
                  getModuleConfig() ;
                  return true ;
               } ) ;
            }
         } ) ;
      }

      //下一步
      $scope.GotoModSsql = function(){
         var isAllClear1 = $scope.ConfForm1.check() ;
         var isAllClear2 = $scope.ConfForm2.check() ;
         $scope.IsAllClear = isAllClear1 && isAllClear2 ;
         if( isAllClear1 && isAllClear2 )
         {
            var formVal1 = $scope.ConfForm1.getValue() ;
            var formVal2 = $scope.ConfForm2.getValue() ;
            var tempHostInfo = [] ;
			   $.each( $scope.HostList, function( index, value ){
               if( value['checked'] == true )
               {
				      tempHostInfo.push( { 'HostName': value['HostName'] } ) ;
               }
			   } ) ;
            if( tempHostInfo.length == 0 )
            {
               $.each( $scope.HostList, function( index, value ){
				      tempHostInfo.push( { 'HostName': value['HostName'] } ) ;
			      } ) ;
            }
            var businessConf = {} ;
            businessConf['ClusterName']  = clusterName ;
            businessConf['BusinessName'] = $scope.ModuleName ;
            businessConf['BusinessType'] = 'sequoiasql' ;
            businessConf['DeployMod'] = formVal1['type'] ;
            businessConf['Property'] = [
               { "Name": "deploy_standby", "Value": formVal2['deploy_standby'] },
               { "Name": "segment_num", "Value": formVal2['segment_num'] + '' }
            ] ;
            businessConf['HostInfo'] = tempHostInfo ;
            //$rootScope.tempData( 'Deploy', 'ModuleConfig', businessConf ) ;
            getModuleConfig( businessConf ) ;
         }
      }

      $scope.SelectAll = function(){
         $.each( $scope.HostList, function( index ){
            $scope.HostList[index]['checked'] = true ;
         } ) ;
      }

      $scope.Unselect = function(){
         $.each( $scope.HostList, function( index ){
            $scope.HostList[index]['checked'] = !$scope.HostList[index]['checked'] ;
         } ) ;
      }
  
      //创建 选择安装业务的主机 弹窗
      $scope.CreateSwitchHostModel = function(){
         var hostBox = null ;
         var grid = null ;
         var tempHostList = $.extend( true, [], $scope.HostList ) ;
         $scope.Components.Modal.icon = '' ;
         $scope.Components.Modal.title = $scope.autoLanguage( '主机列表' ) ;
         $scope.Components.Modal.isShow = true ;
         $scope.Components.Modal.Context = function( bodyEle ){
            var div  = $( '<div></div>' ) ;
            var btn1 = $compile( '<button ng-click="SelectAll()"></button>' )( $scope ).addClass( 'btn btn-default' ).text( $scope.autoLanguage( '全选' ) ) ;
            var btn2 = $compile( '<button ng-click="Unselect()"></button>' )( $scope ).addClass( 'btn btn-default' ).text( $scope.autoLanguage( '反选' ) ) ;
            div.append( btn1 ).append( '&nbsp;' ).append( btn2 ) ;

            hostBox = $( '<div></div>' ).css( { 'marginTop': '10px' } ) ;

            grid = $compile( '\
<div class="Grid" style="border-bottom:1px solid #E3E7E8;" ng-grid="HostGridOptions"">\
   <div class="GridHeader">\
      <div class="GridTr">\
         <div class="GridTd Ellipsis"></div>\
         <div class="GridTd Ellipsis">{{autoLanguage("主机名")}}</div>\
         <div class="GridTd Ellipsis">{{autoLanguage("IP地址")}}</div>\
         <div class="clear-float"></div>\
      </div>\
   </div>\
   <div class="GridBody">\
      <div class="GridTr" ng-repeat="hostInfo in HostList track by $index">\
         <div class="GridTd Ellipsis" style="word-break:break-all;">\
            <input type="checkbox" ng-model="HostList[$index][\'checked\']"/>\
         </div>\
         <div class="GridTd Ellipsis" style="word-break:break-all;">{{hostInfo[\'HostName\']}}</div>\
         <div class="GridTd Ellipsis" style="word-break:break-all;">{{hostInfo[\'IP\']}}</div>\
         <div class="clear-float"></div>\
      </div>\
   </div>\
</div>' )( $scope ) ;

            hostBox.append( grid ) ;

            $compile( bodyEle )( $scope ).append( div ).append( hostBox ) ;
            $scope.$apply() ;
         }
         $scope.Components.Modal.onResize = function( width, height ){
            $( grid ).css( {
               'width': width - 10,
               'max-height': height - 40
            } ) ;
            $scope.bindResize() ;
         }
         $scope.Components.Modal.ok = function(){
            return true ;
         }
         $scope.Components.Modal.close = function(){
            $scope.HostList = tempHostList ;
            return true ;
         }
      }

      getHostList() ;

   } ) ;
}());