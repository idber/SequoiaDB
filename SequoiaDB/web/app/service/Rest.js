(function(){
   var sacApp = window.SdbSacManagerModule ;
   sacApp.service( 'SdbRest', function( $q, $rootScope, $location, Loading, SdbFunction ){
      var g = this ;
      function restBeforeSend( jqXHR )
      {
	      var id = SdbFunction.LocalData( 'SdbSessionID' ) ;
	      if( id !== null )
	      {
		      jqXHR.setRequestHeader( 'SdbSessionID', id ) ;
	      }
	      var language = SdbFunction.LocalData( 'SdbLanguage' )
	      if( language !== null )
	      {
		      jqXHR.setRequestHeader( 'SdbLanguage', language ) ;
	      }
      }

      //网络状态
      var NORMAL = 1 ;
      var INSTABLE = 2 ;
      var ERROR = 3 ;

      g._lastErrorEvent = null ;

      //网络当前状态
      g._status = NORMAL ;

      //网络连接错误的次数
      g._errorNum = 0 ;

      //执行器状态
      var IDLE = 0
      var RUNNING = 1 ;

      //执行器当前状态
      g._runStatus = IDLE ;

      //网络请求队列
      g._queue = [] ;

      //执行循环模块
      g._execLoop = function( task, loop ){
         if( task['scope'] === false || task['scope'] === $location.url() )
         {
            if( task['loop'] === loop )
            {
               if( task['delay'] > 0 )
               {
                  setTimeout( function(){
                     g._queue.push( task ) ;
                  }, task['delay'] ) ;
               }
               else
               {
                  g._queue.push( task ) ;
               }
            }
         }
      }

      //网络执行器
      g._run = function(){
         g._runStatus = RUNNING ;
         if( g._status == NORMAL )
         {
            if( g._queue.length > 0 )
            {
               var task = g._queue.shift() ;
               if( task['scope'] !== false && task['scope'] !== $location.url() )
               {
                  //作用域不是全页面 并且 已经切换页面了
                  setTimeout( g._run, 1 ) ;
                  return ;
               }
               if( typeof( task['init'] ) == 'function' )
               {
                  var value = task['init']() ;
                  if( typeof( value ) !== 'undefined' )
                  {
                     task['data'] = value ;
                  }
               }
               g._post( task['type'], task['url'], task['data'],
                        task['before'],
                        function( json, textStatus, jqXHR ){
                           g._errorNum = 0 ;
                           g._status = NORMAL ;
                           if( typeof( task['success'] ) == 'function' )
                           {
                              task['success']( json, textStatus, jqXHR ) ;
                           }
                           g._execLoop( task, 'success' ) ;
                        },
                        function( errInfo ){
                           g._errorNum = 0 ;
                           g._status = NORMAL ;
                           if( typeof( task['failed'] ) == 'function' )
                           {
                              task['failed']( errInfo, function(){
                                 g._queue.push( task ) ;
                              } ) ;
                           }
                           g._execLoop( task, 'failed' ) ;
                        },
                        function( XMLHttpRequest, textStatus, errorThrown ){
                           ++g._errorNum ;
                           g._status = INSTABLE ;
                           if( typeof( task['error'] ) == 'function' )
                           {
                              task['error']( XMLHttpRequest, textStatus, errorThrown ) ;
                           }
                           //g._queue.unshift( task ) ;
                           g._lastErrorEvent = task['failed'] ;
                        },
                        function( XMLHttpRequest, textStatus ){
                           if( typeof( task['complete'] ) == 'function' )
                           {
                              task['complete']( XMLHttpRequest, textStatus ) ;
                           }
                           g._execLoop( task, true ) ;
                        },
                        task['showLoading'], task['errJson'] ) ;
               setTimeout( g._run, 1 ) ;
            }
            else
            {
               setTimeout( g._run, 100 ) ;
            }
         }
         else
         {
            g.getPing( function( times ){
               if( times >= 0 )
               {
                  g._errorNum = 0 ;
                  g._status = NORMAL ;
                  if( typeof( g._lastErrorEvent ) == 'function' )
                  {
                     g._lastErrorEvent( { "errno": -15, "description": "Network error", "detail": "Network error, request status unknown." } ) ;
                     g._lastErrorEvent = null ;
                  }
               }
               else
               {
                  ++g._errorNum ;
                  if( g._errorNum >= 10 )
                  {
                     g._status = ERROR ;
                  }
               }
               setTimeout( g._run, 1000 ) ;
            } ) ;
         }
      }

      //获取网络状态
      g.getNetworkStatus = function(){
         return g._status ;
      }

      /*
         网络调度器
         type: POST,GET
         url: 路径
         data: post的数据
         event: 事件
               init     初始化      init的返回值将会代替data的值
               before   post前
               success  成功
               failed   失败
               error    错误
               complete 完成
         options 选项
               delay: 延迟多少毫秒
               loop: 循环; 'success':执行成功时循环 'failed':执行失败时循环 true:成功失败都循环
               scope: 作用域; false:所有页面  true:当前页面, 默认是true
      */
      g._insert = function( type, url, data, event, options, errJson ){
         if( typeof( options ) != 'object' || options === null )
         {
            options = {} ;
         }
         if( typeof( options['delay'] ) != 'number' )
         {
            options['delay'] = 0 ;
         }
         if( typeof( options['scope'] ) == 'undefined' )
         {
            options['scope'] = $location.url() ;
         }
         else
         {
            options['scope'] = options['scope'] ? $location.url() : false ;
         }
         var task = $.extend( {}, { 'type': type, 'url': url, 'data': data }, event, options ) ;
         task['errJson'] = errJson ;
         if( task['delay'] > 0 && !task['loop'] )
         {
            setTimeout( function(){
               g._queue.push( task ) ;
            }, task['delay'] ) ;
         }
         else
         {
            g._queue.push( task ) ;
         }
         if( g._runStatus == IDLE )
         {
            g._run() ;
         }
      }

      //发送请求
      g._post = function( type, url, data, before, success, failed, error, complete, showLoading, errJson ){
         if( typeof( showLoading ) == 'undefined' ) showLoading = true ;
         if( showLoading )
         {
            Loading.create() ;
         }
         $.ajax( { 'type': type, 'url': url, 'data': data, 'success': function( json, textStatus, jqXHR ){
            json = trim( json ) ;
            if( json.length == 0 && typeof( failed ) === 'function' )
            {
               //收到响应，但是没有任何数据
               try
               {
                  failed( { "errno": -10, "description": "System error", "detail": "No rest response data." } ) ;
               }
               catch( e )
               {
                  printfDebug( e.stack ) ;
                  $rootScope.Components.Confirm.isShow = true ;
                  $rootScope.Components.Confirm.type = 1 ;
                  $rootScope.Components.Confirm.title = 'System error' ;
                  $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                  $rootScope.Components.Confirm.ok = function(){
                     Loading.close() ;
                     $rootScope.Components.Confirm.isShow = false ;
                  }
               }
            }
            else
            {
               var jsonArr = g._parseJsons( json, errJson ) ;
               if( jsonArr.length == 0 )
               {
                  //有数据，但是没有记录，理论上不会发生
                  if( typeof( failed ) === 'function' )
                  {
                     try
                     {
                        failed( { "errno": -10, "description": "System error", "detail": "Rest response data error." } ) ;
                     }
                     catch( e )
                     {
                        printfDebug( e.stack ) ;
                        $rootScope.Components.Confirm.isShow = true ;
                        $rootScope.Components.Confirm.type = 1 ;
                        $rootScope.Components.Confirm.title = 'System error' ;
                        $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                        $rootScope.Components.Confirm.ok = function(){
                           Loading.close() ;
                           $rootScope.Components.Confirm.isShow = false ;
                        }
                     }
                  }
               }
               else if( jsonArr[0]['errno'] === 0 && typeof( success ) == 'function' )
               {
                  if( isArray( errJson ) )
                  {
                     errJson.splice( 0, 1 ) ;
                  }
                  jsonArr.splice( 0, 1 ) ;
                  try
                  {
                     success( jsonArr, textStatus, jqXHR ) ;
                  }
                  catch( e )
                  {
                     printfDebug( e.stack ) ;
                     $rootScope.Components.Confirm.isShow = true ;
                     $rootScope.Components.Confirm.type = 1 ;
                     $rootScope.Components.Confirm.title = 'System error' ;
                     $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                     $rootScope.Components.Confirm.ok = function(){
                        Loading.close() ;
                        $rootScope.Components.Confirm.isShow = false ;
                     }
                  }
               }
               else if( jsonArr[0]['errno'] === -62 )
               {
                  //session id 不存在
                  window.location.href = './login.html#/Login' ;
               }
               else if( typeof( failed ) === 'function' )
               {
                  //其他错误
                  try
                  {
                     failed( jsonArr[0] ) ;
                  }
                  catch( e )
                  {
                     printfDebug( e.stack ) ;
                     $rootScope.Components.Confirm.isShow = true ;
                     $rootScope.Components.Confirm.type = 1 ;
                     $rootScope.Components.Confirm.title = 'System error' ;
                     $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                     $rootScope.Components.Confirm.ok = function(){
                        Loading.close() ;
                        $rootScope.Components.Confirm.isShow = false ;
                     }
                  }
               }
            }
         }, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
            if( typeof( error ) === 'function' )
            {
               try
               {
                  error( XMLHttpRequest, textStatus, errorThrown ) ;
               }
               catch( e )
               {
                  printfDebug( e.stack ) ;
                  $rootScope.Components.Confirm.isShow = true ;
                  $rootScope.Components.Confirm.type = 1 ;
                  $rootScope.Components.Confirm.title = 'System error' ;
                  $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                  $rootScope.Components.Confirm.ok = function(){
                     Loading.close() ;
                     $rootScope.Components.Confirm.isShow = false ;
                  }
               }
            }
         }, 'complete': function ( XMLHttpRequest, textStatus ) {
            if( typeof( complete ) == 'function' )
            {
               try
               {
                  complete( XMLHttpRequest, textStatus ) ;
               }
               catch( e )
               {
                  printfDebug( e.stack ) ;
                  $rootScope.Components.Confirm.isShow = true ;
                  $rootScope.Components.Confirm.type = 1 ;
                  $rootScope.Components.Confirm.title = 'System error' ;
                  $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                  $rootScope.Components.Confirm.ok = function(){
                     Loading.close() ;
                     $rootScope.Components.Confirm.isShow = false ;
                  }
               }
            }
            if( showLoading )
            {
               Loading.cancel() ;
            }
         }, 'beforeSend': function( XMLHttpRequest ){
            restBeforeSend( XMLHttpRequest ) ;
            if( typeof( before ) === 'function' )
            {
               try
               {
                  return before( XMLHttpRequest ) ;
               }
               catch( e )
               {
                  printfDebug( e.stack ) ;
                  $rootScope.Components.Confirm.isShow = true ;
                  $rootScope.Components.Confirm.type = 1 ;
                  $rootScope.Components.Confirm.title = 'System error' ;
                  $rootScope.Components.Confirm.context = 'Javascript error: ' + e.message ;
                  $rootScope.Components.Confirm.ok = function(){
                     Loading.close() ;
                     $rootScope.Components.Confirm.isShow = false ;
                  }
               }
            }
         } } ) ;
      }

      //发送请求(废弃)
      g._post2 = function( data, showLoading ){
         var defferred = $q.defer() ;
         if( typeof( showLoading ) == 'undefined' ) showLoading = true ;
         if( showLoading )
         {
            Loading.create() ;
         }
         $.ajax( { 'type': 'POST', 'url': '/', 'data': data, 'success': function( json, textStatus, jqXHR ){
            json = trim( json ) ;
            if( json.length == 0 )
            {
               //收到响应，但是没有任何数据
               defferred.reject( { "errno": -10, "description": "System error", "detail": "No rest response data." } ) ;
            }
            else
            {
               var jsonArr = g._parseJsons( json ) ;
               if( jsonArr.length == 0 )
               {
                  //有数据，但是没有记录，理论上不会发生
                  defferred.reject( { "errno": -10, "description": "System error", "detail": "Rest response data error." } ) ;
               }
               else if( jsonArr[0]['errno'] === 0 )
               {
                  jsonArr.splice( 0, 1 ) ;
                  defferred.resolve( jsonArr ) ;
               }
               else if( jsonArr[0]['errno'] === -62 )
               {
                  //session id 不存在
                  window.location.href = './login.html#/Login' ;
               }
               else
               {
                  //其他错误
                  defferred.reject( jsonArr[0] ) ;
               }
            }
         }, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
            defferred.reject() ;
         }, 'complete': function ( XMLHttpRequest, textStatus ) {
            if( showLoading )
            {
               Loading.cancel() ;
            }
         }, 'beforeSend': function( XMLHttpRequest ){
            restBeforeSend( XMLHttpRequest ) ;
         } } ) ;
         return defferred.promise ;
      }

      //测试发送(测试用)
      g._postTest = function( url, success, failed, error )
      {
         //Loading.create() ;
         g.getFile( url, true, function( jsons ){
            var jsonList = g._parseJsons( jsons ) ;
            if( jsonList[0]['errno'] == 0 )
            {
               jsonList.splice( 0, 1 ) ;
               if( typeof( success ) == 'function' )
               {
                  success( jsonList ) ;
               }
            }
            else
            {
               if( typeof( failed ) == 'function' )
               {
                  failed( jsonList[0] ) ;
               }
            }
            //Loading.cancel() ;
         } ) ;
      }

      //解析响应的Json
      g._parseJsons = function( str, errJson )
      {
	      var json_array = [] ;
	      var i = 0, len = str.length ;
	      var chars, level, isEsc, isString, start, end, subStr, json, errType ;
         errType = isArray( errJson ) ;
	      while( i < len )
	      {
		      while( i < len ){	chars = str.charAt( i ) ;	if( chars === '{' ){	break ;	}	++i ;	}
		      level = 0, isEsc = false, isString = false, start = i ;
		      while( i < len )
		      {
               var isJson = true ;
			      chars = str.charAt( i ) ;
			      if( isEsc ){	isEsc = false ;	}
			      else
			      {
				      if( ( chars === '{' || chars === '[' ) && isString === false ){	++level ;	}
				      else if( ( chars === '}' || chars === ']' ) && isString === false )
				      {
					      --level ;
					      if( level === 0 )
					      {
						      ++i ;
						      end = i ;
						      subStr = str.substring( start, end ) ;
                        try{
                           json = JSON.parse( subStr ) ;
                        }catch(e){
                           isJson = false ;
                           json = { " ": subStr } ;
                        }
                        if( errType == true )
                        {
                           errJson.push( !isJson ) ;
                        }
						      json_array.push( json ) ;
						      break ;
					      }
				      }
				      else if( chars === '"' ){	isString = !isString ;	}
				      else if( chars === '\\' ){	isEsc = true ;	}
			      }
			      ++i ;
		      }
	      }
	      return json_array ;
      }

      //获取文件
      g.getFile = function( url, async, success, error )
      {
         $.ajax( {
            'async': async,
            'url': url,
            'type': 'GET',
            'dataType': 'text',
            'success': success,
            'error': error
         } ) ;
      }

      //获取配置文件(废弃)
      g.getConfig = function( fileName, success )
      {
         var language = SdbFunction.LocalData( 'SdbLanguage' ) ;
         var newUrl = './config/' + fileName + '_' + language ;
         $.get( newUrl, {}, function( reData ){
            success( reData ) ;
         }, 'json' ) ;
      }

      //获取语言文件
      g.getLanguage = function( fileName, success )
      {
         var language = SdbFunction.LocalData( 'SdbLanguage' ) ;
         var newUrl = './app/language/' + fileName + '_' + language ;
         $.get( newUrl, {}, function( reData ){
            success( reData ) ;
         }, 'json' ) ;
      }
      
      //om系统操作
      g.OmOperation = function( data, event, options ){
         g._insert( 'POST', '/', data, event, options ) ;
      }

      //数据操作
      g.DataOperation = function( data, success, failed, error, complete, errJson, showLoading ){
         g._insert( 'POST', '/', data, {
            'before': function( jqXHR ){
	            var clusterName = SdbFunction.LocalData( 'SdbClusterName' ) ;
	            if( clusterName !== null )
	            {
		            jqXHR.setRequestHeader( 'SdbClusterName', clusterName ) ;
	            }
	            var businessName = SdbFunction.LocalData( 'SdbModuleName' )
	            if( businessName !== null )
	            {
		            jqXHR.setRequestHeader( 'SdbBusinessName', businessName ) ;
	            }
            },
            'success': success,
            'failed': failed,
            'error': error,
            'complete': complete
         }, {
            'showLoading': showLoading,
            'delay': -1,
            'loop': false
         }, errJson ) ;
      }

      //数据操作( 手工设置cluster和module )
      g.DataOperation2 = function( clusterName, businessName, data, event, options, errJson ){
         var oldBefore = event ? event['before'] : null ;
         event['before'] = function( jqXHR ){
	         if( clusterName !== null )
	         {
		         jqXHR.setRequestHeader( 'SdbClusterName', clusterName ) ;
	         }
	         if( businessName !== null )
	         {
		         jqXHR.setRequestHeader( 'SdbBusinessName', businessName ) ;
	         }
            if( typeof( oldBefore ) == 'function' )
            {
               return oldBefore( jqXHR ) ;
            }
         }
         g._insert( 'POST', '/', data, event, options, errJson ) ;
      }

      //sequoiasql操作
      g.SequoiaSQL = function( data, success, failed, error, complete ){
         var clusterName = SdbFunction.LocalData( 'SdbClusterName' ) ;
	      if( clusterName != null )
	      {
		      data['ClusterName'] = clusterName ;
	      }
	      var businessName = SdbFunction.LocalData( 'SdbModuleName' )
	      if( businessName != null )
	      {
            data['BusinessName'] = businessName ;
	      }
         Loading.create() ;
         g._insert( 'POST', '/', data, {
            'success': function( returnData ){
               var taskID = returnData[0] ;
               var queryTask = function(){
                  var taskData = { 'cmd': 'query task', 'filter': JSON.stringify( taskID ) } ;
                  g._insert( 'POST', '/', taskData, {
                     'success': function( taskInfo ){
                        if( taskInfo[0]['Status'] == 0 )
                        {
                           queryTask()
                           return ;
                        }
                        if( taskInfo[0]['Status'] == 3 || taskInfo[0]['Status'] == 4 )
                        {
                           success( taskInfo[0]['ResultInfo'], true ) ;
                           Loading.cancel() ;
                           return ;
                        }
                        success( taskInfo[0]['ResultInfo'], false ) ;
                        setTimeout( queryTask, 100 ) ;
                     },
                     'failed': function( errorInfo ){
                        Loading.cancel() ;
                        if( typeof( failed ) == 'function')
                        {
                           failed( errorInfo ) ;
                        }
                     },
                     'error': function( XMLHttpRequest, textStatus, errorThrown ){
                        Loading.cancel() ;
                        if( typeof( error ) === 'function' )
                        {
                           error( XMLHttpRequest, textStatus, errorThrown ) ;
                        }
                     },
                     'comolete': complete
                  }, {
                     'showLoading': false
                  } ) ;
               }
               queryTask() ;
            },
            'failed': function( errorInfo ){
               Loading.cancel() ;
               if( typeof( failed ) == 'function')
               {
                  failed( errorInfo ) ;
               }
            },
            'error': function( XMLHttpRequest, textStatus, errorThrown ){
               Loading.cancel() ;
               if( typeof( error ) === 'function' )
               {
                  error( XMLHttpRequest, textStatus, errorThrown ) ;
               }
            },
            'complete': complete
         } ) ;
      }

      //SQL(自动获取cluster和module)
      g.Exec = function( sql, event, options ){
         var data = { 'cmd': 'exec', 'sql': sql } ;
         var oldBefore = event ? event['before'] : null ;
         event['before'] = function( jqXHR ){
	         var clusterName = SdbFunction.LocalData( 'SdbClusterName' ) ;
	         if( clusterName !== null )
	         {
		         jqXHR.setRequestHeader( 'SdbClusterName', clusterName ) ;
	         }
	         var businessName = SdbFunction.LocalData( 'SdbModuleName' )
	         if( businessName !== null )
	         {
		         jqXHR.setRequestHeader( 'SdbBusinessName', businessName ) ;
	         }
            if( typeof( oldBefore ) == 'function' )
            {
               return oldBefore( jqXHR ) ;
            }
         }
         g._insert( 'POST', '/', data, event, options ) ;
      }

      //SQL(手工设置cluster和module)
      g.Exec2 = function( clusterName, businessName, sql, event, options ){
         var data = { 'cmd': 'exec', 'sql': sql } ;
         var oldBefore = event ? event['before'] : null ;
         event['before'] = function( jqXHR ){
	         if( clusterName !== null )
	         {
		         jqXHR.setRequestHeader( 'SdbClusterName', clusterName ) ;
	         }
	         if( businessName !== null )
	         {
		         jqXHR.setRequestHeader( 'SdbBusinessName', businessName ) ;
	         }
            if( typeof( oldBefore ) == 'function' )
            {
               return oldBefore( jqXHR ) ;
            }
         }
         g._insert( 'POST', '/', data, event, options ) ;
      }

      //登录
      g.Login = function( username, password, success, failed, error, complete ){
         password = $.md5( password ) ;
         var timestamp = parseInt( ( new Date().getTime() ) / 1000 ) ;
	      var data = { 'cmd' : 'login', 'user': username, 'passwd': password, 'Timestamp': timestamp } ;
         g._post( 'POST', '/', data, null, success, failed, error, complete, false ) ;
      }

      //修改密码
      g.ChangePasswd = function( username, password, newPassword, success, failed, error, complete ){
         var timestamp = parseInt( ( new Date().getTime() ) / 1000 ) ;
         password = $.md5( password ) ;
         newPassword = $.md5( newPassword ) ;
	      var data = { 'cmd' : 'change passwd', 'User': username, 'Passwd': password, 'Newpasswd': newPassword, 'Timestamp': timestamp } ;
         g._insert( 'POST', '/', data, {
            'success': success,
            'failed': failed,
            'error': error,
            'complete': complete
         }, {
            'showLoading': false
         } ) ;
      }

      g.getPing = function( complete ){
         var time1 = $.now() ;
         g.getFile( './app/language/test', true, function( text ){
            var time2 = $.now() ;
            complete( time2 - time1 ) ;
         }, function(){
            complete( -1 ) ;
         } ) ;
      }

      g.GetLog = function( data, success, error, complete, showLoading ){
         if( typeof( showLoading ) == 'undefined' ) showLoading = true ;
         if( showLoading )
         {
            Loading.create() ;
         }
         $.ajax( { 'type': 'POST', 'url': '/', 'data': data, 'success': function( text, textStatus, jqXHR ){
            if( typeof( success ) === 'function' ) success( text, textStatus, jqXHR ) ;
         }, 'error': function( XMLHttpRequest, textStatus, errorThrown ) {
            if( typeof( error ) === 'function' ) error( XMLHttpRequest, textStatus, errorThrown ) ;
         }, 'complete': function ( XMLHttpRequest, textStatus ) {
            if( typeof( complete ) == 'function' ) complete( XMLHttpRequest, textStatus ) ;
            if( showLoading )
            {
               Loading.cancel() ;
            }
         }, 'beforeSend': function( XMLHttpRequest ){
            restBeforeSend( XMLHttpRequest ) ;
         } } ) ;
      }
   } ) ;
}());