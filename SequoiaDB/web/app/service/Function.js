(function () {
   var sacApp = window.SdbSacManagerModule;
   sacApp.service( 'SdbFunction', function( $rootScope, $window ) {
      var g = this;
     
      //获取json的键列表
      g.getJsonKeys = function( json, maxKey, keyList, keyWord )
      {
         if( typeof( maxKey ) == 'undefined' ) maxKey = 0 ;
         if( typeof( keyList ) == 'undefined' ) keyList = [] ;
         if( typeof( keyWord ) == 'undefined' ) keyWord = '' ;
         if( keyWord.length > 0 ) keyWord += '.' ;
         if( keyList.length >= maxKey && maxKey > 0 ) return keyList ;
         $.each( json, function( key, value ){
            if( keyList.length >= maxKey && maxKey > 0 ) return false ;
            var newKey = keyWord + key ;
            var valueType = typeof (value);
            if( valueType == 'object' )
            {
               if( value == null )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( isArray( value ) )
               {
                  keyList = g.getJsonKeys( value, maxKey, keyList, newKey ) ;
               }
               else if( typeof( value['$binary'] ) == 'string' && typeof( value['$type'] ) == 'string' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$timestamp'] ) == 'string' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$date'] ) == 'string' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$code'] ) == 'string' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$minKey'] ) == 'number' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$maxKey'] ) == 'number')
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$undefined'] ) == 'number' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$oid'] ) == 'string' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else if( typeof( value['$regex'] ) == 'string' && typeof( value['$options'] ) == 'string' )
               {
                  if( $.inArray( newKey, keyList ) == -1 )
                  {
                     keyList.push( newKey ) ;
                  }
               }
               else
               {
                  keyList = g.getJsonKeys( value, maxKey, keyList, newKey ) ;
               }
            }
            else
            {
               if( $.inArray( newKey, keyList ) == -1 )
               {
                  keyList.push( newKey ) ;
               }
            }
         } ) ;
         return keyList ;
      }

      //获取json的值
      g.getJsonValues = function( json, keyList, valueList ){
         function getFieldValue( json2, key )
         {
            var pointIndex = key.indexOf( '.' ) ;
            if( pointIndex > 0 )
            {
               var fields = key.split( '.', 2 ) ;
               if( typeof( json2[ fields[0] ] ) == 'undefined' )
               {
                  return '' ;
               }
               else
               {
                  return getFieldValue( json2[ fields[0] ], key.substr( pointIndex + 1 ) ) ;
               }
            }
            else
            {
               var value = json2[ key ] ;
               var valueType = typeof( value ) ;
               if( valueType == 'object' )
               {
                  if( value == null )
                  {
                     return 'null' ;
                  }
                  else if( typeof( value['$binary'] ) == 'string' && typeof( value['$type'] ) == 'string' )
                  {
                     return value['$binary'] ;
                  }
                  else if( typeof( value['$timestamp'] ) == 'string' )
                  {
                      return value['$timestamp'] ;
                  }
                  else if( typeof( value['$date'] ) == 'string' )
                  {
                      return value['$date'] ;
                  }
                  else if( typeof( value['$code'] ) == 'string' )
                  {
                     return value['$code'] ;
                  }
                  else if( typeof( value['$minKey'] ) == 'number' )
                  {
                     return 'minKey' ;
                  }
                  else if( typeof( value['$maxKey'] ) == 'number')
                  {
                     return 'maxKey' ;
                  }
                  else if( typeof( value['$undefined'] ) == 'number' )
                  {
                     return 'undefined' ;
                  }
                  else if( typeof( value['$oid'] ) == 'string' )
                  {
                     return value['$oid'] ;
                  }
                  else if( typeof( value['$regex'] ) == 'string' && typeof( value['$options'] ) == 'string' )
                  {
                     return value['$regex'] ;
                  }
                  else if( isArray( value ) )
                  {
                     return '[ Array ]' ;
                  }
                  else
                  {
                     return '[ Object ]' ;
                  }
               }
               else if( valueType == 'boolean' )
               {
                  var newVal = value ? 'true' : 'false' ;
                  return newVal ;
               }
               else
               {
                  return value ;
               }
            }
         }
         if( typeof( valueList ) == 'undefined' ) valueList = [] ;
         $.each( keyList, function( index, key ){
            if( key == '' )
            {
               valueList.push( '' ) ;
               return true ;
            }
            var value = getFieldValue( json, key ) ;
            valueList.push( value ) ;
         } ) ;
         return valueList ;
      }

      //获取浏览器类型和版本
      g.getBrowserInfo = function()
      {
	      var agent = window.navigator.userAgent.toLowerCase() ;
	      var regStr_ie = /msie [\d.]+;/gi ;
	      var regStr_ff = /firefox\/[\d.]+/gi ;
	      var regStr_chrome = /chrome\/[\d.]+/gi ;
	      var regStr_saf = /safari\/[\d.]+/gi ;
	      var temp = '' ;
	      var info = [] ;
	      if( agent.indexOf( 'msie' ) > 0 )
	      {
		      temp = agent.match( regStr_ie ) ;
		      info.push( 'ie' ) ;
	      }
	      else if( agent.indexOf( 'firefox' ) > 0 )
	      {
		      temp = agent.match( regStr_ff ) ;
		      info.push( 'firefox' ) ;
	      }
	      else if( agent.indexOf( 'chrome' ) > 0 )
	      {
		      temp = agent.match( regStr_chrome ) ;
		      info.push( 'chrome' ) ;
	      }
	      else if( agent.indexOf( 'safari' ) > 0 && agent.indexOf( 'chrome' ) < 0 )
	      {
		      temp = agent.match( regStr_saf ) ;
		      info.push( 'safari' ) ;
	      }
	      else
	      {
		      if( agent.indexOf( 'trident' ) > 0 && agent.indexOf( 'rv' ) > 0 )
		      {
			      info.push( 'ie' ) ;
			      temp = '11' ;
		      }
		      else
		      {
			      temp = '0' ;
			      info.push( 'unknow' ) ;
		      }
	      }
	      verinfo = ( temp + '' ).replace( /[^0-9.]/ig, '' ) ;
	      info.push( parseInt( verinfo ) ) ;
	      return info ;
      }

      //判断浏览器可以使用什么存储方式
      g._userdata = {} ;
      g.setBrowserStorage = function()
      {
	      var browser = g.getBrowserInfo() ;
         var storageType ;
	      if( browser[0] === 'ie' && browser[1] <= 7 )
	      {
            storageType = 'cookie' ;
	      }
	      else
	      {
		      if( window.localStorage )
		      {
			      storageType = 'localStorage' ;
		      }
		      else
		      {
			      if( navigator.cookieEnabled === true )
			      {
				      storageType = 'cookie' ;
			      }
			      else
			      {
				      storageType = '' ;
			      }
		      }
	      }
	      return storageType ;
      }

      g.storageType = g.setBrowserStorage() ;
      //本地数据操作
      g.LocalData = function( key, value ){
         if( typeof( value ) == 'undefined' )
         {
            //读取本地数据
            var newValue = null ;
	         if ( g.storageType === 'localStorage' )
	         {
		         newValue = window.localStorage.getItem( key ) ;
	         }
	         else if ( g.storageType === 'cookie' )
	         {
		         newValue = $.cookie( key ) ;
	         }
	         return newValue ;
         }
         else if( value == null )
         {
            //删除本地数据
            if ( g.storageType === 'localStorage' )
	         {
		         window.localStorage.removeItem( key ) ;
	         }
	         else if ( g.storageType === 'cookie' )
	         {
		         $.removeCookie( key ) ;
	         }
         }
         else
         {
            //写入本地数据
            if ( g.storageType === 'localStorage' )
	         {
		         window.localStorage.setItem( key, value ) ;
	         }
	         else if ( g.storageType === 'cookie' )
	         {
		         var saveTime = new Date() ;
		         saveTime.setDate( saveTime.getDate() + 365 ) ;
		         $.cookie( key, value, { 'expires': saveTime } ) ;
	         }
         }
      }

      //定时器
      g.Timeout = function( execFun, delay, isApply ){
         var timer = setTimeout( function(){
            if( timer != null )
            {
               clearTimeout( timer ) ;
               timer = null ;
            }
            execFun() ;
            if( isApply )
            {
               $rootScope.$apply() ;
            }
         }, delay ) ;
         $rootScope.$on( '$locationChangeStart', function( event, newUrl, oldUrl ){
            if( timer != null )
            {
               clearTimeout( timer ) ;
               timer = null ;
            }
         } ) ;
      }

      //周期定时器
      g.Interval = function( execFun, delay, isApply ){
         var timer = setInterval( function(){
            execFun() ;
            if( isApply )
            {
               $rootScope.$apply() ;
            }
         }, delay ) ;
         $rootScope.$on( '$locationChangeStart', function( event, newUrl, oldUrl ){
            if( timer != null )
            {
               clearInterval( timer ) ;
               timer = null ;
            }
         } ) ;
      }

      //监控onResize
      var listOfResizeFun = [] ;
      $rootScope.$watch( 'onResize', function(){
         var length = listOfResizeFun.length ;
         for( var i = 0; i < length; ++i )
         {
            listOfResizeFun[i]['fun']() ;
         }
      } ) ;

      //解除绑定defer
      var unbindDefer = function( scope ){
         var length = listOfResizeFun.length ;
         for( var i = 0; i < length; ++i )
         {
            if( scope['_bindDeferId'] == listOfResizeFun[i]['id'] )
            {
               listOfResizeFun.splice( i, 1 ) ;
               break ;
            }
         }
      }

      //绑定重绘函数，并且在scope周期结束之后，销毁
      g.defer = function( scope, resizeFun ){
         var id = listOfResizeFun.length ;
         scope['_bindDeferId'] = id ;
         listOfResizeFun.push( { 'id': id, 'fun': resizeFun } ) ;
         angular.element( $window ).bind( 'resize', resizeFun ) ;
         scope.$on( '$destroy', function(){
            angular.element( $window ).unbind( 'resize', resizeFun ) ;
            unbindDefer( scope ) ;
         } ) ;
      }
   } ) ;

}());