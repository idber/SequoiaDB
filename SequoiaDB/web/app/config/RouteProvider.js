(function(){
   var sacApp = window.SdbSacManagerModule ;
   sacApp.config( function( $sceProvider, $routeProvider, $controllerProvider, $compileProvider ){
      //控制器工厂
      window.SdbSacManagerModule.controllerProvider = $controllerProvider ;
      //指令工厂
      window.SdbSacManagerModule.compileProvider = $compileProvider ;
      //兼容IE7
      $sceProvider.enabled( false ) ;
      /* --- 路由列表 -- */
      var aRoute = window.SdbSacManagerConf.nowRoute ;
      var len = aRoute.length ;
      for( var i = 0; i < len; ++i )
      {
         $routeProvider.when( aRoute[i]['path'], aRoute[i]['options'] ) ;
      }
      //默认访问
      $routeProvider.otherwise( window.SdbSacManagerConf.defaultRoute ) ;
   } ) ;
   //自定义排序
   sacApp.filter( 'orderObjectBy', function(){
      return function( items, field, reverse ){
         var filtered = [] ;
         var fields = field.split( '.' ) ;
         var fieldLength = fields.length ;
         var length = items.length ;
         var sortValue = null ;
         for( var i = 0; i < length; ++i )
         {
            sortValue = items[i] ;
            for( var k = 0; k < fieldLength; ++k )
               sortValue = sortValue[ fields[k] ] ;
            if( isNaN( sortValue ) )
               sortValue = String( sortValue ) ;
            else
               sortValue = Number( sortValue ) ;
            filtered.push( { 'v': sortValue, 's': items[i] } ) ;
         }
         filtered.sort( function( a, b ){
            var t1 = typeof( a['v'] ) ;
            var t2 = typeof( b['v'] ) ;
            if( t1 != t2 )
               return t1 > t2 ? 1 : -1 ;
            return ( a['v'] > b['v'] ? 1 : -1 ) ;
         } ) ;
         if( reverse )
         {
            filtered.reverse() ;
         }
         var result = [] ;
         for( var i = 0; i < length; ++i )
         {
            result.push( filtered[i]['s'] ) ;
         }
         return result ;
      } ;
   } ) ;
}());