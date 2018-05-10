(function(){
   var sacApp = window.SdbSacManagerModule ;
   //控制器
   sacApp.controllerProvider.register( 'Transfer', function( $location ){
      $location.path( '/Deploy/Index' ).search( { 'r': new Date().getTime() } ) ;
   } ) ;
}());