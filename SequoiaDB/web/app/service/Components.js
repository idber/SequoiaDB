(function(){
   var sacApp = window.SdbSacManagerModule ;
   //提示标签
   sacApp.service( 'Tip', function( $window ){
      var g = this ;
      g.tipEle = null ;
      g.create = function(){
         g.tipEle = $( '<div></div>' ).addClass( 'tooltip' ).html( '<div class="arrow"></div><div class="inner"></div>' ).appendTo( $( 'body' ) ) ;
      }
      g.hide = function(){
         $( g.tipEle ).hide() ;
      }
      g.show = function( text, mTop, mLeft ){
         var left = 0 ;
         var top = 0 ;
         var buttonL = mLeft ;
         var buttonT = mTop ;
         var buttonW = 22 ;
         var buttonH = 22 ;
         $( g.tipEle ).removeClass().addClass( 'tooltip' ).show().children( '.inner' ).text( text ) ;
         var tooltipW = $( g.tipEle ).outerWidth() ;
         var tooltipH = $( g.tipEle ).outerHeight() ;
         var className = '' ;
         var sdbWidth  = $( window ).width() ;
         var sdbHeight = $( window ).height() ;
         if( buttonL + buttonW + tooltipW < sdbWidth && className === '' )
         {
            className = 'tooltip right' ;
            left = buttonL + buttonW ;
            if( buttonT + parseInt( buttonH * 0.5 ) + tooltipH < sdbHeight )
            {
               top = buttonT + parseInt( buttonH * 0.5 ) ;
            }
            else if ( tooltipH - parseInt( buttonH * 0.5 ) < buttonT )
            {
               className += ' right-bottom' ;
               top = buttonT + parseInt( buttonH * 0.5 ) - tooltipH ;
            }
            else
            {
               className = '' ;
            }
         }
         else if( tooltipW + 10 < buttonL && className === '' )
         {
            className = 'tooltip left' ;
            left = buttonL - tooltipW - 10 ;
            if( buttonT + parseInt( buttonH * 0.5 ) + tooltipH < sdbHeight )
            {
               top = buttonT + parseInt( buttonH * 0.5 ) ;
            }
            else if ( tooltipH - 18 - parseInt( buttonH * 0.5 ) < buttonT )
            {
               className += ' left-bottom' ;
               top = buttonT + parseInt( buttonH * 0.5 ) - tooltipH ;
            }
            else
            {
               className = '' ;
            }
         }
         else if( tooltipW < buttonL && className === '' )
         {
            className = 'tooltip top top-right' ;
            left = buttonL - tooltipW ;
            if( tooltipH < buttonT )
            {
               top = buttonT - tooltipH ;
            }
            else if ( tooltipH + buttonT + buttonH < sdbHeight )
            {
               className = 'tooltip bottom bottom-right' ;
               top = buttonT + buttonH ;
            }
            else
            {
               className = '' ;
            }
         }
         else if ( buttonL + buttonW + tooltipW < sdbWidth && className === '' )
         {
            className = 'tooltip top top-left' ;
            left = buttonL + buttonW ;
            if( tooltipH < buttonT )
            {
               top = buttonT - tooltipH ;
            }
            else if ( tooltipH + buttonT + buttonH < sdbHeight )
            {
               className = 'tooltip bottom bottom-left' ;
               top = buttonT + buttonH ;
            }
            else
            {
               className = '' ;
            }
         }
         else if ( parseInt( tooltipW * 0.5 ) < buttonL && 
                   parseInt( tooltipW * 0.5 ) + buttonL + parseInt( buttonW * 0.5 ) < sdbWidth && 
                   className === '' )
         {
            className = 'tooltip' ;
            if( tooltipH < buttonT )
            {
               className += ' top' ;
               top = buttonT - tooltipH ;
               left = buttonL + parseInt( buttonW * 0.5 ) - parseInt( tooltipW * 0.5 ) ;
            }
            else if ( buttonT + buttonH + tooltipH < sdbHeight )
            {
               className += ' bottom' ;
               top = buttonT + buttonH ;
               left = buttonL + parseInt( buttonW * 0.5 ) - parseInt( tooltipW * 0.5 ) ;
            }
            else
            {
               className = '' ;
            }
         }
         if( className === '' )
         {
            try{
               throw new Error( '该组件无法在页面正常显示' ) ;
            }
            catch( e ){
               printfDebug( e.stack ) ;
            }
            $( g.tipEle ).removeClass().addClass( 'tooltip right right-bottom' ).css( { 'left': mLeft + buttonW, 'top': mTop + buttonH } ) ;
            return ;
         }
         $( g.tipEle ).removeClass().addClass( className ).css( { 'left': left, 'top': top } ) ;
      }
      g.auto = function(){
         var timeSet = null ;
         var timeSet2 = null ;
         var showTime = 1500 ;
         var hideTime = 7000 ;
         var isShow = false ;
         //angular.element( $window ).bind( 'mousemove', function ( ele ) {
         $( document ).on( 'mousemove', function( ele ){
            var pageX = ele['pageX'] ;
            var pageY = ele['pageY'] ;
            ele = $( ele['target'] ) ;
            function checkEleParent( element )
            {
               var isEvent = false ;
               if( element !== null )
               {
                  if( $( element ).attr( 'data-desc' ) )
                  {
                     isEvent = true ;
                     var text = $( element ).attr( 'data-desc' ) ;
                     g.show( text, pageY, pageX ) ;
                  }
                  else if( $( element ).hasClass( 'Ellipsis' ) )
                  {
                     isEvent = true ;
                     var text = $( element ).text() ;
                     text = trim( text ) ;
                     if( text.length > 0 )
                     {
                        if( timeSet != null )
                        {
                           clearTimeout( timeSet ) ;
                           timeSet = null ;
                        }
                        if( timeSet2 != null )
                        {
                           clearTimeout( timeSet2 ) ;
                        }
                        if( isShow == false )
                        {
                           timeSet = setTimeout( function(){
                              g.show( text, pageY, pageX ) ;
                              timeSet = null ;
                              isShow = true ;
                           }, showTime ) ;
                           timeSet2 = setTimeout( function(){
                              g.hide() ;
                              timeSet2 = null ;
                              isShow = false ;
                           }, hideTime ) ;
                        }
                        else
                        {
                           g.show( text, pageY, pageX ) ;
                           timeSet2 = setTimeout( function(){
                              g.hide() ;
                              timeSet2 = null ;
                              isShow = false ;
                           }, hideTime - showTime ) ;
                        }
                     }
                  }
                  if( $( element ).get(0) !== document && $( element ).get(0).parentNode !== document.body && isEvent === false )
                  {
                     checkEleParent( $( element ).get(0).parentNode ) ;
                  }
               }
            }
            checkEleParent( ele ) ;
         } ) ;
         //angular.element( $window ).bind( 'mouseout', function ( ele ) {
         $( document ).on( 'mouseout', function( ele ){
            if( timeSet != null )
            {
               clearTimeout( timeSet ) ;
               timeSet = null ;
            }
            g.hide() ;
         } ) ;
      }
   } ) ;
   //创建loading
   sacApp.service( 'Loading', function( $window ){
      var g = this ;
      var counter = 0 ;
      var mask = $( '<div></div>' ).addClass( 'mask-screen alpha30' ).css( { 'background-color': '#FFF', 'z-index': 99999 } ) ;
      var icon = $( '<div></div>').addClass( 'roundLoading' ).html( '<img src="./images/loading.gif">' ).css( { 'z-index': 100000 } ) ; ;
      var tip = $( '<div>loading</div>' ).appendTo( icon ) ;
      var timer = null ;
      var pointNum = 0 ;
      $( document ).keydown( function( event ){
         if( counter > 0 )
         {
            if ( ( event.altKey ) && ( ( event.keyCode == 37 ) || ( event.keyCode == 39 ) ) )    
            { 
               return false;
            }
            if( event.keyCode == 8 )
            {
               return false ;
            }
         }
      } ) ;
      g.create = function(){
         counter += 1 ;
         if( counter == 1 )
         {
            mask.appendTo( document.body ) ;
            icon.appendTo( document.body ) ;
            timer = setInterval( function(){
               ++pointNum ;
               if( pointNum == 1 )
               {
                  $( tip ).text( "loading." ) ;
               }
               else if( pointNum == 2 )
               {
                  $( tip ).text( "loading.." ) ;
               }
               else if( pointNum == 3 )
               {
                  $( tip ).text( "loading..." ) ;
                  pointNum = 0 ;
               }
            }, 800 ) ;
            g.resize() ;
         }
      }
      
      g.resize = function(){
         var left ;
         var top ;
         var bodyWidth = $( window ).width() ;  
         var bodyHeight = $( window ).height() ;
         left = ( bodyWidth - $( icon ).width() ) * 0.5 ;
         top = ( bodyHeight - $( icon ).height() - 50 ) * 0.5 ;
         $( icon ).css( { 'left': left, 'top': top } ) ;
      }

      g.cancel = function(){
         if( counter > 0 )
         {
            counter -= 1 ;
            if( counter == 0 )
            {
               clearInterval( timer ) ;
               timer = null ;
               icon.detach() ;
               mask.detach() ;
            }
         }
      }

      g.close = function(){
         counter = 0 ;
         clearInterval( timer ) ;
         timer = null ;
         icon.detach() ;
         mask.detach() ;
      }

      angular.element( $window ).bind( 'resize', function(){
         g.resize() ;
      } ) ;
   } ) ;

   //填写信息用的弹窗(废弃)
   sacApp.service( 'FormModal', function( $window, SdbFunction, SdbRest ){
      var g = this ;
      g.zIndex = 3000 ;
      g.list = {} ;
      g.text = { 'minLen': '?长度不能少于?。', 'maxLen': '?长度不能大于?。', 'null': '?是必填项。', 'ok': '确定', 'close': '取消', 'disabled': '?不能含?符号。' } ;

      //设置文本
      g.setText = function( text ){
         $.each( text, function( key, value ){
            g.text[key] = value ;
         } ) ;
      }

      //创建
      g.create = function( options, success, failed, close, onBeforeCreate ){
         options = ( typeof( options ) == 'undefined' ? {} : options ) ;
         if( typeof( options['id'] ) == 'undefined' ) throw 'option的id是必填的' ;
         var id = options['id'] ;
         options['icon'] = ( typeof( options['icon'] ) == 'undefined' ? 'fa-cog' : options['icon'] ) ;
         options['width'] = ( typeof( options['width'] ) == 'undefined' ? 'auto' : options['width'] ) ;
         options['height'] = ( typeof( options['height'] ) == 'undefined' ? 'auto' : options['height'] ) ;
         options['title'] = ( typeof( options['title'] ) == 'undefined' ? '' : options['title'] ) ;
         options['ok'] = ( typeof( options['ok'] ) == 'undefined' ? true : Boolean( options['ok'] ) ) ;

         if( typeof( g.list[ id ] ) == 'undefined' )
         {
            var mask  = $( '<div class="mask-screen unalpha"></div>' ).appendTo( document.body ) ;
            var modal = $( '<div class="modal2"></div>' ).appendTo( document.body ) ;
            var header = $( '<div class="header"></div>' ) ;
            var headerTable = $( '<table></table>' ) ;
            var headerTableTr = $( '<tr></tr>' ) ;
            var headerTableTrTd1 = $( '<td class="icon"><i class="fa fa-lg ' + options['icon'] + '"></i></td>' ) ;
            var headerTableTrTd2 = $( '<td class="title"></td>' ).text( options['title'] ) ;
            var headerTableTrTd3 = $( '<td class="tool"></td>' ) ;
            var modalRestore = $( '<div class="restore"></div>' ).hide() ;
            var modalMaximize = $( '<div class="maximize"></div>' ) ;
            var modalClose = $( '<div class="close"></div>' ) ;
            var body = $( '<div class="body"></div>' ) ;
            var bodyTable = $( '<table class="table loosen"></table>' ) ;
            var foot = $( '<div class="foot"></div>' ) ;
            var btnOK = $( '<button class="btn btn-primary"></button>' ).text( g.text['ok'] ) ;
            var btnClose = $( '<button class="btn btn-default"></button>' ).text( g.text['close'] ) ;
            var btnSize = $( '<div class="sizeBtn"></div>' ) ;
            modalRestore.on( 'click', function(){
               (function(modalId){
                  modalRestore.hide() ;
                  modalMaximize.show() ;
                  g.resize( modalId, false ) ;
               }(id));
            } ) ;
            modalMaximize.on( 'click', function(){
               (function(modalId){
                  modalRestore.show() ;
                  modalMaximize.hide() ;
                  g.resize( modalId, true ) ;
               }(id));
            } ) ;
            modalClose.on( 'click', function(){
               (function(modalId){
                  g.close( modalId ) ;
               }(id));
            } ) ;
            if( options['ok'] == true )
            {
               btnOK.on( 'click', function(){
                  (function(modalId){
                     g.ok( modalId ) ;
                  }(id));
               } ) ;
            }
            btnClose.on( 'click', function(){
               (function(modalId){
                  g.close( modalId ) ;
               }(id));
            } ) ;
            //监听鼠标按下 用于调整窗口大小
            btnSize.on( 'mousedown', function( event ){
               (function(modalId){
                  g.startSetSize( modalId, event ) ;
               }(id));
            } ) ;
            //监听双击事件
            headerTableTrTd2.on( 'dblclick', function(){
               (function(modalId){
                  if( modalMaximize.is( ':hidden' ) )
                  {
                     modalRestore.hide() ;
                     modalMaximize.show() ;
                     g.resize( modalId, false ) ;
                  }
                  else
                  {
                     modalRestore.show() ;
                     modalMaximize.hide() ;
                     g.resize( modalId, true ) ;
                  }
               }(id));
            } ) ;
            //监听鼠标按下 用于移动窗口
            headerTableTrTd2.on( 'mousedown', function( event ){
               (function(modalId){
                  g.startMoveModel( modalId, event ) ;
               }(id));
            } ) ;
            headerTableTrTd3.append( modalRestore ) ;
            headerTableTrTd3.append( modalMaximize ) ;
            headerTableTrTd3.append( modalClose ) ;
            headerTableTr.append( headerTableTrTd1 ) ;
            headerTableTr.append( headerTableTrTd2 ) ;
            headerTableTr.append( headerTableTrTd3 ) ;
            headerTable.append( headerTableTr ) ;
            header.append( headerTable ) ;
            body.append( bodyTable ) ;
            if( options['ok'] == true )
            {
               foot.append( btnOK ) ;
               foot.append( '&nbsp;&nbsp;' ) ;
            }
            foot.append( btnClose ) ;
            modal.append( header ) ;
            modal.append( body ) ;
            modal.append( foot ) ;
            modal.append( btnSize ) ;
            g.list[ id ] = { id: id, options: options, element: { mask: mask, modal: modal, body: body, headerTitle: headerTableTrTd2, bodyTable: bodyTable, restore: modalRestore, maximize: modalMaximize }, event: { success: success, failed: failed, close: close } } ;
         }
         else
         {
            $.each( options['input'], function( key, value ){
               g.list[ id ]['options']['input'][ key ] = value ;
            } ) ;
            $( g.list[ id ]['element']['restore'] ).hide() ;
            $( g.list[ id ]['element']['maximize'] ).show() ;
            $( g.list[ id ]['element']['mask'] ).appendTo( document.body ) ;
            $( g.list[ id ]['element']['modal'] ).removeData( 'x' ).removeData( 'y' ).removeData( 'width' ).removeData( 'height' ).appendTo( document.body ) ;
         }
         g.resize( id ) ;
         if( g.list[ id ]['options']['input'].length == 0 )
         {
            var action = g.list[ id ]['options']['action'] ;
            if( typeof( action ) == 'string' )
            {
               SdbRest.getConfig( action, function( data ){
                  g.list[ id ]['options']['input'] = data ;
                  if( typeof( onBeforeCreate ) == 'function' )
                  {
                     g.list[ id ]['options']['input'] = onBeforeCreate( g.list[ id ]['options']['input'] ) ;
                  }
                  g.createInput( id ) ;
               } ) ;
            }
         }
         else
         {
            if( typeof( onBeforeCreate ) == 'function' )
            {
               g.list[ id ]['options']['input'] = onBeforeCreate( g.list[ id ]['options']['input'] ) ;
            }
            g.createInput( id ) ;
         }
      }

      //添加一个可以增加的输入框
      g._createListTr = function( key, valid, inputChild )
      {
         var max, min ;
         var inputEle = $( '<tr></tr>' ).attr( 'data-type', 'list' ) ;
         var tmp = valid.split( '-' ) ;
         min = parseInt( tmp[0] ) ;
         max = parseInt( tmp[1] ) ;
         var browser = SdbFunction.getBrowserInfo() ;
         $.each( inputChild, function( index, child ){
            var childTd ;
            if( typeof( child['webName'] ) == 'string' )
            {
               var nameEle = $( '<td></td>' ).text( child['webName'] ).appendTo( inputEle ) ;
               childTd = $( '<td></td>' ).appendTo( inputEle ) ;
               if( index == 0 )
               {
                  nameEle.addClass( 'tdFirst' ) ;
               }
               else
               {
                  nameEle.css( 'text-align', 'right' ) ;
               }
               //防止ie7的宽度错乱
               if( browser[0] == 'ie' && browser[1] == 7 )
               {
                  nameEle.css( 'width', '70px' ) ;
                  childTd.css( 'width', '20%' ) ;
               }
            }
            else
            {
               childTd = $( '<td></td>' ).attr( 'colspan', 2 ).appendTo( inputEle ) ;
               if( index == 0 )
               {
                  childTd.addClass( 'tdFirst' ) ;
               }
               //防止ie7的宽度错乱
               if( browser[0] == 'ie' && browser[1] == 7 )
               {
                  childTd.css( 'width', '20%' ) ;
               }
            }
            var childEle ;
            switch( child['type'] )
            {
            case 'string':
               childEle = $( '<input />' ).addClass( 'form-control' ).attr( 'data-valid', child['valid'] ).appendTo( childTd ).attr( 'data-key', key ) ;
               break ;
            case 'select':
               childEle = $( '<select></select>' ).addClass( 'form-control' ).appendTo( childTd ) ;
               var valids = child['valid'].split( ',' ) ;
               $.each( valids, function( index, valid ){
                  var tmp = valid.split( ':' ) ;
                  $( '<option></option>' ).text( tmp[1] ).val( tmp[0] ).appendTo( childEle ) ;
               } ) ;
               break ;
            default:
               throw '类型错误' ;
            }
            childEle.bind( 'input propertychange', function(){
               var element = this ;
               var valid = $( element ).attr( 'data-valid' )
               if( typeof( valid ) == 'string' )
               {
                  var value = $( element ).val() ;
                  g._checkInput( value, valid, element ) ;
               }
            } ) ;
         } ) ;
         var add = $( '<td style="width:30px;"><div class="listBtn"><i class="fa fa-plus"></i></div></td>' ).appendTo( inputEle ) ;
         var remove = $( '<td style="width:30px;"><div class="listBtn"><i class="fa fa-minus"></i></div></td>' ).appendTo( inputEle ) ;
         add.on( 'click', function(){
            (function(inputChild2,element){
               var tr = $( remove ).parent() ;
               var table = tr.parent() ;
               var listNum = $( '> tr', table ).length ;
               if( listNum < max || isNaN( max ) )
               {
                  var newLine = g._createListTr( key, valid, inputChild2 ) ;
                  var b = $( element ).parent() ;
                  b.after( newLine ) ;
               }
            }(inputChild,add));
         } ) ;
         remove.on( 'click', function(){
            var tr = $( remove ).parent() ;
            var table = tr.parent() ;
            var listNum = $( '> tr', table ).length ;
            if( ( listNum > min || isNaN( min ) ) && listNum > 1 )
            {
               tr.remove() ;
            }
         } ) ;
         return inputEle ;
      }

      //创建可以增加的输入框
      g._createList = function( key, valid, inputChild ){ 
         var inputEle = $( '<table></table>' ).addClass( 'table loosen' ) ;
         inputEle.append( g._createListTr( key, valid, inputChild ) ) ;
         return inputEle ;
      }
/*
<div class="jsonEdit">
   <div class="jsonModel">
      <div class="tool">
         <button class="toolBtn" data-desc="视图模式"><i class="fa fa-list"></i></button>
         <button class="toolBtn" data-desc="缩进"><i class="fa fa-indent"></i></button>
         <button class="toolBtn" data-desc="取消缩进"><i class="fa fa-dedent"></i></button>
      </div>
      <div class="editBox">
         <textarea class="form-control" style="height:500px;border:0;outline:none;"></textarea>
      </div>
   </div>
   <div class="viewModel">
      <div class="tool">
         <button class="toolBtn" data-desc="字符串模式"><i class="fa fa-font"></i></button>
         <button class="toolBtn" data-desc="展开"><i class="fa fa-indent"></i></button>
         <button class="toolBtn" data-desc="收缩"><i class="fa fa-dedent"></i></button>
         <input class="form-control toolSearch" data-desc="搜索" />
      </div>
      <div class="editBox">
         <table class="table">
            <tr>
               <td class="lineBox treeBox">
                  <div class="treeEdit">
                     <table class="table loosen" style="width:auto;">
                        <tr>
                           <td style="width:20px;"><button class="caretBtn"><i class="caret caret-right caret-5x"></i></button></td>
                           <td><div class="fontBox">object</div></td>
                           <td></td>
                           <td><div class="fontBox valueBox">{10}</div></td>
                        </tr>
                     </table>
                  </div>
               </td>
               <td class="lineBox">
                  <button class="treeBtn" data-desc="Object">{}</button>
                  <div class="treeMenu">
                     <ul>
                        <li class="active"><button class="treeBtn"><i class="fa fa-copy"></i></button> 1</li>
                        <li><button class="treeBtn"><i class="fa fa-copy"></i></button> Obje1111ct</li>
                     </ul>
                  </div>
               </td>
               <td class="lineBox"><button class="treeBtn" data-desc="复制"><i class="fa fa-copy"></i></button></td>
               <td class="lineBox"><button class="treeBtn treeBtnColor2" data-desc="删除"><i class="fa fa-remove"></i></button></td>
            </tr>
            <tr>
               <td class="lineBox treeBox">
                  <div class="treeEdit" style="padding-left:16px;">
                     <table class="table loosen" style="width:auto;">
                        <tr>
                           <td style="width:20px;"></td>
                           <td><button class="treeBtn treeBtnColor1"><i class="fa fa-plus"></i></button></td>
                           <td></td>
                           <td></td>
                        </tr>
                     </table>
                  </div>
               </td>
               <td class="lineBox"></td>
               <td class="lineBox"></td>
               <td class="lineBox"></td>
            </tr>
         </table>
      </div>
   </div>
</div>
*/
      //获取没有使用的Field
      g._getUnusedFieldName = function( data ){
         var num = 0 ;
         var field = 'field' ;
         while( true )
         {
            var isUsed = false ;
            field = 'field' ;
            if( num > 0 )
            {
               field += '_' + num ;
            }
            $.each( data, function( key ){
               if( field == key )
               {
                  isUsed = true ;
                  return false ;
               }
            } ) ;
            ++num ;
            if( isUsed == false )
            {
               break ;
            }
         }
         return field ;
      }

      //创建json字段
      g._createJsonField = function( element, type, level, key, value, isFirst ){
         var trEle  = $( '<tr></tr>' ) ;
         var td1 = $( '<td></td>' ).addClass( 'lineBox treeBox' ).appendTo( trEle ) ;
         var td2 = $( '<td></td>' ).addClass( 'lineBox' ).appendTo( trEle ) ;
         var td3 = $( '<td></td>' ).addClass( 'lineBox' ).appendTo( trEle ) ;
         var td4 = $( '<td></td>' ).addClass( 'lineBox' ).appendTo( trEle ) ;
         //td1
         {
            var jsonBody = $( '<div></div>' ).addClass( 'treeEdit' ).appendTo( td1 ) ;
            if( level > 0 )
            {
               jsonBody.css( 'padding-left', 16 ) ;
            }
            var jsonTable = $( '<table></table>' ).addClass( 'table loosen' ).css( 'width', 'auto' ).appendTo( jsonBody ) ;
            var jsonTbody = $( '<tbody></tbody>' ).appendTo( jsonTable ) ;
            var jsonTr = $( '<tr></tr>' ).appendTo( jsonTbody ) ;
            var jsonTd1 = $( '<td></td>' ).css( 'width', 20 ).appendTo( jsonTr ) ;
            var jsonTd2 = $( '<td></td>' ).appendTo( jsonTr ) ;
            var jsonTd3 = $( '<td></td>' ).appendTo( jsonTr ) ;
            var jsonTd4 = $( '<td></td>' ).appendTo( jsonTr ) ;
            switch( type )
            {
            case 'object':
               var caretBtn = $( '<button></button>' ).addClass( 'caretBtn' ).appendTo( jsonTd1 ) ;
               $( '<i></i>' ).addClass( 'caret caret-right caret-5x' ).appendTo( caretBtn ) ;
               break ;
            case 'array':
               break ;
            case 'string':
               jsonTd3.text( ':' ) ;
               break ;
            case 'plus':
               var treeBtn = $( '<button></button>' ).addClass( 'treeBtn treeBtnColor1' ).appendTo( jsonTd2 ) ;
               treeBtn.on( 'click', function(){
                  var data = element['root'].data( 'json' ) ;
                  var num = getObjectSize( data ) ;
                  var fieldName = g._getUnusedFieldName( data ) ;
                  data[ fieldName ] = 'value' ;
                  element['root'].data( 'json', data ) ;
                  var trEle = g._createJsonField( element, 'string', level, fieldName, 'value', false ) ;
                  $( '> tr:eq(' + num + ')', element['table'] ).after( trEle ) ;
               } ) ;
               $( '<i></i>' ).addClass( 'fa fa-plus' ).appendTo( treeBtn ) ;
               break ;
            }
            if( type != 'plus' )
            {
               var keyEle = $( '<div></div>' ).addClass( 'fontBox' ).text( key ).appendTo( jsonTd2 ) ;
               var valueEle = $( '<div></div>' ).addClass( 'fontBox valueBox' ).text( value ).appendTo( jsonTd4 ) ;
            }
         }
         //td2
         {
            function createButton( type )
            {
               var button = $( '<button></button>' ).addClass( 'treeBtn' ).attr( 'data-desc', type ) ;
               switch( type )
               {
               case 'object':
                  button.text( '{}' ) ;
                  break ;
               case 'array':
                  button.text( '[]' ) ;
                  break ;
               case 'binary':
                  button.text( '01' ) ;
                  break ;
               case 'code':
                  $( '<i></i>' ).addClass( 'fa fa-code' ).appendTo( button ) ;
                  break ;
               case 'oid':
                  button.text( 'oid' ).css( 'font-size', '90%' ) ;
                  break ;
               case 'regex':
                  button.text( 'reg' ).css( 'font-size', '90%' ) ;
                  break ;
               case 'auto':
                  button.text( 'A' ) ;
                  break ;
               case 'string':
                  button.text( '""' ) ;
                  break ;
               case 'time':
                  $( '<i></i>' ).addClass( 'fa fa-calendar' ).appendTo( button ) ;
                  break ;
               case 'undefined':
                  button.text( 'U' ) ;
                  break ;
               default:
                  throw '未识别类型1' ;
                  break ;
               }
               return button ;
            }
            function createMenu( type )
            {
               var typeList = [ 'object', 'array', 'auto', 'string', 'oid', 'time', 'binary', 'regex', 'code' ] ;
               var menuUl = $( '<ul></ul>' ) ;
               $.each( typeList, function( index, thisType ){
                  var li = $( '<li></li>' ).appendTo( menuUl ) ;
                  var button = createButton( thisType ).appendTo( li ) ;
                  if( thisType == 'object' )
                  {
                     button.on( 'click', function(){
                        var cEdit = $( '> .treeEdit', td1 ) ;
                        var cTr = $( '> table > tbody > tr', cEdit ) ;
                        cEdit.css( 'padding-left', ( ( level + 1 ) * 16 ) ) ;
                     } ) ;
                  }
                  if( thisType == type )
                  {
                     li.addClass( 'active' ) ;
                  }
                  
               } ) ;
               return menuUl ;
            }
            if( type != 'plus' )
            {
               var typeButton = createButton( type ).appendTo( td2 ) ;
               var menu = $( '<div></div>' ).addClass( 'treeMenu' ).appendTo( td2 ) ;
               var menuUl = createMenu( type ).appendTo( menu ) ;
               typeButton.on( 'click', function(){

                  menu.show() ;
               } ) ;
            }
         }
         //td3
         {
            if( type != 'plus' && isFirst == false )
            {
               var copyButton = $( '<button></button>' ).addClass( 'treeBtn' ).attr( 'data-desc', '复制' ).appendTo( td3 ) ;
               $( '<i></i>' ).addClass( 'fa fa-copy' ).appendTo( copyButton ) ;
            }
         }
         //td4
         {
            if( type != 'plus' && isFirst == false )
            {
               var removeButton = $( '<button></button>' ).addClass( 'treeBtn treeBtnColor2' ).attr( 'data-desc', '删除' ).attr( 'data-key', key ).appendTo( td4 ) ;
               removeButton.on( 'click', function(){
                  var key = $( this ).attr( 'data-key' ) ;
                  var data = element['root'].data( 'json' ) ;
                  delete data[ key ] ;
                  element['root'].data( 'json', data ) ;
                  $( this ).parent().parent().remove() ;
               } ) ;
               $( '<i></i>' ).addClass( 'fa fa-remove' ).appendTo( removeButton ) ;
            }
         }
         return trEle ;
      }

      //创建可以构造Json的输入框
      g._createJson = function( key ){
         var inputEle  = $( '<div></div>' ).addClass( 'jsonEdit' ).data( 'json', {} ) ;
         var jsonModel = $( '<div></div>' ).addClass( 'jsonModel' ).appendTo( inputEle ) ;
         var jsonTool  = $( '<div></div>' ).addClass( 'tool' ).appendTo( jsonModel ) ;
         var jsonEdit  = $( '<div></div>' ).addClass( 'editBox' ).appendTo( jsonModel ) ;
         var jsonToolBtn1 = $( '<button></button>' ).addClass( 'toolBtn' ).attr( 'data-desc', '视图模式' ).html( '<i class="fa fa-font"></i>' ).appendTo( jsonTool ) ;
         var jsonToolBtn2 = $( '<button></button>' ).addClass( 'toolBtn' ).attr( 'data-desc', '缩进' ).html( '<i class="fa fa-indent"></i>' ).appendTo( jsonTool ) ;
         var jsonToolBtn3 = $( '<button></button>' ).addClass( 'toolBtn' ).attr( 'data-desc', '取消缩进' ).html( '<i class="fa fa-dedent"></i>' ).appendTo( jsonTool ) ;
         var jsonTextarea = $( '<textarea></textarea>' ).addClass( 'form-control textBox' ).appendTo( jsonEdit ) ;

         var viewModel = $( '<div></div>' ).addClass( 'viewModel' ).appendTo( inputEle ) ;
         var viewTool  = $( '<div></div>' ).addClass( 'tool' ).appendTo( viewModel ) ;
         var viewEdit  = $( '<div></div>' ).addClass( 'editBox' ).appendTo( viewModel ) ;
         var viewToolBtn1 = $( '<button></button>' ).addClass( 'toolBtn' ).attr( 'data-desc', '字符串模式' ).html( '<i class="fa fa-list"></i>' ).appendTo( viewTool ) ;
         var viewToolBtn2 = $( '<button></button>' ).addClass( 'toolBtn' ).attr( 'data-desc', '展开' ).html( '<i class="fa fa-indent"></i>' ).appendTo( viewTool ) ;
         var viewToolBtn3 = $( '<button></button>' ).addClass( 'toolBtn' ).attr( 'data-desc', '收缩' ).html( '<i class="fa fa-dedent"></i>' ).appendTo( viewTool ) ;
         var viewSearch = $( '<input/>' ).addClass( 'form-control toolSearch' ).attr( 'data-desc', '搜索' ).appendTo( viewTool ) ;
         var viewTable = $( '<table></table>' ).addClass( 'table' ).appendTo( viewEdit ) ;
         var viewTbody = $( '<tbody></tbody>' ).appendTo( viewTable ) ;

         var mask = $( '<div></div>' ).addClass( 'mask-screen unalpha' ).css( 'z-index', 1005 ) ;
         var element = { 'root': inputEle, 'area': jsonTextarea, 'table': viewTbody, 'mask': mask } ;

         var trEle = g._createJsonField( element, 'object', 0, 'object', '{6}', true ) ;
         $( trEle ).appendTo( viewTbody ) ;
         trEle = g._createJsonField( element, 'plus', 0, '', '', false ) ;
         $( trEle ).appendTo( viewTbody ) ;

         return inputEle ;
      }
	  
      //创建一个输入框
      g._createInput = function( id, input ){
         var inputEle ;
         //创建标签
         switch( input['type'] )
         {
         case 'string':
         case 'array':
            inputEle = $( '<input />' ) ;
            break ;
         case 'text':
            inputEle = $( '<textarea></textarea>' ) ;
            break ;
         case 'select':
            inputEle = $( '<select></select>' ) ;
            var valids = input['valid'].split( ',' ) ;
            $.each( valids, function( index, valid ){
               var tmp = valid.split( ':' ) ;
               $( '<option></option>' ).text( tmp[1] ).val( tmp[0] ).appendTo( inputEle ) ;
            } ) ;
            break ;
         case 'list':
            inputEle = g._createList( input['webName'], input['valid'], input['child'] ) ;
            break ;
         case 'json':
            inputEle = g._createJson( input['webName'] ) ;
            break ;
         default:
            throw '类型错误' ;
         }
         //添加属性
         switch( input['type'] )
         {
         case 'string':
         case 'text':
         case 'array':
            if( typeof( input['valid'] ) == 'string' )
            {
               inputEle.attr( 'data-valid', input['valid'] ).attr( 'data-key', input['webName'] ) ;
            }
         case 'select':
            inputEle.addClass( 'form-control' ) ;
            break ;
         case 'list':
            break ;
         case 'json':
            if( typeof( input['valid'] ) == 'string' )
            {
               inputEle.attr( 'data-valid', input['valid'] ).attr( 'data-key', input['webName'] ) ;
            }
            break ;
         default:
            throw '类型错误' ;
         }
         //监听值变化
         switch( input['type'] )
         {
         case 'string':
         case 'text':
         case 'select':
            (function( thisID, isRelyon ){
               inputEle.bind( 'input propertychange', function(){
                  var element = this ;
                  var valid = $( element ).attr( 'data-valid' )
                  if( typeof( valid ) == 'string' )
                  {
                     var value = $( element ).val() ;
                     g._checkInput( value, valid, element ) ;
                  }
                  if( isRelyon == true )
                  {
                     g.checkRelyon( thisID ) ;
                  }
               } ) ;
            }(id,input['relyon'])) ;
            break ;
         case 'list':
            break ;
         case 'array':
            var browser = SdbFunction.getBrowserInfo() ;
            if( browser[0] == 'ie' && browser[1] < 9 )
            {
               //IE7 和 IE8不支持自动输出分隔功能
               break ;
            }
            (function(){
               var inputEleValueLen = 0 ;
               inputEle.bind( 'input propertychange', function(){
                  var element = this ;
                  var value = $( element ).val() ;
                  var len = value.length ;
                  if( value.charAt( len - 1 ) == ',' && len > inputEleValueLen )
                  {
                     $( element ).val( value + ' ' ) ;
                  }
                  else if( value.charAt( len - 1 ) == ' ' && len > inputEleValueLen )
                  {
                     value = value.substr( 0, len - 1 ) ;
                     $( element ).val( value + ', ' ) ;
                  }
                  else if( ( value.charAt( len - 1 ) == ',' || value.charAt( len - 1 ) == ' ' ) && len < inputEleValueLen )
                  {
                     var lastKey = value.lastIndexOf( ' ' ) ;
                     if( lastKey >= 0 )
                     {
                        value = value.substr( 0, lastKey - 1 ) ;
                        $( element ).val( value ) ;
                     }
                  }
                  len = $( element ).val().length ;
                  inputEleValueLen = len ;
               } ) ;
            }()) ;
            break ;
         case 'json':
            break ;
         default:
            throw '类型错误' ;
         }
         return inputEle ;
      }

      //创建所有输入框
      g.createInput = function( id )
      {
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var bodyTable = options['element']['bodyTable'] ;
         var inputList = options['options']['input'] ;
         $.each( inputList, function( index, input ){
            switch( input['type'] )
            {
            case 'string':
            case 'text':
            case 'select':
            case 'list':
            case 'array':
            case 'json':
               var tr  = $( '<tr></tr>' ).appendTo( bodyTable ).attr( 'data-name', input['name'] )  ;
               var td1 = $( '<td></td>' ).css( 'width', 140 ).text( input['webName'] ).appendTo( tr ) ;
               var td2 = $( '<td></td>' ).css( 'width', 10 ).appendTo( tr ) ;
               var td3 = $( '<td></td>' ).attr( 'data-type', 'value' ).appendTo( tr ) ;
               var td4 = $( '<td></td>' ).css( { 'width': 50, 'color': '#00B8E6', 'font-size': '110%' } ).appendTo( tr ) ;

               var inputEle = g._createInput( id, input ) ;
               if( typeof( input['default'] ) != 'undefined' )
               {
                  inputEle.val( input['default'] ) ;
               }
               if( typeof( input['desc'] ) == 'string' )
               {
                  $( '<i class="fa fa-info-circle"></i>' ).attr( 'data-desc', input['desc'] ).appendTo( td4 ) ;
                  if( input['type'] == 'list' || input['type'] == 'json' )
                  {
                     td4.css( { 'vertical-align': 'top', 'padding-top': 10 } ) ;
                  }
                  else
                  {
                     td4.css( { 'vertical-align': 'middle' } ) ;
                  }
               }
               switch( input['type'] )
               {
               case 'string':
               case 'text':
               case 'list':
               case 'json':
                  if( typeof( input['valid'] ) == 'string' && input['valid'].length > 0 )
                  {
                     td2.append( '<span class="require">*</span>' ) ;
                  }
               }
               td3.append( inputEle ) ;
               break ;
            case 'note':
               var tr = $( '<tr></tr>' ).appendTo( bodyTable ) ;
               var td = $( '<td></td>' ).attr( 'colspan', 4 ).appendTo( tr ) ;
               var noteEle = g._createNote( input['text'] ) ;
               td.append( noteEle ) ;
               break ;
			   case 'table':
				   var tr = $( '<tr></tr>' ).appendTo( bodyTable ) ;
				   var td = $( '<td></td>' ).addClass( 'maskTd' ).attr( 'colspan', 4 ).appendTo( tr ) ;
				   var tableEle = g._createTable( input['title'], input['rows'] ) ;
				   td.append( tableEle ) ;
				   break ;
            default:
               throw '类型错误' ;
            }
         } ) ;
         g.checkRelyon( id ) ;
      }

      //创建一个注意提示
      g._createNote = function( text )
      {
         var inputEle = $( '<div></div>' ).addClass( 'note' ) ;
         var noteTitle = $( '<p></p>' ).text( 'Note:' ).appendTo( inputEle ) ;
         if( SdbFunction.isArray( text ) )
         {
            var noteBody = $( '<ul></ul>' ).appendTo( inputEle ) ;
            $.each( text, function( index, sentence ){
               var li = $( '<li></li>' ).text( sentence ).appendTo( noteBody ) ;
            } ) ;
         }
         else
         {
            var noteBody = $( '<p></p>' ).css( { 'text-indent': '25px', 'margin': '10px 0' } ).text( text ).appendTo( inputEle ) ;
         }
         return inputEle ;
      }

      //创建一个表格
      g._createTable = function( title, rows )
      {
         var inputEle = $( '<table></table>' ).attr( 'class', 'table loosen border') ;
         var titleTr = $( '<tr></tr>' ).css( { 'font-weight':'bold' } ).appendTo( inputEle ) ;
         //遍历表格头的数组
         $.each( title, function( index, cell ){
            var td = $( '<td></td>' ).text( cell['text'] ).css( 'background-color', '#F1F4F5' ).appendTo( titleTr ) ;
            //判断，如果有width属性，那就设置该列的宽度
            if( typeof( cell['width'] ) != 'undefined' )
            {
               td.css( 'width', cell['width'] ) ;
            }
         } ) ;
         //遍历表格的每一行
         $.each( rows, function( index, row ){
            var tr = $( '<tr></tr>' ).appendTo( inputEle ) ;
            //遍历表格每一列
            $.each( row, function( index2, cell ){
               var td = $( '<td></td>' ).appendTo( tr ) ;
               if( typeof( cell[ 'href' ] ) != 'undefined' )
               {
                  $( '<a></a>' ).addClass( 'linkButton' ).attr( 'href', cell['href'] ).text( cell['text'] ).appendTo( td );   
               }
               else
               {
                  $( td ).text( cell['text'] );
               }
            } ) ;
         } ) ;
         return inputEle ;
      }

      //检查所有依赖关系
      g.checkRelyon = function( id )
      {
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var relyonFun = options['options']['relyon'] ;
         var inputList = options['options']['input'] ;
         var bodyTable = options['element']['bodyTable'] ;
         if( typeof( relyonFun ) == 'function' )
         {
            var values = g.getInputValue( id ) ;
            $.each( inputList, function( index2, inputInfo ){
               if( relyonFun( inputInfo['name'], values ) == true )
               {
                  $( 'tr[data-name="' + inputInfo['name'] + '"]', bodyTable ).show() ;
               }
               else
               {
                  $( 'tr[data-name="' + inputInfo['name'] + '"]', bodyTable ).hide() ;
               }
            } ) ;
         }
      }

      //开始移动窗口
      g.startMoveModel = function( id, event ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var modal = options['element']['modal'] ;
         var pageX = event['pageX'] ;
         var pageY = event['pageY'] ;
         modal.data( 'x', pageX ) ;
         modal.data( 'y', pageY ) ;
         $( document.body ).addClass( 'unselect' ) ;
         //监听鼠标移动
         $( document ).on( 'mousemove', function( event2 ){
            modal.addClass( 'alpha' ) ;
            g.moveModel( id, event2 ) ;
         } ) ;
         //监听鼠标松开
         $( document ).on( 'mouseup', function(){
            g.endMoveModel( id ) ;
         } ) ;
      }

      //正在移动窗口
      g.moveModel = function( id, event ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var modal = options['element']['modal'] ;
         if( modal.hasClass( 'alpha' ) )
         {
            var bodyWidth = $( window ).width() ;
            var bodyHeight = $( window ).height() ;
            var modalWidth = modal.width() ;
            var modalHeight = modal.height() ;
            var x = modal.data( 'x' ) ;
            var y = modal.data( 'y' ) ;
            var pageX = event['pageX'] ;
            var pageY = event['pageY'] ;
            modal.data( 'x', pageX ) ;
            modal.data( 'y', pageY ) ;
            var left = parseInt( modal.css( 'left' ) ) ;
            var top = parseInt( modal.css( 'top' ) ) ;
            var offsetLeft = pageX - x ;
            var offsetTop = pageY - y ;
            top  += offsetTop ;
            left += offsetLeft ;
            if( top <= 0 ) top = 0 ;
            if( left <= 0 ) left = 0 ;
            if( top + modalHeight + 5 >= bodyHeight ) top = bodyHeight - modalHeight - 5 ;
            if( left + modalWidth + 5 >= bodyWidth ) left = bodyWidth - modalWidth - 5 ;
            modal.css( { top: top, left: left } ) ;
         }
      }

      //结束移动窗口
      g.endMoveModel = function( id ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var modal = options['element']['modal'] ;
         modal.removeClass( 'alpha' ) ;
         $( document.body ).removeClass( 'unselect' ) ;
         $( document ).off( 'mousemove' ) ;
         $( document ).off( 'mouseup' ) ;
      }

      //开始调整窗口大小
      g.startSetSize = function( id, event ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var modal = options['element']['modal'] ;
         $( document.body ).addClass( 'unselect' ) ;
         //监听鼠标移动
         $( document ).on( 'mousemove', function( event2 ){
            modal.addClass( 'alpha' ) ;
            g.setSize( id, event2 ) ;
         } ) ;
         //监听鼠标松开
         $( document ).on( 'mouseup', function(){
            g.endSetSize( id ) ;
         } ) ;
      }

      //正在调整窗口大小
      g.setSize = function( id, event ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var bodyWidth = $( window ).width() ;
         var bodyHeight = $( window ).height() ;
         var modal = options['element']['modal'] ;
         var body = options['element']['body'] ;
         var left = parseInt( modal.css( 'left' ) ) ;
         var top = parseInt( modal.css( 'top' ) ) ;
         var pageX = event['pageX'] + 5 ;
         var pageY = event['pageY'] + 5 ;
         var width = pageX - left ;
         var height = pageY - top ;
         if( width < 600 ) width = 600 ;
         if( height < 450 ) height = 450 ;
         if( top + height + 10 >= bodyHeight ) height = bodyHeight - top - 10 ;
         if( left + width + 10 >= bodyWidth ) width = bodyWidth - left - 10 ;
         modal.data( 'width', width ) ;
         modal.data( 'height', height ) ;
         modal.width( width ).height( height ) ;
         width -= 42 ;
         height -= 106 ;
         body.width( width ).height( height ) ;
      }

      //结束调整窗口大小
      g.endSetSize = function( id ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var modal = options['element']['modal'] ;
         modal.removeClass( 'alpha' ) ;
         $( document.body ).removeClass( 'unselect' ) ;
         $( document ).off( 'mousemove' ) ;
         $( document ).off( 'mouseup' ) ;
      }

      //调整窗口宽高和位置
      g.resize = function( id, isFull ){
         isFull = ( typeof( isFull ) == 'undefined' ? false : isFull ) ;
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var modal = options['element']['modal'] ;
         var modalWidth = modal.data( 'width' ) ;
         var modalHeight = modal.data( 'height' ) ;
         var body = options['element']['body'] ;
         var bodyWidth = $( window ).width() ;
         var bodyHeight = $( window ).height() ;
         var width, height, left, top ;
         if( isFull )
         {
            width = bodyWidth - 12 ;
            height = bodyHeight - 12 ;
            if( width < 600 ) width = 600 ;
            if( height < 450 ) height = 450 ;
            left = 6 ;
            top = 6 ;
         }
         else if( typeof( modalWidth ) == 'number' && typeof( modalHeight ) == 'number' )
         {
            width = parseInt( modalWidth ) ;
            height = parseInt( modalHeight ) ;
            if( width < 600 ) width = 600 ;
            if( height < 450 ) height = 450 ;
            if( width >= bodyWidth ) width = bodyWidth - 12 ;
            if( height >= bodyHeight ) height = bodyHeight - 12 ;
            left = ( bodyWidth - width ) * 0.5 ;
            top = ( bodyHeight - height ) * 0.5 ;
         }
         else
         {
            width  = options['options']['width'] ;
            height = options['options']['height'] ;
            if( width == 'auto' ) width = bodyWidth * 0.5 ;
            if( height == 'auto' ) height = bodyHeight * 0.5 ;
            width = parseInt( width ) ;
            height = parseInt( height ) ;
            if( width < 600 ) width = 600 ;
            if( height < 450 ) height = 450 ;
            left = ( bodyWidth - width ) * 0.5 ;
            top = ( bodyHeight - height ) * 0.5 ;
         }
         modal.width( width ).height( height ).css( { left: left, top: top } ) ;
         width -= 42 ;
         height -= 106 ;
         body.width( width ).height( height ) ;
      }

      //关闭窗口事件
      g.close = function( id ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var event = options['event'] ;
         var mask = options['element']['mask'] ;
         var modal = options['element']['modal'] ;
         var bodyTable = options['element']['bodyTable'] ;
         bodyTable.empty() ;
         mask.detach() ;
         modal.detach() ;
         if( typeof( event['close'] ) == 'function' )
         {
            event['close']() ;
         }
      }

      //确定事件
      g.ok = function( id ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var event = options['event'] ;
         var rc = g.checkInput( id ) ;
         if( rc )
         {
            if( typeof( event['success'] ) == 'function' )
            {
               var rv = g.getInputValue( id ) ;
               event['success']( rv ) ;
            }
            g.close( id ) ;
         }
         else
         {
            if( typeof( event['failed'] ) == 'function' )
            {
               event['failed']() ;
            }
         }
      }

      //判断输入的值是否符合规则
      g._checkInput = function( value, valid, element ){
         var rc = true ;
         if( valid == '' ) return rc ;
         var type = valid.charAt( 0 ) ;
         valid = valid.substr( 1 ) ;
         var key = $( element ).attr( 'data-key' ) ;
         switch( type )
         {
         case 's':
            var disableChar = [] ;
            if( valid.indexOf( '!' ) > 0 )
            {
               var tmp = valid.split( '!' ) ;
               valid = tmp[0] ;
               disableChar = tmp[1].split( '' ) ;
            }
            var range = valid.split( '-' ) ;
            var min = parseInt( range[0] ) ;
            var max = parseInt( range[1] ) ;
            var len = value.length ;
            if( len == 0 )
            {
               rc = false ;
               $( element ).addClass( 'form-danger' ).attr( 'data-desc', sprintf( g.text['null'], key ) ) ;
            }
            else if( !isNaN( min ) && len < min )
            {
               rc = false ;
               $( element ).addClass( 'form-danger' ).attr( 'data-desc', sprintf( g.text['minLen'], key, min ) ) ;
            }
            else if( !isNaN( max ) && len > max )
            {
               rc = false ;
               $( element ).addClass( 'form-danger' ).attr( 'data-desc', sprintf( g.text['maxLen'], key, max ) ) ;
            }
            else
            {
               var hasDisableChar = false ;
               $.each( disableChar, function( index, chars ){
                  if( value.indexOf( chars ) >= 0 )
                  {
                     rc = false ;
                     hasDisableChar = true ;
                     $( element ).addClass( 'form-danger' ).attr( 'data-desc', sprintf( g.text['disabled'], key, chars ) ) ;
                     return false ;
                  }
               } ) ;
               if( hasDisableChar == false )
               {
                  $( element ).removeClass( 'form-danger' ).removeClass( 'form-warning' ).removeAttr( 'data-desc' ) ;
               }
            }
            break ;
         case 'i':
            rc = false ;
            break ;
         default:
            throw '未知类型' ;
         }
         return rc ;
      }

      //检查所有输入值
      g.checkInput = function( id ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var rc = true ;
         var options = g.list[ id ] ;
         var bodyTable = options['element']['bodyTable'] ;
         $( 'tr[data-name]', bodyTable ).each( function( index, tr ){
            if( $( tr ).is( ':hidden' ) == false )
            {
               $( '.form-control', tr ).each( function( index2, element ){
                  var valid = $( element ).attr( 'data-valid' )
                  if( typeof( valid ) == 'string' )
                  {
                     var value = $( element ).val() ;
                     if( !g._checkInput( value, valid, element ) )
                     {
                        rc = false ;
                     }
                  }
               } ) ;
            }
         } ) ;
         return rc ;
      }

      //获取输入的值
      g.getInputValue = function( id ){
         if( typeof( g.list[ id ] ) == 'undefined' ) throw 'id不存在' ;
         var options = g.list[ id ] ;
         var bodyTable = options['element']['bodyTable'] ;
         var inputList = options['options']['input'] ;
         var value = {} ;
         $.each( inputList, function( index, input ){
            var tr = $( 'tr[data-name="' + input['name'] + '"]', bodyTable ) ;
            if( $( tr ).is( ':hidden' ) == false )
            {
               switch( input['type'] )
               {
               case 'string':
               case 'text':
               case 'select':
               case 'array':
                  value[ input['name'] ] = $( '.form-control', tr ).val() ;
                  break ;
               case 'list':
                  value[ input['name'] ] = [] ;
                  $( 'tr[data-type="list"]', tr ).each( function( index, trChild ){
                     var lineValue = [] ;
                     $( '.form-control', trChild ).each( function( index, element ){
                        lineValue.push( $( element ).val() ) ;
                     } ) ;
                     value[ input['name'] ].push( lineValue ) ;
                  } ) ;
                  break ;
               case 'note':
                  break ;
               default:
                  throw '不支持的类型' ;
               }
            }
         } ) ;
         return value ;
      }

      angular.element( $window ).bind( 'resize', function () {
         $.each( g.list, function( index, option ){
            g.resize( option['id'] ) ;
         } ) ;
      } ) ;
   } ) ;
}());