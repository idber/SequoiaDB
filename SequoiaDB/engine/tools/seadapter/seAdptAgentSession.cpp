/*******************************************************************************


   Copyright (C) 2011-2018 SequoiaDB Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.

   Source File Name = seAdptAgentSession.cpp

   Descriptive Name = Agent session on search engine adapter.

   When/how to use: this program may be used on binary and text-formatted
   versions of PMD component. This file contains main function for sdbcm,
   which is used to do cluster managing.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/14/2017  YSD  Initial Draft

   Last Changed =

*******************************************************************************/
#include "seAdptAgentSession.hpp"
#include "rtnContextBuff.hpp"
#include "utilCommon.hpp"

using engine::_pmdEDUCB ;

namespace seadapter
{
   BEGIN_OBJ_MSG_MAP( _seAdptAgentSession, _pmdAsyncSession)
      ON_MSG( MSG_BS_QUERY_REQ, _onOPMsg )
      ON_MSG( MSG_BS_GETMORE_REQ, _onOPMsg )
   END_OBJ_MSG_MAP()

   _seAdptAgentSession::_seAdptAgentSession( UINT64 sessionID )
   : _pmdAsyncSession( sessionID )
   {
      _imContext = NULL ;
      _seCltFactory = sdbGetSeCltFactory() ;
      _esClt = NULL ;
      _contextIDHWM = 0 ;
   }

   _seAdptAgentSession::~_seAdptAgentSession()
   {
      for ( CTX_MAP_ITR itr = _ctxMap.begin(); itr != _ctxMap.end(); )
      {
         SDB_OSS_DEL itr->second ;
         _ctxMap.erase( itr++ ) ;
      }

      // Be sure to release the client at last, because the context needs it to
      // release scroll.
      if ( _esClt )
      {
         SDB_OSS_DEL _esClt ;
      }
   }

   SDB_SESSION_TYPE _seAdptAgentSession::sessionType() const
   {
      return SDB_SESSION_SE_AGENT ;
   }

   EDU_TYPES _seAdptAgentSession::eduType() const
   {
      return EDU_TYPE_SE_AGENT ;
   }

   void _seAdptAgentSession::onRecieve( const NET_HANDLE netHandle,
                                        MsgHeader *msg )
   {
   }

   BOOLEAN _seAdptAgentSession::timeout( UINT32 interval )
   {
      return FALSE ;
   }

   void _seAdptAgentSession::onTimer( UINT64 timerID, UINT32 interval )
   {
   }

   void _seAdptAgentSession::_onAttach()
   {
   }

   void _seAdptAgentSession::_onDetach()
   {
   }

   INT32 _seAdptAgentSession::_onOPMsg( NET_HANDLE handle, MsgHeader *msg )
   {
      INT32 rc = SDB_OK ;

      MsgOpReply reply ;
      BSONObj resultObj;
      const CHAR *msgBody = NULL ;
      INT32 bodySize = 0 ;
      utilCommObjBuff objBuff ;
      INT64 contextID = -1 ;

      rc = objBuff.init() ;
      if ( rc )
      {
         PD_LOG( PDERROR, "Initialize result buffer failed[ %d ]", rc ) ;
      }
      else
      {
         switch ( msg->opCode )
         {
            case MSG_BS_QUERY_REQ:
               rc = _onQueryReq( msg, objBuff, contextID ) ;
               break ;
            case MSG_BS_GETMORE_REQ:
               rc = _onGetmoreReq( msg, objBuff, contextID ) ;
               break ;
            default:
               rc = SDB_UNKNOWN_MESSAGE ;
               break ;
         }
      }

      reply.header.opCode = MAKE_REPLY_TYPE( msg->opCode ) ;
      reply.header.messageLength = sizeof( MsgOpReply ) ;
      reply.header.requestID = msg->requestID ;
      reply.header.TID = msg->TID ;
      reply.header.routeID.value = 0 ;

      if ( objBuff.valid() )
      {
         if ( SDB_OK != rc )
         {
            if ( 0 == objBuff.getObjNum() )
            {
               _errorInfo =
                  utilGetErrorBson( rc, _pEDUCB->getInfo( EDU_INFO_ERROR ) ) ;
               objBuff.appendObj( _errorInfo ) ;
            }
         }

         // Maybe data, maybe the error message.
         if ( objBuff.getObjNum() > 0 )
         {
            msgBody = objBuff.data() ;
            bodySize = objBuff.dataSize() ;
            reply.numReturned = objBuff.getObjNum() ;
            reply.contextID = contextID ;
         }
      }
      else
      {
         reply.numReturned = 0 ;
      }

      reply.flags = rc ;
      reply.header.messageLength += bodySize ;
      rc = _reply( &reply, msgBody, bodySize ) ;
      PD_RC_CHECK( rc, PDERROR, "Reply the message failed[ %d ]" ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   // Handle query message from data node.
   // It will analyze the original query, fetch the neccessary data, and rewrite
   // the items, then return the new message.
   INT32 _seAdptAgentSession::_onQueryReq( MsgHeader *msg,
                                           utilCommObjBuff &objBuff,
                                           INT64 &contextID,
                                           pmdEDUCB *eduCB )
   {
      INT32 rc = SDB_OK ;
      INT32 flag = 0 ;
      CHAR *pCollectionName = NULL ;
      SINT64 numToSkip = 0 ;
      SINT64 numToReturn = 0 ;
      CHAR *pQuery = NULL ;
      CHAR *pFieldSelector = NULL ;
      CHAR *pOrderBy = NULL ;
      CHAR *pHint = NULL ;

      string indexName ;
      string typeName ;
      seAdptContextQuery *context = NULL ;

      rc = msgExtractQuery( (CHAR *)msg, &flag, &pCollectionName,
                            &numToSkip, &numToReturn, &pQuery, &pFieldSelector,
                            &pOrderBy, &pHint ) ;
      PD_RC_CHECK( rc, PDERROR, "Session[ %s ] extract query message "
                   "failed[ %d ]", sessionName(), rc ) ;

      if ( !_esClt )
      {
         rc = _seCltFactory->create( &_esClt ) ;
         if ( rc )
         {
            PD_LOG_MSG( PDERROR, "Connect to search engine failed[ %d ]", rc ) ;
            goto error ;
         }
      }

      try
      {
         BSONObj matcher( pQuery ) ;
         BSONObj selector ( pFieldSelector ) ;
         BSONObj orderBy ( pOrderBy ) ;
         BSONObj hint ( pHint ) ;
         BSONObj newHint ;

         rc = _selectIndex( pCollectionName, hint, newHint ) ;
         PD_RC_CHECK( rc, PDERROR, "Select index for collection[%s] failed[%d]",
                      pCollectionName, rc ) ;

         context = SDB_OSS_NEW seAdptContextQuery() ;
         if ( !context )
         {
            rc = SDB_OOM ;
            PD_LOG_MSG( PDERROR, "Allocate memory for query context failed, "
                        "size[ %d ]", sizeof( seAdptContextQuery ) ) ;
            goto error ;
         }

         rc = context->open( _imContext, _esClt, matcher, selector, orderBy,
                             newHint, objBuff, eduCB ) ;
         if ( rc )
         {
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG_MSG( PDERROR, "Open context for rewrite query failed[ %d ]",
                           rc ) ;
            }
            goto error ;
         }
         contextID = _contextIDHWM++ ;
         _ctxMap[ contextID ] = context ;
      }
      catch ( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Session[ %s ] create BSON objects for query "
                     "items failed: %s", sessionName(), e.what() ) ;
         rc = SDB_INVALIDARG ;
         goto error ;
      }

   done:
      if ( _imContext && _imContext->isMetaLocked() )
      {
         _imContext->metaUnlock() ;
      }
      return rc ;
   error:
      if ( context )
      {
         SDB_OSS_DEL context ;
      }
      goto done ;
   }

   // Generate another new message.
   INT32 _seAdptAgentSession::_onGetmoreReq( MsgHeader *msg,
                                             utilCommObjBuff &objBuff,
                                             INT64 &contextID,
                                             pmdEDUCB *eduCB )
   {
      INT32 rc = SDB_OK ;
      seAdptContextBase *context = NULL ;

      contextID = ((MsgOpGetMore *)msg)->contextID ;

      CTX_MAP_ITR itr = _ctxMap.find( contextID ) ;
      if ( _ctxMap.end() == itr )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Context can not be found" ) ;
         goto error ;
      }

      context = itr->second ;
      rc = context->getMore( 1, objBuff ) ;
      if ( rc )
      {
         if ( SDB_DMS_EOC != rc )
         {
            PD_LOG_MSG( PDERROR, "Get more rewrite query failed[ %d ]", rc ) ;
         }
         goto error ;
      }

   done:
      return rc ;
   error:
      if ( itr != _ctxMap.end() )
      {
         SDB_OSS_DEL itr->second ;
         _ctxMap.erase( itr ) ;
      }
      goto done ;
   }

   // Send reply message. It contains a header, and an optinal body.
   INT32 _seAdptAgentSession::_reply( MsgOpReply *header, const CHAR *buff,
                                      UINT32 size )
   {
      INT32 rc = SDB_OK ;

      if ( size > 0 )
      {
         rc = routeAgent()->syncSend( _netHandle, (MsgHeader *)header,
                                      (void *)buff, size ) ;
      }
      else
      {
         rc = routeAgent()->syncSend( _netHandle, (void *)header ) ;
      }

      PD_RC_CHECK( rc, PDERROR, "Session[ %s ] send reply message failed[ %d ]",
                   sessionName(), rc ) ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _seAdptAgentSession::_defaultMsgFunc ( NET_HANDLE handle,
                                                MsgHeader * msg )
   {
      return _onOPMsg( handle, msg ) ;
   }

   INT32 _seAdptAgentSession::_selectIndex( const CHAR *clName,
                                            const BSONObj &hint,
                                            BSONObj &newHint )
   {
      INT32 rc = SDB_OK ;
      seIdxMetaMgr* idxMetaMgr = NULL ;
      BSONObjBuilder builder ;
      UINT32 textIdxNum = 0 ;
      map<string, UINT16> idxNameMap ;
      UINT16 imID = SEADPT_INVALID_IMID ;

      idxMetaMgr = sdbGetSeAdapterCB()->getIdxMetaMgr() ;

      idxMetaMgr->getIdxNamesByCL( clName, idxNameMap ) ;
      if ( 0 == idxNameMap.size() )
      {
         rc = SDB_RTN_INDEX_NOTEXIST ;
         PD_LOG_MSG( PDERROR, "No index information found for collection[%s]",
                     clName ) ;
         goto error ;
      }

      try
      {
         // Traverse the hint to find text indices. They should be removed from
         // hint.
         BSONObjIterator itr( hint ) ;
         while ( itr.more() )
         {
            string esIdxName ;
            BSONElement ele = itr.next() ;
            string idxName = ele.str() ;
            if ( idxName.empty() )
            {
               continue ;
            }

            map<string, UINT16>::iterator itr =
                  idxNameMap.find( idxName.c_str() )  ;
            if ( itr != idxNameMap.end() )
            {
               // If multiple text indices specified in the hint, use the first
               // one.
               if ( 0 == textIdxNum )
               {
                  imID = itr->second ;
               }
               textIdxNum++ ;
            }
            else
            {
               builder.append( ele ) ;
            }
         }

         // If no text index in the hint, and there is only one text index on
         // the collection, use it for search. Otherwise, report error.
         if ( 0 == textIdxNum )
         {
            if ( 1 == idxNameMap.size() )
            {
               imID = idxNameMap.begin()->second ;
            }
            else
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "Text index name should be specified in "
                                    "hint when there are multiple text indices") ;
               goto error ;
            }
         }

         if ( _imContext )
         {
            idxMetaMgr->releaseIMContext( _imContext ) ;
         }

         rc = idxMetaMgr->getIMContext( &_imContext, imID, SHARED ) ;
         PD_RC_CHECK( rc, PDERROR, "Get index meta context failed[%d]", rc ) ;
         newHint = builder.obj() ;
      }
      catch ( std::exception &e )
      {
         rc = SDB_SYS ;
         PD_LOG( PDERROR, "Unexpected exception occurred: %s", e.what() ) ;
         goto error ;
      }

      PD_LOG( PDDEBUG, "Original hint[ %s ], new hint[ %s ]",
              hint.toString( FALSE, TRUE ).c_str(),
              newHint.toString( FALSE, TRUE ).c_str() ) ;
   done:
      return rc ;
   error:
      goto done ;
   }
}

