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

   Source File Name = pmdRestSession.cpp

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          04/14/2014  Xu Jianhui  Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdRestSession.hpp"
#include "pmdController.hpp"
#include "omManager.hpp"
#include "pmdEDUMgr.hpp"
#include "msgDef.h"
#include "fmpDef.h"
#include "utilCommon.hpp"
#include "ossMem.hpp"
#include "rtnCommand.hpp"
#include "../omsvc/omCommand.hpp"
#include "rtn.hpp"
#include "msgAuth.hpp"
#include "pmdTrace.hpp"
#include "../bson/bson.h"
#include "authCB.hpp"
#include "utilStr.hpp"
//#include "ossUtil.h"
//#include "pd.hpp"
#include "msg.h"
#include "omDef.hpp"

using namespace bson ;

namespace engine
{
   void _sendOpError2Web ( INT32 rc, restAdaptor *pAdptor,
                           restResponse &response, pmdRestSession *pRestSession,
                           pmdEDUCB* pEduCB )
   {
      SDB_ASSERT( ( NULL != pAdptor ) && ( NULL != pRestSession )
                  && ( NULL != pEduCB ), "pAdptor or pRestSession or pEduCB"
                  " can't be null" ) ;

      BSONObj _errorInfo = utilGetErrorBson( rc, pEduCB->getInfo(
                                             EDU_INFO_ERROR ) ) ;

      response.setOPResult( rc, _errorInfo ) ;
      pAdptor->sendRest( pRestSession->socket(), &response ) ;
   }

   #define PMD_REST_SESSION_SNIFF_TIMEOUT    ( 10 * OSS_ONE_SEC )

   /*
      util
   */
   // PD_TRACE_DECLARE_FUNCTION( SDB__UTILMSGFLAG, "utilMsgFlag" )
   static INT32 utilMsgFlag( const string strFlag, INT32 &flag )
   {
      PD_TRACE_ENTRY( SDB__UTILMSGFLAG ) ;
      INT32 rc = SDB_OK ;
      flag = 0 ;
      INT32 subFlag = 0 ;

      vector<string> subFlags ;
      subFlags = utilStrSplit( strFlag, REST_FLAG_SEP ) ;

      vector<string>::iterator it = subFlags.begin() ;
      for ( ; it != subFlags.end(); it++ )
      {
         utilStrTrim( *it ) ;
         const char *strSubFlag = ( *it ).c_str() ;

         // treat it as string flag
         if ( 0 == ossStrcmp( strSubFlag,
                              REST_VALUE_FLAG_UPDATE_KEEP_SK ) )
         {
            flag |= FLG_UPDATE_KEEP_SHARDINGKEY ;
            continue ;
         }
         if ( 0 == ossStrcmp( strSubFlag,
                              REST_VALUE_FLAG_QUERY_KEEP_SK_IN_UPDATE ) )
         {
            flag |= FLG_QUERY_KEEP_SHARDINGKEY_IN_UPDATE ;
            continue ;
         }
         if ( 0 == ossStrcmp( strSubFlag,
                              REST_VALUE_FLAG_QUERY_FORCE_HINT ) )
         {
            flag |= FLG_QUERY_FORCE_HINT ;
            continue ;
         }
         if ( 0 == ossStrcmp( strSubFlag,
                              REST_VALUE_FLAG_QUERY_PARALLED ) )
         {
            flag |= FLG_QUERY_PARALLED ;
            continue ;
         }
         if ( 0 == ossStrcmp( strSubFlag,
                              REST_VALUE_FLAG_QUERY_WITH_RETURNDATA ) )
         {
            flag |= FLG_QUERY_WITH_RETURNDATA ;
            continue ;
         }

         // treat it as number
         rc = utilStr2Num( strSubFlag, subFlag );
         if ( rc )
         {
            PD_LOG( PDERROR, "Unrecognized flag: %s, rc: %d",
                    strSubFlag, rc ) ;
            goto error ;
         }
         flag |= subFlag ;

      }

   done :
      PD_TRACE_EXITRC( SDB__UTILMSGFLAG, rc ) ;
      return rc ;
   error :
      goto done ;
   }

   /*
      _restSessionInfo implement
   */
   void _restSessionInfo::releaseMem()
   {
      pmdEDUCB::CATCH_MAP_IT it = _catchMap.begin() ;
      while ( it != _catchMap.end() )
      {
         SDB_OSS_FREE( it->second ) ;
         ++it ;
      }
      _catchMap.clear() ;
   }

   void _restSessionInfo::pushMemToMap( _pmdEDUCB::CATCH_MAP &catchMap )
   {
      _pmdEDUCB::CATCH_MAP_IT it = _catchMap.begin() ;
      while ( it != _catchMap.end() )
      {
         catchMap.insert( std::make_pair( it->first, it->second ) ) ;
         ++it ;
      }
      _catchMap.clear() ;
   }

   void _restSessionInfo::makeMemFromMap( _pmdEDUCB::CATCH_MAP &catchMap )
   {
      _pmdEDUCB::CATCH_MAP_IT it = catchMap.begin() ;
      while ( it != catchMap.end() )
      {
         _catchMap.insert( std::make_pair( it->first, it->second ) ) ;
         ++it ;
      }
      catchMap.clear() ;
   }

   /*
      _pmdRestSession implement
   */
   _pmdRestSession::_pmdRestSession( SOCKET fd )
   :_pmdSession( fd )
   {
      _pFixBuff         = NULL ;
      _pSessionInfo     = NULL ;
      _pRTNCB           = NULL ;

      _wwwRootPath      = pmdGetOptionCB()->getWWWPath() ;
      _pRestTransfer    = SDB_OSS_NEW RestToMSGTransfer( this ) ;
      _pRestTransfer->init() ;
   }

   _pmdRestSession::~_pmdRestSession()
   {
      if ( _pFixBuff )
      {
         sdbGetPMDController()->releaseFixBuf( _pFixBuff ) ;
         _pFixBuff = NULL ;
      }

      if ( NULL != _pRestTransfer )
      {
         SDB_OSS_DEL _pRestTransfer ;
         _pRestTransfer = NULL ;
      }
   }

   INT32 _pmdRestSession::getServiceType() const
   {
      return CMD_SPACE_SERVICE_LOCAL ;
   }

   SDB_SESSION_TYPE _pmdRestSession::sessionType() const
   {
      return SDB_SESSION_REST ;
   }

   INT32 _pmdRestSession::run()
   {
      INT32 rc               = SDB_OK ;
      restAdaptor *pAdptor   = sdbGetPMDController()->getRestAdptor() ;
      pmdEDUMgr *pEDUMgr     = NULL ;
      monDBCB *mondbcb       = pmdGetKRCB()->getMonDBCB () ;
      string sessionID ;

      if ( !_pEDUCB )
      {
         rc = SDB_SYS ;
         goto error ;
      }

      pEDUMgr = _pEDUCB->getEDUMgr() ;

      while ( !_pEDUCB->isDisconnected() && !_socket.isClosed() )
      {
         restRequest request( _pEDUCB ) ;
         restResponse response( _pEDUCB ) ;

         _pEDUCB->resetInterrupt() ;
         _pEDUCB->resetInfo( EDU_INFO_ERROR ) ;
         _pEDUCB->resetLsn() ;
         _pEDUCB->updateTransConf() ;

         rc = request.init() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to init request, rc:%d", rc ) ;
            goto error ;
         }

         rc = response.init() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to init response, rc:%d", rc ) ;
            goto error ;
         }

         // sniff wether has data
         rc = sniffData( _pSessionInfo ? OSS_ONE_SEC :
                         PMD_REST_SESSION_SNIFF_TIMEOUT ) ;
         if ( SDB_TIMEOUT == rc )
         {
            if ( _pSessionInfo )
            {
               saveSession() ;
               sdbGetPMDController()->detachSessionInfo( _pSessionInfo ) ;
               _pSessionInfo = NULL ;
               continue ;
            }
            else
            {
               rc = SDB_OK ;
               break ;
            }
         }
         else if ( rc < 0 )
         {
            break ;
         }

#ifdef SDB_ENTERPRISE

#ifdef SDB_SSL
         if ( _isAwaitingHandshake() )
         {
            if ( pmdGetOptionCB()->useSSL() )
            {
               CHAR buff[ 4 ] = { 0 } ;
               INT32 recvLen  = 0 ;

               rc = _socket.recv( buff, sizeof( buff ), recvLen,
                                  PMD_REST_SESSION_SNIFF_TIMEOUT,
                                  MSG_PEEK, TRUE, TRUE ) ;
               if ( rc < 0 )
               {
                  break;
               }

               // https handshake
               // 22 is the SSL handshake message type
               if ( 22 == buff[0] )
               {
                  rc = _socket.doSSLHandshake ( NULL, 0 ) ;
                  if ( rc )
                  {
                     break ;
                  }

                  _setHandshakeReceived() ;

                  continue;
               }
            }

             _setHandshakeReceived() ;
         }
#endif

#endif /* SDB_ENTERPRISE */
         // recv rest header
         rc = pAdptor->recvHeader( socket(), &request ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Session[%s] failed to recv rest header, "
                    "rc: %d", sessionName(), rc ) ;
            if ( SDB_REST_EHS == rc )
            {
               response.setResponse( HTTP_BADREQ ) ;
               pAdptor->sendRest( socket(), &response ) ;
            }
            else if ( SDB_APP_FORCED != rc )
            {
               _sendOpError2Web( rc, pAdptor, response, this, _pEDUCB ) ;
            }
            break ;
         }

         request.buildResponse( response ) ;

         // session is not exist
         if ( !_pSessionInfo )
         {
            // find session id
            sessionID = request.getHeader( OM_REST_HEAD_SESSIONID ) ;
            // if 'SessionID' exist, attach the sessionInfo
            if ( FALSE == sessionID.empty() )
            {
               PD_LOG( PDINFO, "Rest session: %s", sessionID.c_str() ) ;
               _pSessionInfo = sdbGetPMDController()->attachSessionInfo(
                                  sessionID ) ;
            }

            // if session exist, restore
            if ( _pSessionInfo )
            {
               _client.setAuthed( TRUE ) ;
               restoreSession() ;
            }
         }
         // recv body
         rc = pAdptor->recvBody( socket(), &request ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Session[%s] failed to recv rest body, "
                    "rc: %d", sessionName(), rc ) ;
            if ( SDB_REST_EHS == rc )
            {
               response.setResponse( HTTP_BADREQ ) ;
               pAdptor->sendRest( socket(), &response ) ;
            }
            else if ( SDB_APP_FORCED != rc )
            {
               _sendOpError2Web( rc, pAdptor, response, this, _pEDUCB ) ;
            }
            break ;
         }

         // update active time
         if ( _pSessionInfo )
         {
            _pSessionInfo->active() ;
         }

         // increase process event count
         _pEDUCB->incEventCount() ;
         mondbcb->addReceiveNum() ;

         // activate edu
         if ( SDB_OK != ( rc = pEDUMgr->activateEDU( _pEDUCB ) ) )
         {
            PD_LOG( PDERROR, "Session[%s] activate edu failed, rc: %d",
                    sessionName(), rc ) ;
            break ;
         }

         // process msg
         rc = _processMsg( request, response ) ;
         if ( rc )
         {
            break ;
         }

         if ( FALSE == request.isKeepAlive() ||
              FALSE == response.isKeepAlive() )
         {
            break ;
         }

         // wait edu
         if ( SDB_OK != ( rc = pEDUMgr->waitEDU( _pEDUCB ) ) )
         {
            PD_LOG( PDERROR, "Session[%s] wait edu failed, rc: %d",
                    sessionName(), rc ) ;
            break ;
         }

         rc = SDB_OK ;
      } // end while

   done:
      disconnect() ;
      return rc ;
   error:
      goto done ;
   }


   INT32 _pmdRestSession::_translateMSG( restAdaptor *pAdaptor,
                                         restRequest &request,
                                         MsgHeader **msg )
   {
      SDB_ASSERT( NULL != msg, "msg can't be null" ) ;

      INT32 rc = SDB_OK ;
      rc = _pRestTransfer->trans( pAdaptor, request, msg ) ;
      if ( SDB_OK != rc )
      {
         //PD_LOG( PDERROR, "transfer rest message failed:rc=%d", rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }


   INT32 _pmdRestSession::_fetchOneContext( SINT64 &contextID,
                                            rtnContextBuf &contextBuff )
   {
      INT32 rc = SDB_OK ;
      rtnContext *pContext = _pRTNCB->contextFind( contextID ) ;
      if ( NULL == pContext )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG( PDERROR, "can't fine context,contextID=%u", rc, contextID ) ;
         contextID = -1 ;
      }
      else
      {
         rc = pContext->getMore( -1, contextBuff, _pEDUCB ) ;
         if ( rc || pContext->eof() )
         {
            _pRTNCB->contextDelete( contextID, _pEDUCB ) ;
            if ( SDB_DMS_EOC != rc )
            {
               PD_LOG( PDERROR, "getmore failed:rc=%d,contextID=%u", rc,
                       contextID ) ;
               goto error ;
            }
         }
      }

      rc = SDB_OK ;
   done:
      return rc ;
   error:
      contextID = -1 ;
      goto done ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_PMDRESTSN_PROMSG, "_pmdRestSession::_processMsg" )
   INT32 _pmdRestSession::_processMsg( restRequest &request,
                                       restResponse &response )
   {
      PD_TRACE_ENTRY( SDB_PMDRESTSN_PROMSG );
      INT32 rc = SDB_OK ;
      restAdaptor *pAdaptor = sdbGetPMDController()->getRestAdptor() ;
      string subCommand ;

      subCommand = request.getQuery( OM_REST_FIELD_COMMAND ) ;
      if ( OM_LOGOUT_REQ == subCommand )
      {
         if ( isAuthOK() )
         {
            doLogout() ;
            pAdaptor->sendRest( socket(), &response ) ;
            goto done ;
         }
         else
         {
            rc = SDB_PMD_SESSION_NOT_EXIST ;
            PD_LOG_MSG( PDERROR, "session does not exist:rc=%d", rc ) ;
            _sendOpError2Web( rc, pAdaptor, response, this, _pEDUCB ) ;
            goto done ;
         }
      }

      rc = _processBusinessMsg( pAdaptor, request, response ) ;
      if ( SDB_UNKNOWN_MESSAGE == rc )
      {
         // in this case we should send response(SDB_UNKNOWN_MESSAGE) to
         // the client
         PD_LOG_MSG( PDERROR, "translate message failed:rc=%d", rc ) ;
         _sendOpError2Web( rc, pAdaptor, response, this, _pEDUCB ) ;
      }

   done:
      PD_TRACE_EXITRC ( SDB_PMDRESTSN_PROMSG, rc );
      return rc ;
   }

   INT32 _pmdRestSession::_checkAuth( restRequest *request )
   {
      INT32 rc = SDB_OK ;

      if ( request->isHeaderExist( OM_REST_HEAD_SDBUSER ) &&
           request->isHeaderExist( OM_REST_HEAD_SDBPASSWD ) )
      {
         string userName = request->getHeader( OM_REST_HEAD_SDBUSER ) ;
         string passwd   = request->getHeader( OM_REST_HEAD_SDBPASSWD ) ;

         BSONObj bsonAuth = BSON( SDB_AUTH_USER << userName <<
                                  SDB_AUTH_PASSWD << passwd ) ;
         if ( !getClient()->isAuthed() )
         {
            rc = getClient()->authenticate( userName.c_str(), passwd.c_str() ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "authenticate failed:rc=%d", rc ) ;
               goto error ;
            }
         }
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdRestSession::_processBusinessMsg( restAdaptor *pAdaptor,
                                               restRequest &request,
                                               restResponse &response )
   {
      INT32 rc        = SDB_OK ;
      INT32 rtnCode   = SDB_OK ;
      INT64 contextID = -1 ;
      rtnContextBuf contextBuff ;
      BOOLEAN needReplay = FALSE ;
      BOOLEAN needRollback = FALSE ;
      MsgHeader *msg = NULL ;

      rc = _translateMSG( pAdaptor, request, &msg ) ;
      if ( SDB_OK != rc )
      {
         if ( SDB_UNKNOWN_MESSAGE != rc )
         {
            PD_LOG( PDERROR, "translate message failed:rc=%d", rc ) ;
            _sendOpError2Web( rc, pAdaptor, response, this, _pEDUCB ) ;
         }

         // if SDB_UNKNOWN_MESSAGE == rc, we should try _processOMRestMsg
         goto error ;
      }

      rc = _checkAuth( &request ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "check auth failed:rc=%d", rc ) ;
         _sendOpError2Web( rc, pAdaptor, response, this, eduCB() ) ;
         goto error ;
      }

      rtnCode = getProcessor()->processMsg( msg, contextBuff, contextID,
                                            needReplay, needRollback ) ;
      if ( rtnCode )
      {
         BSONObj tmp ;
         BSONObjBuilder builder ;

         if ( contextBuff.recordNum() > 0 )
         {
            BSONObj errorInfo( contextBuff.data() ) ;

            if ( !errorInfo.hasField( OM_REST_RES_RETCODE ) )
            {
               builder.append( OM_REST_RES_RETCODE, rtnCode ) ;
            }

            builder.appendElements( errorInfo ) ;
         }
         else
         {
            BSONObj errorInfo = utilGetErrorBson( rtnCode,
                                          _pEDUCB->getInfo( EDU_INFO_ERROR ) ) ;
            builder.appendElements( errorInfo ) ;
         }

         tmp = builder.obj() ;

         response.setOPResult( rtnCode, tmp ) ;

         rc = pAdaptor->sendRest( socket(), &response ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "failed to send response: rc=%d", rc ) ;
            goto error ;
         }

         goto error ;
      }

      _dealWithLoginReq( rtnCode, request, response ) ;

      {
         BSONObj tmp = BSON( OM_REST_RES_RETCODE << rtnCode ) ;

         response.setOPResult( rtnCode, tmp ) ;
      }

      if ( contextBuff.recordNum() > 0 )
      {
         rc = pAdaptor->setResBody( socket(), &response,
                                    contextBuff.data(),
                                    contextBuff.size(),
                                    contextBuff.recordNum() ) ;
         if ( rc )
         {
            PD_LOG( PDERROR, "failed to send response: rc=%d", rc ) ;
            goto error ;
         }
      }

      if ( -1 != contextID )
      {
         rtnContext *pContext = _pRTNCB->contextFind( contextID ) ;

         while ( NULL != pContext )
         {
            rc = pContext->getMore( -1, contextBuff, _pEDUCB ) ;
            if ( rc )
            {
               _pRTNCB->contextDelete( contextID, _pEDUCB ) ;
               contextID = -1 ;
               if ( SDB_DMS_EOC != rc )
               {
                  PD_LOG_MSG( PDERROR, "getmore failed:rc=%d", rc ) ;
                  _sendOpError2Web( rc, pAdaptor, response,
                                    this, _pEDUCB ) ;
                  goto error ;
               }

               rc = SDB_OK ;
               break ;
            }

            rc = pAdaptor->setResBody( socket(), &response,
                                       contextBuff.data(),
                                       contextBuff.size(),
                                       contextBuff.recordNum() ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "failed to send response: rc=%d", rc ) ;
               goto error ;
            }
         }
      }

      rc = pAdaptor->setResBodyEnd( socket(), &response ) ;
      if ( rc )
      {
         PD_LOG( PDERROR, "failed to send response: rc=%d", rc ) ;
         goto error ;
      }

   done:
      if ( -1 != contextID )
      {
         _pRTNCB->contextDelete( contextID, _pEDUCB ) ;
         contextID = -1 ;
      }
      if ( NULL != msg )
      {
         SDB_OSS_FREE( msg ) ;
         msg = NULL ;
      }
      return rc ;
   error:
      goto done ;
   }

   INT32 _pmdRestSession::_dealWithLoginReq( INT32 result, restRequest &request,
                                             restResponse &response )
   {
      INT32 rc = SDB_OK ;
      string subCommand ;

      if ( SDB_OK != result )
      {
         goto done ;
      }

      subCommand = request.getQuery( OM_REST_FIELD_COMMAND ) ;
      if ( OM_LOGIN_REQ == subCommand )
      {
         string user = request.getQuery( OM_REST_FIELD_LOGIN_NAME ) ;
         string sessionId ;

         rc = doLogin( user, socket()->getLocalIP() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "login failed:rc=%d", rc ) ;
         }

         sessionId = getSessionID() ;

         response.putHeader( OM_REST_HEAD_SESSIONID, sessionId ) ;
      }

   done:
      return rc ;
   }

   void _pmdRestSession::_onAttach()
   {
      pmdKRCB *krcb = pmdGetKRCB() ;
      _pRTNCB = krcb->getRTNCB() ;

      if ( NULL != sdbGetPMDController()->getRSManager() )
      {
         sdbGetPMDController()->getRSManager()->registerEDU( eduCB() ) ;
      }
   }

   void _pmdRestSession::_onDetach()
   {
      // save session info
      if ( _pSessionInfo )
      {
         saveSession() ;
         sdbGetPMDController()->detachSessionInfo( _pSessionInfo ) ;
         _pSessionInfo = NULL ;
      }

      if ( NULL != sdbGetPMDController()->getRSManager() )
      {
         sdbGetPMDController()->getRSManager()->unregEUD( eduCB() ) ;
      }
   }

   INT32 _pmdRestSession::getFixBuffSize() const
   {
      return sdbGetPMDController()->getFixBufSize() ;
   }

   CHAR* _pmdRestSession::getFixBuff ()
   {
      if ( !_pFixBuff )
      {
         _pFixBuff = sdbGetPMDController()->allocFixBuf() ;
      }
      return _pFixBuff ;
   }

   void _pmdRestSession::restoreSession()
   {
      pmdEDUCB::CATCH_MAP catchMap ;
      _pSessionInfo->pushMemToMap( catchMap ) ;
      eduCB()->restoreBuffs( catchMap ) ;
   }

   void _pmdRestSession::saveSession()
   {
      pmdEDUCB::CATCH_MAP catchMap ;
      eduCB()->saveBuffs( catchMap ) ;
      _pSessionInfo->makeMemFromMap( catchMap ) ;
   }

   BOOLEAN _pmdRestSession::isAuthOK()
   {
      if ( NULL != _pSessionInfo )
      {
         if ( _pSessionInfo->_authOK )
         {
            return TRUE ;
         }
      }

      return FALSE ;
   }

   string _pmdRestSession::getLoginUserName()
   {
      if ( isAuthOK() )
      {
         return _pSessionInfo->_attr._userName ;
      }

      return "" ;
   }

   const CHAR* _pmdRestSession::getSessionID()
   {
      if ( NULL != _pSessionInfo )
      {
         if ( _pSessionInfo->_authOK )
         {
            return _pSessionInfo->_id.c_str();
         }
      }

      return "" ;
   }

   INT32 _pmdRestSession::doLogin( const string & username,
                                   UINT32 localIP )
   {
      INT32 rc = SDB_OK ;

      doLogout() ;

      _pSessionInfo = sdbGetPMDController()->newSessionInfo( username,
                                                             localIP ) ;
      if ( !_pSessionInfo )
      {
         PD_LOG ( PDERROR, "Failed to allocate session" ) ;
         rc = SDB_OOM ;
      }
      else
      {
         _pSessionInfo->_authOK = TRUE ;
      }
      return rc ;
   }

   void _pmdRestSession::doLogout()
   {
      if ( _pSessionInfo )
      {
         sdbGetPMDController()->releaseSessionInfo( _pSessionInfo->_id ) ;
         _pSessionInfo = NULL ;
      }
   }

   typedef struct restCommand2Func_s
   {
      string commandName ;
      restTransFunc func ;
   } restCommand2Func ;

   RestToMSGTransfer::RestToMSGTransfer( pmdRestSession *session )
                     :_restSession( session )
   {
   }

   RestToMSGTransfer::~RestToMSGTransfer()
   {
   }

   INT32 RestToMSGTransfer::init()
   {
      INT32 rc  = SDB_OK ;
      INT32 i   = 0 ;
      INT32 len = 0 ;
      static restCommand2Func s_commandArray[] = {
         { REST_CMD_NAME_QUERY,  &RestToMSGTransfer::_convertQuery },
         { REST_CMD_NAME_INSERT, &RestToMSGTransfer::_convertInsert },
         { REST_CMD_NAME_DELETE, &RestToMSGTransfer::_convertDelete },
         { REST_CMD_NAME_UPDATE, &RestToMSGTransfer::_convertUpdate },
         { REST_CMD_NAME_UPSERT, &RestToMSGTransfer::_convertUpsert },
         { REST_CMD_NAME_QUERY_UPDATE,
                                 &RestToMSGTransfer::_convertQueryUpdate },
         { REST_CMD_NAME_QUERY_REMOVE,
                                 &RestToMSGTransfer::_convertQueryRemove },
         { CMD_NAME_CREATE_COLLECTIONSPACE,
                                 &RestToMSGTransfer::_convertCreateCS },
         { CMD_NAME_CREATE_COLLECTION,
                                 &RestToMSGTransfer::_convertCreateCL },
         { CMD_NAME_DROP_COLLECTIONSPACE,
                                 &RestToMSGTransfer::_convertDropCS },
         { CMD_NAME_DROP_COLLECTION,
                                 &RestToMSGTransfer::_convertDropCL },
         { CMD_NAME_CREATE_INDEX, &RestToMSGTransfer::_convertCreateIndex },
         { CMD_NAME_DROP_INDEX,  &RestToMSGTransfer::_convertDropIndex },
         { CMD_NAME_SPLIT,       &RestToMSGTransfer::_convertSplit },

         { REST_CMD_NAME_TRUNCATE_COLLECTION,
                              &RestToMSGTransfer::_convertTruncateCollection },
         { REST_CMD_NAME_ATTACH_COLLECTION,
                                 &RestToMSGTransfer::_coverAttachCollection },
         { REST_CMD_NAME_DETACH_COLLECTION,
                                 &RestToMSGTransfer::_coverDetachCollection },
         { CMD_NAME_ALTER_COLLECTION,
                                 &RestToMSGTransfer::_convertAlterCollection },

         { CMD_NAME_GET_COUNT,   &RestToMSGTransfer::_convertGetCount },

         { CMD_NAME_LIST_GROUPS, &RestToMSGTransfer::_convertListGroups },
         { REST_CMD_NAME_START_GROUP,
                                 &RestToMSGTransfer::_convertStartGroup },
         { REST_CMD_NAME_STOP_GROUP,
                                 &RestToMSGTransfer::_convertStopGroup },

         { REST_CMD_NAME_START_NODE,
                                 &RestToMSGTransfer::_convertStartNode },
         { REST_CMD_NAME_STOP_NODE,
                                 &RestToMSGTransfer::_convertStopNode },

         { CMD_NAME_CRT_PROCEDURE,
                                 &RestToMSGTransfer::_convertCreateProcedure },
         { CMD_NAME_RM_PROCEDURE,
                                 &RestToMSGTransfer::_convertRemoveProcedure },
         { CMD_NAME_LIST_PROCEDURES,
                                 &RestToMSGTransfer::_convertListProcedures },
         { CMD_NAME_LIST_CONTEXTS,
                                 &RestToMSGTransfer::_convertListContexts },
         { CMD_NAME_LIST_CONTEXTS_CURRENT,
                                 &RestToMSGTransfer::_convertListContextsCurrent },
         { CMD_NAME_LIST_SESSIONS,
                                 &RestToMSGTransfer::_convertListSessions },
         { CMD_NAME_LIST_SESSIONS_CURRENT,
                                 &RestToMSGTransfer::_convertListSessionsCurrent },
         { CMD_NAME_LIST_COLLECTIONS,
                                 &RestToMSGTransfer::_convertListCollections },
         { CMD_NAME_LIST_COLLECTIONSPACES,
                                 &RestToMSGTransfer::_convertListCollectionSpaces },
         { CMD_NAME_LIST_STORAGEUNITS,
                                 &RestToMSGTransfer::_convertListStorageUnits },
         { CMD_NAME_CREATE_DOMAIN,
                                 &RestToMSGTransfer::_convertCreateDomain },
         { CMD_NAME_DROP_DOMAIN, &RestToMSGTransfer::_convertDropDomain },
         { CMD_NAME_ALTER_DOMAIN,
                                 &RestToMSGTransfer::_convertAlterDomain },
         { CMD_NAME_LIST_DOMAINS,
                                 &RestToMSGTransfer::_convertListDomains },
         { CMD_NAME_LIST_TASKS,  &RestToMSGTransfer::_convertListTasks },
         { REST_CMD_NAME_LISTINDEXES,
                                 &RestToMSGTransfer::_convertListIndexes },
//         { CMD_NAME_LIST_CL_IN_DOMAIN, &RestToMSGTransfer::_convertQuery },
         { CMD_NAME_SNAPSHOT_CONTEXTS,
                                 &RestToMSGTransfer::_convertSnapshotContext },
         { CMD_NAME_SNAPSHOT_CONTEXTS_CURRENT,
                                 &RestToMSGTransfer::_convertSnapshotContextCurrent },
         { CMD_NAME_SNAPSHOT_SESSIONS,
                                 &RestToMSGTransfer::_convertSnapshotSessions },
         { CMD_NAME_SNAPSHOT_SESSIONS_CURRENT,
                                 &RestToMSGTransfer::_convertSnapshotSessionsCurrent },
         { CMD_NAME_SNAPSHOT_COLLECTIONS,
                                 &RestToMSGTransfer::_convertSnapshotCollections },
         { CMD_NAME_SNAPSHOT_COLLECTIONSPACES,
                                 &RestToMSGTransfer::_convertSnapshotCollectionSpaces },
         { CMD_NAME_SNAPSHOT_DATABASE,
                                 &RestToMSGTransfer::_convertSnapshotDatabase },
         { CMD_NAME_SNAPSHOT_SYSTEM,
                                 &RestToMSGTransfer::_convertSnapshotSystem },
         { CMD_NAME_SNAPSHOT_CATA,
                                 &RestToMSGTransfer::_convertSnapshotCata },
         { CMD_NAME_SNAPSHOT_ACCESSPLANS,
                                 &RestToMSGTransfer::_convertSnapshotAccessPlans },
         { CMD_NAME_SNAPSHOT_HEALTH,
                                 &RestToMSGTransfer::_convertSnapshotHealth },
         { CMD_NAME_SNAPSHOT_CONFIGS,
                                 &RestToMSGTransfer::_convertSnapshotConfigs },
         { CMD_NAME_LIST_LOBS,   &RestToMSGTransfer::_convertListLobs },
         { OM_LOGIN_REQ,         &RestToMSGTransfer::_convertLogin },
         { REST_CMD_NAME_EXEC,   &RestToMSGTransfer::_convertExec },
         { CMD_NAME_FORCE_SESSION,
                                 &RestToMSGTransfer::_convertForceSession },
         { CMD_NAME_ANALYZE,     &RestToMSGTransfer::_convertAnalyze },
         { CMD_NAME_UPDATE_CONFIG,
                                 &RestToMSGTransfer::_convertUpdateConfig },
         { CMD_NAME_DELETE_CONFIG,
                                 &RestToMSGTransfer::_convertDeleteConfig }
      } ;

      len = sizeof( s_commandArray ) / sizeof( restCommand2Func ) ;
      for ( i = 0 ; i < len ; i++ )
      {
         _mapTransFunc.insert( _value_type( s_commandArray[i].commandName,
                                            s_commandArray[i].func ) ) ;
      }

      return rc ;
   }

   INT32 RestToMSGTransfer::trans( restAdaptor *pAdaptor, restRequest &request,
                                   MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      string subCommand ;
      _iterator iter ;

      subCommand = request.getQuery( OM_REST_FIELD_COMMAND ) ;
      if ( subCommand.empty() )
      {
         rc = SDB_UNKNOWN_MESSAGE ;
         if ( !pmdGetKRCB()->isCBValue( SDB_CB_OMSVC ) )
         {
            // we will have another flow if it's OMSVC. we can't say it's
            // a error now
            PD_LOG_MSG( PDERROR, "can't resolve field:field=%s",
                        OM_REST_FIELD_COMMAND ) ;
         }

         goto error ;
      }

      iter = _mapTransFunc.find( subCommand ) ;
      if ( iter != _mapTransFunc.end() )
      {
         restTransFunc func = iter->second ;
         rc = (this->*func)( pAdaptor, request, msg ) ;
      }
      else
      {
         if ( !pmdGetKRCB()->isCBValue( SDB_CB_OMSVC ) )
         {
            // we will have another flow if it's OMSVC. we can't say it's
            // a error now
            PD_LOG_MSG( PDERROR, "unsupported command:command=%s",
                        subCommand.c_str() ) ;
         }
         rc = SDB_UNKNOWN_MESSAGE ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateCS( restAdaptor *pAdaptor,
                                              restRequest &request,
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTIONSPACE ;
      string option ;
      string collectionSpace ;
      BSONObj optionObj ;
      BSONObj query ;

      collectionSpace = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionSpace.empty() )
      {
         //try another key name
         collectionSpace = request.getQuery( REST_KEY_NAME_COLLECTIONSPACE ) ;
         if ( collectionSpace.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collectionspace's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTIONSPACE ) ;
            goto error ;
         }
      }

      option = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if ( FALSE == option.empty() )
      {
         rc = fromjson( option.c_str(), optionObj, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_OPTIONS, option.c_str() ) ;
            goto error ;
         }
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_NAME, collectionSpace ) ;
         {
            BSONObjIterator it ( optionObj ) ;
            while ( it.more() )
            {
               builder.append( it.next() ) ;
            }
         }

         query = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateCL( restAdaptor *pAdaptor,
                                              restRequest &request,
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_CREATE_COLLECTION ;
      string option ;
      string collection ;
      BSONObj optionObj ;
      BSONObj query ;

      collection = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collection.empty() )
      {
         collection = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collection.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      option = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if ( FALSE == option.empty() )
      {
         rc = fromjson( option.c_str(), optionObj, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_OPTIONS, option.c_str() ) ;
            goto error ;
         }
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_NAME, collection ) ;
         {
            BSONObjIterator it ( optionObj ) ;
            while ( it.more() )
            {
               builder.append( it.next() ) ;
            }
         }

         query = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDropCS( restAdaptor *pAdaptor,
                                            restRequest &request,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTIONSPACE ;
      string collectionSpace ;
      BSONObj query ;

      collectionSpace = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionSpace.empty() )
      {
         collectionSpace = request.getQuery( REST_KEY_NAME_COLLECTIONSPACE ) ;
         if ( collectionSpace.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collectionspace's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTIONSPACE ) ;
            goto error ;
         }
      }

      query = BSON( FIELD_NAME_NAME << collectionSpace ) ;
      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDropCL( restAdaptor *pAdaptor,
                                            restRequest &request,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_DROP_COLLECTION ;
      string collection ;
      BSONObj query ;

      collection = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collection.empty() )
      {
         collection = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collection.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      query = BSON( FIELD_NAME_NAME  << collection ) ;
      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertQueryBasic( restAdaptor* pAdaptor,
                                                restRequest &request,
                                                string &collectionName,
                                                BSONObj& match,
                                                BSONObj& selector,
                                                BSONObj& order,
                                                BSONObj& hint,
                                                INT32* flag,
                                                SINT64* skip,
                                                SINT64* returnRow )
   {
      INT32 rc = SDB_OK ;
      string orderStr ;
      string hintStr ;
      string matchStr ;
      string selectorStr ;
      string flagStr ;
      string skipStr ;
      string returnRowStr ;
      string tableName ;

      SDB_ASSERT( pAdaptor, "pAdaptor can't be null") ;
      SDB_ASSERT( flag, "flag can't be null") ;
      SDB_ASSERT( skip, "skip can't be null") ;
      SDB_ASSERT( returnRow, "returnRow can't be null") ;

      tableName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( tableName.empty() )
      {
         tableName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( tableName.empty() )
         {
            PD_LOG_MSG( PDERROR, "get field failed:field=%s[or %s]",
                     FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }
      collectionName = tableName ;

      matchStr = request.getQuery( FIELD_NAME_FILTER ) ;
      if ( matchStr.empty() )
      {
         matchStr = request.getQuery( REST_KEY_NAME_MATCHER ) ;
      }

      selectorStr = request.getQuery( FIELD_NAME_SELECTOR ) ;

      orderStr = request.getQuery( FIELD_NAME_SORT ) ;
      if ( orderStr.empty() )
      {
         orderStr = request.getQuery( REST_KEY_NAME_ORDERBY ) ;
      }

      hintStr = request.getQuery( FIELD_NAME_HINT ) ;
      flagStr = request.getQuery( REST_KEY_NAME_FLAG ) ;
      skipStr = request.getQuery( FIELD_NAME_SKIP ) ;

      returnRowStr = request.getQuery( FIELD_NAME_RETURN_NUM ) ;
      if ( returnRowStr.empty() )
      {
         returnRowStr = request.getQuery( REST_KEY_NAME_LIMIT ) ;
      }

      if ( FALSE == matchStr.empty() )
      {
         rc = fromjson( matchStr.c_str(), match, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s], "
                        "value=%s", FIELD_NAME_FILTER,
                        REST_KEY_NAME_MATCHER, matchStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == selectorStr.empty() )
      {
         rc = fromjson( selectorStr.c_str(), selector, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_SELECTOR, selectorStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == orderStr.empty() )
      {
         rc = fromjson( orderStr.c_str(), order, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s], "
                        "value=%s", FIELD_NAME_SORT, REST_KEY_NAME_ORDERBY,
                        orderStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == hintStr.empty() )
      {
         rc = fromjson( hintStr.c_str(), hint, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_HINT, hintStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == flagStr.empty() )
      {
         rc = utilMsgFlag( flagStr, *flag ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,"
                        "value=%s", REST_KEY_NAME_FLAG, flagStr.c_str() ) ;
            goto error ;
         }
         *flag = *flag | FLG_QUERY_WITH_RETURNDATA ;
      }

      if ( FALSE == skipStr.empty() )
      {
         *skip = ossAtoll( skipStr.c_str() ) ;
      }

      if ( FALSE == returnRowStr.empty() )
      {
         *returnRow = ossAtoll( returnRowStr.c_str() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertQuery( restAdaptor *pAdaptor,
                                           restRequest &request,
                                           MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      string table ;

      {
         BSONObj order ;
         BSONObj hint ;
         BSONObj match ;
         BSONObj selector ;
         INT32 flag  = 0 ;
         SINT64 skip = 0 ;
         SINT64 returnRow = -1 ;

         rc = _convertQueryBasic( pAdaptor, request, table, match, selector,
                                  order, hint, &flag, &skip, &returnRow ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "convert basic queryMsg failed:rc=%d", rc ) ;
            goto error ;
         }

         rc = msgBuildQueryMsg( &pBuff, &buffSize, table.c_str(), flag, 0, skip,
                                returnRow, &match, &selector, &order, &hint ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "build queryMSG failed:rc=%d", rc ) ;
            goto error ;
         }
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertQueryUpdate( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg )
   {
      return _convertQueryModify( pAdaptor, request, msg, TRUE ) ;
   }

   INT32 RestToMSGTransfer::_convertQueryRemove( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg )
   {
      return _convertQueryModify( pAdaptor, request, msg, FALSE ) ;
   }

   INT32 RestToMSGTransfer::_convertQueryModify( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg,
                                                 BOOLEAN isUpdate )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      INT32 flag            = 0 ;
      SINT64 skip           = 0 ;
      SINT64 returnRow      = -1 ;
      BSONObj order ;
      BSONObj hint ;
      BSONObj match ;
      BSONObj selector ;
      BSONObj newHint ;
      string table ;

      rc = _convertQueryBasic( pAdaptor, request, table, match, selector,
                               order, hint, &flag, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "convert basic queryMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder newHintBuilder ;
         BSONObjBuilder modifyBuilder ;
         BSONObj modify ;

         // create $Modify
         if ( isUpdate )
         {
            string updateStr ;
            string returnNewStr ;
            BSONObj update ;
            BOOLEAN returnNew = FALSE ;

            updateStr = request.getQuery( REST_KEY_NAME_UPDATOR ) ;
            if ( updateStr.empty() )
            {
               PD_LOG_MSG( PDERROR, "get field failed:field=%s",
                           REST_KEY_NAME_UPDATOR ) ;
               rc = SDB_INVALIDARG ;
               goto error ;
            }

            rc = fromjson( updateStr.c_str(), update, 0 ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                           FIELD_NAME_OP_UPDATE, updateStr.c_str() ) ;
               goto error ;
            }

            returnNewStr = request.getQuery( FIELD_NAME_RETURNNEW ) ;
            if ( FALSE == returnNewStr.empty() )
            {
               // true
               if ( 0 == ossStrcasecmp( returnNewStr.c_str(), "true") )
               {
                  returnNew = TRUE ;
               }
               // false
               else if ( 0 == ossStrcasecmp( returnNewStr.c_str(), "false") )
               {
                  returnNew = FALSE ;
               }
               else
               {
                  PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                           FIELD_NAME_RETURNNEW, returnNewStr.c_str() ) ;
                  rc = SDB_INVALIDARG ;
                  goto error ;
               }
            }

            modifyBuilder.append( FIELD_NAME_OP, FIELD_OP_VALUE_UPDATE ) ;
            modifyBuilder.appendObject( FIELD_NAME_OP_UPDATE, update.objdata() ) ;
            modifyBuilder.appendBool( FIELD_NAME_RETURNNEW, returnNew ) ;
         }
         else
         {
            modifyBuilder.append( FIELD_NAME_OP, FIELD_OP_VALUE_REMOVE ) ;
            modifyBuilder.appendBool( FIELD_NAME_OP_REMOVE, TRUE ) ;
         }
         modify = modifyBuilder.obj() ;

         // create new hint with $Modify
         if ( !hint.isEmpty() )
         {
            newHintBuilder.appendElements( hint ) ;
         }
         newHintBuilder.appendObject( FIELD_NAME_MODIFY, modify.objdata() ) ;
         newHint = newHintBuilder.obj() ;
      }
      catch ( std::exception &e )
      {
         PD_LOG ( PDWARNING, "Failed to create $Modify, %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      flag |= FLG_QUERY_MODIFY ;

      rc = msgBuildQueryMsg( &pBuff, &buffSize, table.c_str(), flag, 0, skip,
                             returnRow, &match, &selector, &order, &newHint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build queryMSG failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertInsert( restAdaptor *pAdaptor,
                                            restRequest &request,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      SINT32 flag           = 0 ;
      string flagStr ;
      string collectionName ;
      string insertorStr ;
      BSONObj insertor ;

      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      flagStr = request.getQuery( REST_KEY_NAME_FLAG ) ;
      if ( FALSE == flagStr.empty() )
      {
         flag = ossAtoi( flagStr.c_str() ) ;
      }

      insertorStr = request.getQuery( REST_KEY_NAME_INSERTOR ) ;
      if ( insertorStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed",
                     REST_KEY_NAME_INSERTOR ) ;
         goto error ;
      }

      rc = fromjson( insertorStr.c_str(), insertor, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                     REST_KEY_NAME_INSERTOR, insertorStr.c_str() ) ;
         goto error ;
      }

      rc = msgBuildInsertMsg( &pBuff, &buffSize, collectionName.c_str(), flag,
                              0, &insertor );
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build insertMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertUpsert( restAdaptor *pAdaptor,
                                            restRequest &request,
                                            MsgHeader **msg )
   {
      return _convertUpdateBase( pAdaptor, request, msg, TRUE ) ;
   }

   INT32 RestToMSGTransfer::_convertUpdate( restAdaptor *pAdaptor,
                                            restRequest &request,
                                            MsgHeader **msg )
   {
      return _convertUpdateBase( pAdaptor, request, msg, FALSE ) ;
   }

   INT32 RestToMSGTransfer::_convertUpdateBase( restAdaptor *pAdaptor,
                                                restRequest &request,
                                                MsgHeader **msg,
                                                BOOLEAN isUpsert )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      SINT32 flag           = 0 ;
      string collectionName ;
      string updatorStr ;
      string matchStr ;
      string hintStr ;
      string flagStr ;
      BSONObj updator ;
      BSONObj matcher ;
      BSONObj hint ;

      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      flagStr = request.getQuery( REST_KEY_NAME_FLAG ) ;
      if ( FALSE == flagStr.empty() )
      {
         rc = utilMsgFlag( flagStr, flag ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,"
                        "value=%s", REST_KEY_NAME_FLAG, flagStr.c_str() ) ;
            goto error ;
         }
      }

      matchStr = request.getQuery( FIELD_NAME_FILTER ) ;
      if ( matchStr.empty() )
      {
         matchStr = request.getQuery( REST_KEY_NAME_MATCHER ) ;
      }

      if ( FALSE == matchStr.empty() )
      {
         rc = fromjson( matchStr.c_str(), matcher, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s],"
                        "value=%s", FIELD_NAME_FILTER,
                        REST_KEY_NAME_MATCHER, matchStr.c_str() ) ;
            goto error ;
         }
      }

      updatorStr = request.getQuery( REST_KEY_NAME_UPDATOR ) ;
      if ( updatorStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed",
                     REST_KEY_NAME_UPDATOR ) ;
         goto error ;
      }

      rc = fromjson( updatorStr.c_str(), updator, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                     REST_KEY_NAME_UPDATOR, updatorStr.c_str() ) ;
         goto error ;
      }

      hintStr = request.getQuery( FIELD_NAME_HINT ) ;
      if ( FALSE == hintStr.empty() )
      {
         rc = fromjson( hintStr.c_str(), hint, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                        FIELD_NAME_HINT, hintStr.c_str() ) ;
            goto error ;
         }
      }

      if ( isUpsert )
      {
         string setOnInsertStr ;
         BSONObj setOnInsert ;

         setOnInsertStr = request.getQuery( REST_KEY_NAME_SET_ON_INSERT ) ;
         if ( FALSE == setOnInsertStr.empty() )
         {
            rc = fromjson( setOnInsertStr.c_str(), setOnInsert, 0 ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                           REST_KEY_NAME_SET_ON_INSERT,
                           setOnInsertStr.c_str() ) ;
               goto error ;
            }

            try
            {
               BSONObjBuilder newHintBuilder ;
               if ( !hint.isEmpty() )
               {
                  newHintBuilder.appendElements( hint ) ;
               }
               newHintBuilder.append( FIELD_NAME_SET_ON_INSERT, setOnInsert ) ;
               hint = newHintBuilder.obj() ;
            }
            catch ( std::exception &e )
            {
               PD_LOG_MSG ( PDERROR, "Failed to create BSON object: %s",
                        e.what() ) ;
               rc = SDB_SYS ;
               goto error ;
            }
         }

         flag |= FLG_UPDATE_UPSERT ;
      }

      rc = msgBuildUpdateMsg( &pBuff, &buffSize, collectionName.c_str(), flag,
                              0, &matcher, &updator, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build updateMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDelete( restAdaptor *pAdaptor,
                                            restRequest &request,
                                            MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      SINT32 flag           = 0 ;
      string collectionName ;
      string deletorStr ;
      string hintStr ;
      string flagStr ;
      BSONObj deletor ;
      BSONObj hint ;

      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      flagStr = request.getQuery( REST_KEY_NAME_FLAG ) ;
      if ( FALSE == flagStr.empty() )
      {
         flag = ossAtoi( flagStr.c_str() ) ;
      }

      deletorStr = request.getQuery( REST_KEY_NAME_DELETOR ) ;
      if ( deletorStr.empty() )
      {
         deletorStr = request.getQuery( REST_KEY_NAME_MATCHER ) ;
      }

      if ( FALSE == deletorStr.empty() )
      {
         rc = fromjson( deletorStr.c_str(), deletor, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s],"
                        "value=%s", REST_KEY_NAME_DELETOR,
                        REST_KEY_NAME_MATCHER, deletorStr.c_str() ) ;
            goto error ;
         }
      }

      hintStr = request.getQuery( FIELD_NAME_HINT ) ;
      if ( FALSE == hintStr.empty() )
      {
         rc = fromjson( hintStr.c_str(), hint, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                        FIELD_NAME_HINT, hintStr.c_str() ) ;
            goto error ;
         }
      }

      rc = msgBuildDeleteMsg( &pBuff, &buffSize, collectionName.c_str(), flag,
                              0, &deletor, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build deleteMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertAlterCollection( restAdaptor *pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_ALTER_COLLECTION ;
      string optionStr ;
      string collectionName ;
      BSONObj option ;
      BSONObj query ;

      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      optionStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if ( optionStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get alter collection's %s failed",
                     FIELD_NAME_OPTIONS ) ;
         goto error ;
      }

      rc = fromjson( optionStr.c_str(), option, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                     FIELD_NAME_OPTIONS, optionStr.c_str() ) ;
         goto error ;
      }

      query = BSON( FIELD_NAME_NAME << collectionName <<
                   FIELD_NAME_OPTIONS << option ) ;
      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateIndex( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg )
   {
      INT32 rc                = SDB_OK ;
      CHAR *pBuff             = NULL ;
      INT32 buffSize          = 0 ;
      const CHAR *pCommand    = CMD_ADMIN_PREFIX CMD_NAME_CREATE_INDEX ;
      string collectionName ;
      string indexName ;
      string indexDef ;
      string unique ;
      string enforced ;
      string sortBufferSizeStr ;
      BSONObj indexDefObj ;

      // bson must use the original type of bool
      bool isUnique   = false ;
      bool isEnforced = false ;
      INT32 sortBufferSize = SDB_INDEX_SORT_BUFFER_DEFAULT_SIZE ;

      // collection name
      collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
      if ( collectionName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     REST_KEY_NAME_COLLECTION ) ;
         goto error ;
      }

      // index name
      indexName = request.getQuery( FIELD_NAME_INDEXNAME ) ;
      if ( indexName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_INDEXNAME ) ;
         goto error ;
      }

      // index define
      indexDef = request.getQuery( IXM_FIELD_NAME_INDEX_DEF ) ;
      if ( indexDef.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     IXM_FIELD_NAME_INDEX_DEF ) ;
         goto error ;
      }

      rc = fromjson( indexDef.c_str(), indexDefObj, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                     IXM_FIELD_NAME_INDEX_DEF, indexDef.c_str() ) ;
         goto error ;
      }

      // unique
      unique = request.getQuery( IXM_FIELD_NAME_UNIQUE ) ;
      if ( FALSE == unique.empty() )
      {
         if ( ossStrcasecmp( unique.c_str(), "true" ) == 0 )
         {
            isUnique = true ;
         }
         else if ( ossStrcasecmp( unique.c_str(), "false" ) == 0 )
         {
            isUnique = false ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        IXM_FIELD_NAME_UNIQUE, unique.c_str() ) ;
            goto error ;
         }
      }

      // enforced
      enforced = request.getQuery( IXM_FIELD_NAME_ENFORCED ) ;
      if ( FALSE == enforced.empty() )
      {
         if ( ossStrcasecmp( enforced.c_str(), "true" ) == 0 )
         {
            isEnforced = true ;
         }
         else if ( ossStrcasecmp( enforced.c_str(), "false" ) == 0 )
         {
            isEnforced = false ;
         }
         else
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        IXM_FIELD_NAME_ENFORCED, enforced.c_str() ) ;
            goto error ;
         }
      }

      // SortBufferSize
      sortBufferSizeStr = request.getQuery( IXM_FIELD_NAME_SORT_BUFFER_SIZE ) ;
      if ( FALSE == sortBufferSizeStr.empty() )
      {
         sortBufferSize = ossAtoi ( sortBufferSizeStr.c_str() ) ;
         if ( sortBufferSize < 0 )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "field's value error:field=%s, value=%s",
                        IXM_FIELD_NAME_SORT_BUFFER_SIZE,
                        sortBufferSizeStr.c_str() ) ;
            goto error ;
         }
      }

      {
         BSONObj index ;
         BSONObj query ;
         BSONObj hint ;

         index = BSON( IXM_FIELD_NAME_KEY << indexDefObj <<
                       IXM_FIELD_NAME_NAME << indexName <<
                       IXM_FIELD_NAME_UNIQUE << isUnique <<
                       IXM_FIELD_NAME_ENFORCED << isEnforced ) ;

         query = BSON( FIELD_NAME_COLLECTION << collectionName <<
                       FIELD_NAME_INDEX << index ) ;

         hint = BSON( IXM_FIELD_NAME_SORT_BUFFER_SIZE << sortBufferSize ) ;

         rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                                &query, NULL, NULL, &hint ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                        pCommand, rc ) ;
            goto error ;
         }
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDropIndex( restAdaptor *pAdaptor,
                                               restRequest &request,
                                               MsgHeader **msg )
   {
      INT32 rc                = SDB_OK ;
      CHAR *pBuff             = NULL ;
      INT32 buffSize          = 0 ;
      const CHAR *pCommand    = CMD_ADMIN_PREFIX CMD_NAME_DROP_INDEX ;
      string indexName ;
      string collectionName ;

      // collection name
      collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
      if ( collectionName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     REST_KEY_NAME_COLLECTION ) ;
         goto error ;
      }

      // index name
      indexName = request.getQuery( FIELD_NAME_INDEXNAME ) ;
      if ( indexName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_INDEXNAME ) ;
         goto error ;
      }

      {
         BSONObj index ;
         BSONObj query ;
         index = BSON( "" << indexName ) ;
         query = BSON( FIELD_NAME_COLLECTION << collectionName
                       << FIELD_NAME_INDEX << index ) ;

         rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                                &query, NULL, NULL, NULL ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                        pCommand, rc ) ;
            goto error ;
         }
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSplit( restAdaptor *pAdaptor,
                                           restRequest &request,
                                           MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SPLIT ;
      BOOLEAN isUsePercent = FALSE ;
      INT32 percent        = 0 ;
      bool bAsync   = false ;
      string collectionName ;
      string sourceStr ;
      string targetStr ;
      string percentStr ;
      string splitQueryStr ;
      string splitEndQueryStr ;
      string asyncStr ;
      BSONObj splitQuery ;
      BSONObj splitEndQuery ;
      BSONObj query ;

      //1.FIELD_NAME_NAME & FIELD_NAME_SOURCE & FIELD_NAME_TARGET must exist
      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      sourceStr = request.getQuery( FIELD_NAME_SOURCE ) ;
      if ( sourceStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get split's %s failed", FIELD_NAME_SOURCE ) ;
         goto error ;
      }

      targetStr = request.getQuery( FIELD_NAME_TARGET ) ;
      if ( targetStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get split's %s failed", FIELD_NAME_TARGET ) ;
         goto error ;
      }

      //2.FIELD_NAME_SPLITENDQUERY or FIELD_NAME_SPLITPERCENT must exist one
      percentStr = request.getQuery( FIELD_NAME_SPLITPERCENT ) ;
      if ( percentStr.empty() )
      {
         splitQueryStr = request.getQuery( FIELD_NAME_SPLITQUERY ) ;
         if ( splitQueryStr.empty() )
         {
            splitQueryStr = request.getQuery( REST_KEY_NAME_LOWBOUND ) ;
            if ( splitQueryStr.empty() )
            {
               rc = SDB_INVALIDARG ;
               PD_LOG_MSG( PDERROR, "get split's %s[or %s] failed",
                           FIELD_NAME_SPLITQUERY, REST_KEY_NAME_LOWBOUND ) ;
               goto error ;
            }
         }

         rc = fromjson( splitQueryStr.c_str(), splitQuery, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s],"
                        "value=%s", FIELD_NAME_SPLITQUERY,
                        REST_KEY_NAME_LOWBOUND, splitQueryStr.c_str() ) ;
            goto error ;
         }

         splitEndQueryStr = request.getQuery( FIELD_NAME_SPLITENDQUERY ) ;
         if ( splitEndQueryStr.empty() )
         {
            splitEndQueryStr = request.getQuery( REST_KEY_NAME_UPBOUND ) ;
         }

         if ( FALSE == splitEndQueryStr.empty() )
         {
            rc = fromjson( splitEndQueryStr.c_str(), splitEndQuery, 0 ) ;
            if ( SDB_OK != rc )
            {
               PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                           FIELD_NAME_SPLITENDQUERY, splitEndQueryStr.c_str() ) ;
               goto error ;
            }
         }
      }
      else
      {
         isUsePercent = TRUE ;
         percent      = ossAtoi( percentStr.c_str() ) ;
      }

      asyncStr = request.getQuery( FIELD_NAME_ASYNC ) ;
      if ( FALSE == asyncStr.empty() )
      {
         if ( ossStrcasecmp( asyncStr.c_str(), "TRUE" ) == 0 )
         {
            bAsync = true ;
         }
      }

      if ( isUsePercent )
      {
         query = BSON( FIELD_NAME_NAME << collectionName << FIELD_NAME_SOURCE
                       << sourceStr << FIELD_NAME_TARGET << targetStr
                       << FIELD_NAME_SPLITPERCENT << ( FLOAT64 )percent
                       << FIELD_NAME_ASYNC << bAsync ) ;
      }
      else
      {
         query = BSON( FIELD_NAME_NAME << collectionName << FIELD_NAME_SOURCE
                       << sourceStr << FIELD_NAME_TARGET << targetStr
                       << FIELD_NAME_SPLITQUERY << splitQuery
                       << FIELD_NAME_SPLITENDQUERY << splitEndQuery
                       << FIELD_NAME_ASYNC << bAsync ) ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertTruncateCollection( restAdaptor *pAdaptor,
                                                        restRequest &request,
                                                        MsgHeader **msg )
   {
      INT32 rc                = SDB_OK ;
      INT32 buffSize          = 0 ;
      CHAR *pBuff             = NULL ;
      const CHAR *pCommand    = CMD_ADMIN_PREFIX CMD_NAME_TRUNCATE ;
      string collectionName ;
      BSONObj query ;

      // collection full name
      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed",
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_COLLECTION, collectionName ) ;
         query = ob.obj () ;
      }
      catch ( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_coverAttachCollection( restAdaptor *pAdaptor,
                                                    restRequest &request,
                                                    MsgHeader **msg )
   {
      INT32 rc                = SDB_OK ;
      CHAR *pBuff             = NULL ;
      INT32 buffSize          = 0 ;
      const CHAR *pCommand    = CMD_ADMIN_PREFIX CMD_NAME_LINK_CL ;
      string lowboundStr ;
      string upboundStr ;
      string collectionName ;
      string subCLName ;
      BSONObj lowbound ;
      BSONObj upbound ;
      BSONObj query ;

      // collection name
      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      // subcl name
      subCLName = request.getQuery( REST_KEY_NAME_SUBCLNAME ) ;
      if ( subCLName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field[%s] failed",
                     REST_KEY_NAME_SUBCLNAME ) ;
         goto error ;
      }

      // lowbound
      lowboundStr = request.getQuery( REST_KEY_NAME_LOWBOUND ) ;
      if ( lowboundStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field[%s] failed",
                     REST_KEY_NAME_LOWBOUND ) ;
         goto error ;
      }

      rc = fromjson( lowboundStr.c_str(), lowbound, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                     REST_KEY_NAME_LOWBOUND, lowboundStr.c_str() ) ;
         goto error ;
      }

      // upbound
      upboundStr = request.getQuery( REST_KEY_NAME_UPBOUND ) ;
      if ( upboundStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field[%s] failed",
                     REST_KEY_NAME_UPBOUND ) ;
         goto error ;
      }

      rc = fromjson( upboundStr.c_str(), upbound, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                     REST_KEY_NAME_UPBOUND, upboundStr.c_str() ) ;
         goto error ;
      }

      query = BSON( FIELD_NAME_NAME << collectionName
                    << FIELD_NAME_SUBCLNAME << subCLName
                    << FIELD_NAME_LOWBOUND << lowbound
                    << FIELD_NAME_UPBOUND << upbound ) ;

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &query,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_coverDetachCollection( restAdaptor *pAdaptor,
                                                    restRequest &request,
                                                    MsgHeader **msg )
   {
      INT32 rc                = SDB_OK ;
      INT32 buffSize          = 0 ;
      CHAR *pBuff             = NULL ;
      const CHAR *pCommand    = CMD_ADMIN_PREFIX CMD_NAME_UNLINK_CL ;
      string collectionName ;
      string subCLName ;
      BSONObj query ;

      collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
      if ( collectionName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get collection's %s failed",
                     REST_KEY_NAME_COLLECTION ) ;
         goto error ;
      }

      // subcl name
      subCLName = request.getQuery( REST_KEY_NAME_SUBCLNAME ) ;
      if ( subCLName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field[%s] failed",
                     REST_KEY_NAME_SUBCLNAME ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_NAME, collectionName ) ;
         ob.append ( FIELD_NAME_SUBCLNAME, subCLName ) ;
         query = ob.obj () ;
      }
      catch ( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListGroups( restAdaptor *pAdaptor,
                                                restRequest &request,
                                                MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_GROUPS ;

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, NULL,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertStartGroup( restAdaptor *pAdaptor,
                                                restRequest &request,
                                                MsgHeader **msg )
   {
      INT32 rc             = SDB_OK ;
      INT32 buffSize       = 0 ;
      CHAR *pBuff          = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_ACTIVE_GROUP ;
      string name ;
      BSONObj query ;

      // Name
      name = request.getQuery( FIELD_NAME_NAME ) ;
      if ( name.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_GROUPNAME, name ) ;
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertStopGroup( restAdaptor *pAdaptor,
                                               restRequest &request,
                                               MsgHeader **msg )
   {
      INT32 rc             = SDB_OK ;
      INT32 buffSize       = 0 ;
      CHAR *pBuff          = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN_GROUP ;
      string name ;
      BSONObj query ;

      // Name
      name = request.getQuery( FIELD_NAME_NAME ) ;
      if ( name.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_GROUPNAME, name ) ;
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertStartNode( restAdaptor *pAdaptor,
                                               restRequest &request,
                                               MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      INT32 buffSize        = 0 ;
      CHAR *pBuff           = NULL ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_STARTUP_NODE ;
      string hostname ;
      string svcname ;
      BSONObj query ;

      // HostName
      hostname = request.getQuery( FIELD_NAME_HOST ) ;
      if ( hostname.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_HOST ) ;
         goto error ;
      }

      // svcname
      svcname = request.getQuery( OM_CONF_DETAIL_SVCNAME ) ;
      if ( svcname.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     OM_CONF_DETAIL_SVCNAME ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_HOST, hostname ) ;
         ob.append ( OM_CONF_DETAIL_SVCNAME, svcname ) ;
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertStopNode( restAdaptor *pAdaptor,
                                              restRequest &request,
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      INT32 buffSize        = 0 ;
      CHAR *pBuff           = NULL ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SHUTDOWN_NODE ;
      string hostname ;
      string svcname ;
      BSONObj query ;

      // HostName
      hostname = request.getQuery( FIELD_NAME_HOST ) ;
      if ( hostname.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_HOST ) ;
         goto error ;
      }

      // svcname
      svcname = request.getQuery( OM_CONF_DETAIL_SVCNAME ) ;
      if ( svcname.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     OM_CONF_DETAIL_SVCNAME ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_HOST, hostname ) ;
         ob.append ( OM_CONF_DETAIL_SVCNAME, svcname ) ;
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertGetCount( restAdaptor *pAdaptor,
                                              restRequest &request,
                                              MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_GET_COUNT ;
      string collectionName ;
      string matchStr ;
      string hintStr ;
      BSONObj matcher ;
      BSONObj hint ;

      collectionName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( collectionName.empty() )
      {
         collectionName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( collectionName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s[or %s] failed",
                        FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            goto error ;
         }
      }

      matchStr = request.getQuery( FIELD_NAME_FILTER ) ;
      if ( matchStr.empty() )
      {
         matchStr = request.getQuery( REST_KEY_NAME_MATCHER ) ;
      }

      if ( FALSE == matchStr.empty() )
      {
         rc = fromjson( matchStr.c_str(), matcher, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s],"
                        "value=%s", FIELD_NAME_FILTER, REST_KEY_NAME_MATCHER,
                        matchStr.c_str() ) ;
            goto error ;
         }
      }

      hintStr = request.getQuery( FIELD_NAME_HINT ) ;
      if ( FALSE == hintStr.empty() )
      {
         rc = fromjson( hintStr.c_str(), hint, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s,value=%s",
                        FIELD_NAME_HINT, hintStr.c_str() ) ;
            goto error ;
         }
      }

      {
         BSONObjBuilder builder ;
         builder.append( FIELD_NAME_COLLECTION, collectionName ) ;
         if ( !hint.isEmpty() )
         {
            builder.append( FIELD_NAME_HINT, hint ) ;
         }

         hint = builder.obj() ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &matcher,
                             NULL, NULL, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListBase( restAdaptor *pAdaptor,
                                              restRequest &request,
                                              BSONObj &match, BSONObj &selector,
                                              BSONObj &order )
   {
      BSONObj hint ;
      SINT64  skip ;
      SINT64  returnRow ;
      return _convertSnapshotBase( pAdaptor, request, match, selector,
                                   order, hint, &skip, &returnRow ) ;
   }

   INT32 RestToMSGTransfer::_convertListContexts( restAdaptor *pAdaptor,
                                                  restRequest &request,
                                                  MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_CONTEXTS ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListContextsCurrent( restAdaptor *pAdaptor,
                                                         restRequest &request,
                                                         MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_CONTEXTS_CURRENT ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListSessions( restAdaptor *pAdaptor,
                                                  restRequest &request,
                                                  MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_SESSIONS ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListSessionsCurrent( restAdaptor *pAdaptor,
                                                         restRequest &request,
                                                         MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_SESSIONS_CURRENT ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListCollections( restAdaptor *pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_COLLECTIONS ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListCollectionSpaces( restAdaptor *pAdaptor,
                                                          restRequest &request,
                                                          MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_COLLECTIONSPACES ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListStorageUnits( restAdaptor *pAdaptor,
                                                      restRequest &request,
                                                      MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_STORAGEUNITS ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateProcedure( restAdaptor *pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader **msg )
   {
      INT32 rc             = SDB_OK ;
      INT32 buffSize       = 0 ;
      CHAR *pBuff          = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_CRT_PROCEDURE ;
      string jsCode ;
      BSONObj query ;

      // code
      jsCode = request.getQuery( REST_KEY_NAME_CODE ) ;
      if ( jsCode.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     REST_KEY_NAME_CODE ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.appendCode ( FIELD_NAME_FUNC, jsCode ) ;
         ob.append ( FMP_FUNC_TYPE, FMP_FUNC_TYPE_JS ) ;
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertRemoveProcedure( restAdaptor *pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      INT32 buffSize        = 0 ;
      CHAR *pBuff           = NULL ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_RM_PROCEDURE ;
      string funcName ;
      BSONObj query ;

      // function
      funcName = request.getQuery( REST_KEY_NAME_FUNCTION ) ;
      if ( funcName.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     REST_KEY_NAME_FUNCTION ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_FUNC, funcName ) ;
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListProcedures( restAdaptor *pAdaptor,
                                                    restRequest &request,
                                                    MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      INT32 buffSize = 0 ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_LIST_PROCEDURES ;
      string skipStr ;
      string returnRowStr ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      //Skip
      skipStr = request.getQuery( FIELD_NAME_SKIP ) ;

      //ReturnNum(Limit)
      returnRowStr = request.getQuery( FIELD_NAME_RETURN_NUM ) ;
      if ( returnRowStr.empty() )
      {
         returnRowStr = request.getQuery( REST_KEY_NAME_LIMIT ) ;
      }

      if ( FALSE == skipStr.empty() )
      {
         skip = ossAtoll( skipStr.c_str() ) ;
      }

      if ( FALSE == returnRowStr.empty() )
      {
         returnRow = ossAtoll( returnRowStr.c_str() ) ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, skip,
                             returnRow, &match, &selector, &order,
                             NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertAlterDomain( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      INT32 buffSize = 0 ;
      CHAR *pBuff = NULL ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_ALTER_DOMAIN ;
      string name ;
      string optionsStr ;
      BSONObj query ;
      BSONObj options ;

      // name
      name = request.getQuery( FIELD_NAME_NAME ) ;
      if( name.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      // options
      optionsStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if ( optionsStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_OPTIONS ) ;
         goto error ;
      }

      rc = fromjson( optionsStr.c_str(), options, 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                     FIELD_NAME_OPTIONS, optionsStr.c_str() ) ;
         goto error ;
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_NAME, name ) ;
         ob.append ( FIELD_NAME_OPTIONS, options ) ;
         query = ob.obj () ;
      }
      catch ( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListDomains( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_DOMAINS ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertCreateDomain( restAdaptor *pAdaptor,
                                                  restRequest &request,
                                                  MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      INT32 buffSize        = 0 ;
      CHAR *pBuff           = NULL ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_CREATE_DOMAIN ;
      string name ;
      string optionsStr ;
      BSONObj query ;
      BSONObj options ;

      // name
      name = request.getQuery( FIELD_NAME_NAME ) ;
      if ( name.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      //options
      optionsStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if( FALSE == optionsStr.empty() )
      {
         rc = fromjson( optionsStr.c_str(), options, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, "
                        "value=%s", FIELD_NAME_OPTIONS, optionsStr.c_str() ) ;
            goto error ;
         }
      }

      try
      {
         BSONObjBuilder ob ;
         ob.append ( FIELD_NAME_NAME, name ) ;
         if( FALSE == optionsStr.empty() )
         {
            ob.append ( FIELD_NAME_OPTIONS, options ) ;
         }
         query = ob.obj () ;
      }
      catch ( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDropDomain( restAdaptor *pAdaptor,
                                                restRequest &request,
                                                MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_DROP_DOMAIN ;
      string name ;
      BSONObj query ;

      // name
      name = request.getQuery( FIELD_NAME_NAME ) ;
      if ( name.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get rest field failed:field=%s",
                     FIELD_NAME_NAME ) ;
         goto error ;
      }

      query = BSON( FIELD_NAME_NAME << name ) ;
      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListTasks( restAdaptor *pAdaptor,
                                               restRequest &request,
                                               MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_TASKS ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListIndexes( restAdaptor *pAdaptor,
                                                 restRequest &request,
                                                 MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj hint ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_GET_INDEXES ;
      string table ;

      table = request.getQuery( FIELD_NAME_NAME ) ;
      if ( table.empty() )
      {
         table = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( table.empty() )
         {
            PD_LOG_MSG( PDERROR, "get field failed:field=%s[or %s]",
                     FIELD_NAME_NAME, REST_KEY_NAME_COLLECTION ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }
      }

      hint = BSON( FIELD_NAME_COLLECTION << table ) ;

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, NULL,
                             NULL, NULL, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListCSInDomain( restAdaptor *pAdaptor,
                                                    restRequest &request,
                                                    MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_CS_IN_DOMAIN ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListCLInDomain( restAdaptor *pAdaptor,
                                                    restRequest &request,
                                                    MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_LIST_CL_IN_DOMAIN ;

      rc = _convertListBase( pAdaptor, request, match, selector, order ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert list failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertListLobs( restAdaptor *pAdaptor,
                                              restRequest &request,
                                              MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj match ;
      CHAR *pBuff          = NULL ;
      INT32 buffSize       = 0 ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_LIST_LOBS ;
      string clName ;

      clName = request.getQuery( FIELD_NAME_NAME ) ;
      if ( clName.empty() )
      {
         clName = request.getQuery( REST_KEY_NAME_COLLECTION ) ;
         if ( clName.empty() )
         {
            rc = SDB_INVALIDARG ;
            PD_LOG_MSG( PDERROR, "get collection's %s failed",
                        FIELD_NAME_NAME ) ;
            goto error ;
         }
      }

      match = BSON( FIELD_NAME_COLLECTION << clName ) ;
      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
                             NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

//   INT32 RestToMSGTransfer::_convertListLobsPieces( restAdaptor *pAdaptor,
//                                                    MsgHeader **msg )
//   {
//      INT32 rc = SDB_OK ;
//      BSONObj match ;
//      CHAR *pBuff          = NULL ;
//      INT32 buffSize       = 0 ;
//      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_LIST_LOBS ;
//      const CHAR *clName   = NULL ;

//      pAdaptor->getQuery( _restSession, FIELD_NAME_NAME, &clName ) ;
//      if ( NULL == clName )
//      {
//         rc = SDB_INVALIDARG ;
//         PD_LOG_MSG( PDERROR, "get collection's %s failed",
//                     FIELD_NAME_NAME ) ;
//         goto error ;
//      }

//      match = BSON( FIELD_NAME_COLLECTION << clName
//                    << FIELD_NAME_LOB_LIST_PIECES_MODE << TRUE ) ;
//      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
//                             NULL, NULL, NULL ) ;
//      if ( SDB_OK != rc )
//      {
//         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
//                     pCommand, rc ) ;
//         goto error ;
//      }

//      *msg = ( MsgHeader * )pBuff ;

//   done:
//      return rc ;
//   error:
//      goto done ;
//   }

   INT32 RestToMSGTransfer::_convertSnapshotBase( restAdaptor *pAdaptor,
                                                  restRequest &request,
                                                  BSONObj &match, BSONObj &selector,
                                                  BSONObj &order,
                                                  BSONObj &hint,
                                                  SINT64* skip,
                                                  SINT64* returnRow )
   {
      INT32 rc = SDB_OK ;
      string matchStr ;
      string selectorStr ;
      string orderStr ;
      string hintStr ;
      string skipStr ;
      string returnRowStr ;

      matchStr = request.getQuery( FIELD_NAME_FILTER ) ;
      if ( matchStr.empty() )
      {
         matchStr = request.getQuery( REST_KEY_NAME_MATCHER ) ;
      }

      selectorStr = request.getQuery( FIELD_NAME_SELECTOR ) ;

      orderStr = request.getQuery( FIELD_NAME_SORT ) ;
      if ( orderStr.empty() )
      {
         orderStr = request.getQuery( REST_KEY_NAME_ORDERBY ) ;
      }
      hintStr = request.getQuery( FIELD_NAME_HINT ) ;
      skipStr = request.getQuery( FIELD_NAME_SKIP ) ;

      returnRowStr = request.getQuery( FIELD_NAME_RETURN_NUM ) ;
      if ( returnRowStr.empty() )
      {
         returnRowStr = request.getQuery( REST_KEY_NAME_LIMIT ) ;
      }

      if ( FALSE == matchStr.empty() )
      {
         rc = fromjson( matchStr.c_str(), match, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s], "
                        "value=%s", FIELD_NAME_FILTER, REST_KEY_NAME_MATCHER,
                        matchStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == selectorStr.empty() )
      {
         rc = fromjson( selectorStr.c_str(), selector, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_SELECTOR, selectorStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == orderStr.empty() )
      {
         rc = fromjson( orderStr.c_str(), order, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s[or %s], "
                        "value=%s", FIELD_NAME_SORT, REST_KEY_NAME_ORDERBY,
                        orderStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == hintStr.empty() )
      {
         rc = fromjson( hintStr.c_str(), hint, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_HINT, hintStr.c_str() ) ;
            goto error ;
         }
      }

      if ( FALSE == skipStr.empty() )
      {
         *skip = ossAtoll( skipStr.c_str() ) ;
      }

      if ( FALSE == returnRowStr.empty() )
      {
         *returnRow = ossAtoll( returnRowStr.c_str() ) ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotContext( restAdaptor *pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONTEXTS ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotContextCurrent(
                                                     restAdaptor *pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX
                              CMD_NAME_SNAPSHOT_CONTEXTS_CURRENT ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotSessions( restAdaptor *pAdaptor,
                                                      restRequest &request,
                                                      MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SESSIONS ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotSessionsCurrent(
                                                        restAdaptor *pAdaptor,
                                                        restRequest &request,
                                                        MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX
                              CMD_NAME_SNAPSHOT_SESSIONS_CURRENT ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotCollections( restAdaptor *pAdaptor,
                                                         restRequest &request,
                                                         MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_COLLECTIONS ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotCollectionSpaces(
                                                         restAdaptor *pAdaptor,
                                                         restRequest &request,
                                                         MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX
                              CMD_NAME_SNAPSHOT_COLLECTIONSPACES ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotDatabase( restAdaptor *pAdaptor,
                                                      restRequest &request,
                                                      MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_DATABASE ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotSystem( restAdaptor *pAdaptor,
                                                    restRequest &request,
                                                    MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_SYSTEM ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotCata( restAdaptor *pAdaptor,
                                                  restRequest &request,
                                                  MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CATA ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotAccessPlans ( restAdaptor * pAdaptor,
                                                          restRequest &request,
                                                          MsgHeader ** msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_ACCESSPLANS ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotHealth ( restAdaptor * pAdaptor,
                                                     restRequest &request,
                                                     MsgHeader ** msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_HEALTH ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertSnapshotConfigs ( restAdaptor * pAdaptor,
                                                      restRequest &request,
                                                      MsgHeader ** msg )
   {
      INT32 rc = SDB_OK ;
      BSONObj selector ;
      BSONObj order ;
      BSONObj match ;
      BSONObj hint ;
      SINT64 skip = 0 ;
      SINT64 returnRow = -1 ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONFIGS ;

      rc = _convertSnapshotBase( pAdaptor, request, match, selector, order,
                                 hint, &skip, &returnRow ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0,
                             skip, returnRow, &match,
                             &selector, &order, &hint ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_buildExecMsg( CHAR **ppBuffer, INT32 *bufferSize,
                                           const CHAR *pSql, UINT64 reqID )
   {
      INT32 rc = SDB_OK ;
      if ( NULL == pSql )
      {
         rc = SDB_INVALIDARG ;
         goto error ;
      }
      {
         INT32 sqlLen = ossStrlen( pSql ) + 1 ;
         MsgOpSql *sqlMsg = NULL ;
         INT32 len = sizeof( MsgOpSql ) +
                     ossRoundUpToMultipleX( sqlLen, 4 ) ;
         if ( len < 0 )
         {
            ossPrintf ( "Packet size overflow"OSS_NEWLINE ) ;
            rc = SDB_INVALIDARG ;
            goto error ;
         }

         rc = msgCheckBuffer( ppBuffer, bufferSize, len ) ;
         if ( rc )
         {
            ossPrintf ( "Failed to check buffer, rc = %d"OSS_NEWLINE, rc ) ;
            goto error ;
         }

         sqlMsg                       = ( MsgOpSql *)( *ppBuffer ) ;
         sqlMsg->header.requestID     = reqID ;
         sqlMsg->header.opCode        = MSG_BS_SQL_REQ ;
         sqlMsg->header.messageLength = sizeof( MsgOpSql ) + sqlLen ;
         sqlMsg->header.routeID.value = 0 ;
         sqlMsg->header.TID           = ossGetCurrentThreadID() ;
         ossMemcpy( *ppBuffer + sizeof( MsgOpSql ),
                    pSql, sqlLen ) ;
      }
   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertExec( restAdaptor *pAdaptor,
                                          restRequest &request,
                                          MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      CHAR *pBuff          = NULL ;
      INT32 buffSize       = 0 ;
      string sql ;

      sql = request.getQuery( REST_KEY_NAME_SQL ) ;
      if ( sql.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get exec's field failed:field=%s",
                     REST_KEY_NAME_SQL ) ;
         goto error ;
      }

      rc = _buildExecMsg( &pBuff, &buffSize, sql.c_str(), 0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build exec command failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertForceSession( restAdaptor *pAdaptor,
                                                  restRequest &request,
                                                  MsgHeader **msg )
   {
      INT32 rc = SDB_OK ;
      INT32 buffSize = 0 ;
      SINT64 sessionId = 0 ;
      CHAR *pBuff = NULL ;
      const CHAR *pCommand   = CMD_ADMIN_PREFIX CMD_NAME_FORCE_SESSION ;
      string sessionIdStr ;
      string optionsStr ;
      BSONObj query ;
      BSONObj options ;

      sessionIdStr = request.getQuery( FIELD_NAME_SESSIONID ) ;
      if( sessionIdStr.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get exec's field failed:field=%s",
                     FIELD_NAME_SESSIONID ) ;
         goto error ;
      }

      sessionId = ossAtoll( sessionIdStr.c_str() ) ;

      optionsStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if( FALSE == optionsStr.empty() )
      {
         rc = fromjson( optionsStr.c_str(), options, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_OPTIONS, optionsStr.c_str() ) ;
            goto error ;
         }
      }

      try
      {
         BSONObjBuilder ob ;
         BSONObjIterator it( options ) ;
         ob.appendNumber( FIELD_NAME_SESSIONID, sessionId ) ;
         while( it.more() )
         {
            BSONElement ele = it.next() ;
            if ( 0 != ossStrcmp( ele.fieldName(), FIELD_NAME_SESSIONID ) )
            {
               ob.append( ele ) ;
            }
         }
         query = ob.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

//   INT32 RestToMSGTransfer::_convertSnapshotListLobs( restAdaptor *pAdaptor,
//                                                     MsgHeader **msg )
//   {
//      INT32 rc = SDB_OK ;
//      BSONObj selector ;
//      BSONObj order ;
//      BSONObj match ;
//      CHAR *pBuff           = NULL ;
//      INT32 buffSize        = 0 ;
//      const CHAR *pCommand  = CMD_ADMIN_PREFIX CMD_NAME_SNAPSHOT_CONTEXTS ;

//      rc = _convertListBase( pAdaptor, match, selector, order ) ;
//      if ( SDB_OK != rc )
//      {
//         PD_LOG( PDERROR, "convert snapshot failed:rc=%d", rc ) ;
//         goto error ;
//      }

//      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1, &match,
//                             &selector, &order, NULL ) ;
//      if ( SDB_OK != rc )
//      {
//         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
//                     pCommand, rc ) ;
//         goto error ;
//      }

//      *msg = ( MsgHeader * )pBuff ;

//   done:
//      return rc ;
//   error:
//      goto done ;
//   }

   INT32 RestToMSGTransfer::_convertLogin( restAdaptor *pAdaptor,
                                           restRequest &request,
                                           MsgHeader **msg )
   {
      INT32 rc              = SDB_OK ;
      CHAR *pBuff           = NULL ;
      INT32 buffSize        = 0 ;
      string user ;
      string password ;

      user = request.getQuery( OM_REST_FIELD_LOGIN_NAME ) ;
      if ( user.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get login name failed:field=%s",
                     OM_REST_FIELD_LOGIN_NAME ) ;
         goto error ;
      }

      password = request.getQuery( OM_REST_FIELD_LOGIN_PASSWD ) ;
      if ( password.empty() )
      {
         rc = SDB_INVALIDARG ;
         PD_LOG_MSG( PDERROR, "get passwd failed:field=%s",
                     OM_REST_FIELD_LOGIN_PASSWD ) ;
         goto error ;
      }

      rc = msgBuildAuthMsg( &pBuff, &buffSize, user.c_str(), password.c_str(),
                            0 ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "buildAuthMsg failed:rc=%d", rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertAnalyze ( restAdaptor * pAdaptor,
                                              restRequest &request,
                                              MsgHeader ** msg )
   {
      INT32 rc = SDB_OK ;
      INT32 buffSize = 0 ;
      CHAR *pBuff = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_ANALYZE ;
      string optionsStr ;
      BSONObj query ;
      BSONObj options ;

      optionsStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if( FALSE == optionsStr.empty() )
      {
         rc = fromjson( optionsStr.c_str(), options, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_OPTIONS, optionsStr.c_str() ) ;
            goto error ;
         }
      }

      try
      {
         BSONObjBuilder builder ;
         builder.appendElements( options ) ;
         query = builder.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;

   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertUpdateConfig ( restAdaptor * pAdaptor,
                                                   restRequest &request,
                                                   MsgHeader ** msg )
   {
      INT32 rc = SDB_OK ;
      INT32 buffSize = 0 ;
      CHAR *pBuff = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_UPDATE_CONFIG ;
      string configsStr ;
      string optionsStr ;
      BSONObj query ;
      BSONObj configs ;
      BSONObj options ;

      configsStr = request.getQuery( FIELD_NAME_CONFIGS ) ;
      if( FALSE == configsStr.empty() )
      {
         rc = fromjson( configsStr.c_str(), configs, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_CONFIGS, configsStr.c_str() ) ;
            goto error ;
         }
      }

      optionsStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if( FALSE == optionsStr.empty() )
      {
         rc = fromjson( optionsStr.c_str(), options, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_OPTIONS, optionsStr.c_str() ) ;
            goto error ;
         }
      }

      try
      {
         BSONObjBuilder queryBuilder ;
         queryBuilder.appendElements( options ) ;
         queryBuilder.append( FIELD_NAME_CONFIGS, configs ) ;
         query = queryBuilder.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;

   error:
      goto done ;
   }

   INT32 RestToMSGTransfer::_convertDeleteConfig ( restAdaptor * pAdaptor,
                                                   restRequest &request,
                                                   MsgHeader ** msg )
   {
      INT32 rc = SDB_OK ;

      INT32 buffSize = 0 ;
      CHAR *pBuff = NULL ;
      const CHAR *pCommand = CMD_ADMIN_PREFIX CMD_NAME_DELETE_CONFIG ;
      string configsStr ;
      string optionsStr ;
      BSONObj query ;
      BSONObj configs ;
      BSONObj options ;

      configsStr = request.getQuery( FIELD_NAME_CONFIGS ) ;
      if( FALSE == configsStr.empty() )
      {
         rc = fromjson( configsStr.c_str(), configs, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_CONFIGS, configsStr.c_str() ) ;
            goto error ;
         }
      }

      optionsStr = request.getQuery( FIELD_NAME_OPTIONS ) ;
      if( FALSE == optionsStr.empty() )
      {
         rc = fromjson( optionsStr.c_str(), options, 0 ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG_MSG( PDERROR, "field's format error:field=%s, value=%s",
                        FIELD_NAME_OPTIONS, optionsStr.c_str() ) ;
            goto error ;
         }
      }

      try
      {
         BSONObjBuilder queryBuilder ;
         queryBuilder.appendElements( options ) ;
         queryBuilder.append( FIELD_NAME_CONFIGS, configs ) ;
         query = queryBuilder.obj() ;
      }
      catch( std::exception &e )
      {
         PD_LOG_MSG( PDERROR, "Failed to create BSON object: %s", e.what() ) ;
         rc = SDB_SYS ;
         goto error ;
      }

      rc = msgBuildQueryMsg( &pBuff, &buffSize, pCommand, 0, 0, 0, -1,
                             &query, NULL, NULL, NULL ) ;
      if ( SDB_OK != rc )
      {
         PD_LOG_MSG( PDERROR, "build command failed:command=%s, rc=%d",
                     pCommand, rc ) ;
         goto error ;
      }

      *msg = ( MsgHeader * )pBuff ;

   done:
      return rc ;

   error:
      goto done ;
   }

}

