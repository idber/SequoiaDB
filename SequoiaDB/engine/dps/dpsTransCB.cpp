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

   Source File Name = dpsTransCB.cpp

   Descriptive Name =

   When/how to use: this program may be used on binary and text-formatted
   versions of runtime component. This file contains code logic for
   common functions for coordinator node.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================

   Last Changed =

*******************************************************************************/

#include "dpsTransCB.hpp"
#include "dpsTransLockMgr.hpp"
#include "pdTrace.hpp"
#include "dpsTrace.hpp"
#include "pmdEDU.hpp"
#include "pmdEDUMgr.hpp"
#include "pmd.hpp"
#include "pmdDef.hpp"
#include "dpsLogRecord.hpp"
#include "dpsMessageBlock.hpp"
#include "dpsLogRecordDef.hpp"
#include "dpsTransLockCallback.hpp"
#include "dpsTransVersionCtrl.hpp"
#include "pmdStartup.hpp"

namespace engine
{

   dpsTransCB::dpsTransCB()
   :_TransIDL48Cur( 1 )
   {
      _TransIDH16          = 0;
      _isOn                = FALSE ;
      _doRollback          = FALSE ;
      _isNeedSyncTrans     = TRUE ;
      _maxUsedSize         = 0 ;
      _logFileTotalSize    = 0 ;
      _accquiredSpace      = 0 ;
      _transLockMgr        = NULL ;      
      _oldVCB              = NULL;
   }

   dpsTransCB::~dpsTransCB()
   {
   }

   SDB_CB_TYPE dpsTransCB::cbType () const
   {
      return SDB_CB_TRANS ;
   }

   const CHAR* dpsTransCB::cbName () const
   {
      return "TRANSCB" ;
   }

   INT32 dpsTransCB::init ()
   {
      INT32 rc = SDB_OK ;
      DPS_LSN_OFFSET startLsnOffset = DPS_INVALID_LSN_OFFSET ;

      _isOn = pmdGetOptionCB()->transactionOn() ;
      _rollbackEvent.signal() ;

      // register event handle
      pmdGetKRCB()->regEventHandler( this ) ;

      // only enable/setup old version CB if transaction is turned on
      // Each session decide when to setup/use the old copy based on 
      // isolation level (TRANS_ISOLATION_RC)
      if( _isOn )
      {
         _transLockMgr = SDB_OSS_NEW dpsTransLockManager() ;
         if ( !_transLockMgr )
         {
            rc = SDB_OOM ;
            goto error ;
         }

         // Initialize trans lock manager
         //   . allocate memory and under layer structures for the LRBs,
         //     LRB Headers, LRB Header Hash buckets
         rc = _transLockMgr->init( pmdGetOptionCB()->transLRBInit(),
                                   pmdGetOptionCB()->transLRBTotal() ) ;
         if ( SDB_OK != rc )
         {
            PD_LOG( PDERROR, "Failed to initialize lock manager, rc: %d",
                    rc ) ;
            goto error ;
         }

         _oldVCB = SDB_OSS_NEW oldVersionCB() ;
         if ( !_oldVCB )
         {
            rc = SDB_OOM ;
            goto error ;
         }
         rc = _oldVCB->init() ;
         if ( rc )
         {
            PD_LOG( PDERROR, "Failed to init OldVersionCB, rc: %d", rc ) ;
            goto error ;
         }
      }

      // if enabled dps
      if ( pmdGetKRCB()->isCBValue( SDB_CB_DPS ) &&
           !pmdGetKRCB()->isRestore() )
      {
         UINT64 logFileSize = pmdGetOptionCB()->getReplLogFileSz() ;
         UINT32 logFileNum = pmdGetOptionCB()->getReplLogFileNum() ;
         _logFileTotalSize = logFileSize * logFileNum ;

         if ( logFileNum >= 5 )
         {
            // reserved 2 log-file: because size of rollback-log is bigger than operation-log
            UINT64 reservedSize = logFileSize * 2 ;

            // (1).the max-size of operation-log(update) is 2*DMS_RECORD_MAX_SZ,
            // (2).the max-size of unavailable space in cross-file is
            // 2*DMS_RECORD_MAX_SZ*(logFileNum -2),
            // the available size is: availableSize =
            // _logFileTotalSize - (2) - reservedSize;
            // the availableSize can used for operation-log and rollback-log,
            // so the size of operation-log is:
            // _maxUsedSize = availableSize / 2;
            _maxUsedSize = ( _logFileTotalSize - reservedSize
                                 - 2 * DMS_RECORD_MAX_SZ * ( logFileNum - 2 ) ) / 2 ;

            // if the logFileSize is 32M, the caculation method is:
            // if the transaction-operation-log  caused X(MB) of log-file,
            // the rollback-log will caused up to 2X( 1X for normal rollback-log
            // and 1X for cross-file-space )
            UINT64 temp = ( _logFileTotalSize - reservedSize ) / 3 ;
            if ( _maxUsedSize > temp || 0 == _maxUsedSize )
            {
               _maxUsedSize = temp ;
            }
         }
         else
         {
            _maxUsedSize = 0 ;
         }

         rc = sdbGetDPSCB()->readOldestBeginLsnOffset( startLsnOffset ) ;
         PD_RC_CHECK( rc, PDERROR, "Read oldest begin lsn offset failed:rc=%d",
                      rc ) ;
         PD_LOG( PDEVENT, "oldest begin lsn offset=%llu", startLsnOffset ) ;
         if ( _isOn && startLsnOffset != DPS_INVALID_LSN_OFFSET &&
              SDB_ROLE_STANDALONE != pmdGetDBRole() )
         {
            rc = syncTransInfoFromLocal( startLsnOffset ) ;
            if ( rc )
            {
               PD_LOG( PDERROR, "Failed to sync trans info from local, rc: %d",
                       rc ) ;
               goto error ;
            }
         }
         setIsNeedSyncTrans( FALSE ) ;

         // if have trans info, need log
         if ( getTransMap()->size() > 0 )
         {
            PD_LOG( PDEVENT, "Restored trans info, have %d trans not "
                    "be complete, the oldest lsn offset is %lld",
                    getTransMap()->size(), getOldestBeginLsn() ) ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dpsTransCB::active ()
   {
      return SDB_OK ;
   }

   INT32 dpsTransCB::deactive ()
   {
      return SDB_OK ;
   }

   INT32 dpsTransCB::fini ()
   {
      // unregister event handle
      pmdGetKRCB()->unregEventHandler( this ) ;

      if ( _transLockMgr )
      {
         _transLockMgr->fini() ;
         SDB_OSS_DEL _transLockMgr ;
         _transLockMgr = NULL ;
      }

      if ( _oldVCB )
      {
         _oldVCB->fini() ;
         SDB_OSS_DEL _oldVCB ;
         _oldVCB = NULL ;
      }
      return SDB_OK ;
   }

   void dpsTransCB::onConfigChange()
   {
   }

   DPS_TRANS_ID dpsTransCB::allocTransID()
   {
      DPS_TRANS_ID temp = 0;
      while ( 0 == temp )
      {
         temp = _TransIDL48Cur.inc();
         temp = DPS_TRANS_GET_SN( temp ) | _TransIDH16 |
                DPS_TRANSID_FIRSTOP_BIT;
      }
      return temp ;
   }

   void dpsTransCB::onRegistered( const MsgRouteID &nodeID )
   {
      _TransIDH16 = (DPS_TRANS_ID)nodeID.columns.nodeID << 48 ;
   }

   void dpsTransCB::onPrimaryChange( BOOLEAN primary,
                                     SDB_EVENT_OCCUR_TYPE occurType )
   {
      // change to primary, start trans rollback
      if ( primary )
      {
         if ( SDB_EVT_OCCUR_BEFORE == occurType )
         {
            _doRollback = TRUE ;
            _rollbackEvent.reset() ;
         }
         else
         {
            startRollbackTask() ;
         }
      }
      // change to secondary, stop trans rollback
      else
      {
         if ( SDB_EVT_OCCUR_AFTER == occurType )
         {
            stopRollbackTask() ;
            termAllTrans() ;
         }
      }
   }

   DPS_TRANS_ID dpsTransCB::getRollbackID( DPS_TRANS_ID transID )
   {
      return transID | DPS_TRANSID_ROLLBACKTAG_BIT ;
   }

   DPS_TRANS_ID dpsTransCB::getTransID( DPS_TRANS_ID rollbackID )
   {
      return DPS_TRANS_GET_ID( rollbackID ) ;
   }

   BOOLEAN dpsTransCB::isRollback( DPS_TRANS_ID transID )
   {
      if ( transID & DPS_TRANSID_ROLLBACKTAG_BIT )
      {
         return TRUE;
      }
      return FALSE;
   }

   BOOLEAN dpsTransCB::isFirstOp( DPS_TRANS_ID transID )
   {
      return DPS_TRANS_IS_FIRSTOP( transID ) ? TRUE : FALSE ;
   }

   void dpsTransCB::clearFirstOpTag( DPS_TRANS_ID &transID )
   {
      DPS_TRANS_CLEAR_FIRSTOP( transID ) ;
   }

   INT32 dpsTransCB::startRollbackTask()
   {
      INT32 rc = SDB_OK ;
      EDUID eduID = PMD_INVALID_EDUID ;
      pmdEDUMgr *pEduMgr = NULL ;
      _isNeedSyncTrans = FALSE ;
      _doRollback = TRUE ;
      _rollbackEvent.reset() ;
      pEduMgr = pmdGetKRCB()->getEDUMgr() ;
      eduID = pEduMgr->getSystemEDU( EDU_TYPE_DPSROLLBACK ) ;
      if ( PMD_INVALID_EDUID != eduID )
      {
         rc = pEduMgr->postEDUPost( eduID, PMD_EDU_EVENT_ACTIVE,
                                    PMD_EDU_MEM_NONE, NULL ) ;
      }
      else
      {
         rc = SDB_SYS ;
      }

      if ( rc )
      {
         _doRollback = FALSE ;
         _rollbackEvent.signalAll() ;
      }
      return rc ;
   }

   INT32 dpsTransCB::stopRollbackTask()
   {
      _doRollback = FALSE ;
      _rollbackEvent.signalAll() ;
      return SDB_OK ;
   }

   INT32 dpsTransCB::waitRollback( UINT64 millicSec )
   {
      return _rollbackEvent.wait( millicSec ) ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DPSTRANSCB_SVTRANSINFO, "dpsTransCB::updateTransInfo" )
   void dpsTransCB::updateTransInfo( DPS_TRANS_ID transID,
                                     DPS_LSN_OFFSET lsnOffset )
   {
      PD_TRACE_ENTRY ( SDB_DPSTRANSCB_SVTRANSINFO ) ;
      if ( DPS_INVALID_TRANS_ID == transID ||
           sdbGetDPSCB()->isInRestore() )
      {
         goto done ;
      }

      if ( pmdGetKRCB()->isCBValue( SDB_CB_CLS ) &&
           pmdIsPrimary() )
      {
         // in primary, transaction-info save in EDUCB
         goto done ;
      }
      else
      {
         transID = getTransID( transID );
         ossScopedLock _lock( &_MapMutex );

         if ( DPS_INVALID_LSN_OFFSET == lsnOffset )
         {
            // invalid-lsn means the transaction is complete
            _TransMap.erase( transID ) ;
         }
         else
         {
            _TransMap[ transID ] = lsnOffset ;
         }
      }

   done:
      PD_TRACE_EXIT ( SDB_DPSTRANSCB_SVTRANSINFO ) ;
      return ;
   }

   void dpsTransCB::addTransInfo( DPS_TRANS_ID transID,
                                  DPS_LSN_OFFSET lsnOffset )
   {
      transID = getTransID( transID );
      ossScopedLock _lock( &_MapMutex );
      if ( _TransMap.find( transID ) == _TransMap.end() )
      {
         // it is means transaction is synchronous by log if transID is exist
         _TransMap[ transID ] = lsnOffset;
      }
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DPSTRANSCB_ADDTRANSCB, "dpsTransCB::addTransCB" )
   void dpsTransCB::addTransCB( DPS_TRANS_ID transID, _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB_DPSTRANSCB_ADDTRANSCB ) ;
      {
         transID = getTransID( transID );
         ossScopedLock _lock( &_CBMapMutex );
         _cbMap[ transID ] = eduCB;
      }
      PD_TRACE_EXIT ( SDB_DPSTRANSCB_ADDTRANSCB );
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DPSTRANSCB_DELTRANSCB, "dpsTransCB::delTransCB" )
   void dpsTransCB::delTransCB( DPS_TRANS_ID transID )
   {
      PD_TRACE_ENTRY( SDB_DPSTRANSCB_DELTRANSCB ) ;
      {
         transID = getTransID( transID );
         ossScopedLock _lock( &_CBMapMutex ) ;
         _cbMap.erase( transID );
      }
      PD_TRACE_EXIT ( SDB_DPSTRANSCB_DELTRANSCB );
   }

   void dpsTransCB::dumpTransEDUList( TRANS_EDU_LIST & eduList )
   {
      ossScopedLock _lock( &_CBMapMutex ) ;
      TRANS_CB_MAP::iterator iter = _cbMap.begin() ;
      while( iter != _cbMap.end() )
      {
         eduList.push( iter->second->getID() ) ;
         ++iter ;
      }
   }

   TRANS_MAP *dpsTransCB::getTransMap()
   {
      return &_TransMap;
   }

   UINT32 dpsTransCB::getTransCBSize ()
   {
      ossScopedLock _lock( &_CBMapMutex ) ;
      return _cbMap.size() ;
   }

   void dpsTransCB::clearTransInfo()
   {
      _TransMap.clear();
      _cbMap.clear();
      _beginLsnIdMap.clear();
      _idBeginLsnMap.clear();
   }

   BOOLEAN dpsTransCB::rollbackTransInfoFromLog( const dpsLogRecord &record )
   {
      BOOLEAN ret = TRUE ;
      DPS_LSN_OFFSET lsnOffset = DPS_INVALID_LSN_OFFSET ;
      DPS_TRANS_ID transID = DPS_INVALID_TRANS_ID ;
      dpsLogRecord::iterator itr = record.find( DPS_LOG_PUBLIC_TRANSID ) ;
      if ( !itr.valid() )
      {
         goto done ;
      }

      transID = *( (DPS_TRANS_ID*)itr.value() ) ;
      if ( transID != DPS_INVALID_TRANS_ID )
      {
         itr = record.find( DPS_LOG_PUBLIC_PRETRANS ) ;
         if ( !itr.valid() )
         {
            lsnOffset = DPS_INVALID_LSN_OFFSET ;
         }
         else
         {
            lsnOffset = *((DPS_LSN_OFFSET *)itr.value()) ;
         }

         if ( LOG_TYPE_TS_COMMIT == record.head()._type )
         {
            DPS_LSN_OFFSET firstLsn = DPS_INVALID_LSN_OFFSET ;
            itr = record.find( DPS_LOG_PUBLIC_FIRSTTRANS ) ;

            if ( DPS_INVALID_LSN_OFFSET == lsnOffset ||
                 !itr.valid() )
            {
               // In the old version, commit log have not pre trans lsn and
               // first trans lsn. So, we can't rollback trans info
               ret = FALSE ;
               goto done ;
            }
            firstLsn = *( ( DPS_LSN_OFFSET*)itr.value() ) ;

            addBeginLsn( firstLsn, transID ) ;
         }
         else if ( !isRollback( transID ) )
         {
            if ( isFirstOp( transID ) )
            {
               delBeginLsn( transID ) ;
            }
         }
         else // is rollback
         {
            DPS_LSN_OFFSET relatedLsn = DPS_INVALID_LSN_OFFSET ;
            itr = record.find( DPS_LOG_PUBLIC_RELATED_TRANS ) ;
            if ( !itr.valid() )
            {
               // In the old version, rollback log have not related trans lsn,
               // so, we can't rollback trans info correctly
               ret = FALSE ;
               goto done ;
            }
            relatedLsn = *( ( DPS_LSN_OFFSET*)itr.value() ) ;
            if ( DPS_INVALID_LSN_OFFSET == lsnOffset )
            {
               // In the last rollback trans lsn, pre trans lsn is invalid
               addBeginLsn( relatedLsn, transID ) ;
            }
            lsnOffset = relatedLsn ;
         }

         updateTransInfo( transID, lsnOffset ) ;
      }

   done:
      return ret ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG, "dpsTransCB::saveTransInfoFromLog" )
   void dpsTransCB::saveTransInfoFromLog( const dpsLogRecord &record )
   {
      PD_TRACE_ENTRY( SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG ) ;

      DPS_LSN_OFFSET lsnOffset = DPS_INVALID_LSN_OFFSET;
      DPS_TRANS_ID transID = DPS_INVALID_TRANS_ID;
      dpsLogRecord::iterator itr = record.find( DPS_LOG_PUBLIC_TRANSID ) ;
      if ( !itr.valid() )
      {
         goto done ;
      }
      transID = *( (DPS_TRANS_ID *)itr.value() ) ;
      if ( transID != DPS_INVALID_TRANS_ID )
      {
         if ( isRollback( transID ) )
         {
            itr = record.find( DPS_LOG_PUBLIC_PRETRANS ) ;
            if ( !itr.valid() )
            {
               lsnOffset = DPS_INVALID_LSN_OFFSET ;
            }
            else
            {
               lsnOffset = *((DPS_LSN_OFFSET *)itr.value()) ;
            }
         }
         else if ( LOG_TYPE_TS_COMMIT == record.head()._type )
         {
            lsnOffset = DPS_INVALID_LSN_OFFSET ;
         }
         else
         {
            lsnOffset = record.head()._lsn ;
            if ( isFirstOp( transID ) )
            {
               addBeginLsn( lsnOffset, transID ) ;
            }
         }

         if ( DPS_INVALID_LSN_OFFSET == lsnOffset )
         {
            delBeginLsn( transID ) ;
         }
         updateTransInfo( transID, lsnOffset ) ;
      }

   done:
      PD_TRACE_EXIT ( SDB_DPSTRANSCB_SAVETRANSINFOFROMLOG ) ;
      return ;
   }

   void dpsTransCB::addBeginLsn( DPS_LSN_OFFSET beginLsn, DPS_TRANS_ID transID )
   {
      SDB_ASSERT( beginLsn != DPS_INVALID_LSN_OFFSET, "invalid begin-lsn" ) ;
      SDB_ASSERT( transID != DPS_INVALID_TRANS_ID, "invalid transaction-ID" ) ;
      transID = getTransID( transID );
      ossScopedLock _lock( &_lsnMapMutex );
      _beginLsnIdMap[ beginLsn ] = transID;
      _idBeginLsnMap[ transID ] = beginLsn;
   }

   void dpsTransCB::delBeginLsn( DPS_TRANS_ID transID )
   {
      transID = getTransID( transID );
      ossScopedLock _lock( &_lsnMapMutex );
      DPS_LSN_OFFSET beginLsn;
      TRANS_ID_LSN_MAP::iterator iter = _idBeginLsnMap.find( transID ) ;
      if ( iter != _idBeginLsnMap.end() )
      {
         beginLsn = iter->second;
         _beginLsnIdMap.erase( beginLsn );
         _idBeginLsnMap.erase( transID );
      }
   }

   DPS_LSN_OFFSET dpsTransCB::getBeginLsn( DPS_TRANS_ID transID )
   {
      transID = getTransID( transID ) ;
      ossScopedLock _lock( &_lsnMapMutex ) ;
      TRANS_ID_LSN_MAP::iterator iter = _idBeginLsnMap.find( transID ) ;
      if ( iter != _idBeginLsnMap.end() )
      {
         return iter->second ;
      }
      return DPS_INVALID_LSN_OFFSET ;
   }

   DPS_LSN_OFFSET dpsTransCB::getOldestBeginLsn()
   {
      ossScopedLock _lock( &_lsnMapMutex );
      if ( _beginLsnIdMap.size() > 0 )
      {
         return _beginLsnIdMap.begin()->first;
      }
      return DPS_INVALID_LSN_OFFSET;
   }

   BOOLEAN dpsTransCB::isNeedSyncTrans()
   {
      return _isNeedSyncTrans;
   }

   void dpsTransCB::setIsNeedSyncTrans( BOOLEAN isNeed )
   {
      _isNeedSyncTrans = isNeed;
   }

   INT32 dpsTransCB::syncTransInfoFromLocal( DPS_LSN_OFFSET beginLsn )
   {
      INT32 rc = SDB_OK ;
      DPS_LSN curLsn ;
      curLsn.offset = beginLsn ;
      _dpsMessageBlock mb(DPS_MSG_BLOCK_DEF_LEN) ;
      SDB_DPSCB *dpsCB = pmdGetKRCB()->getDPSCB() ;
      if ( !_isNeedSyncTrans || DPS_INVALID_LSN_OFFSET == beginLsn )
      {
         goto done;
      }
      clearTransInfo() ;
      while ( curLsn.offset!= DPS_INVALID_LSN_OFFSET &&
              curLsn.compareOffset( dpsCB->expectLsn().offset ) < 0 )
      {
         mb.clear();
         rc = dpsCB->search( curLsn, &mb );
         PD_RC_CHECK( rc, PDERROR, "Failed to search %lld in dpsCB, rc=%d",
                      curLsn.offset, rc );
         _dpsLogRecord record ;
         rc = record.load( mb.readPtr() ) ;
         PD_RC_CHECK( rc, PDERROR, "Failed to load log record, rc=%d", rc );
         saveTransInfoFromLog( record );
         curLsn.offset += record.head()._length;
      }
   done:
      return rc;
   error:
      goto done;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DPSTRANSCB_TERMALLTRANS, "dpsTransCB::termAllTrans" )
   void dpsTransCB::termAllTrans()
   {
      PD_TRACE_ENTRY( SDB_DPSTRANSCB_TERMALLTRANS ) ;

      ossScopedLock _lock( &_CBMapMutex );
      TRANS_CB_MAP::iterator iterMap = _cbMap.begin();
      while( iterMap != _cbMap.end() )
      {
         iterMap->second->postEvent( pmdEDUEvent(
                                     PMD_EDU_EVENT_TRANS_STOP ) ) ;
         _cbMap.erase( iterMap++ );
      }

      PD_TRACE_EXIT ( SDB_DPSTRANSCB_TERMALLTRANS );
   }


   INT32 dpsTransCB::transLockGetX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                    UINT16 collectionID,
                                    const dmsRecordID *recordID,
                                    _IContext *pContext,
                                    dpsTransRetInfo * pdpsTxResInfo,
                                    _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->acquire( eduCB->getTransExecutor(),
                                     lockId, DPS_TRANSLOCK_X,
                                     pContext, pdpsTxResInfo, 
                                     callback );
   }


   INT32 dpsTransCB::transLockGetU( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                    UINT16 collectionID,
                                    const dmsRecordID *recordID,
                                    _IContext *pContext,
                                    dpsTransRetInfo * pdpsTxResInfo,
                                    _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->acquire( eduCB->getTransExecutor(),
                                     lockId, DPS_TRANSLOCK_U,
                                     pContext, pdpsTxResInfo,
                                     callback ) ;
   }


   INT32 dpsTransCB::transLockGetS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                    UINT16 collectionID,
                                    const dmsRecordID *recordID,
                                    _IContext *pContext,
                                    dpsTransRetInfo * pdpsTxResInfo,
                                    _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->acquire( eduCB->getTransExecutor(),
                                     lockId, DPS_TRANSLOCK_S,
                                     pContext, pdpsTxResInfo, 
                                     callback );
   }


   INT32 dpsTransCB::transLockGetIX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                     UINT16 collectionID,
                                     _IContext *pContext,
                                     dpsTransRetInfo * pdpsTxResInfo )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, NULL );
      return _transLockMgr->acquire( eduCB->getTransExecutor(),
                                     lockId, DPS_TRANSLOCK_IX,
                                     pContext, pdpsTxResInfo );
   }


   INT32 dpsTransCB::transLockGetIS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                     UINT16 collectionID,
                                     _IContext *pContext,
                                     dpsTransRetInfo * pdpsTxResInfo )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, NULL );
      return _transLockMgr->acquire( eduCB->getTransExecutor(),
                                    lockId, DPS_TRANSLOCK_IS,
                                    pContext, pdpsTxResInfo );
   }


   void dpsTransCB::transLockRelease( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                      UINT16 collectionID,
                                      const dmsRecordID *recordID,
                                      _dpsITransLockCallback * callback )
   {
      if ( 0 == eduCB->getTransExecutor()->getLockCount() )
      {
         return ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );

      return _transLockMgr->release( eduCB->getTransExecutor(),
                                     lockId, FALSE, callback );
   }

   void dpsTransCB::transLockReleaseAll( _pmdEDUCB *eduCB,
                                         _dpsITransLockCallback * callback )
   {
      if ( 0 == eduCB->getTransExecutor()->getLockCount() )
      {
         return ;
      }
      else
      {
         _transLockMgr->releaseAll( eduCB->getTransExecutor(), callback );
      }
   }

   BOOLEAN dpsTransCB::isTransOn() const
   {
      return _isOn ;
   }

   INT32 dpsTransCB::transLockTestS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                     UINT16 collectionID,
                                     const dmsRecordID *recordID,
                                     dpsTransRetInfo * pdpsTxResInfo,
                                     _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->testAcquire( eduCB->getTransExecutor(),
                                        lockId, DPS_TRANSLOCK_S,
                                        pdpsTxResInfo,
                                        callback );
   }

   INT32 dpsTransCB::transLockTestIS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                      UINT16 collectionID,
                                      const dmsRecordID *recordID,
                                      dpsTransRetInfo * pdpsTxResInfo )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, NULL );
      return _transLockMgr->testAcquire( eduCB->getTransExecutor(),
                                        lockId, DPS_TRANSLOCK_IS,
                                        pdpsTxResInfo );
   }

   INT32 dpsTransCB::transLockTestX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                     UINT16 collectionID,
                                     const dmsRecordID *recordID,
                                     dpsTransRetInfo * pdpsTxResInfo,
                                     _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->testAcquire( eduCB->getTransExecutor(),
                                        lockId, DPS_TRANSLOCK_X,
                                        pdpsTxResInfo,
                                        callback ) ;
   }


   INT32 dpsTransCB::transLockTestU( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                     UINT16 collectionID,
                                     const dmsRecordID *recordID,
                                     dpsTransRetInfo * pdpsTxResInfo,
                                     _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->testAcquire( eduCB->getTransExecutor(),
                                        lockId, DPS_TRANSLOCK_U,
                                        pdpsTxResInfo,
                                        callback ) ;
   }


   INT32 dpsTransCB::transLockTestIX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                      UINT16 collectionID,
                                      const dmsRecordID *recordID,
                                      dpsTransRetInfo * pdpsTxResInfo )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, NULL );
      return _transLockMgr->testAcquire( eduCB->getTransExecutor(),
                                        lockId, DPS_TRANSLOCK_IX,
                                        pdpsTxResInfo );
   }


   INT32 dpsTransCB::transLockTryX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                    UINT16 collectionID,
                                    const dmsRecordID *recordID,
                                    dpsTransRetInfo * pdpsTxResInfo,
                                    _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->tryAcquire( eduCB->getTransExecutor(),
                                       lockId, DPS_TRANSLOCK_X,
                                       pdpsTxResInfo, callback );
   }

   INT32 dpsTransCB::transLockTryU( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                    UINT16 collectionID,
                                    const dmsRecordID *recordID,
                                    dpsTransRetInfo * pdpsTxResInfo,
                                    _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->tryAcquire( eduCB->getTransExecutor(),
                                       lockId, DPS_TRANSLOCK_U,
                                       pdpsTxResInfo,
                                       callback );
   }


   INT32 dpsTransCB::transLockTryS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                                    UINT16 collectionID,
                                    const dmsRecordID *recordID,
                                    dpsTransRetInfo * pdpsTxResInfo,
                                    _dpsITransLockCallback * callback )
   {
      if ( !_isOn )
      {
         return SDB_OK ;
      }
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->tryAcquire( eduCB->getTransExecutor(),
                                       lockId, DPS_TRANSLOCK_S,
                                       pdpsTxResInfo,
                                       callback ) ;
   }


   BOOLEAN dpsTransCB::hasWait( UINT32 logicCSID, UINT16 collectionID,
                                const dmsRecordID *recordID)
   {
      if ( !_isOn )
      {
         return FALSE ;
      }
      SDB_ASSERT( collectionID!=DMS_INVALID_MBID, "invalid collectionID" ) ;
      SDB_ASSERT( recordID, "recordID can't be NULL" ) ;
      dpsTransLockId lockId( logicCSID, collectionID, recordID );
      return _transLockMgr->hasWait( lockId );
   }

   INT32 dpsTransCB::reservedLogSpace( UINT32 length, _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK;
      UINT64 usedSize = 0;
      if ( !_isOn || ( cb && cb->isInRollback() ) )
      {
         goto done ;
      }

      {
         ossScopedLock _lock( &_maxFileSizeMutex );
         _accquiredSpace += length;
      }
      usedSize = usedLogSpace();
      if ( usedSize + _accquiredSpace >= _maxUsedSize )
      {
         rc = SDB_DPS_LOG_FILE_OUT_OF_SIZE ;
         ossScopedLock _lock( &_maxFileSizeMutex );
         if ( _accquiredSpace >= length )
         {
            _accquiredSpace -= length ;
         }
         else
         {
            _accquiredSpace = 0 ;
         }
         goto error ;
      }
   done:
      return rc;
   error:
      goto done;
   }

   void dpsTransCB::releaseLogSpace( UINT32 length, _pmdEDUCB *cb )
   {
      if ( !_isOn || ( cb && cb->isInRollback() ) )
      {
         return ;
      }
      ossScopedLock _lock( &_maxFileSizeMutex );
      if ( _accquiredSpace >= length )
      {
         _accquiredSpace -= length ;
      }
      else
      {
         _accquiredSpace = 0 ;
      }
   }

   UINT64 dpsTransCB::usedLogSpace()
   {
      DPS_LSN_OFFSET beginLsnOffset;
      DPS_LSN_OFFSET curLsnOffset;
      DPS_LSN curLsn;
      SDB_DPSCB *dpsCB = pmdGetKRCB()->getDPSCB();
      UINT64 usedSize = 0;

      beginLsnOffset = getOldestBeginLsn();
      if ( DPS_INVALID_LSN_OFFSET == beginLsnOffset )
      {
         goto done;
      }
      curLsn = dpsCB->expectLsn();
      curLsnOffset = curLsn.offset;
      if ( DPS_INVALID_LSN_OFFSET == curLsnOffset )
      {
         goto done;
      }

      beginLsnOffset = beginLsnOffset % _logFileTotalSize ;
      curLsnOffset = curLsnOffset % _logFileTotalSize ;
      usedSize = ( curLsnOffset + _logFileTotalSize - beginLsnOffset ) %
                   _logFileTotalSize ;
   done:
      return usedSize ;
   }

   UINT64 dpsTransCB::remainLogSpace()
   {
      UINT64 remainSize = _logFileTotalSize ;
      UINT64 allocatedSize = 0 ;

      if ( !_isOn )
      {
         goto done ;
      }

      {
      ossScopedLock _lock( &_maxFileSizeMutex ) ;
      allocatedSize = _accquiredSpace + usedLogSpace() ;
      }
      if ( _maxUsedSize > allocatedSize )
      {
         remainSize = _maxUsedSize - allocatedSize ;
      }
      else
      {
         remainSize = 0 ;
      }
   done:
      return remainSize ;
   }


   dpsTransLockManager * dpsTransCB::getLockMgrHandle()
   {
      return ( _transLockMgr->isInitialized() ? (  _transLockMgr ) : NULL ) ; 
   }


   /*
      get global trans cb
   */
   dpsTransCB* sdbGetTransCB ()
   {
      static dpsTransCB s_transCB ;
      return &s_transCB ;
   }

}
