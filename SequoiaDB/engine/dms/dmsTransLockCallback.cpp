/*******************************************************************************


   Copyright (C) 2011-2019 SequoiaDB Ltd.

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

   Source File Name = dmsTransLockCallback.cpp

   Descriptive Name = Data Management Service Lock Callback Functions

   When/how to use: this program may be used on binary and text-formatted
   versions of data management component. This file contains structure for
   DMS storage unit and its methods.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          08/02/2019  CYX Initial Draft

   Last Changed =

*******************************************************************************/

#include "pmdEDU.hpp"
#include "pdTrace.hpp"
#include "dmsTrace.hpp"
#include "dmsTransLockCallback.hpp"
#include "dmsStorageDataCommon.hpp"
#include "dpsTransVersionCtrl.hpp"
#include "rtnIXScanner.hpp"

using namespace bson ;


namespace engine
{

   /*
      _dmsMemRecordRW implement
   */
   class _dmsMemRecordRW : public _dmsRecordRW
   {
      public:
         _dmsMemRecordRW( dpsOldRecordPtr ptr )
         :_dmsRecordRW()
         {
            if ( ptr.get() )
            {
               _isDirectMem = TRUE ;
               _ptr = ( const dmsRecord* )ptr.get() ;
            }
         }
   } ;
   typedef _dmsMemRecordRW dmsMemRecordRW ;

   /*
      Extend data function
   */
   BOOLEAN dmsIsExtendDataValid( const dpsLRBExtData *pExtData )
   {
      return 0 != pExtData->_data ? TRUE : FALSE ;
   }

   BOOLEAN dmsExtendDataReleaseCheck( const dpsLRBExtData *pExtData )
   {
      const oldVersionContainer *pOldVer = NULL ;
      pOldVer = ( const oldVersionContainer * )( pExtData->_data ) ;
      if ( pOldVer && !pOldVer->isIndexObjEmpty() )
      {
         return FALSE ;
      }
      return TRUE ;
   }

   void dmsReleaseExtendData( dpsLRBExtData *pExtData )
   {
      oldVersionContainer *pOldVer = NULL ;
      pOldVer = ( oldVersionContainer * )( pExtData->_data ) ;
      if ( pOldVer )
      {
         delete pOldVer ;
         pExtData->_data = 0 ;
      }
   }

   void dmsOnTransLockRelease( const dpsTransLockId &lockId,
                               DPS_TRANSLOCK_TYPE lockMode,
                               UINT32 refCounter,
                               dpsLRBExtData *pExtData )
   {
      oldVersionContainer *oldVer   = NULL ;

      // early exit if this is not record lock OR not in X mode OR
      // there is no old record setup.
      if ( ( !lockId.isLeafLevel() )               ||
           ( DPS_TRANSLOCK_X != lockMode )         ||
           ( 0 != refCounter )                     ||
           ( NULL == pExtData )                    ||
           ( 0 == pExtData->_data ) )
      {
         goto done ;
      }

      oldVer = ( oldVersionContainer* )( pExtData->_data ) ;
      SDB_ASSERT( oldVer->getRecordID() ==
                  dmsRecordID(lockId.extentID(), lockId.offset()),
                  "LockID is not the same" ) ;

      /// when no old index
      if ( dmsExtendDataReleaseCheck( pExtData ) )
      {
         oldVer->releaseRecord() ;
      }
      else
      {
         oldVer->setRecordDeleted() ;

         PD_LOG( PDDEBUG, "Set old record for rid[%s] in memory to deleted",
                 lockId.toString().c_str() ) ;

         /// DOTO:XUJIANHUI
         /// PUT the rid to backgroud task to recycle
      }

   done:
      return ;
   }

   static BOOLEAN _dmsIsKeyUndefined( const BSONObj &keyObj )
   {
      BSONObjIterator itr( keyObj ) ;
      while ( itr.more() )
      {
         BSONElement e = itr.next() ;
         if ( Undefined != e.type() )
         {
            return FALSE ;
         }
      }
      return TRUE ;
   }

   dmsTransLockCallback::dmsTransLockCallback()
   {
      _transCB    = NULL ;
      _oldVer     = NULL ;
      _eduCB      = NULL ;
      _recordRW   = NULL ;

      _csLID      = ~0 ;
      _csID       = DMS_INVALID_SUID ;
      _clID       = DMS_INVALID_MBID ;
      _latchedIdxLid = DMS_INVALID_EXTENT ;
      _latchedIdxMode = -1 ;
      _pScanner      = NULL ;

      clearStatus() ;
   }

   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSTRANSLOCKCALLBACK_DMSTRANSLOCKCALLBACK, "dmsTransLockCallback::dmsTransLockCallback" )
   dmsTransLockCallback::dmsTransLockCallback( dpsTransCB *transCB,
                                               _pmdEDUCB *eduCB )
   {
      PD_TRACE_ENTRY( SDB_DMSTRANSLOCKCALLBACK_DMSTRANSLOCKCALLBACK ) ;

      _transCB    = transCB ;
      _oldVer     = NULL ;
      _eduCB      = eduCB ;
      _recordRW   = NULL ;

      _csLID      = ~0 ;
      _csID       = DMS_INVALID_SUID ;
      _clID       = DMS_INVALID_MBID ;
      _latchedIdxLid = DMS_INVALID_EXTENT ;
      _latchedIdxMode = -1 ;
      _pScanner      = NULL ;

      clearStatus() ;

      PD_TRACE_EXIT( SDB_DMSTRANSLOCKCALLBACK_DMSTRANSLOCKCALLBACK );
   }

   dmsTransLockCallback::~dmsTransLockCallback()
   {
   }

   void dmsTransLockCallback::setBaseInfo( dpsTransCB *transCB,
                                           _pmdEDUCB *eduCB )
   {
      SDB_ASSERT( eduCB, "EDUCB can't be NULL" ) ;
      _transCB = transCB ;
      _eduCB   = eduCB ;
   }

   void dmsTransLockCallback::setIDInfo( INT32 csID, UINT16 clID, UINT32 csLID )
   {
      _csID = csID ;
      _clID = clID ;
      _csLID = csLID ;
   }

   void dmsTransLockCallback::setIXScanner( _rtnIXScanner *pScanner )
   {
      _latchedIdxLid = pScanner->getIdxLID() ;
      _latchedIdxMode = pScanner->getLockModeByType( SCANNER_TYPE_MEM_TREE ) ;
      _pScanner = pScanner ;
   }

   void dmsTransLockCallback::detachRecordRW()
   {
      _recordRW = NULL ;
   }

   void dmsTransLockCallback::attachRecordRW( _dmsRecordRW *recordRW )
   {
      _recordRW = recordRW ;
   }

   void dmsTransLockCallback::clearStatus()
   {
      _oldVer           = NULL ;
      _skipRecord       = FALSE ;
      _result           = SDB_OK ;
      _useOldVersion    = FALSE ;
      _recordPtr        = dpsOldRecordPtr() ;
      _recordInfo.reset() ;
   }

   const dmsTransRecordInfo* dmsTransLockCallback::getTransRecordInfo() const
   {
      return &_recordInfo ;
   }

   // Description:
   //    Function called after lock acquirement. There are two cases to handle:
   //
   // CASE 1: Under RC, TB scanner failed to get record lock in S mode due to
   // incompatibility, we'll try to use the saved old copy if it exist.
   // Note that normally tb scanner or idx scanner will wait on record lock
   // unless transaction isolation level is RC and translockwait is set to NO
   //
   // CASE 2: Update successfully acquire X record lock within a transaction,
   // we'll try to copy the data to lrbHrd if it's not already there
   // Note that we have decided to defer the copy to the actual update time,
   // because there are cases where X lock was acquired, but no update will
   // be made due to other critieras. However, we will do some preparation
   // work at this time.
   //
   // Input:
   //    lockId: lock id to operate on
   //    rc:  rc from the lock acquire
   //    requestLockMode: lock mode requested (IS/IX/S/U/X)
   //    opMode: lock operation mode (TRY/ACQUIRE/TEST)
   // Output:
   //    pdpsTxResInfo: return information about the lock
   // Dependency:
   //    caller must hold lrb bucket latch
   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSTRANSLOCKCALLBACK_AFTERLOCKACQUIRE, "dmsTransLockCallback::afterLockAcquire" )
   void dmsTransLockCallback::afterLockAcquire( const dpsTransLockId &lockId,
                                                INT32 irc,
                                                DPS_TRANSLOCK_TYPE requestLockMode,
                                                UINT32 refCounter,
                                                DPS_TRANSLOCK_OP_MODE_TYPE opMode,
                                                const dpsTransLRBHeader *pLRBHeader,
                                                dpsLRBExtData *pExtData )
   {
      PD_TRACE_ENTRY( SDB_DMSTRANSLOCKCALLBACK_AFTERLOCKACQUIRE ) ;

      BOOLEAN notTransOrRollback = FALSE ;

      /// when not leaf level, do nothing
      if ( !lockId.isLeafLevel() )
      {
         goto done ;
      }

      clearStatus() ;

      /// not in transaction
      if ( DPS_INVALID_TRANS_ID == _eduCB->getTransID() ||
           _eduCB->isInRollback() )
      {
         notTransOrRollback = TRUE ;
      }

      // Handle case 1 mentioned above
      // When the update was done by non transaction session, it's
      // possible that the oldRecord does not exist. We do nothing
      // in that case
      if( (SDB_DPS_TRANS_LOCK_INCOMPATIBLE == irc ) &&
          (DPS_TRANSLOCK_S == requestLockMode)     &&
          (DPS_TRANSLOCK_OP_MODE_TRY == opMode) )
      {
         SDB_ASSERT( pExtData, "ExtData is invalid " ) ;

         if ( !pExtData || 0 == pExtData->_data )
         {
            goto done ;
         }
         else if ( notTransOrRollback )
         {
            goto done ;
         }

         _oldVer = (oldVersionContainer*)(pExtData->_data) ;
         SDB_ASSERT( _oldVer->getRecordID() ==
                     dmsRecordID(lockId.extentID(), lockId.offset()),
                     "LockID is not the same" ) ;

         if ( _oldVer->isRecordDeleted() )
         {
            _oldVer = NULL ;
            goto done ;
         }

         _recordPtr = _oldVer->getRecordPtr() ;

         if ( _oldVer->isRecordNew() )
         {
            _skipRecord = TRUE ;
         }
         else if ( _recordPtr.get() && !_oldVer->isRecordDummy() )
         {
            // We need to re-verify the record with the
            // index again. Here is how this could happen:
            // Session 1 did update, changed index from 1 to 2, paused;
            // session 2 does index scan, searching for record with 
            // index 2. It found the index on disk. Did get record lock
            // and ended up using the old version from memory. But 
            // the old version record contain the index 1. We must verify
            // and skip this record.
            if ( _pScanner && _latchedIdxLid != DMS_INVALID_EXTENT &&
                 SCANNER_TYPE_DISK == _pScanner->getCurScanType() &&
                 _oldVer->idxLidExist( _latchedIdxLid ) )
            {
               _skipRecord = TRUE ;
               /// remove the duplicate rid
               _pScanner->removeDuplicatRID( _oldVer->getRecordID() ) ;
               goto done ;
            }

#ifdef _DEBUG
            PD_LOG( PDDEBUG, "Use old copy for rid[%s] from memory",
                    lockId.toString().c_str() ) ;
#endif
            // setup the buffer pointer in dmsRecordRW
            *_recordRW = dmsMemRecordRW( _recordPtr ) ;

            // set the return info if we successfully used old version
            _useOldVersion = TRUE ;
         }
      } // end of case 1

      // Handle case 2 mentioned above
      // X record lock request from a transaction need to prepare to set
      // up old copy if the copy is not already there
      else if ( SDB_OK == irc )
      {
         SDB_ASSERT( pExtData, "ExtData is invalid " ) ;
         SDB_ASSERT( refCounter > 0, "Ref count must > 0" ) ;

         _recordInfo._refCount = refCounter ;

         if ( !pExtData )
         {
            goto done ;
         }

         /// when pExtData->_data is 0, should create oldver
         if ( 0 == pExtData->_data )
         {
            if ( DPS_TRANSLOCK_X == requestLockMode &&
                 DPS_TRANSLOCK_OP_MODE_TEST != opMode &&
                 !notTransOrRollback )
            {
               memBlockPool *pPool = _transCB->getOldVCB()->getMemBlockPool() ;
               dmsRecordID rid( lockId.extentID(), lockId.offset() ) ;
               _oldVer = new ( pPool, std::nothrow ) oldVersionContainer( rid ) ;
               if ( !_oldVer )
               {
                  _result = SDB_OOM ;
                  PD_LOG( PDERROR, "Alloc oldVersionContainer faild" ) ;
                  goto done ;
               }
               pExtData->_data = (UINT64)_oldVer ;

               /// set callback
               pExtData->setValidFunc(
                  (DPS_EXTDATA_VALID_FUNC)dmsIsExtendDataValid ) ;
               pExtData->setReleaseCheckFunc(
                  (DPS_EXTDATA_RELEASE_CHECK)dmsExtendDataReleaseCheck ) ;
               pExtData->setReleaseFunc(
                  (DPS_EXTDATA_RELEASE)dmsReleaseExtendData ) ;
               pExtData->setOnLockReleaseFunc(
                  (DPS_EXTDATA_ON_LOCKRELEASE)dmsOnTransLockRelease ) ;
            }
         }
         else
         {
            _oldVer = (oldVersionContainer*)pExtData->_data ;
            SDB_ASSERT( _oldVer->getRecordID() ==
                        dmsRecordID(lockId.extentID(), lockId.offset()),
                        "LockID is not the same" ) ;

            /// from memory tree
            if ( _pScanner && _latchedIdxLid != DMS_INVALID_EXTENT &&
                 SCANNER_TYPE_MEM_TREE == _pScanner->getCurScanType() )
            {
               _skipRecord = TRUE ;
               /// remove the duplicate rid
               _pScanner->removeDuplicatRID( _oldVer->getRecordID() ) ;
               _oldVer = NULL ;
               goto done ;
            }

            /// check the record whether is deleted
            if ( _oldVer->isRecordDeleted() &&
                 DPS_TRANSLOCK_X == requestLockMode )
            {
               _oldVer->releaseRecord() ;

               PD_LOG( PDDEBUG, "Delete old record for rid[%s] from memory",
                       lockId.toString().c_str() ) ;
            }

            if ( _oldVer->isRecordNew() &&
                 _oldVer->getOwnnerTID() == _eduCB->getTID() )
            {
               _recordInfo._transInsert = TRUE ;
            }

            if ( notTransOrRollback )
            {
               _oldVer = NULL ;
            }
            // Special case is the new record created within the same
            // transaction, we should not setup oldVer for it because
            // there is no older verion for it.
            else if ( _oldVer->isRecordNew() )
            {
               _oldVer = NULL ;
            }
         }

#ifdef _DEBUG
         if ( _oldVer )
         {
            PD_LOG( PDDEBUG, "Set oldVer[%u] for rid[%s] in memory",
                    _oldVer, lockId.toString().c_str() ) ;
         }
#endif //_DEBUG
      }

   done :
      PD_TRACE_EXIT( SDB_DMSTRANSLOCKCALLBACK_AFTERLOCKACQUIRE );
      return ;
   }

   // Description:
   //    Function called before lock release(in dpsTransLockManager::_release)
   // Under RC, before X lock is released, the update is responsible
   // to free up the kept old version record and clean up all related indexes
   // from the in-memory index tree. All information were kept in lrbHdr->oldVer
   // The latching protocal has to be:
   // 1. LRB hash bkt latch must be held(X) to tranverse/update lrbHdrs/LRBs
   // 2. preIdxTree latch must be held in X to insert/delete node in the tree,
   //    oldVersionCB(_oldVersionCBLatch) need to be held in S before
   //    accessing individual index tree.
   // 3. Request preIdxTree latch while holding LRB hash bkt latch is forbidden,
   //    But reverse order is OK. Note that the scanner will hold tree latch 
   //    and acquire lock.
   //
   // Input:
   //    lockId: lock id to operate on
   //    lockMode: lock mode requested (IS/IX/S/U/X)
   //    refCounter: current reference counter of the lock
   //    oldVer: pointer to oldVersionContainer
   // Output:
   //    oldVer: pointer to oldVersionContainer
   // Dependency:
   //    caller MUST hold the record lock and LRB hash bkt latch in X
   // which protects all the update on oldVer
   // PD_TRACE_DECLARE_FUNCTION ( SDB_DMSTRANSLOCKCALLBACK_BEFORELOCKRELEASE, "dmsTransLockCallback::beforeLockRelease" )
   void dmsTransLockCallback::beforeLockRelease( const dpsTransLockId &lockId,
                                                 DPS_TRANSLOCK_TYPE lockMode,
                                                 UINT32 refCounter,
                                                 const dpsTransLRBHeader *pLRBHeader,
                                                 dpsLRBExtData *pExtData )
   {
      PD_TRACE_ENTRY( SDB_DMSTRANSLOCKCALLBACK_BEFORELOCKRELEASE ) ;

      dmsOnTransLockRelease( lockId, lockMode, refCounter, pExtData ) ;

      PD_TRACE_EXIT( SDB_DMSTRANSLOCKCALLBACK_BEFORELOCKRELEASE );
   }

   // Description
   INT32 dmsTransLockCallback::saveOldVersionRecord( const _dmsRecordRW *pRecordRW,
                                                     const dmsRecordID &rid,
                                                     const BSONObj &obj,
                                                     UINT32 ownnerTID )
   {
      INT32  rc      = SDB_OK ;

      // if the oldRecord does not exist, we will create one
      if ( _oldVer && _oldVer->isRecordEmpty() )
      {
         /// when not use rollback segment
         if ( !_eduCB->getTransExecutor()->useRollbackSegment() )
         {
            _oldVer->setRecordDummy( ownnerTID ) ;
         }
         else
         {
            const dmsRecord *pRecord= pRecordRW->readPtr( 0 ) ;
            memBlockPool *pPool = _transCB->getOldVCB()->getMemBlockPool() ;

            // 1. get to overflow record if needed
            if ( pRecord->isOvf() )
            {
               dmsRecordID ovfRID = pRecord->getOvfRID() ;
               dmsRecordRW ovfRW = pRecordRW->derive( ovfRID );
               ovfRW.setNothrow( pRecordRW->isNothrow() ) ;
               pRecord = ovfRW.readPtr( 0 ) ;
            }
            // 2. save record
            rc = _oldVer->saveRecord( pRecord, obj, ownnerTID, pPool ) ;
            if ( rc )
            {
               goto error ;
            }
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dmsTransLockCallback::_checkInsertIndex( preIdxTreePtr &treePtr,
                                                  _INSERT_CURSOR &insertCursor,
                                                  const ixmIndexCB *indexCB,
                                                  BOOLEAN isUnique,
                                                  BOOLEAN isEnforce,
                                                  const BSONObj &keyObj,
                                                  const dmsRecordID &rid,
                                                  _pmdEDUCB *cb,
                                                  utilInsertResult *insertResult )
   {
      INT32 rc = SDB_OK ;

      // If there is an old verion of the index in our in memory tree, we
      // should not insert the index because the update/delete transaction
      // has not committed yet.
      if ( isUnique && !cb->isInRollback() )
      {
         preIdxTreeNodeValue idxValue ;

         if ( !treePtr.get() && _INSERT_NONE == insertCursor )
         {
            oldVersionCB *pOVCB = _transCB->getOldVCB() ;
            globIdxID gid( _csID, _clID, indexCB->getLogicalID() ) ;
            treePtr = pOVCB->getIdxTree( gid, FALSE ) ;
         }
         insertCursor = _INSERT_CHECK ;

         if ( !treePtr.get() )
         {
            goto done ;
         }

         if ( treePtr->isKeyExist( keyObj, idxValue ) )
         {
            // Internally will test X lock on the record to make sure key
            // does not exist and avoid duplicate key false alarm
            // for example:
            //   db.cs.createCL( "c1" )
            //   db.cs.c1.createIndex( "a", {a:1})
            //   db.cs.c1.insert( {a:1, b:1} )
            //   db.transBegin()
            //   db.cs.c1.remove() // or delete { a:1, b:1 }
            //   db.cs.c1.insert({a:1, b:1}) ==> fails due to dupblicate key
            //
            if ( idxValue.getOwnnerTID() == cb->getTID() )
            {
               goto done ;
            }
            else if ( !isEnforce && _dmsIsKeyUndefined( keyObj ) )
            {
               goto done ;
            }

            rc = SDB_IXM_DUP_KEY ;
            if ( NULL != insertResult )
            {
               insertResult->setDupErrInfo( indexCB->getName(),
                                            indexCB->keyPattern(),
                                            keyObj ) ;
            }
            PD_LOG ( PDERROR, "Insert index key(%s) with rid(%d, %d) "
                     "failed, rc: %d", keyObj.toString().c_str(),
                     rid._extent, rid._offset, rc ) ;
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dmsTransLockCallback::onInsertRecord( _dmsMBContext *context,
                                               const BSONObj &object,
                                               const dmsRecordID &rid,
                                               const _dmsRecordRW *pRecordRW,
                                               _pmdEDUCB* cb )
   {
      if ( _oldVer )
      {
         _oldVer->setRecordNew() ;
      }
      return SDB_OK ;
   }

   INT32 dmsTransLockCallback::onDeleteRecord( _dmsMBContext *context,
                                               const BSONObj &object,
                                               const dmsRecordID &rid,
                                               const _dmsRecordRW *pRecordRW,
                                               _pmdEDUCB* cb )
   {
      return saveOldVersionRecord( pRecordRW, rid, object, cb->getTID() ) ;
   }

   INT32 dmsTransLockCallback::onUpdateRecord( _dmsMBContext *context,
                                               const BSONObj &orignalObj,
                                               const BSONObj &newObj,
                                               const dmsRecordID &rid,
                                               const _dmsRecordRW *pRecordRW,
                                               _pmdEDUCB *cb )
   {
      return saveOldVersionRecord( pRecordRW, rid, orignalObj, cb->getTID() ) ;
   }

   INT32 dmsTransLockCallback::onInsertIndex( _dmsMBContext *context,
                                              const ixmIndexCB *indexCB,
                                              BOOLEAN isUnique,
                                              BOOLEAN isEnforce,
                                              const BSONObjSet &keySet,
                                              const dmsRecordID &rid,
                                              _pmdEDUCB *cb,
                                              utilInsertResult *insertResult )
   {
      INT32 rc = SDB_OK ;
      preIdxTreePtr treePtr ;
      _INSERT_CURSOR insertCursor = _INSERT_NONE ;

      if ( !_transCB || !_transCB->isTransOn() )
      {
         goto done ;
      }

      for ( BSONObjSet::const_iterator cit = keySet.begin() ;
            cit != keySet.end() ;
            ++cit )
      {
         rc = _checkInsertIndex( treePtr, insertCursor, indexCB,
                                 isUnique, isEnforce, *cit,
                                 rid, cb, insertResult ) ;
         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dmsTransLockCallback::_checkDeleteIndex( preIdxTreePtr &treePtr,
                                                  _DELETE_CURSOR &deleteCursor,
                                                  const ixmIndexCB *indexCB,
                                                  BOOLEAN isUnique,
                                                  const BSONObj &keyObj,
                                                  const dmsRecordID &rid,
                                                  _pmdEDUCB* cb )
   {
      INT32 rc = SDB_OK ;
      BOOLEAN hasLocked = FALSE ;
      globIdxID gid( _csID, _clID, indexCB->getLogicalID() ) ;

      if ( !_oldVer || cb->isInRollback() )
      {
         /// not in transaction, or in trans rollback
         goto done ;
      }
      else if ( _oldVer->isRecordNew() )
      {
         goto done ;
      }
      else if ( _oldVer->isRecordDummy() && !isUnique )
      {
         /// when not use rollback segment
         goto done ;
      }
      else if ( _DELETE_NONE == deleteCursor )
      {
         //    check if oldVer has the index, Note that if it exist in
         //    the set, the index must exist in the tree as well.
         //    This is valid scenario because one transaction can do multiple
         //    update of the same record. But we only need to keep the last
         //    committed version which happened to be the first copy.
         if ( _oldVer->idxLidExist( gid._idxLID ) )
         {
            deleteCursor = _DELETE_IGNORE ;
            goto done ;
         }
         else
         {
            if ( !treePtr.get() )
            {
               oldVersionCB *pVerCB = _transCB->getOldVCB() ;
               rc = pVerCB->getOrCreateIdxTree( gid, indexCB,
                                                treePtr, FALSE ) ;
               if ( rc )
               {
                  PD_LOG( PDERROR, "Create memory index tree(%s) "
                          "failed, rc: %d", gid.toString().c_str(), rc ) ;
                  goto error ;
               }
            }

            deleteCursor = _DELETE_SAVE ;
            /// insert into oldVer's indexSet
            _oldVer->insertIdxTree( treePtr ) ;
         }
      }
      else if ( _DELETE_IGNORE == deleteCursor )
      {
         goto done ;
      }

      if ( _latchedIdxLid == gid._idxLID && _latchedIdxMode != -1 )
      {
         if ( EXCLUSIVE == _latchedIdxMode )
         {
            hasLocked = TRUE ;
         }
         else
         {
            PD_LOG( PDERROR, "Lock mode(%d) is not EXCLUSIVE(%d)",
                    _latchedIdxMode, EXCLUSIVE ) ;
            SDB_ASSERT( FALSE, "Lock mode is invalid" ) ;
            rc = SDB_SYS ;
            goto error ;
         }
      }

      rc = treePtr->insertWithOldVer( &keyObj, rid, _oldVer, hasLocked ) ;
      if ( rc )
      {
         PD_LOG ( PDERROR, "Insert index keys(%s) with rid(%d, %d) "
                  "failed, rc: %d", keyObj.toString().c_str(),
                  rid._extent, rid._offset, rc ) ;
         goto error ;
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dmsTransLockCallback::onDeleteIndex( _dmsMBContext *context,
                                              const ixmIndexCB *indexCB,
                                              BOOLEAN isUnique,
                                              const BSONObjSet &keySet,
                                              const dmsRecordID &rid,
                                              _pmdEDUCB *cb )
   {
      INT32 rc = SDB_OK ;
      _DELETE_CURSOR deleteCursor = _DELETE_NONE ;
      preIdxTreePtr treePtr ;

      if ( !_transCB || !_transCB->isTransOn() )
      {
         goto done ;
      }

      /// insert key to mem tree
      for ( BSONObjSet::const_iterator cit = keySet.begin() ;
            cit != keySet.end() ;
            ++cit )
      {
         rc = _checkDeleteIndex( treePtr,deleteCursor, indexCB,
                                 isUnique, *cit, rid, cb ) ;
         if ( rc )
         {
            goto error ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dmsTransLockCallback::onUpdateIndex( _dmsMBContext *context,
                                              const ixmIndexCB *indexCB,
                                              BOOLEAN isUnique,
                                              BOOLEAN isEnforce,
                                              const BSONObjSet &oldKeySet,
                                              const BSONObjSet &newKeySet,
                                              const dmsRecordID &rid,
                                              BOOLEAN isRollback,
                                              _pmdEDUCB* cb )
   {
      INT32 rc = SDB_OK ;
      BSONObjSet::const_iterator itori ;
      BSONObjSet::const_iterator itnew ;
      preIdxTreePtr treePtr ;
      _DELETE_CURSOR deleteCursor = _DELETE_NONE ;
      _INSERT_CURSOR insertCursor = _INSERT_NONE ;
      BOOLEAN hasChanged = FALSE ;

      /// not use transaction
      if ( !_transCB || !_transCB->isTransOn() )
      {
         goto done ;
      }
      /// rollback
      else if ( isRollback )
      {
         goto done ;
      }

      if ( oldKeySet.size() != newKeySet.size() )
      {
         hasChanged = TRUE ;
      }

      itori = oldKeySet.begin() ;
      itnew = newKeySet.begin() ;
      while ( oldKeySet.end() != itori && newKeySet.end() != itnew )
      {
         INT32 result = (*itori).woCompare((*itnew), BSONObj(), FALSE ) ;
         if ( 0 == result )
         {
            // new and original are the same, we don't need to change
            // anything in the index
            itori++ ;
            itnew++ ;
         }
         else if ( result < 0 )
         {
            // original smaller than new, that means the original doesn't
            // appear in the new list anymore, let's delete it
            hasChanged = TRUE ;
            itori++ ;
         }
         else
         {
            hasChanged = TRUE ;
            rc = _checkInsertIndex( treePtr, insertCursor, indexCB,
                                    isUnique, isEnforce, *itnew,
                                    rid, cb, NULL ) ;
            if ( rc )
            {
               goto error ;
            }
            itnew++ ;
         }
      }

      // insert rest of itnew
      while ( newKeySet.end() != itnew )
      {
         rc = _checkInsertIndex( treePtr, insertCursor, indexCB,
                                 isUnique, isEnforce, *itnew,
                                 rid, cb, NULL ) ;
         if ( rc )
         {
            goto error ;
         }
         ++itnew ;
      }

      if ( hasChanged )
      {
         // insert the original index into the in-memory old version tree
         itori = oldKeySet.begin() ;
         while ( oldKeySet.end() != itori )
         {
            rc = _checkDeleteIndex( treePtr, deleteCursor, indexCB,
                                    isUnique, *itori, rid, cb ) ;
            if ( rc )
            {
               goto error ;
            }
            ++itori ;
         }
      }

   done:
      return rc ;
   error:
      goto done ;
   }

   INT32 dmsTransLockCallback::onDropIndex( _dmsMBContext *context,
                                            const ixmIndexCB *indexCB,
                                            _pmdEDUCB *cb )
   {
      if ( _transCB && _transCB->getOldVCB() )
      {
         oldVersionCB *pOldVCB = _transCB->getOldVCB() ;
         globIdxID gid( _csID, _clID, indexCB->getLogicalID() ) ;
         pOldVCB->delIdxTree( gid, FALSE ) ;
      }

      return SDB_OK ;
   }

   INT32 dmsTransLockCallback::onRebuildIndex( _dmsMBContext *context,
                                               const ixmIndexCB *indexCB,
                                               _pmdEDUCB *cb )
   {
      // TODO:XUJIANHUI
      return SDB_OK ;
   }

   INT32 dmsTransLockCallback::onCreateIndex( _dmsMBContext *context,
                                              const ixmIndexCB *indexCB,
                                              _pmdEDUCB *cb )
   {
      // TODO:XUJIANHUI
      return SDB_OK ;
   }

}

