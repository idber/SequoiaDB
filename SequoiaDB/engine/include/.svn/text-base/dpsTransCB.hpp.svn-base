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

   Source File Name = dpsTransCB.hpp

   Descriptive Name = Operating System Services Types Header

   When/how to use: this program may be used on binary and text-formatted
   versions of OSS component. This file contains declare for data types used in
   SequoiaDB.

   Dependencies: N/A

   Restrictions: N/A

   Change Activity:
   defect Date        Who Description
   ====== =========== === ==============================================
          09/14/2012  YW  Initial Draft

   Last Changed =

*******************************************************************************/

#ifndef DPSTRANSCB_HPP_
#define DPSTRANSCB_HPP_

#include <queue>
#include "oss.hpp"
#include "ossAtomic.hpp"
#include "dpsDef.hpp"
#include "ossLatch.hpp"
#include "dms.hpp"
#include "dpsTransLockMgr.hpp"
#include "dpsLogRecord.hpp"
#include "sdbInterface.hpp"
#include "ossEvent.hpp"
#include "ossMemPool.hpp"

namespace engine
{
   class _pmdEDUCB ;
   class _dmsExtScanner ;
   class _dmsIXSecScanner ;
   class oldVersionCB ;
   class dpsTransLockManager ;
   class _dpsITransLockCallback ;

   typedef ossPoolMap<DPS_TRANS_ID, DPS_LSN_OFFSET> TRANS_MAP ;
   typedef ossPoolMap<DPS_TRANS_ID, _pmdEDUCB * >   TRANS_CB_MAP ;
   typedef ossPoolMap<DPS_LSN_OFFSET, DPS_TRANS_ID> TRANS_LSN_ID_MAP ;
   typedef ossPoolMap<DPS_TRANS_ID, DPS_LSN_OFFSET> TRANS_ID_LSN_MAP ;
   typedef std::queue< EDUID >                      TRANS_EDU_LIST ;

   /*
      dpsTransCB define
   */
   class dpsTransCB : public _IControlBlock, public _IEventHander
   {
      friend class _dmsExtScannerBase ;
      friend class _dmsExtScanner ;
      friend class _dmsIXSecScanner ;
   public:
      dpsTransCB() ;
      virtual ~dpsTransCB() ;

      virtual SDB_CB_TYPE cbType() const ;
      virtual const CHAR* cbName() const ;

      virtual INT32  init () ;
      virtual INT32  active () ;
      virtual INT32  deactive () ;
      virtual INT32  fini () ;
      virtual void   onConfigChange() ;

      virtual void   onRegistered( const MsgRouteID &nodeID ) ;
      virtual void   onPrimaryChange( BOOLEAN primary,
                                      SDB_EVENT_OCCUR_TYPE occurType ) ;

      /*
      *TransactionID:
      +---------------+-----------+-----------+
      | nodeID(16bit) | TAG(8bit) | SN(40bit) |
      +---------------+-----------+-----------+
      */
      DPS_TRANS_ID allocTransID() ;
      DPS_TRANS_ID getRollbackID( DPS_TRANS_ID transID ) ;
      DPS_TRANS_ID getTransID( DPS_TRANS_ID rollbackID ) ;

      oldVersionCB * getOldVCB () { return _oldVCB ; }

      BOOLEAN isRollback( DPS_TRANS_ID transID ) ;
      BOOLEAN isFirstOp( DPS_TRANS_ID transID ) ;
      void    clearFirstOpTag( DPS_TRANS_ID &transID ) ;

      INT32 startRollbackTask() ;
      INT32 stopRollbackTask() ;
      BOOLEAN isDoRollback() const { return _doRollback ; }
      INT32   waitRollback( UINT64 millicSec = -1 ) ;

      void addTransInfo( DPS_TRANS_ID transID, DPS_LSN_OFFSET lsnOffset ) ;
      void updateTransInfo( DPS_TRANS_ID transID, DPS_LSN_OFFSET lsnOffset ) ;

      void addTransCB( DPS_TRANS_ID transID, _pmdEDUCB *eduCB ) ;
      void delTransCB( DPS_TRANS_ID transID ) ;
      void dumpTransEDUList( TRANS_EDU_LIST  &eduList ) ;
      UINT32 getTransCBSize() ;
      void termAllTrans() ;
      TRANS_MAP *getTransMap() ;

      void clearTransInfo() ;

      void saveTransInfoFromLog( const dpsLogRecord &record ) ;
      BOOLEAN rollbackTransInfoFromLog( const dpsLogRecord &record ) ;

      void addBeginLsn( DPS_LSN_OFFSET beginLsn, DPS_TRANS_ID transID ) ;
      void delBeginLsn( DPS_TRANS_ID transID ) ;
      DPS_LSN_OFFSET getBeginLsn( DPS_TRANS_ID transID ) ;
      DPS_LSN_OFFSET getOldestBeginLsn() ;

      BOOLEAN isNeedSyncTrans() ;
      void setIsNeedSyncTrans( BOOLEAN isNeed ) ;

      INT32 syncTransInfoFromLocal( DPS_LSN_OFFSET beginLsn ) ;

      // get record-X-lock: also get the space-IS-lock and collection-IX-lock
      // get collection-X-lock: also get the space-IX-lock
      INT32 transLockGetX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                           UINT16 collectionID = DMS_INVALID_MBID,
                           const dmsRecordID *recordID = NULL,
                           _IContext *pContext = NULL,
                           dpsTransRetInfo * pdpsTxResInfo = NULL,
                           _dpsITransLockCallback *callback = NULL ) ;

      // get record-U-lock: also get the space-IS-lock and collection-IS-lock
      INT32 transLockGetU( _pmdEDUCB *eduCB, UINT32 logicCSID,
                           UINT16 collectionID,
                           const dmsRecordID *recordID,
                           _IContext *pContext = NULL,
                           dpsTransRetInfo * pdpsTxResInfo = NULL,
                           _dpsITransLockCallback *callback = NULL ) ;

      // get record-S-lock: also get the space-IS-lock and collection-IS-lock
      // get collection-S-lock: also get the space-IS-lock
      INT32 transLockGetS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                           UINT16 collectionID = DMS_INVALID_MBID,
                           const dmsRecordID *recordID = NULL,
                           _IContext *pContext = NULL,
                           dpsTransRetInfo * pdpsTxResInfo = NULL,
                           _dpsITransLockCallback *callback = NULL ) ;

      // also get the space-IS-lock
      INT32 transLockGetIX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                            UINT16 collectionID = DMS_INVALID_MBID,
                            _IContext *pContext = NULL,
                            dpsTransRetInfo * pdpsTxResInfo = NULL ) ;

      // also get the space-IS-lock
      INT32 transLockGetIS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                            UINT16 collectionID = DMS_INVALID_MBID,
                            _IContext *pContext = NULL,
                            dpsTransRetInfo * pdpsTxResInfo = NULL ) ;

      // release record-lock: also release the space-lock and collection-lock
      // release collection-lock: also release the space-lock
      void transLockRelease( _pmdEDUCB *eduCB, UINT32 logicCSID,
                             UINT16 collectionID = DMS_INVALID_MBID,
                             const dmsRecordID *recordID = NULL, 
                             _dpsITransLockCallback *callback = NULL ) ;

      void transLockReleaseAll( _pmdEDUCB *eduCB,
                                _dpsITransLockCallback * callback = NULL ) ;

      BOOLEAN isTransOn() const ;

      // test if the lock can be got.
      // test record-S-lock: also test the space-IS-lock and collection-IS-lock
      // test collection-IS-lock: also test the space-IS-lock
      INT32 transLockTestS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                            UINT16 collectionID = DMS_INVALID_MBID,
                            const dmsRecordID *recordID = NULL,
                            dpsTransRetInfo * pdpsTxResInfo = NULL,
                            _dpsITransLockCallback *callback = NULL ) ;

      INT32 transLockTestIS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                             UINT16 collectionID = DMS_INVALID_MBID,
                             const dmsRecordID *recordID = NULL,
                             dpsTransRetInfo * pdpsTxResInfo = NULL ) ;

      // test if the lock can be got.
      // test record-X-lock: also test the space-IS-lock and collection-IX-lock
      INT32 transLockTestX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                            UINT16 collectionID = DMS_INVALID_MBID,
                            const dmsRecordID *recordID = NULL,
                            dpsTransRetInfo * pdpsTxResInfo = NULL,
                            _dpsITransLockCallback *callback = NULL ) ;

      INT32 transLockTestIX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                             UINT16 collectionID = DMS_INVALID_MBID,
                             const dmsRecordID *recordID = NULL,
                             dpsTransRetInfo * pdpsTxResInfo = NULL ) ;

      // test if the lock can be got.
      // test record-U-lock: also test the space-IS-lock and collection-IS-lock
      INT32 transLockTestU( _pmdEDUCB *eduCB, UINT32 logicCSID,
                            UINT16 collectionID ,
                            const dmsRecordID *recordID,
                            dpsTransRetInfo * pdpsTxResInfo = NULL,
                            _dpsITransLockCallback *callback = NULL ) ;


      // try to get record-X-lock: also try to get the space-IS-lock and
      // collection-IX-lock
      // try to get collection-X-lock: also try to get the space-IX-lock
      INT32 transLockTryX( _pmdEDUCB *eduCB, UINT32 logicCSID,
                           UINT16 collectionID = DMS_INVALID_MBID,
                           const dmsRecordID *recordID = NULL,
                           dpsTransRetInfo * pdpsTxResInfo = NULL,
                           _dpsITransLockCallback * callback = NULL ) ;


      // try to get record-U-lock: also try to get the space-IS-lock and
      // collection-IS-lock
      INT32 transLockTryU( _pmdEDUCB *eduCB, UINT32 logicCSID,
                           UINT16 collectionID ,
                           const dmsRecordID *recordID,
                           dpsTransRetInfo * pdpsTxResInfo = NULL,
                           _dpsITransLockCallback *callback = NULL ) ;

      // try to get record-S-lock: also try to get the space-IS-lock and
      // collection-IS-lock
      // try to get collection-S-lock: also try to get the space-IS-lock
      INT32 transLockTryS( _pmdEDUCB *eduCB, UINT32 logicCSID,
                           UINT16 collectionID = DMS_INVALID_MBID,
                           const dmsRecordID *recordID = NULL,
                           dpsTransRetInfo * pdpsTxResInfo = NULL,
                           _dpsITransLockCallback * callback = NULL ) ;

      BOOLEAN hasWait( UINT32 logicCSID, UINT16 collectionID,
                       const dmsRecordID *recordID) ;

      INT32 reservedLogSpace( UINT32 length, _pmdEDUCB *cb ) ;

      void releaseLogSpace( UINT32 length, _pmdEDUCB *cb ) ;

      UINT64 remainLogSpace() ;

      UINT64 usedLogSpace() ;

      dpsTransLockManager * getLockMgrHandle() ;

   private:
   private:
      DPS_TRANS_ID      _TransIDH16 ;
      ossAtomic64       _TransIDL48Cur ;
      ossSpinXLatch     _MapMutex ;
      TRANS_MAP         _TransMap ;
      ossSpinXLatch     _CBMapMutex ;
      TRANS_CB_MAP      _cbMap ;
      BOOLEAN           _isOn ;
      BOOLEAN           _doRollback ;
      ossEvent          _rollbackEvent ;
      ossSpinXLatch     _lsnMapMutex ;
      TRANS_LSN_ID_MAP  _beginLsnIdMap ;
      TRANS_ID_LSN_MAP  _idBeginLsnMap ;
      BOOLEAN           _isNeedSyncTrans ;
      ossSpinXLatch     _maxFileSizeMutex ;
      UINT64            _maxUsedSize ;
      UINT64            _logFileTotalSize ;
      UINT64            _accquiredSpace ;

      dpsTransLockManager  *_transLockMgr ;
      oldVersionCB         *_oldVCB ;  // control block holding old(last committed)
                                       // version of record and index key value

   } ;

   /*
      get global cb obj
   */
   dpsTransCB* sdbGetTransCB () ;

}

#endif // DPSTRANSCB_HPP_
