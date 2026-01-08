/**
 * @file ckvacuum.cpp
 * @brief Cuckoo index VACUUM functions.
 *
 * This file contains functions for vacuuming a cuckoo index,
 * including bulk deletion and cleanup.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#include "cuckoo.h"

extern "C" {
#include "access/genam.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
}

/**
 * @brief Bulk delete index entries pointing to deleted heap tuples.
 *
 * Scans the entire index and removes tuples whose heap TIDs are
 * reported as deleted by the callback function.
 *
 * @param info Vacuum information.
 * @param stats Existing stats to update, or NULL to create new.
 * @param callback Function to check if a TID should be deleted.
 * @param callback_state State to pass to callback.
 * @return Updated vacuum statistics.
 */
IndexBulkDeleteResult *ckbulkdelete(IndexVacuumInfo *info,
                                    IndexBulkDeleteResult *stats,
                                    IndexBulkDeleteCallback callback,
                                    void *callback_state) {
  Relation index = info->index;
  BlockNumber blkno;
  BlockNumber npages;
  CuckooFreeBlockArray notFullPage;
  int countPage = 0;
  CuckooState state;
  Buffer buffer;
  Page page;
  CuckooMetaPageData *metaData;
  GenericXLogState *gxlogState;

  if (stats == NULL)
    stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

  initCuckooState(&state, index);

  /*
   * Iterate over all data pages.
   * We don't worry about pages added concurrently - they can't
   * contain tuples we need to delete.
   */
  npages = RelationGetNumberOfBlocks(index);
  for (blkno = CUCKOO_HEAD_BLKNO; blkno < npages; blkno++) {
    CuckooTuple *itup, *itupPtr, *itupEnd;

    vacuum_delay_point();

    buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
                                info->strategy);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    gxlogState = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);

    /* Skip empty/deleted pages */
    if (PageIsNew(page) || CuckooPageIsDeleted(page)) {
      UnlockReleaseBuffer(buffer);
      GenericXLogAbort(gxlogState);
      continue;
    }

    /*
     * Scan tuples on this page.
     * itup = current tuple being examined
     * itupPtr = where to write next surviving tuple
     */
    itup = itupPtr = CuckooPageGetTuple(&state, page, FirstOffsetNumber);
    itupEnd = CuckooPageGetTuple(
        &state, page, OffsetNumberNext(CuckooPageGetMaxOffset(page)));

    while (itup < itupEnd) {
      /* Check if this tuple should be deleted */
      if (callback(&itup->heapPtr, callback_state)) {
        /* Delete: decrement count */
        CuckooPageGetOpaque(page)->maxoff--;
        stats->tuples_removed += 1;
      } else {
        /* Keep: copy to itupPtr position if needed */
        if (itupPtr != itup) {
          memmove((Pointer)itupPtr, (Pointer)itup, state.sizeOfCuckooTuple);
        }
        itupPtr = CuckooPageGetNextTuple(&state, itupPtr);
      }

      itup = CuckooPageGetNextTuple(&state, itup);
    }

    /* Verify we counted correctly */
    Assert(itupPtr ==
           CuckooPageGetTuple(&state, page,
                              OffsetNumberNext(CuckooPageGetMaxOffset(page))));

    /*
     * Add page to notFullPage list if it has space and isn't empty.
     */
    if (CuckooPageGetMaxOffset(page) != 0 &&
        CuckooPageGetFreeSpace(&state, page) >= state.sizeOfCuckooTuple &&
        countPage < (int)CuckooMetaBlockN) {
      notFullPage[countPage++] = blkno;
    }

    /* Did we delete anything? */
    if (itupPtr != itup) {
      /* Is the page now empty? */
      if (CuckooPageGetMaxOffset(page) == 0)
        CuckooPageSetDeleted(page);

      /* Adjust pd_lower */
      ((PageHeader)page)->pd_lower = (Pointer)itupPtr - page;

      GenericXLogFinish(gxlogState);
    } else {
      GenericXLogAbort(gxlogState);
    }

    UnlockReleaseBuffer(buffer);
  }

  /*
   * Update the metapage's notFullPage list.
   * This info may be slightly stale, but ckinsert() will cope.
   */
  buffer = ReadBuffer(index, CUCKOO_METAPAGE_BLKNO);
  LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

  gxlogState = GenericXLogStart(index);
  page = GenericXLogRegisterBuffer(gxlogState, buffer, 0);

  metaData = CuckooPageGetMeta(page);
  memcpy(metaData->notFullPage, notFullPage, sizeof(BlockNumber) * countPage);
  metaData->nStart = 0;
  metaData->nEnd = countPage;

  GenericXLogFinish(gxlogState);
  UnlockReleaseBuffer(buffer);

  return stats;
}

/**
 * @brief Post-VACUUM cleanup.
 *
 * Collects statistics and updates the free space map.
 *
 * @param info Vacuum information.
 * @param stats Stats from bulk delete, or NULL if none performed.
 * @return Updated vacuum statistics.
 */
IndexBulkDeleteResult *ckvacuumcleanup(IndexVacuumInfo *info,
                                       IndexBulkDeleteResult *stats) {
  Relation index = info->index;
  BlockNumber npages;
  BlockNumber blkno;

  if (info->analyze_only)
    return stats;

  if (stats == NULL)
    stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

  /*
   * Scan all pages to collect statistics and update FSM.
   */
  npages = RelationGetNumberOfBlocks(index);
  stats->num_pages = npages;
  stats->pages_free = 0;
  stats->num_index_tuples = 0;

  for (blkno = CUCKOO_HEAD_BLKNO; blkno < npages; blkno++) {
    Buffer buffer;
    Page page;

    vacuum_delay_point();

    buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
                                info->strategy);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    if (PageIsNew(page) || CuckooPageIsDeleted(page)) {
      RecordFreeIndexPage(index, blkno);
      stats->pages_free++;
    } else {
      stats->num_index_tuples += CuckooPageGetMaxOffset(page);
    }

    UnlockReleaseBuffer(buffer);
  }

  IndexFreeSpaceMapVacuum(info->index);

  return stats;
}
