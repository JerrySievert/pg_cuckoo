/**
 * @file ckscan.cpp
 * @brief Cuckoo index scan functions.
 *
 * This file contains functions for scanning a cuckoo index to find
 * matching tuples.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#include "cuckoo.h"

extern "C" {
#include "access/relscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
}

/**
 * @brief Begin a scan of a cuckoo index.
 *
 * Allocates and initializes scan state.
 *
 * @param r The index relation.
 * @param nkeys Number of scan keys.
 * @param norderbys Number of order-by keys (unused for cuckoo).
 * @return Initialized IndexScanDesc.
 */
IndexScanDesc ckbeginscan(Relation r, int nkeys, int norderbys) {
  IndexScanDesc scan;
  CuckooScanOpaque so;

  scan = RelationGetIndexScan(r, nkeys, norderbys);

  so = (CuckooScanOpaque)palloc(sizeof(CuckooScanOpaqueData));
  initCuckooState(&so->state, scan->indexRelation);
  so->fingerprint = 0;
  so->fingerprintValid = false;

  scan->opaque = so;

  return scan;
}

/**
 * @brief Rescan a cuckoo index with new keys.
 *
 * @param scan The scan descriptor.
 * @param scankey New scan keys.
 * @param nscankeys Number of scan keys.
 * @param orderbys Order-by keys (unused).
 * @param norderbys Number of order-by keys (unused).
 */
void ckrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
              ScanKey orderbys, int norderbys) {
  CuckooScanOpaque so = (CuckooScanOpaque)scan->opaque;

  /* Invalidate cached fingerprint */
  so->fingerprintValid = false;

  if (scankey && scan->numberOfKeys > 0)
    memcpy(scan->keyData, scankey, scan->numberOfKeys * sizeof(ScanKeyData));
}

/**
 * @brief End a cuckoo index scan.
 *
 * @param scan The scan descriptor to clean up.
 */
void ckendscan(IndexScanDesc scan) {
  CuckooScanOpaque so = (CuckooScanOpaque)scan->opaque;

  /* Nothing special to clean up */
  (void)so;
}

/**
 * @brief Get all matching tuples as a bitmap.
 *
 * Scans all index pages and returns TIDs of tuples whose fingerprints
 * match the search fingerprint. Note that this may return false positives
 * which will be filtered out by PostgreSQL when accessing the heap.
 *
 * @param scan The scan descriptor.
 * @param tbm Bitmap to add matching TIDs to.
 * @return Number of matching tuples found.
 */
int64 ckgetbitmap(IndexScanDesc scan, TIDBitmap *tbm) {
  int64 ntids = 0;
  BlockNumber blkno = CUCKOO_HEAD_BLKNO;
  BlockNumber npages;
  BufferAccessStrategy bas;
  CuckooScanOpaque so = (CuckooScanOpaque)scan->opaque;

  /* Compute search fingerprint if not already done */
  if (!so->fingerprintValid) {
    ScanKey skey = scan->keyData;
    Datum *values;
    bool *isnull;

    values = (Datum *)palloc(sizeof(Datum) * so->state.nColumns);
    isnull = (bool *)palloc(sizeof(bool) * so->state.nColumns);

    /* Initialize all columns as NULL */
    for (int i = 0; i < so->state.nColumns; i++) {
      values[i] = (Datum)0;
      isnull[i] = true;
    }

    /* Fill in values from scan keys */
    for (int i = 0; i < scan->numberOfKeys; i++) {
      /*
       * Cuckoo-indexable operators are assumed to be strict,
       * so NULL key means no matches.
       */
      if (skey->sk_flags & SK_ISNULL) {
        pfree(values);
        pfree(isnull);
        return 0;
      }

      /* Set value for this column (sk_attno is 1-based) */
      int attno = skey->sk_attno - 1;
      values[attno] = skey->sk_argument;
      isnull[attno] = false;

      skey++;
    }

    so->fingerprint = computeFingerprint(&so->state, values, isnull);
    so->fingerprintValid = true;

    pfree(values);
    pfree(isnull);
  }

  /*
   * Scan the entire index using bulk read strategy.
   */
  bas = GetAccessStrategy(BAS_BULKREAD);
  npages = RelationGetNumberOfBlocks(scan->indexRelation);
  pgstat_count_index_scan(scan->indexRelation);

  for (blkno = CUCKOO_HEAD_BLKNO; blkno < npages; blkno++) {
    Buffer buffer;
    Page page;

    buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, blkno,
                                RBM_NORMAL, bas);

    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    if (!PageIsNew(page) && !CuckooPageIsDeleted(page)) {
      OffsetNumber offset;
      OffsetNumber maxOffset = CuckooPageGetMaxOffset(page);

      for (offset = 1; offset <= maxOffset; offset++) {
        CuckooTuple *itup = CuckooPageGetTuple(&so->state, page, offset);

        /*
         * Check if fingerprint matches.
         * This is the core of the cuckoo filter lookup:
         * we simply compare the stored fingerprint with the
         * search fingerprint.
         */
        if (itup->fingerprint == so->fingerprint) {
          tbm_add_tuples(tbm, &itup->heapPtr, 1, true);
          ntids++;
        }
      }
    }

    UnlockReleaseBuffer(buffer);
    CHECK_FOR_INTERRUPTS();
  }

  FreeAccessStrategy(bas);

  return ntids;
}
