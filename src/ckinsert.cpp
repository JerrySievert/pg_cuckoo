/**
 * @file ckinsert.cpp
 * @brief Cuckoo index build and insert functions.
 *
 * This file contains functions for building a cuckoo index from scratch
 * and inserting individual tuples.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#include "cuckoo.h"

extern "C" {
#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/tableam.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
}

/* Module magic for PostgreSQL extension */
extern "C" {
PG_MODULE_MAGIC;
}

/**
 * @brief State maintained during index build.
 */
typedef struct CuckooBuildState {
  CuckooState ckstate;  /**< Cuckoo index state */
  int64 indtuples;      /**< Total number of tuples indexed */
  MemoryContext tmpCtx; /**< Temporary memory context */
  PGAlignedBlock data;  /**< Cached page data */
  int count;            /**< Number of tuples in cached page */
} CuckooBuildState;

/**
 * @brief Flush cached page to disk.
 *
 * @param index The index relation.
 * @param buildstate Build state containing cached page.
 */
static void flushCachedPage(Relation index, CuckooBuildState *buildstate) {
  Page page;
  Buffer buffer = CuckooNewBuffer(index);
  GenericXLogState *state;

  state = GenericXLogStart(index);
  page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
  memcpy(page, buildstate->data.data, BLCKSZ);
  GenericXLogFinish(state);
  UnlockReleaseBuffer(buffer);
}

/**
 * @brief Initialize cached page in build state.
 *
 * @param buildstate Build state to initialize.
 */
static void initCachedPage(CuckooBuildState *buildstate) {
  CuckooInitPage(buildstate->data.data, 0);
  buildstate->count = 0;
}

/**
 * @brief Callback for table_index_build_scan during index build.
 *
 * Called for each tuple in the heap. Creates a cuckoo tuple and adds
 * it to the current page, flushing pages as needed.
 *
 * @param index The index relation.
 * @param tid Tuple ID of the heap tuple.
 * @param values Array of indexed values.
 * @param isnull Array indicating NULL values.
 * @param tupleIsAlive Whether the tuple is live (unused for cuckoo).
 * @param state Build state pointer.
 */
static void cuckooBuildCallback(Relation index, ItemPointer tid, Datum *values,
                                bool *isnull, bool tupleIsAlive, void *state) {
  CuckooBuildState *buildstate = (CuckooBuildState *)state;
  MemoryContext oldCtx;
  CuckooTuple *itup;

  oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

  itup = CuckooFormTuple(&buildstate->ckstate, tid, values, isnull);

  /* Try to add tuple to cached page */
  if (CuckooPageAddItem(&buildstate->ckstate, buildstate->data.data, itup)) {
    buildstate->count++;
  } else {
    /* Page is full, flush it and start a new one */
    flushCachedPage(index, buildstate);

    CHECK_FOR_INTERRUPTS();

    initCachedPage(buildstate);

    if (!CuckooPageAddItem(&buildstate->ckstate, buildstate->data.data, itup)) {
      elog(ERROR, "could not add new cuckoo tuple to empty page");
    }

    buildstate->count++;
  }

  buildstate->indtuples++;

  MemoryContextSwitchTo(oldCtx);
  MemoryContextReset(buildstate->tmpCtx);
}

/**
 * @brief Build a new cuckoo index.
 *
 * Scans the entire heap and builds the index from scratch.
 *
 * @param heap The heap relation being indexed.
 * @param index The index relation to build.
 * @param indexInfo Index information.
 * @return Build result with tuple counts.
 */
IndexBuildResult *ckbuild(Relation heap, Relation index, IndexInfo *indexInfo) {
  IndexBuildResult *result;
  double reltuples;
  CuckooBuildState buildstate;

  if (RelationGetNumberOfBlocks(index) != 0)
    elog(ERROR, "index \"%s\" already contains data",
         RelationGetRelationName(index));

  /* Initialize the metapage */
  CuckooInitMetapage(index, MAIN_FORKNUM);

  /* Initialize build state */
  memset(&buildstate, 0, sizeof(buildstate));
  initCuckooState(&buildstate.ckstate, index);
  buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
                                            "Cuckoo build temporary context",
                                            ALLOCSET_DEFAULT_SIZES);
  initCachedPage(&buildstate);

  /* Scan the heap and build index */
  reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
                                     cuckooBuildCallback, &buildstate, NULL);

  /* Flush last page if it has any tuples */
  if (buildstate.count > 0)
    flushCachedPage(index, &buildstate);

  MemoryContextDelete(buildstate.tmpCtx);

  result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
  result->heap_tuples = reltuples;
  result->index_tuples = buildstate.indtuples;

  return result;
}

/**
 * @brief Build an empty cuckoo index in the initialization fork.
 *
 * @param index The index relation.
 */
void ckbuildempty(Relation index) { CuckooInitMetapage(index, INIT_FORKNUM); }

/**
 * @brief Insert a new tuple into the cuckoo index.
 *
 * @param index The index relation.
 * @param values Array of indexed values.
 * @param isnull Array indicating NULL values.
 * @param ht_ctid Heap tuple ID.
 * @param heapRel The heap relation.
 * @param checkUnique Uniqueness check mode (ignored for cuckoo).
 * @param indexUnchanged Whether index columns are unchanged.
 * @param indexInfo Index information.
 * @return Always returns false (no unique constraint violations).
 */
bool ckinsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
              Relation heapRel, IndexUniqueCheck checkUnique,
              bool indexUnchanged, IndexInfo *indexInfo) {
  CuckooState ckstate;
  CuckooTuple *itup;
  MemoryContext oldCtx;
  MemoryContext insertCtx;
  CuckooMetaPageData *metaData;
  Buffer buffer, metaBuffer;
  Page page, metaPage;
  BlockNumber blkno = InvalidBlockNumber;
  OffsetNumber nStart;
  GenericXLogState *state;

  insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                    "Cuckoo insert temporary context",
                                    ALLOCSET_DEFAULT_SIZES);

  oldCtx = MemoryContextSwitchTo(insertCtx);

  initCuckooState(&ckstate, index);
  itup = CuckooFormTuple(&ckstate, ht_ctid, values, isnull);

  /*
   * First, try to insert into the first page in notFullPage array.
   * If successful, we don't need to modify the metapage.
   */
  metaBuffer = ReadBuffer(index, CUCKOO_METAPAGE_BLKNO);
  LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
  metaData = CuckooPageGetMeta(BufferGetPage(metaBuffer));

  if (metaData->nEnd > metaData->nStart) {
    blkno = metaData->notFullPage[metaData->nStart];
    Assert(blkno != InvalidBlockNumber);

    LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);

    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buffer, 0);

    /* Handle recently deleted pages */
    if (PageIsNew(page) || CuckooPageIsDeleted(page))
      CuckooInitPage(page, 0);

    if (CuckooPageAddItem(&ckstate, page, itup)) {
      GenericXLogFinish(state);
      UnlockReleaseBuffer(buffer);
      ReleaseBuffer(metaBuffer);
      MemoryContextSwitchTo(oldCtx);
      MemoryContextDelete(insertCtx);
      return false;
    }

    GenericXLogAbort(state);
    UnlockReleaseBuffer(buffer);
  } else {
    LockBuffer(metaBuffer, BUFFER_LOCK_UNLOCK);
  }

  /*
   * Try other pages in notFullPage array.
   * Need exclusive lock on metapage to update nStart.
   */
  LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);

  nStart = metaData->nStart;

  /* Skip first page if we already tried it */
  if (nStart < metaData->nEnd && blkno == metaData->notFullPage[nStart])
    nStart++;

  for (;;) {
    state = GenericXLogStart(index);

    metaPage = GenericXLogRegisterBuffer(state, metaBuffer, 0);
    metaData = CuckooPageGetMeta(metaPage);

    if (nStart >= metaData->nEnd)
      break;

    blkno = metaData->notFullPage[nStart];
    Assert(blkno != InvalidBlockNumber);

    buffer = ReadBuffer(index, blkno);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    page = GenericXLogRegisterBuffer(state, buffer, 0);

    if (PageIsNew(page) || CuckooPageIsDeleted(page))
      CuckooInitPage(page, 0);

    if (CuckooPageAddItem(&ckstate, page, itup)) {
      metaData->nStart = nStart;
      GenericXLogFinish(state);
      UnlockReleaseBuffer(buffer);
      UnlockReleaseBuffer(metaBuffer);
      MemoryContextSwitchTo(oldCtx);
      MemoryContextDelete(insertCtx);
      return false;
    }

    GenericXLogAbort(state);
    UnlockReleaseBuffer(buffer);
    nStart++;
  }

  /*
   * No space in existing pages, allocate a new one.
   */
  buffer = CuckooNewBuffer(index);

  page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
  CuckooInitPage(page, 0);

  if (!CuckooPageAddItem(&ckstate, page, itup)) {
    elog(ERROR, "could not add new cuckoo tuple to empty page");
  }

  /* Reset notFullPage array to contain just this new page */
  metaData->nStart = 0;
  metaData->nEnd = 1;
  metaData->notFullPage[0] = BufferGetBlockNumber(buffer);

  GenericXLogFinish(state);

  UnlockReleaseBuffer(buffer);
  UnlockReleaseBuffer(metaBuffer);

  MemoryContextSwitchTo(oldCtx);
  MemoryContextDelete(insertCtx);

  return false;
}
