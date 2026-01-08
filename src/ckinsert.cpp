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

#if PG_VERSION_NUM >= 170000
#include "access/parallel.h"
#include "access/relation.h"
#include "commands/progress.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/tuplesort.h"
#endif
}

/* Module magic for PostgreSQL extension */
extern "C" {
PG_MODULE_MAGIC;
}

#if PG_VERSION_NUM >= 170000
/* Forward declarations for parallel build functions */
static int ck_compute_parallel_workers(Relation heap, Relation index);
static void _ck_begin_parallel(CuckooParallelBuildState *buildstate,
                               Relation heap, Relation index,
                               IndexInfo *indexInfo, int request);
static void _ck_parallel_merge(CuckooParallelBuildState *buildstate,
                               Relation heap, Relation index);
static void _ck_end_parallel(CuckooParallelBuildState *buildstate);
static void
_ck_leader_participate_as_worker(CuckooParallelBuildState *buildstate,
                                 Relation heap, Relation index);
#endif

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
 * Uses parallel workers when beneficial (PG17+).
 *
 * @param heap The heap relation being indexed.
 * @param index The index relation to build.
 * @param indexInfo Index information.
 * @return Build result with tuple counts.
 */
IndexBuildResult *ckbuild(Relation heap, Relation index, IndexInfo *indexInfo) {
  IndexBuildResult *result;
  double reltuples;

  if (RelationGetNumberOfBlocks(index) != 0)
    elog(ERROR, "index \"%s\" already contains data",
         RelationGetRelationName(index));

  /* Initialize the metapage */
  CuckooInitMetapage(index, MAIN_FORKNUM);

#if PG_VERSION_NUM >= 170000
  /*
   * Attempt parallel build if beneficial.
   */
  {
    int request;

    request = ck_compute_parallel_workers(heap, index);

    if (request > 0) {
      CuckooParallelBuildState pstate;

      /* Initialize parallel build state */
      memset(&pstate, 0, sizeof(pstate));
      initCuckooState(&pstate.ckstate, index);
      pstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
                                            "Cuckoo parallel build context",
                                            ALLOCSET_DEFAULT_SIZES);
      pstate.isworker = false;

      /* Set up parallel context and launch workers */
      _ck_begin_parallel(&pstate, heap, index, indexInfo, request);

      /* Leader participates in scanning */
      _ck_leader_participate_as_worker(&pstate, heap, index);

      /* Merge results and write pages */
      _ck_parallel_merge(&pstate, heap, index);

      /* Clean up */
      _ck_end_parallel(&pstate);
      MemoryContextDelete(pstate.tmpCtx);

      /* Build result */
      result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));
      result->heap_tuples = pstate.reltuples;
      result->index_tuples = pstate.indtuples;

      return result;
    }
  }
#endif

  /*
   * Serial build path (used when parallel not available or not beneficial).
   */
  {
    CuckooBuildState buildstate;

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
}

/*
 * Parallel index build support (PG17+)
 */
#if PG_VERSION_NUM >= 170000

/**
 * @brief Macro to get parallel table scan descriptor from CuckooShared.
 *
 * The ParallelTableScanDesc follows immediately after CuckooShared in DSM.
 */
#define ParallelTableScanFromCuckooShared(shared)                              \
  ((ParallelTableScanDesc)((char *)(shared) + MAXALIGN(sizeof(CuckooShared))))

/**
 * @brief Flush cached page during parallel build.
 *
 * Similar to flushCachedPage but uses CuckooParallelBuildState.
 */
static void flushCachedPageParallel(Relation index,
                                    CuckooParallelBuildState *buildstate) {
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
 * @brief Initialize cached page for parallel build state.
 */
static void initCachedPageParallel(CuckooParallelBuildState *buildstate) {
  CuckooInitPage(buildstate->data.data, 0);
  buildstate->count = 0;
}

/**
 * @brief Compute number of parallel workers for index build.
 *
 * @param heap Heap relation.
 * @param index Index relation.
 * @return Number of parallel workers to request, or 0 for serial build.
 */
static int ck_compute_parallel_workers(Relation heap, Relation index) {
  /*
   * Use plan_create_index_workers to determine the number of workers.
   * This considers table size, maintenance_work_mem, and
   * max_parallel_maintenance_workers.
   */
  return plan_create_index_workers(RelationGetRelid(heap),
                                   RelationGetRelid(index));
}

/**
 * @brief Callback for parallel heap scan during index build.
 *
 * Called for each tuple in worker's portion of heap. Computes fingerprint
 * and writes to shared tuplesort.
 */
static void cuckooParallelBuildCallback(Relation index, ItemPointer tid,
                                        Datum *values, bool *isnull,
                                        bool tupleIsAlive, void *state) {
  CuckooParallelBuildState *buildstate = (CuckooParallelBuildState *)state;
  MemoryContext oldCtx;
  CuckooTuple itup;

  oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

  /* Form tuple directly on stack to avoid palloc overhead */
  itup.heapPtr = *tid;
  itup.fingerprint = computeFingerprint(&buildstate->ckstate, values, isnull);

  /* Write tuple to shared tuplesort */
  tuplesort_putdatum(buildstate->sortstate, PointerGetDatum(&itup), false);

  buildstate->indtuples++;

  MemoryContextSwitchTo(oldCtx);
  MemoryContextReset(buildstate->tmpCtx);
}

/**
 * @brief Set up parallel context for index build.
 *
 * Creates parallel context, allocates shared memory, and launches workers.
 */
static void _ck_begin_parallel(CuckooParallelBuildState *buildstate,
                               Relation heap, Relation index,
                               IndexInfo *indexInfo, int request) {
  ParallelContext *pcxt;
  int scantuplesortstates;
  Size estckshared;
  Size estsort;
  Size estwalusage;
  Size estbufferusage;
  CuckooShared *ckshared;
  Sharedsort *sharedsort;
  CuckooLeader *leader;
  WalUsage *walusage;
  BufferUsage *bufferusage;
  Snapshot snapshot;
  int querylen;
  const char *querystr;
  char *sharedquery;

  Assert(request > 0);

  /*
   * Enter parallel mode and create context.
   */
  EnterParallelMode();
  pcxt = CreateParallelContext("cuckoo", "_ck_parallel_build_main", request);

  /*
   * Prepare to share query string for worker error reporting.
   */
  querystr = debug_query_string;
  if (!querystr)
    querystr = "";
  querylen = strlen(querystr) + 1;

  /*
   * Calculate the number of participants. Leader will participate
   * in the parallel build, so add 1.
   */
  scantuplesortstates = pcxt->nworkers + 1;

  /*
   * Estimate and allocate shared memory.
   * CuckooShared and ParallelTableScanDesc are allocated together.
   */
  estckshared = MAXALIGN(sizeof(CuckooShared)) +
                table_parallelscan_estimate(heap, SnapshotAny);
  shm_toc_estimate_chunk(&pcxt->estimator, estckshared);
  estsort = tuplesort_estimate_shared(scantuplesortstates);
  shm_toc_estimate_chunk(&pcxt->estimator, estsort);
  shm_toc_estimate_chunk(&pcxt->estimator, querylen);
  estwalusage = MAXALIGN(sizeof(WalUsage) * pcxt->nworkers);
  shm_toc_estimate_chunk(&pcxt->estimator, estwalusage);
  estbufferusage = MAXALIGN(sizeof(BufferUsage) * pcxt->nworkers);
  shm_toc_estimate_chunk(&pcxt->estimator, estbufferusage);

  shm_toc_estimate_keys(&pcxt->estimator, 5);

  /*
   * Initialize DSM and set up shared state.
   */
  InitializeParallelDSM(pcxt);

  /* Allocate and initialize CuckooShared */
  ckshared = (CuckooShared *)shm_toc_allocate(pcxt->toc, estckshared);
  memset(ckshared, 0, sizeof(CuckooShared));

  ckshared->heaprelid = RelationGetRelid(heap);
  ckshared->indexrelid = RelationGetRelid(index);
  ckshared->isconcurrent = indexInfo->ii_Concurrent;
  ckshared->scantuplesortstates = scantuplesortstates;
  ckshared->bitsPerTag = buildstate->ckstate.opts.bitsPerTag;
  ckshared->tagsPerBucket = buildstate->ckstate.opts.tagsPerBucket;
  ckshared->maxKicks = buildstate->ckstate.opts.maxKicks;
  ckshared->nColumns = buildstate->ckstate.nColumns;

  ConditionVariableInit(&ckshared->workersdonecv);
  SpinLockInit(&ckshared->mutex);
  ckshared->nparticipantsdone = 0;
  ckshared->reltuples = 0;
  ckshared->indtuples = 0;

  shm_toc_insert(pcxt->toc, PARALLEL_KEY_CUCKOO_SHARED, ckshared);

  /* Allocate and initialize shared tuplesort state */
  sharedsort = (Sharedsort *)shm_toc_allocate(pcxt->toc, estsort);
  tuplesort_initialize_shared(sharedsort, scantuplesortstates, pcxt->seg);
  shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

  /* Share the query string */
  sharedquery = (char *)shm_toc_allocate(pcxt->toc, querylen);
  memcpy(sharedquery, querystr, querylen);
  shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);

  /* Allocate WAL and buffer usage arrays */
  walusage = (WalUsage *)shm_toc_allocate(pcxt->toc, estwalusage);
  memset(walusage, 0, estwalusage);
  shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);

  bufferusage = (BufferUsage *)shm_toc_allocate(pcxt->toc, estbufferusage);
  memset(bufferusage, 0, estbufferusage);
  shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

  /* Set up parallel table scan */
  snapshot = RegisterSnapshot(GetTransactionSnapshot());
  table_parallelscan_initialize(
      heap, ParallelTableScanFromCuckooShared(ckshared), snapshot);

  /*
   * Set up leader state.
   */
  leader = (CuckooLeader *)palloc0(sizeof(CuckooLeader));
  leader->pcxt = pcxt;
  leader->nparticipanttuplesorts = scantuplesortstates;
  leader->shared = ckshared;
  leader->sharedsort = sharedsort;
  leader->snapshot = snapshot;
  leader->walusage = walusage;
  leader->bufferusage = bufferusage;

  buildstate->leader = leader;

  /*
   * Launch workers.
   */
  LaunchParallelWorkers(pcxt);

  /*
   * If no workers launched, we'll do a serial build.
   */
  if (pcxt->nworkers_launched == 0) {
    ckshared->scantuplesortstates = 1;
    leader->nparticipanttuplesorts = 1;
  }

  /*
   * Initialize leader's tuplesort state as coordinator.
   */
  buildstate->sortstate = tuplesort_begin_datum(
      BYTEAOID,   /* Arbitrary type for raw bytes */
      InvalidOid, /* No comparison operator */
      InvalidOid, /* No collation */
      false,      /* Not null first */
      maintenance_work_mem / leader->nparticipanttuplesorts,
      (SortCoordinate)sharedsort, TUPLESORT_NONE);
}

/**
 * @brief Wait for all workers to complete and merge their results.
 *
 * Leader waits on condition variable, then reads all tuples from
 * the shared tuplesort and writes them to index pages.
 */
static void _ck_parallel_merge(CuckooParallelBuildState *buildstate,
                               Relation heap, Relation index) {
  CuckooLeader *leader = buildstate->leader;
  CuckooShared *ckshared = leader->shared;
  CuckooTuple *itup;
  Datum datum;
  bool isnull;

  /*
   * Wait for all workers to finish.
   */
  for (;;) {
    SpinLockAcquire(&ckshared->mutex);
    if (ckshared->nparticipantsdone >= leader->nparticipanttuplesorts) {
      SpinLockRelease(&ckshared->mutex);
      break;
    }
    SpinLockRelease(&ckshared->mutex);

    ConditionVariableSleep(&ckshared->workersdonecv,
                           WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
  }
  ConditionVariableCancelSleep();

  /*
   * Get final counts from shared state.
   */
  SpinLockAcquire(&ckshared->mutex);
  buildstate->reltuples = ckshared->reltuples;
  buildstate->indtuples = ckshared->indtuples;
  SpinLockRelease(&ckshared->mutex);

  /*
   * Perform final sort and read all tuples.
   */
  tuplesort_performsort(buildstate->sortstate);

  /*
   * Initialize page cache for writing.
   */
  initCachedPageParallel(buildstate);

  /*
   * Read tuples from tuplesort and write to index pages.
   */
  while (tuplesort_getdatum(buildstate->sortstate, true, false, &datum, &isnull,
                            NULL)) {
    itup = (CuckooTuple *)DatumGetPointer(datum);

    /* Try to add tuple to cached page */
    if (!CuckooPageAddItem(&buildstate->ckstate, buildstate->data.data, itup)) {
      /* Page is full, flush it and start a new one */
      flushCachedPageParallel(index, buildstate);
      CHECK_FOR_INTERRUPTS();
      initCachedPageParallel(buildstate);

      if (!CuckooPageAddItem(&buildstate->ckstate, buildstate->data.data,
                             itup)) {
        elog(ERROR, "could not add new cuckoo tuple to empty page");
      }
    }
    buildstate->count++;
  }

  /* Flush last page if it has any tuples */
  if (buildstate->count > 0)
    flushCachedPageParallel(index, buildstate);

  tuplesort_end(buildstate->sortstate);
}

/**
 * @brief Clean up parallel context after build.
 */
static void _ck_end_parallel(CuckooParallelBuildState *buildstate) {
  CuckooLeader *leader = buildstate->leader;
  int i;

  /* Shut down workers */
  WaitForParallelWorkersToFinish(leader->pcxt);

  /* Accumulate instrumentation data */
  for (i = 0; i < leader->pcxt->nworkers_launched; i++) {
    InstrAccumParallelQuery(&leader->bufferusage[i], &leader->walusage[i]);
  }

  /* Clean up snapshot */
  UnregisterSnapshot(leader->snapshot);

  /* Destroy parallel context */
  DestroyParallelContext(leader->pcxt);
  ExitParallelMode();

  pfree(leader);
  buildstate->leader = NULL;
}

/**
 * @brief Leader participates as a worker in parallel build.
 *
 * The leader process also scans its portion of the heap.
 */
static void
_ck_leader_participate_as_worker(CuckooParallelBuildState *buildstate,
                                 Relation heap, Relation index) {
  CuckooShared *ckshared = buildstate->leader->shared;
  ParallelTableScanDesc pscan;
  TableScanDesc scan;
  double reltuples;

  /* Get parallel scan descriptor */
  pscan = ParallelTableScanFromCuckooShared(ckshared);

  /* Create a TableScanDesc from the parallel scan descriptor */
  scan = table_beginscan_parallel(heap, pscan);

  /* Scan our portion of the heap */
  reltuples =
      table_index_build_scan(heap, index, NULL, false, true,
                             cuckooParallelBuildCallback, buildstate, scan);

  /* End the scan */
  table_endscan(scan);

  /* Finish our tuplesort participation */
  tuplesort_performsort(buildstate->sortstate);

  /* Update shared counters */
  SpinLockAcquire(&ckshared->mutex);
  ckshared->nparticipantsdone++;
  ckshared->reltuples += reltuples;
  ckshared->indtuples += buildstate->indtuples;
  SpinLockRelease(&ckshared->mutex);

  ConditionVariableSignal(&ckshared->workersdonecv);
}

/**
 * @brief Worker entry point for parallel index build.
 *
 * Called by PostgreSQL parallel worker infrastructure.
 */
void _ck_parallel_build_main(dsm_segment *seg, shm_toc *toc) {
  CuckooShared *ckshared;
  Sharedsort *sharedsort;
  SortCoordinate coordinate;
  CuckooParallelBuildState buildstate;
  Relation heap;
  Relation index;
  LOCKMODE heapLockmode;
  LOCKMODE indexLockmode;
  ParallelTableScanDesc pscan;
  TableScanDesc scan;
  double reltuples;
  int sortmem;

  /*
   * Look up shared state in TOC.
   */
  ckshared =
      (CuckooShared *)shm_toc_lookup(toc, PARALLEL_KEY_CUCKOO_SHARED, false);
  sharedsort = (Sharedsort *)shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);

  /*
   * Open relations. If concurrent build, use appropriate lock modes.
   */
  if (!ckshared->isconcurrent) {
    heapLockmode = ShareLock;
    indexLockmode = AccessExclusiveLock;
  } else {
    heapLockmode = ShareUpdateExclusiveLock;
    indexLockmode = RowExclusiveLock;
  }

  heap = relation_open(ckshared->heaprelid, heapLockmode);
  index = index_open(ckshared->indexrelid, indexLockmode);

  /*
   * Initialize build state.
   */
  memset(&buildstate, 0, sizeof(buildstate));
  initCuckooState(&buildstate.ckstate, index);
  buildstate.tmpCtx =
      AllocSetContextCreate(CurrentMemoryContext, "Cuckoo parallel build temp",
                            ALLOCSET_DEFAULT_SIZES);
  buildstate.isworker = true;
  buildstate.indtuples = 0;
  buildstate.reltuples = 0;

  /*
   * Attach to shared tuplesort.
   */
  sortmem = maintenance_work_mem / ckshared->scantuplesortstates;
  tuplesort_attach_shared(sharedsort, seg);
  coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
  coordinate->isWorker = true;
  coordinate->nParticipants = -1;
  coordinate->sharedsort = sharedsort;
  buildstate.sortstate =
      tuplesort_begin_datum(BYTEAOID, InvalidOid, InvalidOid, false, sortmem,
                            coordinate, TUPLESORT_NONE);

  /*
   * Scan our portion of the heap.
   */
  pscan = ParallelTableScanFromCuckooShared(ckshared);
  scan = table_beginscan_parallel(heap, pscan);
  reltuples =
      table_index_build_scan(heap, index, NULL, false, true,
                             cuckooParallelBuildCallback, &buildstate, scan);
  table_endscan(scan);

  /*
   * Finish sorting.
   */
  tuplesort_performsort(buildstate.sortstate);

  /*
   * Update shared counters.
   */
  SpinLockAcquire(&ckshared->mutex);
  ckshared->nparticipantsdone++;
  ckshared->reltuples += reltuples;
  ckshared->indtuples += buildstate.indtuples;
  SpinLockRelease(&ckshared->mutex);

  /*
   * Signal that we're done.
   */
  ConditionVariableSignal(&ckshared->workersdonecv);

  /*
   * Clean up.
   */
  tuplesort_end(buildstate.sortstate);
  MemoryContextDelete(buildstate.tmpCtx);
  index_close(index, indexLockmode);
  relation_close(heap, heapLockmode);
}

#endif /* PG_VERSION_NUM >= 170000 */

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
