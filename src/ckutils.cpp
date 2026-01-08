/**
 * @file ckutils.cpp
 * @brief Cuckoo index utility functions.
 *
 * This file contains utility functions for the cuckoo filter index,
 * including the handler function, state initialization, page management,
 * and fingerprint computation.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#include "cuckoo.h"

extern "C" {
#include "access/reloptions.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"
#include "varatt.h"
}

/* Declare the handler function for PostgreSQL */
extern "C" {
PG_FUNCTION_INFO_V1(ckhandler);
}

/* Kind of relation options for cuckoo index */
static relopt_kind ck_relopt_kind;

/* Parse table for fillRelOptions - 3 options */
static relopt_parse_elt ck_relopt_tab[3];

/**
 * @brief Construct default cuckoo options.
 * @return Pointer to newly allocated CuckooOptions with default values.
 */
static CuckooOptions *makeDefaultCuckooOptions(void) {
  CuckooOptions *opts = (CuckooOptions *)palloc0(sizeof(CuckooOptions));
  opts->bitsPerTag = DEFAULT_BITS_PER_TAG;
  opts->tagsPerBucket = DEFAULT_TAGS_PER_BUCKET;
  opts->maxKicks = DEFAULT_MAX_KICKS;
  SET_VARSIZE(opts, sizeof(CuckooOptions));
  return opts;
}

/**
 * @brief Module initialization function.
 *
 * Called when the extension is loaded. Registers reloptions for the
 * cuckoo index access method.
 */
extern "C" void _PG_init(void) {
  ck_relopt_kind = add_reloption_kind();

  /* Option for bits per fingerprint tag */
  add_int_reloption(
      ck_relopt_kind, "bits_per_tag",
      "Number of bits per fingerprint tag (higher = lower false positive rate)",
      DEFAULT_BITS_PER_TAG, MIN_BITS_PER_TAG, MAX_BITS_PER_TAG,
      AccessExclusiveLock);
  ck_relopt_tab[0].optname = "bits_per_tag";
  ck_relopt_tab[0].opttype = RELOPT_TYPE_INT;
  ck_relopt_tab[0].offset = offsetof(CuckooOptions, bitsPerTag);

  /* Option for tags per bucket */
  add_int_reloption(ck_relopt_kind, "tags_per_bucket",
                    "Number of fingerprint tags per bucket (2, 4, or 8)",
                    DEFAULT_TAGS_PER_BUCKET, MIN_TAGS_PER_BUCKET,
                    MAX_TAGS_PER_BUCKET, AccessExclusiveLock);
  ck_relopt_tab[1].optname = "tags_per_bucket";
  ck_relopt_tab[1].opttype = RELOPT_TYPE_INT;
  ck_relopt_tab[1].offset = offsetof(CuckooOptions, tagsPerBucket);

  /* Option for maximum kicks during insert */
  add_int_reloption(ck_relopt_kind, "max_kicks",
                    "Maximum number of relocations during insert",
                    DEFAULT_MAX_KICKS, MIN_MAX_KICKS, MAX_MAX_KICKS,
                    AccessExclusiveLock);
  ck_relopt_tab[2].optname = "max_kicks";
  ck_relopt_tab[2].opttype = RELOPT_TYPE_INT;
  ck_relopt_tab[2].offset = offsetof(CuckooOptions, maxKicks);
}

/**
 * @brief Handler function for cuckoo index access method.
 *
 * Returns an IndexAmRoutine structure populated with all the callbacks
 * needed by PostgreSQL to manage this index type.
 *
 * @param fcinfo Function call info (unused).
 * @return Pointer to IndexAmRoutine structure.
 */
extern "C" Datum ckhandler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

  /* Index capabilities */
  amroutine->amstrategies = CUCKOO_NSTRATEGIES;
  amroutine->amsupport = CUCKOO_NPROC;
  amroutine->amoptsprocnum = CUCKOO_OPTIONS_PROC;
  amroutine->amcanorder = false;
  amroutine->amcanorderbyop = false;
  amroutine->amcanbackward = false;
  amroutine->amcanunique = false;
  amroutine->amcanmulticol = true;
  amroutine->amoptionalkey = true;
  amroutine->amsearcharray = false;
  amroutine->amsearchnulls = false;
  amroutine->amstorage = false;
  amroutine->amclusterable = false;
  amroutine->ampredlocks = false;
  amroutine->amcanparallel = false;
  amroutine->amcaninclude = false;
  amroutine->amusemaintenanceworkmem = false;
  amroutine->amparallelvacuumoptions =
      VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_CLEANUP;
  amroutine->amkeytype = InvalidOid;

  /* Index operation callbacks */
  amroutine->ambuild = ckbuild;
  amroutine->ambuildempty = ckbuildempty;
  amroutine->aminsert = ckinsert;
  amroutine->ambulkdelete = ckbulkdelete;
  amroutine->amvacuumcleanup = ckvacuumcleanup;
  amroutine->amcanreturn = NULL;
  amroutine->amcostestimate = ckcostestimate;
  amroutine->amoptions = ckoptions;
  amroutine->amproperty = NULL;
  amroutine->ambuildphasename = NULL;
  amroutine->amvalidate = ckvalidate;
  amroutine->amadjustmembers = NULL;
  amroutine->ambeginscan = ckbeginscan;
  amroutine->amrescan = ckrescan;
  amroutine->amgettuple = NULL;
  amroutine->amgetbitmap = ckgetbitmap;
  amroutine->amendscan = ckendscan;
  amroutine->ammarkpos = NULL;
  amroutine->amrestrpos = NULL;
  amroutine->amestimateparallelscan = NULL;
  amroutine->aminitparallelscan = NULL;
  amroutine->amparallelrescan = NULL;

  PG_RETURN_POINTER(amroutine);
}

/**
 * @brief Initialize CuckooState structure for an index.
 *
 * Reads the index metadata and initializes hash functions for each
 * indexed column.
 *
 * @param state Pointer to CuckooState to initialize.
 * @param index Relation descriptor for the index.
 */
void initCuckooState(CuckooState *state, Relation index) {
  state->nColumns = index->rd_att->natts;

  /* Initialize hash function for each attribute */
  for (int i = 0; i < index->rd_att->natts; i++) {
    fmgr_info_copy(&(state->hashFn[i]),
                   index_getprocinfo(index, i + 1, CUCKOO_HASH_PROC),
                   CurrentMemoryContext);
    state->collations[i] = index->rd_indcollation[i];
  }

  /* Initialize amcache if needed with options from metapage */
  if (!index->rd_amcache) {
    Buffer buffer;
    Page page;
    CuckooMetaPageData *meta;
    CuckooOptions *opts;

    opts = (CuckooOptions *)MemoryContextAlloc(index->rd_indexcxt,
                                               sizeof(CuckooOptions));

    buffer = ReadBuffer(index, CUCKOO_METAPAGE_BLKNO);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);

    if (!CuckooPageIsMeta(page))
      elog(ERROR, "Relation is not a cuckoo index");

    meta = CuckooPageGetMeta(BufferGetPage(buffer));

    if (meta->magicNumber != CUCKOO_MAGIC_NUMBER)
      elog(ERROR, "Relation is not a cuckoo index");

    *opts = meta->opts;

    UnlockReleaseBuffer(buffer);

    index->rd_amcache = opts;
  }

  memcpy(&state->opts, index->rd_amcache, sizeof(state->opts));
  state->sizeOfCuckooTuple = sizeof(CuckooTuple);
  state->tagMask = (1U << state->opts.bitsPerTag) - 1;
  state->tagsPerBucket = state->opts.tagsPerBucket;
  state->maxKicks = state->opts.maxKicks;
}

/**
 * @brief Compute fingerprint for a set of values.
 *
 * Hashes all non-null values and combines them into a single fingerprint.
 *
 * @param state Cuckoo index state.
 * @param values Array of Datum values to hash.
 * @param isnull Array indicating which values are NULL.
 * @return Computed fingerprint value.
 */
uint32 computeFingerprint(CuckooState *state, Datum *values, bool *isnull) {
  uint32 hash = 0;

  for (int i = 0; i < state->nColumns; i++) {
    if (isnull[i])
      continue;

    uint32 colHash = DatumGetInt32(
        FunctionCall1Coll(&state->hashFn[i], state->collations[i], values[i]));

    /* Combine hashes using mixing function */
    hash ^= colHash;
    hash *= 0x5bd1e995; /* MurmurHash2 mixing constant */
    hash ^= hash >> 15;
  }

  /* Extract fingerprint bits, ensuring non-zero */
  uint32 fingerprint = hash & state->tagMask;
  if (fingerprint == 0)
    fingerprint = 1;

  return fingerprint;
}

/**
 * @brief Create a cuckoo tuple from values.
 *
 * @param state Cuckoo index state.
 * @param iptr Pointer to the heap tuple.
 * @param values Array of indexed values.
 * @param isnull Array indicating which values are NULL.
 * @return Pointer to newly allocated CuckooTuple.
 */
CuckooTuple *CuckooFormTuple(CuckooState *state, ItemPointer iptr,
                             Datum *values, bool *isnull) {
  CuckooTuple *tuple = (CuckooTuple *)palloc0(state->sizeOfCuckooTuple);

  tuple->heapPtr = *iptr;
  tuple->fingerprint = computeFingerprint(state, values, isnull);

  return tuple;
}

/**
 * @brief Add a tuple to a cuckoo index page.
 *
 * @param state Cuckoo index state.
 * @param page Page to add tuple to.
 * @param tuple Tuple to add.
 * @return true if tuple was added, false if page is full.
 */
bool CuckooPageAddItem(CuckooState *state, Page page, CuckooTuple *tuple) {
  CuckooTuple *itup;
  CuckooPageOpaque opaque;
  Pointer ptr;

  /* Verify page is valid */
  Assert(!PageIsNew(page) && !CuckooPageIsDeleted(page));

  /* Check if there's enough free space */
  if (CuckooPageGetFreeSpace(state, page) < state->sizeOfCuckooTuple)
    return false;

  /* Copy tuple to end of page */
  opaque = CuckooPageGetOpaque(page);
  itup = CuckooPageGetTuple(state, page, opaque->maxoff + 1);
  memcpy((Pointer)itup, (Pointer)tuple, state->sizeOfCuckooTuple);

  /* Update maxoff and pd_lower */
  opaque->maxoff++;
  ptr = (Pointer)CuckooPageGetTuple(state, page, opaque->maxoff + 1);
  ((PageHeader)page)->pd_lower = ptr - page;

  Assert(((PageHeader)page)->pd_lower <= ((PageHeader)page)->pd_upper);

  return true;
}

/**
 * @brief Allocate a new buffer for the index.
 *
 * First tries to get a page from the free space map, then extends
 * the relation if necessary.
 *
 * @param index The index relation.
 * @return Buffer for the new page (pinned and locked).
 */
Buffer CuckooNewBuffer(Relation index) {
  Buffer buffer;

  /* First try to get a page from FSM */
  for (;;) {
    BlockNumber blkno = GetFreeIndexPage(index);

    if (blkno == InvalidBlockNumber)
      break;

    buffer = ReadBuffer(index, blkno);

    if (ConditionalLockBuffer(buffer)) {
      Page page = BufferGetPage(buffer);

      if (PageIsNew(page))
        return buffer;

      if (CuckooPageIsDeleted(page))
        return buffer;

      LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    }

    ReleaseBuffer(buffer);
  }

  /* Must extend the file */
  buffer = ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL, EB_LOCK_FIRST);

  return buffer;
}

/**
 * @brief Initialize a cuckoo index page.
 *
 * @param page Page to initialize.
 * @param flags Page flags to set.
 */
void CuckooInitPage(Page page, uint16 flags) {
  CuckooPageOpaque opaque;

  PageInit(page, BLCKSZ, sizeof(CuckooPageOpaqueData));

  opaque = CuckooPageGetOpaque(page);
  opaque->flags = flags;
  opaque->maxoff = 0;
  opaque->cuckoo_page_id = CUCKOO_PAGE_ID;
}

/**
 * @brief Fill in metapage for cuckoo index.
 *
 * @param index The index relation.
 * @param metaPage Page to fill with metadata.
 */
void CuckooFillMetapage(Relation index, Page metaPage) {
  CuckooOptions *opts;
  CuckooMetaPageData *metadata;

  /* Get options from reloptions or use defaults */
  opts = (CuckooOptions *)index->rd_options;
  if (!opts)
    opts = makeDefaultCuckooOptions();

  /* Initialize metapage */
  CuckooInitPage(metaPage, CUCKOO_META);
  metadata = CuckooPageGetMeta(metaPage);
  memset(metadata, 0, sizeof(CuckooMetaPageData));
  metadata->magicNumber = CUCKOO_MAGIC_NUMBER;
  metadata->opts = *opts;
  ((PageHeader)metaPage)->pd_lower += sizeof(CuckooMetaPageData);

  Assert(((PageHeader)metaPage)->pd_lower <= ((PageHeader)metaPage)->pd_upper);
}

/**
 * @brief Initialize metapage for cuckoo index.
 *
 * @param index The index relation.
 * @param forknum Fork to initialize (MAIN_FORKNUM or INIT_FORKNUM).
 */
void CuckooInitMetapage(Relation index, ForkNumber forknum) {
  Buffer metaBuffer;
  Page metaPage;
  GenericXLogState *state;

  /* Allocate new page - should be block 0 */
  metaBuffer = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
  LockBuffer(metaBuffer, BUFFER_LOCK_EXCLUSIVE);
  Assert(BufferGetBlockNumber(metaBuffer) == CUCKOO_METAPAGE_BLKNO);

  /* Initialize metapage contents */
  state = GenericXLogStart(index);
  metaPage =
      GenericXLogRegisterBuffer(state, metaBuffer, GENERIC_XLOG_FULL_IMAGE);
  CuckooFillMetapage(index, metaPage);
  GenericXLogFinish(state);

  UnlockReleaseBuffer(metaBuffer);
}

/**
 * @brief Parse reloptions for cuckoo index.
 *
 * @param reloptions Datum containing reloptions.
 * @param validate Whether to validate options.
 * @return Pointer to CuckooOptions structure.
 */
bytea *ckoptions(Datum reloptions, bool validate) {
  CuckooOptions *rdopts;

  rdopts = (CuckooOptions *)build_reloptions(
      reloptions, validate, ck_relopt_kind, sizeof(CuckooOptions),
      ck_relopt_tab, lengthof(ck_relopt_tab));

  return (bytea *)rdopts;
}
