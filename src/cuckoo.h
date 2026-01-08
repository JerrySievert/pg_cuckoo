/**
 * @file cuckoo.h
 * @brief Header for cuckoo filter index access method.
 *
 * This file contains all data structures and function declarations for
 * the PostgreSQL cuckoo filter index extension.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#ifndef CUCKOO_H_
#define CUCKOO_H_

extern "C" {
#include "postgres.h"

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"
}

/*
 * Support procedure numbers for cuckoo opclass
 */
#define CUCKOO_HASH_PROC 1
#define CUCKOO_OPTIONS_PROC 2
#define CUCKOO_NPROC 2

/*
 * Scan strategies - cuckoo only supports equality
 */
#define CUCKOO_EQUAL_STRATEGY 1
#define CUCKOO_NSTRATEGIES 1

/*
 * Cuckoo filter configuration defaults
 */
#define DEFAULT_BITS_PER_TAG 12
#define MIN_BITS_PER_TAG 4
#define MAX_BITS_PER_TAG 32

#define DEFAULT_TAGS_PER_BUCKET 4
#define MIN_TAGS_PER_BUCKET 2
#define MAX_TAGS_PER_BUCKET 8

#define DEFAULT_MAX_KICKS 500
#define MIN_MAX_KICKS 50
#define MAX_MAX_KICKS 2000

/**
 * @brief Opaque data at end of each cuckoo index page.
 */
typedef struct CuckooPageOpaqueData {
  OffsetNumber maxoff;   /**< Number of index tuples on page */
  uint16 flags;          /**< Page flags (see below) */
  uint16 unused;         /**< Alignment padding */
  uint16 cuckoo_page_id; /**< Page type identifier */
} CuckooPageOpaqueData;

typedef CuckooPageOpaqueData *CuckooPageOpaque;

/*
 * Cuckoo page flags
 */
#define CUCKOO_META (1 << 0)
#define CUCKOO_DELETED (2 << 0)

/*
 * Page ID for identification by pg_filedump and similar utilities
 */
#define CUCKOO_PAGE_ID 0xFF84

/*
 * Macros for accessing cuckoo page structures
 */
#define CuckooPageGetOpaque(page)                                              \
  ((CuckooPageOpaque)PageGetSpecialPointer(page))
#define CuckooPageGetMaxOffset(page) (CuckooPageGetOpaque(page)->maxoff)
#define CuckooPageIsMeta(page)                                                 \
  ((CuckooPageGetOpaque(page)->flags & CUCKOO_META) != 0)
#define CuckooPageIsDeleted(page)                                              \
  ((CuckooPageGetOpaque(page)->flags & CUCKOO_DELETED) != 0)
#define CuckooPageSetDeleted(page)                                             \
  (CuckooPageGetOpaque(page)->flags |= CUCKOO_DELETED)
#define CuckooPageSetNonDeleted(page)                                          \
  (CuckooPageGetOpaque(page)->flags &= ~CUCKOO_DELETED)
#define CuckooPageGetData(page) ((CuckooTuple *)PageGetContents(page))

/*
 * Reserved page numbers
 */
#define CUCKOO_METAPAGE_BLKNO 0
#define CUCKOO_HEAD_BLKNO 1

/**
 * @brief Index tuple stored in cuckoo index.
 *
 * Each tuple contains a heap pointer and a fingerprint (tag) that
 * represents the indexed value(s).
 */
typedef struct CuckooTuple {
  ItemPointerData heapPtr; /**< Pointer to heap tuple */
  uint32 fingerprint;      /**< Cuckoo filter fingerprint (tag) */
} CuckooTuple;

#define CUCKOOTUPLEHDRSZ offsetof(CuckooTuple, fingerprint)

/**
 * @brief Options for cuckoo index, stored in metapage.
 */
typedef struct CuckooOptions {
  int32 vl_len_;     /**< varlena header (do not touch directly!) */
  int bitsPerTag;    /**< Bits per fingerprint tag */
  int tagsPerBucket; /**< Number of tags per bucket (2, 4, or 8) */
  int maxKicks;      /**< Maximum number of relocations during insert */
} CuckooOptions;

/**
 * @brief Array of free block numbers for metapage.
 *
 * Sized to fill remaining space in metapage.
 */
typedef BlockNumber
    CuckooFreeBlockArray[MAXALIGN_DOWN(BLCKSZ - SizeOfPageHeaderData -
                                       MAXALIGN(sizeof(CuckooPageOpaqueData)) -
                                       MAXALIGN(sizeof(uint16) * 2 +
                                                sizeof(uint32) +
                                                sizeof(CuckooOptions))) /
                         sizeof(BlockNumber)];

/**
 * @brief Metadata stored on metapage (block 0).
 */
typedef struct CuckooMetaPageData {
  uint32 magicNumber;               /**< Magic number for validation */
  uint16 nStart;                    /**< Start of notFullPage ring buffer */
  uint16 nEnd;                      /**< End of notFullPage ring buffer */
  CuckooOptions opts;               /**< Index options */
  CuckooFreeBlockArray notFullPage; /**< Pages with free space */
} CuckooMetaPageData;

#define CUCKOO_MAGIC_NUMBER 0xC0C000CF

#define CuckooMetaBlockN (sizeof(CuckooFreeBlockArray) / sizeof(BlockNumber))

#define CuckooPageGetMeta(page) ((CuckooMetaPageData *)PageGetContents(page))

/**
 * @brief Runtime state for cuckoo index operations.
 */
typedef struct CuckooState {
  FmgrInfo hashFn[INDEX_MAX_KEYS]; /**< Hash functions for each column */
  Oid collations[INDEX_MAX_KEYS];  /**< Collations for each column */
  CuckooOptions opts;              /**< Copy of index options */
  int32 nColumns;                  /**< Number of indexed columns */
  Size sizeOfCuckooTuple;          /**< Precomputed tuple size */
  uint32 tagMask;                  /**< Mask for extracting tag bits */
  int tagsPerBucket;               /**< Tags per bucket from options */
  int maxKicks;                    /**< Max kicks from options */
} CuckooState;

/*
 * Macro to get tuple at given offset
 */
#define CuckooPageGetTuple(state, page, offset)                                \
  ((CuckooTuple *)(PageGetContents(page) +                                     \
                   (state)->sizeOfCuckooTuple * ((offset) - 1)))

#define CuckooPageGetNextTuple(state, tuple)                                   \
  ((CuckooTuple *)((Pointer)(tuple) + (state)->sizeOfCuckooTuple))

#define CuckooPageGetFreeSpace(state, page)                                    \
  (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -                                   \
   CuckooPageGetMaxOffset(page) * (state)->sizeOfCuckooTuple -                 \
   MAXALIGN(sizeof(CuckooPageOpaqueData)))

/**
 * @brief Opaque data for cuckoo index scan.
 */
typedef struct CuckooScanOpaqueData {
  uint32 fingerprint;    /**< Search fingerprint */
  bool fingerprintValid; /**< Whether fingerprint has been computed */
  CuckooState state;     /**< Index state */
} CuckooScanOpaqueData;

typedef CuckooScanOpaqueData *CuckooScanOpaque;

/*
 * Function declarations - ckutils.cpp
 */
extern "C" {

extern void initCuckooState(CuckooState *state, Relation index);
extern void CuckooFillMetapage(Relation index, Page metaPage);
extern void CuckooInitMetapage(Relation index, ForkNumber forknum);
extern void CuckooInitPage(Page page, uint16 flags);
extern Buffer CuckooNewBuffer(Relation index);
extern uint32 computeFingerprint(CuckooState *state, Datum *values,
                                 bool *isnull);
extern CuckooTuple *CuckooFormTuple(CuckooState *state, ItemPointer iptr,
                                    Datum *values, bool *isnull);
extern bool CuckooPageAddItem(CuckooState *state, Page page,
                              CuckooTuple *tuple);

/*
 * Function declarations - ckvalidate.cpp
 */
extern bool ckvalidate(Oid opclassoid);

/*
 * Index access method interface functions
 */

/* ckinsert.cpp */
extern bool ckinsert(Relation index, Datum *values, bool *isnull,
                     ItemPointer ht_ctid, Relation heapRel,
                     IndexUniqueCheck checkUnique, bool indexUnchanged,
                     struct IndexInfo *indexInfo);
extern IndexBuildResult *ckbuild(Relation heap, Relation index,
                                 struct IndexInfo *indexInfo);
extern void ckbuildempty(Relation index);

/* ckscan.cpp */
extern IndexScanDesc ckbeginscan(Relation r, int nkeys, int norderbys);
extern int64 ckgetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void ckrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
                     ScanKey orderbys, int norderbys);
extern void ckendscan(IndexScanDesc scan);

/* ckvacuum.cpp */
extern IndexBulkDeleteResult *ckbulkdelete(IndexVacuumInfo *info,
                                           IndexBulkDeleteResult *stats,
                                           IndexBulkDeleteCallback callback,
                                           void *callback_state);
extern IndexBulkDeleteResult *ckvacuumcleanup(IndexVacuumInfo *info,
                                              IndexBulkDeleteResult *stats);

/* ckcost.cpp */
extern void ckcostestimate(PlannerInfo *root, IndexPath *path,
                           double loop_count, Cost *indexStartupCost,
                           Cost *indexTotalCost, Selectivity *indexSelectivity,
                           double *indexCorrelation, double *indexPages);

/* ckutils.cpp - reloptions */
extern bytea *ckoptions(Datum reloptions, bool validate);

} /* extern "C" */

#endif /* CUCKOO_H_ */
