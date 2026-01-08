/**
 * @file ckcost.cpp
 * @brief Cost estimation for cuckoo index scans.
 *
 * This file provides cost estimation used by the query planner
 * to decide whether to use the cuckoo index.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#include "cuckoo.h"

extern "C" {
#include "storage/bufmgr.h"
#include "utils/selfuncs.h"
}

/**
 * @brief Calculate theoretical false positive rate for cuckoo filter.
 *
 * The false positive rate for a cuckoo filter is approximately:
 * FPR = (2 * tags_per_bucket) / 2^bits_per_tag
 *
 * @param bitsPerTag Number of bits per fingerprint tag.
 * @param tagsPerBucket Number of tags per bucket.
 * @return Estimated false positive rate (0.0 to 1.0).
 */
static double calculateFalsePositiveRate(int bitsPerTag, int tagsPerBucket) {
  /*
   * Cuckoo filter FPR formula: (2 * b) / 2^f
   * where b = tags per bucket, f = fingerprint bits
   *
   * For default values (12 bits, 4 tags): (2 * 4) / 4096 = 0.00195 (~0.2%)
   */
  double denominator = (double)(1ULL << bitsPerTag);
  double fpr = (2.0 * tagsPerBucket) / denominator;

  /* Clamp to reasonable bounds */
  if (fpr > 1.0)
    fpr = 1.0;
  if (fpr < 0.0001)
    fpr = 0.0001;

  return fpr;
}

/**
 * @brief Estimate the cost of a cuckoo index scan.
 *
 * Cuckoo indexes must scan all index pages (like bloom indexes),
 * but have very fast per-tuple comparison (just fingerprint equality).
 * The selectivity is based on the theoretical false positive rate.
 *
 * @param root Planner information.
 * @param path Index path being considered.
 * @param loop_count Number of times the scan will be executed.
 * @param indexStartupCost Output: startup cost.
 * @param indexTotalCost Output: total cost.
 * @param indexSelectivity Output: selectivity estimate.
 * @param indexCorrelation Output: correlation (0 for cuckoo).
 * @param indexPages Output: number of index pages.
 */
void ckcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                    Cost *indexStartupCost, Cost *indexTotalCost,
                    Selectivity *indexSelectivity, double *indexCorrelation,
                    double *indexPages) {
  IndexOptInfo *index = path->indexinfo;
  Relation indexRel;
  CuckooOptions *opts;
  GenericCosts costs = {0};
  double falsePositiveRate;

  /*
   * Read the index options to get bits_per_tag and tags_per_bucket
   * for accurate false positive rate estimation.
   */
  indexRel = index_open(index->indexoid, AccessShareLock);

  if (indexRel->rd_options) {
    opts = (CuckooOptions *)indexRel->rd_options;
  } else {
    /* Use defaults if no options set */
    static CuckooOptions defaultOpts = {
        0, DEFAULT_BITS_PER_TAG, DEFAULT_TAGS_PER_BUCKET, DEFAULT_MAX_KICKS};
    opts = &defaultOpts;
  }

  falsePositiveRate =
      calculateFalsePositiveRate(opts->bitsPerTag, opts->tagsPerBucket);

  index_close(indexRel, AccessShareLock);

  /*
   * Cuckoo indexes, like bloom indexes, must visit all index tuples.
   * However, the per-tuple comparison is very fast (single integer compare).
   */
  costs.numIndexTuples = index->tuples;

  /* Use generic estimate for the basics */
  genericcostestimate(root, path, loop_count, &costs);

  /*
   * Adjust selectivity based on false positive rate.
   * The actual selectivity is: true_selectivity + (1 - true_selectivity) * FPR
   * For simplicity, we use: max(generic_selectivity, FPR)
   */
  if (costs.indexSelectivity < falsePositiveRate) {
    costs.indexSelectivity = falsePositiveRate;
  }

  *indexStartupCost = costs.indexStartupCost;
  *indexTotalCost = costs.indexTotalCost;
  *indexSelectivity = costs.indexSelectivity;
  *indexCorrelation = 0.0; /* Cuckoo index has no correlation */
  *indexPages = costs.numIndexPages;
}
