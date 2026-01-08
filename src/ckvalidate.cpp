/**
 * @file ckvalidate.cpp
 * @brief Opclass validator for cuckoo index.
 *
 * This file validates that operator classes for cuckoo indexes
 * are properly defined with the required operators and functions.
 *
 * Copyright (c) 2025
 * SPDX-License-Identifier: PostgreSQL
 */
#include "cuckoo.h"

extern "C" {
#include "access/amvalidate.h"
#include "access/htup_details.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
}

/**
 * @brief Get the name of an operator family.
 *
 * @param opfamilyoid OID of the operator family.
 * @return Name of the operator family (palloc'd string).
 */
static char *ck_get_opfamily_name(Oid opfamilyoid) {
  HeapTuple tp;
  Form_pg_opfamily opfform;
  char *result;

  tp = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamilyoid));
  if (!HeapTupleIsValid(tp))
    return pstrdup("(unknown)");

  opfform = (Form_pg_opfamily)GETSTRUCT(tp);
  result = pstrdup(NameStr(opfform->opfname));
  ReleaseSysCache(tp);

  return result;
}

/**
 * @brief Validate a cuckoo opclass.
 *
 * Checks that the opclass has all required operators and support functions
 * with correct signatures.
 *
 * @param opclassoid OID of the operator class to validate.
 * @return true if the opclass is valid.
 */
bool ckvalidate(Oid opclassoid) {
  bool result = true;
  HeapTuple classtup;
  Form_pg_opclass classform;
  Oid opfamilyoid;
  Oid opcintype;
  Oid opckeytype;
  char *opclassname;
  char *opfamilyname;
  CatCList *proclist;
  CatCList *oprlist;
  List *grouplist;
  OpFamilyOpFuncGroup *opclassgroup;
  int i;
  ListCell *lc;

  /* Fetch opclass information */
  classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
  if (!HeapTupleIsValid(classtup))
    elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
  classform = (Form_pg_opclass)GETSTRUCT(classtup);

  opfamilyoid = classform->opcfamily;
  opcintype = classform->opcintype;
  opckeytype = classform->opckeytype;
  if (!OidIsValid(opckeytype))
    opckeytype = opcintype;
  opclassname = NameStr(classform->opcname);

  /* Fetch opfamily name */
  opfamilyname = ck_get_opfamily_name(opfamilyoid);

  /* Fetch all operators and support functions */
  oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
  proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

  /* Check individual support functions */
  for (i = 0; i < proclist->n_members; i++) {
    HeapTuple proctup = &proclist->members[i]->tuple;
    Form_pg_amproc procform = (Form_pg_amproc)GETSTRUCT(proctup);
    bool ok;

    /*
     * All cuckoo support functions should have matching left/right types.
     */
    if (procform->amproclefttype != procform->amprocrighttype) {
      ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("cuckoo opfamily %s contains support procedure %s "
                            "with cross-type registration",
                            opfamilyname, format_procedure(procform->amproc))));
      result = false;
    }

    /* Only check signatures for procedures in our opclass */
    if (procform->amproclefttype != opcintype)
      continue;

    /* Check procedure numbers and signatures */
    switch (procform->amprocnum) {
    case CUCKOO_HASH_PROC:
      ok = check_amproc_signature(procform->amproc, INT4OID, false, 1, 1,
                                  opckeytype);
      break;
    case CUCKOO_OPTIONS_PROC:
      ok = check_amoptsproc_signature(procform->amproc);
      break;
    default:
      ereport(INFO,
              (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
               errmsg("cuckoo opfamily %s contains function %s with invalid "
                      "support number %d",
                      opfamilyname, format_procedure(procform->amproc),
                      procform->amprocnum)));
      result = false;
      continue;
    }

    if (!ok) {
      ereport(INFO,
              (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
               errmsg("cuckoo opfamily %s contains function %s with wrong "
                      "signature for support number %d",
                      opfamilyname, format_procedure(procform->amproc),
                      procform->amprocnum)));
      result = false;
    }
  }

  /* Check individual operators */
  for (i = 0; i < oprlist->n_members; i++) {
    HeapTuple oprtup = &oprlist->members[i]->tuple;
    Form_pg_amop oprform = (Form_pg_amop)GETSTRUCT(oprtup);

    /* Check it's a valid strategy for cuckoo */
    if (oprform->amopstrategy < 1 ||
        oprform->amopstrategy > CUCKOO_NSTRATEGIES) {
      ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("cuckoo opfamily %s contains operator %s with "
                            "invalid strategy number %d",
                            opfamilyname, format_operator(oprform->amopopr),
                            oprform->amopstrategy)));
      result = false;
    }

    /* Cuckoo doesn't support ORDER BY operators */
    if (oprform->amoppurpose != AMOP_SEARCH ||
        OidIsValid(oprform->amopsortfamily)) {
      ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("cuckoo opfamily %s contains invalid ORDER BY "
                            "specification for operator %s",
                            opfamilyname, format_operator(oprform->amopopr))));
      result = false;
    }

    /* Check operator signature */
    if (!check_amop_signature(oprform->amopopr, BOOLOID, oprform->amoplefttype,
                              oprform->amoprighttype)) {
      ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("cuckoo opfamily %s contains operator %s with "
                            "wrong signature",
                            opfamilyname, format_operator(oprform->amopopr))));
      result = false;
    }
  }

  /* Check for inconsistent groups of operators/functions */
  grouplist = identify_opfamily_groups(oprlist, proclist);
  opclassgroup = NULL;
  foreach (lc, grouplist) {
    OpFamilyOpFuncGroup *thisgroup = (OpFamilyOpFuncGroup *)lfirst(lc);

    if (thisgroup->lefttype == opcintype && thisgroup->righttype == opcintype)
      opclassgroup = thisgroup;
  }

  /* Check that the opclass has all required support functions */
  for (i = 1; i <= CUCKOO_NPROC; i++) {
    if (opclassgroup && (opclassgroup->functionset & (((uint64)1) << i)) != 0)
      continue;
    if (i == CUCKOO_OPTIONS_PROC)
      continue; /* optional */
    ereport(INFO, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                   errmsg("cuckoo opclass %s is missing support function %d",
                          opclassname, i)));
    result = false;
  }

  ReleaseCatCacheList(proclist);
  ReleaseCatCacheList(oprlist);
  ReleaseSysCache(classtup);

  return result;
}
