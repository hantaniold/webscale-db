#include <chidb.h>
#include <chidbInt.h>
#include "btree.h"
#include "dbm.h"

#include <stdlib.h>
#include <stdio.h>

/*
*  This file implements the dbm.
*/

//THIS WILL CREATE A NEW DBM STRUCT
int init_dbm(dbm *input_dbm, chidb *db) {
	input_dbm = (dbm *)calloc(1, sizeof(dbm));
	input_dbm->db = db;
	if (input_dbm == NULL) {
		return CHIDB_ENOMEM;
	} else {
		return reset_dbm(input_dbm);
	}
}

//TODO: THIS NEEDS TO CLEAN Up ALL ALLOCATED CURSORS
//THIS RESETS A DBM TO ITS INITIAL STATE
int reset_dbm(dbm *input_dbm) {
	input_dbm->program_counter = 0;
	return CHIDB_OK;
}

//TODO: ERROR HANDLING
int tick_dbm(dbm *input_dbm, chidb_stmt stmt) {
	switch (stmt.instruction) {
		case DBM_OPENWRITE:
		case DBM_OPENREAD: {
			uint32_t page_num = (input_dbm->registers[stmt.P2]).data.int_val;
			input_dbm->cursors[stmt.P1].node = (BTreeNode *)calloc(1, sizeof(BTreeNode));
			if (chidb_Btree_getNodeByPage(input_dbm->db->bt, page_num, &(input_dbm->cursors[stmt.P1].node)) == CHIDB_OK) {
				return DBM_OK;
			} else {
				return DBM_OPENRW_ERROR;
			}
		}
		case DBM_CLOSE: 
			if (chidb_Btree_freeMemNode(input_dbm->db->bt, input_dbm->cursors[stmt.P1].node) == CHIDB_OK) {
				free(input_dbm->cursors[stmt.P1].curr_cell);
				free(input_dbm->cursors[stmt.P1].prev_cell);
				free(input_dbm->cursors[stmt.P1].next_cell);
				return DBM_OK;
			} else {
				return DBM_GENERAL_ERROR;
			}
			break;
		case DBM_REWIND:
			break;
		case DBM_NEXT:
			break;
		case DBM_PREV:
			break;
		case DBM_SEEK:
			break;
		case DBM_SEEKGT:
			break;
		case DBM_SEEKGE:
			break;
		case DBM_COLUMN:
			break;
		case DBM_KEY:
			break;
		case DBM_INTEGER:
			break;
		case DBM_STRING:
			break;
		case DBM_NULL:
			break;
		case DBM_RESULTROW:
			break;
		case DBM_MAKERECORD:
			break;
		case DBM_INSERT:
			break;
		case DBM_EQ:
			break;
		case DBM_NE:
			break;
		case DBM_LT:
			break;
		case DBM_LE:
			break;
		case DBM_GT:
			break;
		case DBM_GE:
			break;
		case DBM_IDXGT:
			break;
		case DBM_IDXLT:
			break;
		case DBM_IDXLE:
			break;
		case DBM_IDXKEY:
			break;
		case DBM_IDXINSERT:
			break;
		case DBM_CREATETABLE:
			break;
		case DBM_CREATEINDEX:
			break;
		case DBM_SCOPY:
			break;
		case DBM_HALT:
			break; 
	}
	return DBM_INVALID_INSTRUCTION;
} //END OF tick_dbm

//EOF
