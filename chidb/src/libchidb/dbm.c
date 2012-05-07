#include "chidb.h"
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

//THIS RESETS A DBM TO ITS INITIAL STATE
int reset_dbm(dbm *input_dbm) {
	input_dbm->program_counter = 0;
	input_dbm->allocated_registers = 0;
	input_dbm->allocated_cursors = 0;
}

//ALLOCATES A NEW REGISTER IN THE DBM AND RETURNS ITS INTEGER INDEX
int init_cursor(dbm *input_dbm) {
	input_dbm->allocated_cursors += 1;
}





//ALLOCATED A NEW REGISTER IN THE DBM AND RETURNS ITS INTEGER INDEX
int init_register(dbm *input_dbm, dbm_register_type type) {
	input_dbm->allocated_registers += 1;
	input_dbm->registers[input_dbm->allocated_registers].type = type;
}



//TODO: ERROR HANDLING
int tick_dbm(dbm *input_dbm, chidb_stmt stmt) {
	switch (stmt.instruction) {
		case DBM_OPENREAD:
			//npage_t page_num = input_dbm->registers[stmt.P2].data.int_val;
			//input_dbm->cursors[input_dbm->allocated_cursors].node = (BTreeNode *)calloc(1, sizeof(BTreeNode));
			//return chidb_Btree_getNodeByPage(input_dbm->db->bt, page_num, &(input_dbm->cursors[input_dbm->allocated_cursors].node));
		break;
		case DBM_OPENWRITE:
		break;
		case DBM_CLOSE:
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
} //END OF tick_dbm

//EOF
