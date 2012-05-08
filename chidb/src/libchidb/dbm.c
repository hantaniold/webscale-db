#include <chidb.h>
#include <chidbInt.h>
#include "btree.h"
#include "dbm.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

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

int operation_eq(dbm *input_dbm, chidb_stmt stmt) {
	if (input_dbm->registers[stmt.P1].type == input_dbm->registers[stmt.P3].type) {
		switch (input_dbm->registers[stmt.P1].type) {
			case INTEGER:
				if (input_dbm->registers[stmt.P1].data.int_val == input_dbm->registers[stmt.P3].data.int_val) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[stmt.P1].data.str_val, input_dbm->registers[stmt.P3].data.str_val) == 0){
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				if (input_dbm->registers[stmt.P1].data_len == input_dbm->registers[stmt.P3].data_len) {
					if (memcmp(input_dbm->registers[stmt.P1].data.bin_val, input_dbm->registers[stmt.P3].data.bin_val, input_dbm->registers[stmt.P1].data_len) == 0) {
						input_dbm->program_counter = stmt.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				} else {
					return DBM_DATA_REGISTER_LENGTH_MISMATCH;
				} 
				break;
			case NL:
				input_dbm->program_counter = stmt.P2;
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_ne(dbm *input_dbm, chidb_stmt stmt) {
	if (input_dbm->registers[stmt.P1].type == input_dbm->registers[stmt.P3].type) {
		switch (input_dbm->registers[stmt.P1].type) {
			case INTEGER:
				if (input_dbm->registers[stmt.P1].data.int_val != input_dbm->registers[stmt.P3].data.int_val) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[stmt.P1].data.str_val, input_dbm->registers[stmt.P3].data.str_val) != 0){
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				if ((memcmp(input_dbm->registers[stmt.P1].data.bin_val, input_dbm->registers[stmt.P3].data.bin_val, input_dbm->registers[stmt.P1].data_len) != 0) || (input_dbm->registers[stmt.P1].data_len != input_dbm->registers[stmt.P3].data_len)) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = stmt.P2;
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_lt(dbm *input_dbm, chidb_stmt stmt) {
	if (input_dbm->registers[stmt.P1].type == input_dbm->registers[stmt.P3].type) {
		switch (input_dbm->registers[stmt.P1].type) {
			case INTEGER:
				if (input_dbm->registers[stmt.P1].data.int_val < input_dbm->registers[stmt.P3].data.int_val) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[stmt.P1].data.str_val, input_dbm->registers[stmt.P3].data.str_val) < 0){
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[stmt.P1].data.bin_val, input_dbm->registers[stmt.P3].data.bin_val, input_dbm->registers[stmt.P1].data_len) < 0) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = stmt.P2;
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_le(dbm *input_dbm, chidb_stmt stmt) {
	if (input_dbm->registers[stmt.P1].type == input_dbm->registers[stmt.P3].type) {
		switch (input_dbm->registers[stmt.P1].type) {
			case INTEGER:
				if (input_dbm->registers[stmt.P1].data.int_val <= input_dbm->registers[stmt.P3].data.int_val) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[stmt.P1].data.str_val, input_dbm->registers[stmt.P3].data.str_val) <= 0){
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[stmt.P1].data.bin_val, input_dbm->registers[stmt.P3].data.bin_val, input_dbm->registers[stmt.P1].data_len) <= 0) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = stmt.P2;
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_gt(dbm *input_dbm, chidb_stmt stmt) {
	if (input_dbm->registers[stmt.P1].type == input_dbm->registers[stmt.P3].type) {
		switch (input_dbm->registers[stmt.P1].type) {
			case INTEGER:
				if (input_dbm->registers[stmt.P1].data.int_val > input_dbm->registers[stmt.P3].data.int_val) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[stmt.P1].data.str_val, input_dbm->registers[stmt.P3].data.str_val) > 0){
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[stmt.P1].data.bin_val, input_dbm->registers[stmt.P3].data.bin_val, input_dbm->registers[stmt.P1].data_len) > 0) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = stmt.P2;
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_ge(dbm *input_dbm, chidb_stmt stmt) {
	if (input_dbm->registers[stmt.P1].type == input_dbm->registers[stmt.P3].type) {
		switch (input_dbm->registers[stmt.P1].type) {
			case INTEGER:
				if (input_dbm->registers[stmt.P1].data.int_val >= input_dbm->registers[stmt.P3].data.int_val) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[stmt.P1].data.str_val, input_dbm->registers[stmt.P3].data.str_val) >= 0){
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[stmt.P1].data.bin_val, input_dbm->registers[stmt.P3].data.bin_val, input_dbm->registers[stmt.P1].data_len) >= 0) {
					input_dbm->program_counter = stmt.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = stmt.P2;
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}


//TODO: BETTER ERROR HANDLING
int tick_dbm(dbm *input_dbm, chidb_stmt stmt) {
	switch (stmt.instruction) {
		case DBM_OPENWRITE:
		case DBM_OPENREAD: {
			uint32_t page_num = (input_dbm->registers[stmt.P2]).data.int_val;
			input_dbm->cursors[stmt.P1].node = (BTreeNode *)calloc(1, sizeof(BTreeNode));
			if (chidb_Btree_getNodeByPage(input_dbm->db->bt, page_num, &(input_dbm->cursors[stmt.P1].node)) == CHIDB_OK) {
				input_dbm->program_counter += 1;
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
				input_dbm->program_counter += 1;
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
			return operation_eq(input_dbm, stmt);
			break;
		case DBM_NE:
			return operation_ne(input_dbm, stmt);
			break;
		case DBM_LT:
			return operation_lt(input_dbm, stmt);
			break;
		case DBM_LE:
			return operation_le(input_dbm, stmt);
			break;
		case DBM_GT:
			return operation_gt(input_dbm, stmt);
			break;
		case DBM_GE:
			return operation_ge(input_dbm, stmt);
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
