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
	for (int i = 0; i < DBM_MAX_REGISTERS; ++i) {
		input_dbm->registers[i].touched = 0;
		input_dbm->registers[i].type = NL;
	}
	for (int i = 0; i < DBM_MAX_CURSORS; ++i) {
		input_dbm->cursors[i].touched = 0;
	}
	if (input_dbm == NULL) {
		return CHIDB_ENOMEM;
	} else {
		return reset_dbm(input_dbm);
	}
}

int operation_cursor_close(dbm *input_dbm, uint32_t cursor_id) {
	dbm_cursor mycursor = input_dbm->cursors[cursor_id];
	if (mycursor.touched == 1) {
			if (chidb_Btree_freeMemNode(input_dbm->db->bt, input_dbm->cursors[cursor_id].node) == CHIDB_OK) {
				free(mycursor.node);
				free(mycursor.curr_cell);
				free(mycursor.prev_cell);
				free(mycursor.next_cell);
			} else {
				return DBM_MEMORY_FREE_ERROR;
			}
	}
	mycursor.touched = 0;
	return DBM_OK;
}

//TODO: THIS NEEDS TO CLEAN Up ALL ALLOCATED CURSORS
//THIS RESETS A DBM TO ITS INITIAL STATE
int reset_dbm(dbm *input_dbm) {
	input_dbm->program_counter = 0;
	input_dbm->tick_result = DBM_OK;
	input_dbm->readwritestate = 0;
	for (int i = 0; i < DBM_MAX_REGISTERS; ++i) {
		dbm_register myreg = input_dbm->registers[i]; 
		if (myreg.touched == 1) {
			if (myreg.type == STRING) {
				free(myreg.data.str_val);
			}
			if (myreg.type == BINARY) {
				free(myreg.data.bin_val);
			}
		}
		myreg.touched = 0;
		myreg.type = NL;
	}
	for (uint32_t i = 0; i < DBM_MAX_CURSORS; ++i) {
		dbm_cursor mycursor = input_dbm->cursors[i];
		if (mycursor.touched == 1) {
			operation_cursor_close(input_dbm, i);
		}
		mycursor.touched = 0;
	}
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

int operation_key(dbm *input_dbm, chidb_stmt stmt) {
	input_dbm->registers[stmt.P2].type = INTEGER;
	input_dbm->registers[stmt.P2].data.int_val = (uint32_t)input_dbm->cursors[stmt.P1].curr_cell->key;
	input_dbm->registers[stmt.P2].touched = 1;
	return DBM_OK;
}

int operation_integer(dbm *input_dbm, chidb_stmt stmt) {
	input_dbm->registers[stmt.P2].type = INTEGER;
	input_dbm->registers[stmt.P2].data.int_val = stmt.P1;
	input_dbm->registers[stmt.P2].touched = 1;
	return DBM_OK;
}

int operation_string(dbm *input_dbm, chidb_stmt stmt) {
	input_dbm->registers[stmt.P2].type = STRING;
	input_dbm->registers[stmt.P2].data.str_val = (char *)malloc(sizeof(char) * stmt.P1);
	input_dbm->registers[stmt.P2].data_len = (size_t)stmt.P1;
	input_dbm->registers[stmt.P2].touched = 1;
	strcpy(input_dbm->registers[stmt.P2].data.str_val, (char *)stmt.P4);
	return DBM_OK;
}

int operation_rewind(dbm *input_dbm, chidb_stmt stmt) {
	input_dbm->cursors[stmt.P1].curr_cell = (BTreeCell *)calloc(1, sizeof(BTreeCell));
	input_dbm->cursors[stmt.P1].next_cell = (BTreeCell *)calloc(1, sizeof(BTreeCell));
	input_dbm->cursors[stmt.P1].prev_cell = NULL;
	input_dbm->cursors[stmt.P1].cell_num = 0; 
	input_dbm->cursors[stmt.P1].touched = 1;
	int retval = chidb_Btree_getCell(input_dbm->cursors[stmt.P1].node, 0, input_dbm->cursors[stmt.P1].curr_cell);	
	int retval2 = chidb_Btree_getCell(input_dbm->cursors[stmt.P1].node, 1, input_dbm->cursors[stmt.P1].next_cell);
	if (retval2 != CHIDB_OK) {
		input_dbm->cursors[stmt.P1].next_cell = NULL;
	}
	if (retval == CHIDB_OK) {
		return DBM_OK;
	} else {
		return DBM_CELL_NUMBER_BOUNDS;
	}
}

int operation_next(dbm *input_dbm, chidb_stmt stmt) {
	/*
	* IMPLEMENT LATER
	*/
	return DBM_OK;
}

//TODO: BETTER ERROR HANDLING
int tick_dbm(dbm *input_dbm, chidb_stmt stmt) {
	switch (stmt.instruction) {
		case DBM_OPENWRITE:
		case DBM_OPENREAD: {
			if (stmt.instruction == DBM_OPENWRITE) {
				input_dbm->readwritestate = DBM_WRITE_STATE;
			} else {
				input_dbm->readwritestate = DBM_READ_STATE;
			}
			uint32_t page_num = (input_dbm->registers[stmt.P2]).data.int_val;
			input_dbm->cursors[stmt.P1].touched = 1;
			input_dbm->cursors[stmt.P1].node = (BTreeNode *)calloc(1, sizeof(BTreeNode));
			if (chidb_Btree_getNodeByPage(input_dbm->db->bt, page_num, &(input_dbm->cursors[stmt.P1].node)) == CHIDB_OK) {
				input_dbm->tick_result = DBM_OK;
				input_dbm->program_counter += 1;
				return DBM_OK;
			} else {
				input_dbm->tick_result = DBM_OPENRW_ERROR;
				return DBM_HALT_STATE;
			}
		}
		case DBM_CLOSE: {
			int retval = operation_cursor_close(input_dbm, stmt.P1);
			if (retval == DBM_OK) {
				input_dbm->program_counter += 1;
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = DBM_OPENRW_ERROR;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_REWIND:
			if (operation_rewind(input_dbm, stmt) == DBM_OK) {
				input_dbm->program_counter += 1;
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->program_counter = stmt.P2;
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			}
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
		case DBM_KEY: {
			int retval = operation_key(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_INTEGER: {
			int retval = operation_integer(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_STRING: {
			int retval = operation_string(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_NULL:
			break;
		case DBM_RESULTROW:
			break;
		case DBM_MAKERECORD:
			break;
		case DBM_INSERT:
			break;
		case DBM_EQ: {
			int retval = operation_eq(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_NE: {
			int retval = operation_ne(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_LT: {
			int retval = operation_lt(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_LE: {
			int retval = operation_le(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_GT: {
			int retval = operation_gt(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_GE: {
			int retval = operation_ge(input_dbm, stmt);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
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
