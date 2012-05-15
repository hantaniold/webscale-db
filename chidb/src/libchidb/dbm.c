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
dbm * init_dbm(chidb_stmt *stmt, uint8_t stopLoad) {
	dbm * input_dbm = (dbm *)calloc(1, sizeof(dbm));
	if (stmt != NULL) {
		input_dbm->db = stmt->db;
		stmt->input_dbm = input_dbm;
	}
	for (int i = 0; i < DBM_MAX_REGISTERS; ++i) {
		input_dbm->registers[i].touched = 0;
		input_dbm->registers[i].type = NL;
	}
	for (int i = 0; i < DBM_MAX_CURSORS; ++i) {
		input_dbm->cursors[i].touched = 0;
		input_dbm->cursors[i].node = NULL;
		input_dbm->cursors[i].curr_cell = NULL;
		input_dbm->cursors[i].prev_cell = NULL;
		input_dbm->cursors[i].next_cell = NULL;
	}
	if (stmt != NULL && stopLoad != 0) {
		init_lists(stmt);
	}
	reset_dbm(input_dbm);
	return input_dbm;
}

int operation_cursor_close(dbm *input_dbm, uint32_t cursor_id) {
	if (input_dbm->cursors[cursor_id].touched == 1) {
			if (chidb_Btree_freeMemNode(input_dbm->db->bt, input_dbm->cursors[cursor_id].node) == CHIDB_OK) {
				if (input_dbm->cursors[cursor_id].node != NULL) {
					free(input_dbm->cursors[cursor_id].node);
					input_dbm->cursors[cursor_id].node = NULL;
				}
				if (input_dbm->cursors[cursor_id].curr_cell != NULL) {
					free(input_dbm->cursors[cursor_id].curr_cell);
					input_dbm->cursors[cursor_id].curr_cell = NULL;
				}
				if (input_dbm->cursors[cursor_id].next_cell != NULL) {
					free(input_dbm->cursors[cursor_id].next_cell);
					input_dbm->cursors[cursor_id].next_cell = NULL;
				}
				if (input_dbm->cursors[cursor_id].prev_cell != NULL) {
					free(input_dbm->cursors[cursor_id].prev_cell);
					input_dbm->cursors[cursor_id].prev_cell = NULL;
				}
			} else {
				return DBM_MEMORY_FREE_ERROR;
			}
	}
	input_dbm->cursors[cursor_id].touched = 0;
	return DBM_OK;
}

//TODO: THIS NEEDS TO CLEAN Up ALL ALLOCATED CURSORS
//THIS RESETS A DBM TO ITS INITIAL STATE
int reset_dbm(dbm *input_dbm) {
	input_dbm->program_counter = 0;
	input_dbm->tick_result = DBM_OK;
	input_dbm->readwritestate = 0;
	for (int i = 0; i < DBM_MAX_REGISTERS; ++i) {
		if (input_dbm->registers[i].touched == 1) {
			if (input_dbm->registers[i].type == STRING && input_dbm->registers[i].data.str_val != NULL) {
				free(input_dbm->registers[i].data.str_val);
			}
			if (input_dbm->registers[i].type == BINARY) {
				free(input_dbm->registers[i].data.bin_val);
			}
			if (input_dbm->registers[i].type == RECORD) {
				chidb_DBRecord_destroy(input_dbm->registers[i].data.record_val);
			}
		}
		input_dbm->registers[i].touched = 0;
		input_dbm->registers[i].type = NL;
	}
	for (uint32_t i = 0; i < DBM_MAX_CURSORS; ++i) {
		dbm_cursor mycursor = input_dbm->cursors[i];
		if (mycursor.touched == 1) {
			operation_cursor_close(input_dbm, i);
		}
		input_dbm->cursors[i].touched = 0;
	}	
	return CHIDB_OK;
}

void add_nodes(chidb_stmt *stmt, int table_num, BTreeNode *node) {
	if (node->type == PGTYPE_TABLE_LEAF || node->type == PGTYPE_INDEX_LEAF) {
		//printf("TABLE NUM: %i\n", table_num);
		uint32_t start = *(stmt->input_dbm->list_lengths + table_num);
		*(stmt->input_dbm->list_lengths + table_num) += node->n_cells;
		//printf("MY VAL: %i\n", *(stmt->input_dbm->list_lengths + table_num));
		*(stmt->input_dbm->cell_lists + table_num) = realloc(*(stmt->input_dbm->cell_lists + table_num), sizeof(BTreeCell *) * *(stmt->input_dbm->list_lengths + table_num));
		//printf("REALLOC VAL: %i\n", *(stmt->input_dbm->cell_lists + table_num));
		uint32_t end = *(stmt->input_dbm->list_lengths + table_num);
		int ecounter = 0;
		//printf("START: %i\n", start);
		//printf("END: %i\n", end);
		for (int i = start; i < end; ++i) {
			*(*(stmt->input_dbm->cell_lists + table_num) + i) = (BTreeCell *)malloc(sizeof(BTreeCell));
			chidb_Btree_getCell(node, (ncell_t)ecounter, *(*(stmt->input_dbm->cell_lists + table_num) + i));
			ecounter += 1;	
		}
	} else {
		/*
		printf("INVALID TYPE ERROR IN add_nodes\n");
		printf("INVALID TYPE OF: %i\n", node->type);
		printf("INVALID table_num: %i\n", table_num);
		printf("INVALID node ref: %i\n", node);
		*/
	}
}

void recursive_construct(chidb_stmt *stmt, BTree * bt, npage_t page_num, int table_num) {
	BTreeNode *root_node;
	chidb_Btree_getNodeByPage(bt, page_num, &(root_node));
	if (root_node->type == PGTYPE_TABLE_INTERNAL) {
		int n_cells = root_node->n_cells;
		for (int j = 0; j < n_cells; ++j) {
			BTreeCell *curr = (BTreeCell *)malloc(sizeof(BTreeCell));
			chidb_Btree_getCell(root_node, (ncell_t)j, curr);
			npage_t child_page = curr->fields.tableInternal.child_page;
			recursive_construct(stmt, bt, child_page, table_num);
			free(curr);
		}
		if (root_node->right_page != NULL) {
			recursive_construct(stmt, bt, root_node->right_page, table_num);
		}
	} else if (root_node->type == PGTYPE_INDEX_INTERNAL) {
		int n_cells = root_node->n_cells;
		for (int j = 0; j < n_cells; ++j) {
			BTreeCell *curr = (BTreeCell *)malloc(sizeof(BTreeCell));
			chidb_Btree_getCell(root_node, (ncell_t)j, curr);
			npage_t child_page = curr->fields.tableInternal.child_page;
			recursive_construct(stmt, bt, child_page, table_num);
			free(curr);
		}
		if (root_node->right_page != NULL) {
			recursive_construct(stmt, bt, root_node->right_page, table_num);
		}
	} else {
		add_nodes(stmt, table_num, root_node);
	} 
	free(root_node);
}

void init_lists(chidb_stmt *stmt) {
	stmt->input_dbm->cell_lists = (BTreeCell ***)malloc(sizeof(BTreeCell **) * stmt->db->bt->schema_table_size);
	//printf("SCHEMA TABLE SIZE: %i\n", stmt->db->bt->schema_table_size);
	stmt->input_dbm->list_lengths = (uint32_t *)malloc(sizeof(uint32_t) * stmt->db->bt->schema_table_size);
	stmt->input_dbm->num_lists = stmt->db->bt->schema_table_size; 
	for (int i = 0; i < stmt->db->bt->schema_table_size; ++i) {
		//printf("I VALUE FOR LOOP: %i\n", i);
		*(stmt->input_dbm->cell_lists + i) = NULL;
		*(stmt->input_dbm->list_lengths + i) = 0;
		int root_page_num = stmt->db->bt->schema_table[i]->root_page;
		BTreeNode *root_node;
		chidb_Btree_getNodeByPage(stmt->db->bt, root_page_num, &(root_node));
		//printf("LOADING PAGE: %i with type %i\n", root_page_num, root_node->type);
		if (root_node->type == PGTYPE_TABLE_INTERNAL) {
			//printf("INTERNAL CELL\n");
			int n_cells = root_node->n_cells;
			for (int j = 0; j < n_cells; ++j) {
				BTreeCell *curr = (BTreeCell *)malloc(sizeof(BTreeCell));
				chidb_Btree_getCell(root_node, (ncell_t)j, curr);
				npage_t child_page = curr->fields.tableInternal.child_page;
				recursive_construct(stmt, stmt->db->bt, child_page, i);
				free(curr);
			}
			if (root_node->right_page != NULL) {
				recursive_construct(stmt, stmt->db->bt, root_node->right_page, i);
			}
		} else if (root_node->type == PGTYPE_INDEX_INTERNAL) {
			int n_cells = root_node->n_cells;
			for (int j = 0; j < n_cells; ++j) {
				BTreeCell *curr = (BTreeCell *)malloc(sizeof(BTreeCell));
				chidb_Btree_getCell(root_node, (ncell_t)j, curr);
				npage_t child_page = curr->fields.tableInternal.child_page;
				recursive_construct(stmt, stmt->db->bt, child_page, i);
				free(curr);
			}
			if (root_node->right_page != NULL) {
				recursive_construct(stmt, stmt->db->bt, root_node->right_page, i);
			}
		} else {
			//PGTYPE_TABLE_LEAF
			add_nodes(stmt, i, root_node);
		}
		free(root_node);
	}
}

int operation_eq(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P1].type == input_dbm->registers[inst.P3].type) {
		switch (input_dbm->registers[inst.P1].type) {
			case INTEGER:
				if (input_dbm->registers[inst.P1].data.int_val == input_dbm->registers[inst.P3].data.int_val) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (input_dbm->registers[inst.P1].data_len == input_dbm->registers[inst.P3].data_len) {
					if (strcmp(input_dbm->registers[inst.P1].data.str_val, input_dbm->registers[inst.P3].data.str_val) == 0){
						input_dbm->program_counter = inst.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				if (input_dbm->registers[inst.P1].data_len == input_dbm->registers[inst.P3].data_len) {
					if (memcmp(input_dbm->registers[inst.P1].data.bin_val, input_dbm->registers[inst.P3].data.bin_val, input_dbm->registers[inst.P1].data_len) == 0) {
						input_dbm->program_counter = inst.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				} else {
					return DBM_DATA_REGISTER_LENGTH_MISMATCH;
				} 
				break;
			case NL:
				input_dbm->program_counter = inst.P2;
			case RECORD:
				break;
		}
		return DBM_OK;
	} else {
		if (input_dbm->registers[inst.P1].type == NL) {
			switch (input_dbm->registers[inst.P3].type) {
				case INTEGER:
					if (input_dbm->registers[inst.P3].data.int_val == NULL) {
						input_dbm->program_counter = inst.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				break;
				case STRING:
					if (input_dbm->registers[inst.P3].data.str_val == NULL) {
						input_dbm->program_counter = inst.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				break;
				case NL:
					input_dbm->program_counter = inst.P2;
				break;
				//THESE ARE UNIMPLEMENTED BECAUSE THEY WILL NOT BE USED
				case BINARY:
				case RECORD:
				break;
			}
			return DBM_OK;
		}
		if (input_dbm->registers[inst.P3].type == NL) {
			switch (input_dbm->registers[inst.P1].type) {
				case INTEGER:
					if (input_dbm->registers[inst.P1].data.int_val == NULL) {
						input_dbm->program_counter = inst.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				break;
				case STRING:
					if (input_dbm->registers[inst.P1].data.str_val == NULL) {
						input_dbm->program_counter = inst.P2;
					} else {
						input_dbm->program_counter += 1;	
					}
				break;
				case NL:
					input_dbm->program_counter = inst.P2;
				break;
				//THESE ARE UNIMPLEMENTED BECAUSE THEY WILL NOT BE USED
				case BINARY:
				case RECORD:
				break;
			}
			return DBM_OK;
		}
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_ne(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P1].type == input_dbm->registers[inst.P3].type) {
		switch (input_dbm->registers[inst.P1].type) {
			case INTEGER:
				if (input_dbm->registers[inst.P1].data.int_val != input_dbm->registers[inst.P3].data.int_val) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[inst.P1].data.str_val, input_dbm->registers[inst.P3].data.str_val) != 0){
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				if ((memcmp(input_dbm->registers[inst.P1].data.bin_val, input_dbm->registers[inst.P3].data.bin_val, input_dbm->registers[inst.P1].data_len) != 0) || (input_dbm->registers[inst.P1].data_len != input_dbm->registers[inst.P3].data_len)) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				if (!(input_dbm->registers[inst.P1].type == NL && input_dbm->registers[inst.P3].type == NL))  {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case RECORD:
				break;
		}
		return DBM_OK;
    } else if (input_dbm->registers[inst.P1].type == NL || input_dbm->registers[inst.P3].type == NL) {
        int other_reg;
        if (input_dbm->registers[inst.P1].type == NL) {
            other_reg = inst.P3;
        } else {
            other_reg = inst.P1;
        }



        switch (input_dbm->registers[other_reg].type) {
            case INTEGER:
                if (input_dbm->registers[other_reg].data.int_val != NULL) {
                    input_dbm->program_counter = inst.P2;
                } else {
                    input_dbm->program_counter++;
                }
                break;

            case STRING:
				if (input_dbm->registers[other_reg].data.str_val == NULL) {
                    input_dbm->program_counter++;
                } else {
                    input_dbm->program_counter = inst.P2;
                }
                break;
            case NL:
                input_dbm->program_counter = inst.P2;
            case BINARY:
            case RECORD:
                break;

        }
        return DBM_OK;


	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_lt(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P1].type == input_dbm->registers[inst.P3].type) {
		switch (input_dbm->registers[inst.P1].type) {
			case INTEGER:
				if (input_dbm->registers[inst.P1].data.int_val < input_dbm->registers[inst.P3].data.int_val) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[inst.P1].data.str_val, input_dbm->registers[inst.P3].data.str_val) < 0){
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[inst.P1].data.bin_val, input_dbm->registers[inst.P3].data.bin_val, input_dbm->registers[inst.P1].data_len) < 0) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = inst.P2;
				break;
			case RECORD:
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_le(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P1].type == input_dbm->registers[inst.P3].type) {
		switch (input_dbm->registers[inst.P1].type) {
			case INTEGER:
				if (input_dbm->registers[inst.P1].data.int_val <= input_dbm->registers[inst.P3].data.int_val) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[inst.P1].data.str_val, input_dbm->registers[inst.P3].data.str_val) <= 0){
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[inst.P1].data.bin_val, input_dbm->registers[inst.P3].data.bin_val, input_dbm->registers[inst.P1].data_len) <= 0) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = inst.P2;
				break;
			case RECORD:
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_gt(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P1].type == input_dbm->registers[inst.P3].type) {
		switch (input_dbm->registers[inst.P1].type) {
			case INTEGER:
				if (input_dbm->registers[inst.P1].data.int_val > input_dbm->registers[inst.P3].data.int_val) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[inst.P1].data.str_val, input_dbm->registers[inst.P3].data.str_val) > 0){
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[inst.P1].data.bin_val, input_dbm->registers[inst.P3].data.bin_val, input_dbm->registers[inst.P1].data_len) > 0) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = inst.P2;
				break;
			case RECORD:
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}
int operation_ge(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P1].type == input_dbm->registers[inst.P3].type) {
		switch (input_dbm->registers[inst.P1].type) {
			case INTEGER:
				if (input_dbm->registers[inst.P1].data.int_val >= input_dbm->registers[inst.P3].data.int_val) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case STRING:
				if (strcmp(input_dbm->registers[inst.P1].data.str_val, input_dbm->registers[inst.P3].data.str_val) >= 0){
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case BINARY:
				//TODO: UNEQUAL COMPARE LENGTHS CASE
				if (memcmp(input_dbm->registers[inst.P1].data.bin_val, input_dbm->registers[inst.P3].data.bin_val, input_dbm->registers[inst.P1].data_len) >= 0) {
					input_dbm->program_counter = inst.P2;
				} else {
					input_dbm->program_counter += 1;	
				}
				break;
			case NL:
				input_dbm->program_counter = inst.P2;
				break;
			case RECORD:
				break;
		}
		return DBM_OK;
	} else {
		return DBM_REGISTER_TYPE_MISMATCH;
	}
}

int operation_idxgt(dbm *input_dbm, chidb_instruction inst) {
    key_t val1;
    switch(input_dbm->cursors[inst.P1].curr_cell->type) {
        case PGTYPE_INDEX_INTERNAL:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexInternal.keyPk;
            break;
        case PGTYPE_INDEX_LEAF:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexLeaf.keyPk;
            break;
    }
    key_t val2 = (key_t)input_dbm->registers[inst.P3].data.int_val;
    
    if(val1 > val2) {
        input_dbm->program_counter = inst.P2;
    } else {
        input_dbm->program_counter += 1;
    }
    return DBM_OK;
}

int operation_idxge(dbm *input_dbm, chidb_instruction inst) {
    key_t val1;
    switch(input_dbm->cursors[inst.P1].curr_cell->type) {
        case PGTYPE_INDEX_INTERNAL:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexInternal.keyPk;
            break;
        case PGTYPE_INDEX_LEAF:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexLeaf.keyPk;
            break;
    }
    key_t val2 = (key_t)input_dbm->registers[inst.P3].data.int_val;
    
    if(val1 >= val2) {
        input_dbm->program_counter = inst.P2;
    } else {
        input_dbm->program_counter += 1;
    }
    return DBM_OK;
}

int operation_idxlt(dbm *input_dbm, chidb_instruction inst) {
    key_t val1;
    switch(input_dbm->cursors[inst.P1].curr_cell->type) {
        case PGTYPE_INDEX_INTERNAL:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexInternal.keyPk;
            break;
        case PGTYPE_INDEX_LEAF:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexLeaf.keyPk;
            break;
    }
    key_t val2 = (key_t)input_dbm->registers[inst.P3].data.int_val;
    
    if(val1 < val2) {
        input_dbm->program_counter = inst.P2;
    } else {
        input_dbm->program_counter += 1;
    }
    return DBM_OK;
}

int operation_idxle(dbm *input_dbm, chidb_instruction inst) {
    key_t val1;
    switch(input_dbm->cursors[inst.P1].curr_cell->type) {
        case PGTYPE_INDEX_INTERNAL:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexInternal.keyPk;
            break;
        case PGTYPE_INDEX_LEAF:
            val1 = input_dbm->cursors[inst.P1].curr_cell->fields.indexLeaf.keyPk;
            break;
    }
    key_t val2 = (key_t)input_dbm->registers[inst.P3].data.int_val;
    
    if(val1 <= val2) {
        input_dbm->program_counter = inst.P2;
    } else {
        input_dbm->program_counter += 1;
    }
    return DBM_OK;
}

int operation_idxkey(dbm *input_dbm, chidb_instruction inst) {
    key_t key;
    switch(input_dbm->cursors[inst.P1].curr_cell->type) {
        case PGTYPE_INDEX_INTERNAL:
            key = input_dbm->cursors[inst.P1].curr_cell->fields.indexInternal.keyPk;
            break;
        case PGTYPE_INDEX_LEAF:
            key = input_dbm->cursors[inst.P1].curr_cell->fields.indexLeaf.keyPk;
            break;
    }
    input_dbm->registers[inst.P2].data.int_val = (uint32_t)key;
    return DBM_OK;
}


int operation_key(dbm *input_dbm, chidb_instruction inst) {
	input_dbm->registers[inst.P2].type = INTEGER;
	uint32_t table_num = input_dbm->cursors[inst.P1].table_num;
	uint32_t pos = input_dbm->cursors[inst.P1].pos;
	input_dbm->registers[inst.P2].data.int_val = (int32_t)input_dbm->cell_lists[table_num][pos]->key;
	input_dbm->registers[inst.P2].touched = 0;
	return DBM_OK;
}

int operation_integer(dbm *input_dbm, chidb_instruction inst) {
	input_dbm->registers[inst.P2].type = INTEGER;
	input_dbm->registers[inst.P2].data.int_val = inst.P1;
	input_dbm->registers[inst.P2].touched = 0;
	return DBM_OK;
}

int operation_string(dbm *input_dbm, chidb_instruction inst) {
	
	input_dbm->registers[inst.P2].type = STRING;
	if (inst.P4 != NULL) {
		input_dbm->registers[inst.P2].data.str_val = (char *)malloc(inst.P1 * sizeof(char));
		input_dbm->registers[inst.P2].data_len = (size_t)inst.P1;
		input_dbm->registers[inst.P2].touched = 1;
		strncpy(input_dbm->registers[inst.P2].data.str_val, inst.P4, input_dbm->registers[inst.P2].data_len);
	} else {
		input_dbm->registers[inst.P2].touched = 0;
		input_dbm->registers[inst.P2].data_len = 0;
		input_dbm->registers[inst.P2].data.str_val = NULL;
	}
	return DBM_OK;
}

int operation_rewind(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->cursors[inst.P1].touched == 1) {
		input_dbm->cursors[inst.P1].pos = 0;
		input_dbm->program_counter += 1;
		return DBM_OK;
	} else {
		input_dbm->program_counter = inst.P2;
		return DBM_OK;
	}
}

//DBM_MAKERECORD
int operation_db_record(dbm *input_dbm, chidb_instruction inst) {
	input_dbm->registers[inst.P2].type = RECORD;
	input_dbm->registers[inst.P2].touched = 1;
	input_dbm->registers[inst.P2].data.record_val = (DBRecord *)calloc(1, sizeof(DBRecord));
	
	DBRecordBuffer *dbrb = (DBRecordBuffer *)calloc(1, sizeof(DBRecordBuffer));
	
	chidb_DBRecord_create_empty(dbrb, inst.P2);
	
	for (uint32_t i = inst.P1; i < inst.P1 + inst.P2; ++i) {
		switch (input_dbm->registers[i].type) {
			case INTEGER:
				chidb_DBRecord_appendInt32(dbrb, input_dbm->registers[i].data.int_val);
			break;
			case STRING:
				chidb_DBRecord_appendString(dbrb,  input_dbm->registers[i].data.str_val);
			break;
			case NL:
				chidb_DBRecord_appendNull(dbrb);
			break;
			case BINARY:
			break;
			case RECORD:
			break;
		}
	}
	
	chidb_DBRecord_finalize(dbrb, &(input_dbm->registers[inst.P2].data.record_val));
	
	free(dbrb);
	input_dbm->program_counter += 1;	
	return DBM_OK;
}

int operation_next(dbm *input_dbm, chidb_instruction inst) {
	if ((input_dbm->cursors[inst.P1].pos + 1) < *(input_dbm->list_lengths + input_dbm->cursors[inst.P1].table_num)) {
		input_dbm->cursors[inst.P1].pos += 1;
		input_dbm->program_counter = inst.P2;
	} else {
		input_dbm->program_counter += 1;
	}
	return DBM_OK;
}

int operation_prev(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->cursors[inst.P1].cell_num - 1 >= 0) {
		if (input_dbm->cursors[inst.P1].prev_cell == NULL) {
			input_dbm->cursors[inst.P1].prev_cell = (BTreeCell *)malloc(sizeof(BTreeCell));
		}
		if (input_dbm->cursors[inst.P1].curr_cell == NULL) {
			input_dbm->cursors[inst.P1].curr_cell = (BTreeCell *)malloc(sizeof(BTreeCell));
		}
		if (input_dbm->cursors[inst.P1].next_cell == NULL) {
			input_dbm->cursors[inst.P1].next_cell = (BTreeCell *)malloc(sizeof(BTreeCell));
		}
		ncell_t prev = input_dbm->cursors[inst.P1].cell_num;
		input_dbm->cursors[inst.P1].next_cell = input_dbm->cursors[inst.P1].curr_cell;
		input_dbm->cursors[inst.P1].curr_cell = input_dbm->cursors[inst.P1].prev_cell;
		input_dbm->cursors[inst.P1].prev_cell = (BTreeCell *)malloc(sizeof(BTreeCell));
		chidb_Btree_getCell(input_dbm->cursors[inst.P1].node, prev, input_dbm->cursors[inst.P1].prev_cell);
		input_dbm->program_counter = inst.P2;
		input_dbm->cursors[inst.P1].cell_num -= 1;
		return DBM_OK;
	} else {
		// do nothing
		input_dbm->program_counter += 1;
	}
	return DBM_OK;
}

int operation_idxinsert(dbm *input_dbm, chidb_instruction inst) {
  key_t keyIdx = (key_t)input_dbm->registers[inst.P2].data.int_val;
  key_t keyPk = (key_t)input_dbm->registers[inst.P3].data.int_val;
  npage_t nroot = (npage_t)input_dbm->table_root;

  int retval = chidb_Btree_insertInIndex(input_dbm->db->bt, nroot, keyIdx, keyPk);

  if (retval == CHIDB_EDUPLICATE) {
    return DBM_DUPLICATE_KEY;
  }
  if (retval == CHIDB_ENOMEM) {
    return DBM_MEMORY_ERROR;
  }
  if (retval == CHIDB_EIO) {
    return DBM_IO_ERROR;
  }
  return DBM_OK;
}

int operation_scopy(dbm *input_dbm, chidb_instruction inst) {
  input_dbm->registers[inst.P2] = input_dbm->registers[inst.P1];

  return DBM_OK;
}

//DBM_INSERT
int operation_insert_record(dbm *input_dbm, chidb_instruction inst) {
	if (input_dbm->registers[inst.P2].type == RECORD) {
        
        //int retval = chidb_Btree_insertInTable(input_dbm->db->bt, input_dbm->cursors[inst.P1].node->page->npage
		int retval = chidb_Btree_insertInTable(input_dbm->db->bt, (npage_t)(*(input_dbm->db->bt->schema_table))->root_page, (key_t)input_dbm->registers[inst.P3].data.int_val, input_dbm->registers[inst.P2].data.record_val->data, (uint16_t)input_dbm->registers[inst.P2].data.record_val->data_len);
		if (retval == CHIDB_EDUPLICATE) {
			return DBM_DUPLICATE_KEY;
		}
		if (retval == CHIDB_ENOMEM) {
			return DBM_MEMORY_ERROR;
		}
		if (retval == CHIDB_EIO) {
			return DBM_IO_ERROR;
		}
	} else {
		return DBM_INVALID_TYPE;
	}
	return DBM_OK;
}

int operation_column(dbm *input_dbm, chidb_instruction inst) {
	DBRecord *record;
	chidb_DBRecord_unpack(&(record), input_dbm->cursors[inst.P1].curr_cell->fields.tableLeaf.data);
	int type = chidb_DBRecord_getType(record, inst.P2);
	if (type == SQL_NULL) {
		input_dbm->registers[inst.P3].type = NL;
		input_dbm->program_counter += 1;
		return DBM_OK;
	}
	if (type == SQL_INTEGER_1BYTE || type == SQL_INTEGER_2BYTE || type == SQL_INTEGER_4BYTE) {
		input_dbm->registers[inst.P3].type = INTEGER;
		if (type == SQL_INTEGER_1BYTE) {
			int8_t *v = (int8_t *)malloc(sizeof(int8_t));
			chidb_DBRecord_getInt8(record, inst.P2, v);
			input_dbm->registers[inst.P3].data.int_val = (int32_t)(*v);
			free(v);
		}
		if (type == SQL_INTEGER_2BYTE) {
			int16_t *v = (int16_t *)malloc(sizeof(int16_t));
			chidb_DBRecord_getInt16(record, inst.P2, v);
			input_dbm->registers[inst.P3].data.int_val = (int32_t)(*v);
			free(v);
		}
		if (type == SQL_INTEGER_4BYTE) {
			int32_t *v = (int32_t *)malloc(sizeof(int32_t));
			chidb_DBRecord_getInt32(record, inst.P2, v);
			input_dbm->registers[inst.P3].data.int_val = (int32_t)(*v);
			free(v);
		}
		input_dbm->program_counter += 1;
		return DBM_OK;
	}
	if (type == SQL_TEXT) {
		input_dbm->registers[inst.P3].type = STRING;
		int *len = (int *)malloc(sizeof(int));
		chidb_DBRecord_getStringLength(record, inst.P2, len);
		input_dbm->registers[inst.P3].data.str_val = (char *)malloc((*len) * sizeof(char));
		chidb_DBRecord_getString(record, inst.P2, &(input_dbm->registers[inst.P3].data.str_val));
		input_dbm->registers[inst.P3].data_len = (size_t)(*len);
		free(len);
		input_dbm->program_counter += 1;
		return DBM_OK;
	}
	//WE HAVE AN INVALID TYPE
	return DBM_INVALID_TYPE;
}

int tick_dbm(dbm *input_dbm, chidb_instruction inst) {
	switch (inst.instruction) {
		case DBM_OPENWRITE:
		case DBM_OPENREAD: {
			if (inst.instruction == DBM_OPENWRITE) {
				input_dbm->readwritestate = DBM_WRITE_STATE;
			} else {
				input_dbm->readwritestate = DBM_READ_STATE;
			}
			
			uint32_t page_num = (input_dbm->registers[inst.P2]).data.int_val;
			
			input_dbm->cursors[inst.P1].touched = 1;
			input_dbm->cursors[inst.P1].cols = inst.P3;
			input_dbm->tick_result = DBM_OK;
			input_dbm->program_counter += 1;
			
			
			for (int i = 0; i < input_dbm->db->bt->schema_table_size; ++i) {
				int root_page_num = input_dbm->db->bt->schema_table[i]->root_page;
				if (root_page_num == page_num) {
					input_dbm->cursors[inst.P1].table_num = i;
					break;
				}
			}
			
			return DBM_OK;
		}
		case DBM_CLOSE: {
			int retval = operation_cursor_close(input_dbm, inst.P1);
			if (retval == DBM_OK) {
				input_dbm->program_counter += 1;
				input_dbm->tick_result = DBM_OK;
			} else {
				input_dbm->tick_result = DBM_OPENRW_ERROR;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_REWIND: {
			return operation_rewind(input_dbm, inst);
			break;
		}
		case DBM_NEXT: {
			int retval = operation_next(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_PREV: {
			int retval = operation_prev(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_SEEK:
			break;
		case DBM_SEEKGT:
			break;
		case DBM_SEEKGE:
			break;
		case DBM_COLUMN: {
			int retval = operation_column(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_KEY: {
			input_dbm->program_counter += 1;
			return operation_key(input_dbm, inst);
			break;
		}
		case DBM_INTEGER: {
			int retval = operation_integer(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				input_dbm->program_counter += 1;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_STRING: {
			int retval = operation_string(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				input_dbm->program_counter += 1;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_NULL: {
			input_dbm->registers[inst.P2].type = NL;
			input_dbm->registers[inst.P2].touched = 1;
			input_dbm->program_counter += 1;
			input_dbm->tick_result = DBM_OK;
			return DBM_OK;
		}
		case DBM_RESULTROW: {
			input_dbm->tick_result = DBM_OK;
			return DBM_RESULT;
		}
		case DBM_MAKERECORD: {
			int retval = operation_db_record(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_INSERT: {
			int retval = operation_insert_record(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_EQ: {
			int retval = operation_eq(input_dbm, inst);
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
			int retval = operation_ne(input_dbm, inst);
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
			int retval = operation_lt(input_dbm, inst);
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
			int retval = operation_le(input_dbm, inst);
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
			int retval = operation_gt(input_dbm, inst);
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
			int retval = operation_ge(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		
		case DBM_IDXGT: {
			int retval = operation_idxgt(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
    }
    
		case DBM_IDXGE: {
			int retval = operation_idxge(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
    }
    
		case DBM_IDXLT: {
			int retval = operation_idxlt(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
    }
    
		case DBM_IDXLE: {
			int retval = operation_idxle(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
        }
    
		case DBM_IDXKEY: {
			int retval = operation_idxkey(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_IDXINSERT: {
			int retval = operation_idxinsert(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
		}
		case DBM_CREATETABLE:
			break;
		case DBM_CREATEINDEX:
			break;
		case DBM_SCOPY:{
			int retval = operation_scopy(input_dbm, inst);
			if (retval == DBM_OK) {
				input_dbm->tick_result = DBM_OK;
				return DBM_OK;
			} else {
				input_dbm->tick_result = retval;
				return DBM_HALT_STATE;
			}
			break;
    }
		case DBM_HALT:
			if (inst.P1 == 0) {
				input_dbm->tick_result = DBM_OK;
			} else {
				input_dbm->tick_result = inst.P1;
				input_dbm->error_str = inst.P4;
			}
			return DBM_HALT_STATE;
		break;
	}
	return DBM_INVALID_INSTRUCTION;
} //END OF tick_dbm

int generate_result_row(chidb_stmt *stmt) {
	chidb_instruction curr_inst = *(stmt->ins + stmt->input_dbm->program_counter);
	uint32_t index = curr_inst.P1;
	uint32_t bound = index + curr_inst.P2;
	
	DBRecordBuffer *dbrb = (DBRecordBuffer *)calloc(1, sizeof(DBRecordBuffer));
	chidb_DBRecord_create_empty(dbrb, curr_inst.P2);
		
	for (; index < bound; ++index) {
		switch (stmt->input_dbm->registers[index].type) {
			case INTEGER:
				chidb_DBRecord_appendInt32(dbrb, stmt->input_dbm->registers[index].data.int_val);
			break;
			case STRING:
				chidb_DBRecord_appendString(dbrb,  stmt->input_dbm->registers[index].data.str_val);
			break;
			case NL:
				chidb_DBRecord_appendNull(dbrb);
			break;
			case BINARY:
			break;
			case RECORD:
			break;
		}
	}
	chidb_DBRecord_finalize(dbrb, &(stmt->record));
	free(dbrb);
	return CHIDB_OK;
}
//EOF
