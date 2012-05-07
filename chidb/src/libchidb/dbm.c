#include "chidb.h"
#include "btree.h"
#include "dbm.h"

/*
*  This file implements the dbm.
*/

//THIS WILL CREATE A NEW DBM STRUCT
int init_dbm(dbm *input_dbm) {
	input_dbm = (dbm *)calloc(1, sizeof(dbm));
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
	input_dbm->registers[input_dbm->allocated_registers].type = dbm_register_type;
}




