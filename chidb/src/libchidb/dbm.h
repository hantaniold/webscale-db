/*
*  This file defines the structures which the dbm uses.
*/
#include "chidb.h"
#include "btree.h"
#include "parser.h"
#include "record.h"

#define DBM_MAX_REGISTERS (256)
#define DBM_MAX_CURSORS (256)

//INTERNAL DBM ERROR TYPES 
#define DBM_INVALID_INSTRUCTION (9000)
#define DBM_OPENRW_ERROR (9002)
#define DBM_REGISTER_TYPE_MISMATCH (9004)
#define DBM_DATA_REGISTER_LENGTH_MISMATCH (9005)
#define DBM_MEMORY_FREE_ERROR (9006)
#define DBM_CELL_NUMBER_BOUNDS (9007)
#define DBM_INVALID_TYPE (9008)
#define DBM_DUPLICATE_KEY (9009)
#define DBM_MEMORY_ERROR (9010)
#define DBM_IO_ERROR (9011)

//INTERNAL DBM RETURN TYPES
#define DBM_OK (0)
#define DBM_HALT_STATE (1)
#define DBM_RESULT (2)

//INTERNAL DBM READ / WRITE STATES
#define DBM_READ_STATE (0)
#define DBM_WRITE_STATE (1) 

//INSTRUCTION DEFINITIONS
#define DBM_OPENREAD (0)
#define DBM_OPENWRITE (1)
#define DBM_CLOSE (2)
#define DBM_REWIND (3)
#define DBM_NEXT (4)
#define DBM_PREV (5)
#define DBM_SEEK (6)
#define DBM_SEEKGT (7)
#define DBM_SEEKGE (8)
#define DBM_COLUMN (9)
#define DBM_KEY (10)
#define DBM_INTEGER (11)
#define DBM_STRING (12)
#define DBM_NULL (13)
#define DBM_RESULTROW (14)
#define DBM_MAKERECORD (15)
#define DBM_INSERT (16)
#define DBM_EQ (17)
#define DBM_NE (18)
#define DBM_LT (19)
#define DBM_LE (20)
#define DBM_GT (21)
#define DBM_GE (22)
#define DBM_IDXGT (23)
#define DBM_IDXLT (24)
#define DBM_IDXLE (25)
#define DBM_IDXKEY (26)
#define DBM_IDXINSERT (27)
#define DBM_CREATETABLE (28)
#define DBM_CREATEINDEX (29)
#define DBM_SCOPY (30)
#define DBM_HALT (31)

enum dbm_register_type {INTEGER, STRING, BINARY, NL, RECORD};

typedef enum dbm_register_type dbm_register_type;

struct dbm_register {
	dbm_register_type type;
	size_t data_len; //TO BE USED WHEN STORING STRINGS AND BINARY VALUES
	uint8_t touched;
	union internal_data{
		int32_t int_val;
		char *str_val;
		uint8_t *bin_val;
		DBRecord *record_val;
	} data;
};

typedef struct dbm_register dbm_register;

//THIS MAY CHANGE
struct dbm_cursor {
	uint8_t touched;
	uint32_t cell_num;
	BTreeNode *node;
	BTreeCell *curr_cell;
	BTreeCell *prev_cell;
	BTreeCell *next_cell;
};

typedef struct dbm_cursor dbm_cursor;

struct dbm {
	uint32_t program_counter;
	uint32_t tick_result; //stores the result of the last tick operation - used for error tracking
	char *error_str;
	uint8_t readwritestate;
	dbm_register registers[DBM_MAX_REGISTERS];
	dbm_cursor cursors[DBM_MAX_CURSORS];
	chidb *db;
};

typedef struct dbm dbm;

struct chidb_instruction {
	uint32_t instruction;
	uint32_t P1;
	uint32_t P2;
	uint32_t P3;
	char * P4;
};
typedef struct chidb_instruction chidb_instruction;

struct chidb_stmt {
    chidb_instruction *ins;
    int num_instructions;
    DBRecord *record;
    chidb *db;
    SQLStatement *sql;
    uint8_t initialized_dbm;
    dbm *input_dbm;
};

//THIS WILL CREATE A NEW DBM STRUCT
dbm * init_dbm(chidb *db);

//THIS RESETS A DBM TO ITS INITIAL STATE
int reset_dbm(dbm *);

//PRIVATE - SHOULD NOT BE CALLED BY ANYTHING BUT THE DBM ITSELF
//THIS PROCESSES ONE INSTRUCTION IN THE DBM
//INCREMENTS THE PROGRAM COUNTER BY ONE IF NO JUMP OCCURS
int tick_dbm(dbm *input_dbm, chidb_instruction stmt);


