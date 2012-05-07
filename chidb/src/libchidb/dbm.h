/*
*  This file defines the structures which the dbm uses.
*/
#include "chidb.h"
#include "btree.h"

#define DBM_MAX_REGISTERS (256)
#define DBM_MAX_CURSORS (256)

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

enum dbm_register_type {INTEGER, STRING, BINARY, NL};

typedef enum dbm_register_type dbm_register_type;

struct dbm_register {
	dbm_register_type type;
	union {
		uint32_t int_val;
		uint8_t *str_val;
		uint8_t *bin_val;
	} data;
};

typedef struct dbm_register dbm_register;

//THIS MAY CHANGE
struct dbm_cursor {
	BTreeNode *node;
	BTreeCell *curr_cell;
	BTreeCell *prev_cell;
	BTreeCell *next_cell;
};

typedef struct dbm_cursor dbm_cursor;

struct dbm {
	uint32_t program_counter;
	uint32_t allocated_registers;
	uint32_t allocated_cursors;
	dbm_register registers[DBM_MAX_REGISTERS];
	dbm_cursor cursors[DBM_MAX_CURSORS];
	chidb *db;
};

typedef struct dbm dbm;

/*
* TODO: TALK ABOUT THIS
* TENTATIVE def of chidb_stmt
*/

struct chidb_stmt {
	uint32_t instruction;
	uint32_t P1;
	uint32_t P2;
	uint32_t P3;
	uint32_t P4;
};

//THIS WILL CREATE A NEW DBM STRUCT
int init_dbm(dbm *, chidb *);

//THIS RESETS A DBM TO ITS INITIAL STATE
int reset_dbm(dbm *);

//ALLOCATES A NEW REGISTER IN THE DBM AND RETURNS ITS INTEGER INDEX
int init_cursor(dbm *);

//ALLOCATED A NEW REGISTER IN THE DBM AND RETURNS ITS INTEGER INDEX
int init_register(dbm *, dbm_register_type);

//TODO: IMPLEMENT THIS SOON
//PRIVATE - SHOULD NOT BE CALLED BY ANYTHING BUT THE DBM ITSELF
//THIS PROCESSES ONE INSTRUCTION IN THE DBM
int tick_dbm(dbm *input_dbm, chidb_stmt stmt);


