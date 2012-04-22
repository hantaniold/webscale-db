#ifndef PARSER_H_
#define PARSER_H_

#include <chidbInt.h>

#define STMT_SELECT (0)
#define STMT_INSERT (1)
#define STMT_CREATETABLE  (2)
#define STMT_CREATEINDEX  (3)

#define SELECT_ALL (-1)

#define OP_EQ (0)
#define OP_NE (1)
#define OP_LT (2)
#define OP_GT (3)
#define OP_LTE (4)
#define OP_GTE (5)
#define OP_ISNULL (6)
#define OP_ISNOTNULL (7)

#define OP2_COL (0)
#define OP2_INT (1)
#define OP2_STR (2)

#define INS_INT (0)
#define INS_STR (1)
#define INS_NULL (2)

#define CREATETABLE_NOPK (-1)


struct Column
{
	char *table;
	char *name;
};
typedef struct Column Column;

struct ColumnSchema
{
	char *name;
	uint8_t type;
};
typedef struct ColumnSchema ColumnSchema;

struct Condition
{
	uint8_t op2Type;
	Column op1;
	uint8_t op;
	union
	{
		Column col;
		int integer;
		char *string;
	} op2;
};
typedef struct Condition Condition;

struct Value
{
	uint8_t type;
	union
	{
		int integer;
		char *string;
	} val;
};
typedef struct Value Value;

struct SelectStatement
{
	int8_t select_ncols;
	Column *select_cols;
	uint8_t from_ntables;
	char **from_tables;
	uint8_t where_nconds;
	Condition *where_conds;	
};
typedef struct SelectStatement SelectStatement;

struct InsertStatement
{
	char *table;
	uint8_t nvalues;
	Value *values;	
};
typedef struct InsertStatement InsertStatement;

struct CreateTableStatement
{
	char *table;
	int16_t pk;
	uint8_t ncols;
	ColumnSchema *cols;
};
typedef struct CreateTableStatement CreateTableStatement;

struct CreateIndexStatement
{
	char *index;
	Column on;
};
typedef struct CreateIndexStatement CreateIndexStatement;

struct SQLStatement
{
	uint8_t type;
        union {
	  SelectStatement select;
	  InsertStatement insert;
	  CreateTableStatement createTable;
	  CreateIndexStatement createIndex;
	} query;
};
typedef struct SQLStatement SQLStatement;


int chidb_parser(const char *sql, SQLStatement **stmt);

/* SELECT */
int chidb_parser_initSelectStmt(SQLStatement *stmt);
int chidb_parser_addSelectColumn(SQLStatement *stmt, char *table, char *col);
int chidb_parser_addFromTable(SQLStatement *stmt, char *table);
int chidb_parser_newCondition(SQLStatement *stmt);
int chidb_parser_setConditionOperand1(SQLStatement *stmt, char *table, char *col);
int chidb_parser_setConditionOperator(SQLStatement *stmt, uint8_t op);
int chidb_parser_setConditionOperand2Integer(SQLStatement *stmt, int v);
int chidb_parser_setConditionOperand2String(SQLStatement *stmt, char *v);
int chidb_parser_setConditionOperand2Column(SQLStatement *stmt, char *table, char *col);

/* INSERT */
int chidb_parser_initInsertStmt(SQLStatement *stmt);
int chidb_parser_setInsertTable(SQLStatement *stmt, char *table);
int chidb_parser_addInsertIntValue(SQLStatement *stmt, int v);
int chidb_parser_addInsertStrValue(SQLStatement *stmt, char *v);
int chidb_parser_addInsertNullValue(SQLStatement *stmt);

/* CREATE TABLE */
int chidb_parser_initCreateTableStmt(SQLStatement *stmt);
int chidb_parser_setCreateTableName(SQLStatement *stmt, char *table);
int chidb_parser_addCreateTableColumn(SQLStatement *stmt, char* name, uint8_t type, bool pk);

/* CREATE INDEX */
int chidb_parser_initCreateIndexStmt(SQLStatement *stmt, char* index, char *table, char *col);

/* CLEANUP */
int chidb_parser_SQLStatement_destroy(SQLStatement *stmt);
int chidb_parser_SQLStatement_destroyInternal(SQLStatement stmt);
int chidb_parser_SelectStatement_destroyInternal(SelectStatement select);
int chidb_parser_InsertStatement_destroyInternal(InsertStatement insert);
int chidb_parser_CreateTableStatement_destroyInternal(CreateTableStatement createTable);
int chidb_parser_CreateIndexStatement_destroyInternal(CreateIndexStatement createIndex);
int chidb_parser_Condition_destroyInternal(Condition cond);
int chidb_parser_Value_destroyInternal(Value val);
int chidb_parser_Column_destroyInternal(Column col);
int chidb_parser_ColumnSchema_destroyInternal(ColumnSchema cs);

/* TOSTRING AND PRINT */
char* chidb_parser_StatementToString(SQLStatement *stmt);

char* chidb_parser_SelectToString(SQLStatement *stmt);
char* chidb_parser_InsertToString(SQLStatement *stmt);
char* chidb_parser_CreateTableToString(SQLStatement *stmt);
char* chidb_parser_CreateIndexToString(SQLStatement *stmt);
int chidb_parser_printSelect(SQLStatement *stmt);
int chidb_parser_printInsert(SQLStatement *stmt);
int chidb_parser_printCreateTable(SQLStatement *stmt);
int chidb_parser_printCreateIndex(SQLStatement *stmt);

#endif /*PARSER_H_*/
