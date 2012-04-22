#include <stdlib.h>
#include "parser.h"
#include "util.h"

int chidb_parser_initSelectStmt(SQLStatement *stmt)
{
	stmt->type = STMT_SELECT;
	stmt->query.select.select_ncols = 0;
	stmt->query.select.select_cols = NULL;
	stmt->query.select.from_ntables = 0;
	stmt->query.select.from_tables = NULL;
	stmt->query.select.where_nconds = 0;
	stmt->query.select.where_conds = NULL;
	
	return CHIDB_OK;
}

int chidb_parser_addSelectColumn(SQLStatement *stmt, char *table, char *col)
{
	stmt->query.select.select_ncols++;
	stmt->query.select.select_cols = realloc(stmt->query.select.select_cols, stmt->query.select.select_ncols * sizeof(Column));
	stmt->query.select.select_cols[stmt->query.select.select_ncols-1].table = table;
	stmt->query.select.select_cols[stmt->query.select.select_ncols-1].name = col;
	
	return CHIDB_OK;
}

int chidb_parser_addFromTable(SQLStatement *stmt, char *table)
{
	stmt->query.select.from_ntables++;
	stmt->query.select.from_tables = realloc(stmt->query.select.from_tables, stmt->query.select.from_ntables * sizeof(char *));
	stmt->query.select.from_tables[stmt->query.select.from_ntables-1] = table;
	
	return CHIDB_OK;
}

int chidb_parser_newCondition(SQLStatement *stmt)
{
	stmt->query.select.where_nconds++;
	stmt->query.select.where_conds = realloc(stmt->query.select.where_conds, stmt->query.select.where_nconds * sizeof(Condition));
	
	return CHIDB_OK;
}

int chidb_parser_setConditionOperand1(SQLStatement *stmt, char *table, char *col)
{
	int ncond = stmt->query.select.where_nconds - 1;
	
	stmt->query.select.where_conds[ncond].op1.table = table;
	stmt->query.select.where_conds[ncond].op1.name = col;
	
	return CHIDB_OK;
}

int chidb_parser_setConditionOperator(SQLStatement *stmt, uint8_t op)
{
	int ncond = stmt->query.select.where_nconds - 1;
	
	stmt->query.select.where_conds[ncond].op = op;
	
	return CHIDB_OK;
}


int chidb_parser_setConditionOperand2Integer(SQLStatement *stmt, int v)
{
	int ncond = stmt->query.select.where_nconds - 1;
	
	stmt->query.select.where_conds[ncond].op2Type = OP2_INT;
	stmt->query.select.where_conds[ncond].op2.integer = v;
	
	return CHIDB_OK;
}

int chidb_parser_setConditionOperand2String(SQLStatement *stmt, char *v)
{
	int ncond = stmt->query.select.where_nconds - 1;
	
	stmt->query.select.where_conds[ncond].op2Type = OP2_STR;
	stmt->query.select.where_conds[ncond].op2.string = v;
	
	return CHIDB_OK;
}

int chidb_parser_setConditionOperand2Column(SQLStatement *stmt, char *table, char *col)
{
	int ncond = stmt->query.select.where_nconds - 1;
	
	stmt->query.select.where_conds[ncond].op2Type = OP2_COL;
	stmt->query.select.where_conds[ncond].op2.col.table = table;
	stmt->query.select.where_conds[ncond].op2.col.name = col;
	
	return CHIDB_OK;
}

int chidb_parser_initInsertStmt(SQLStatement *stmt)
{
	stmt->type = STMT_INSERT;
	stmt->query.insert.nvalues = 0;
	stmt->query.insert.values = NULL;
	
	return CHIDB_OK;
}

int chidb_parser_setInsertTable(SQLStatement *stmt, char *table)
{
	stmt->query.insert.table = table;
	
	return CHIDB_OK;
}

int chidb_parser_addInsertIntValue(SQLStatement *stmt, int v)
{
	stmt->query.insert.nvalues++;
	stmt->query.insert.values = realloc(stmt->query.insert.values, stmt->query.insert.nvalues * sizeof(Value));
	stmt->query.insert.values[stmt->query.insert.nvalues-1].type = INS_INT;
	stmt->query.insert.values[stmt->query.insert.nvalues-1].val.integer = v;
	
	return CHIDB_OK;
}

int chidb_parser_addInsertStrValue(SQLStatement *stmt, char *v)
{
	stmt->query.insert.nvalues++;
	stmt->query.insert.values = realloc(stmt->query.insert.values, stmt->query.insert.nvalues * sizeof(Value));
	stmt->query.insert.values[stmt->query.insert.nvalues-1].type = INS_STR;
	stmt->query.insert.values[stmt->query.insert.nvalues-1].val.string = v;
	
	return CHIDB_OK;	
}

int chidb_parser_addInsertNullValue(SQLStatement *stmt)
{
	stmt->query.insert.nvalues++;
	stmt->query.insert.values = realloc(stmt->query.insert.values, stmt->query.insert.nvalues * sizeof(Value));
	stmt->query.insert.values[stmt->query.insert.nvalues-1].type = INS_NULL;
	
	return CHIDB_OK;	
}


int chidb_parser_initCreateTableStmt(SQLStatement *stmt)
{
	stmt->type = STMT_CREATETABLE;
	stmt->query.createTable.pk = CREATETABLE_NOPK;
	stmt->query.createTable.ncols = 0;
	stmt->query.createTable.cols = NULL;
	
	return CHIDB_OK;
}

int chidb_parser_setCreateTableName(SQLStatement *stmt, char *table)
{
	stmt->query.createTable.table = table;
	
	return CHIDB_OK;
}

int chidb_parser_addCreateTableColumn(SQLStatement *stmt, char* name, uint8_t type, bool pk)
{
	stmt->query.createTable.ncols++;
	stmt->query.createTable.cols = realloc(stmt->query.createTable.cols, stmt->query.createTable.ncols * sizeof(ColumnSchema));
	stmt->query.createTable.cols[stmt->query.createTable.ncols-1].name = name;
	stmt->query.createTable.cols[stmt->query.createTable.ncols-1].type = type;
	
	if(pk)
		stmt->query.createTable.pk = stmt->query.createTable.ncols-1;
	
	return CHIDB_OK;
}

int chidb_parser_initCreateIndexStmt(SQLStatement *stmt, char* index, char *table, char *col)
{
	stmt->type = STMT_CREATEINDEX;
	stmt->query.createIndex.index = index;
	stmt->query.createIndex.on.table = table;
	stmt->query.createIndex.on.name = col;
	
	return CHIDB_OK;
}

int chidb_parser_SQLStatement_destroy(SQLStatement *stmt) {
  chidb_parser_SQLStatement_destroyInternal(*stmt);
  free(stmt);
  return CHIDB_OK;
}

int chidb_parser_SQLStatement_destroyInternal(SQLStatement stmt) {
  switch(stmt.type){
  case STMT_SELECT:
    chidb_parser_SelectStatement_destroyInternal(stmt.query.select);
    break;
  case STMT_INSERT:
    chidb_parser_InsertStatement_destroyInternal(stmt.query.insert);
    break;
  case STMT_CREATETABLE:
    chidb_parser_CreateTableStatement_destroyInternal(stmt.query.createTable);
    break;
  case STMT_CREATEINDEX:
    chidb_parser_CreateIndexStatement_destroyInternal(stmt.query.createIndex);
  }
  return CHIDB_OK;
}

int chidb_parser_SelectStatement_destroyInternal(SelectStatement select) {
  for(int i = 0; i < select.select_ncols; i++)
    chidb_parser_Column_destroyInternal(select.select_cols[i]);
  free(select.select_cols);
  for(int i = 0; i < select.from_ntables; i++)
    free(select.from_tables[i]);
  free(select.from_tables);
  for(int i = 0; i < select.where_nconds; i++)
    chidb_parser_Condition_destroyInternal(select.where_conds[i]);
  free(select.where_conds);
  return CHIDB_OK;
}

int chidb_parser_InsertStatement_destroyInternal(InsertStatement insert) {
  free(insert.table);
  for(int i = 0; i < insert.nvalues; i++)
    chidb_parser_Value_destroyInternal(insert.values[i]);
  free(insert.values);
  return CHIDB_OK;
}

int chidb_parser_CreateTableStatement_destroyInternal(CreateTableStatement createTable) {
  free(createTable.table);
  for(int i = 0; i < createTable.ncols; i++)
    chidb_parser_ColumnSchema_destroyInternal(createTable.cols[i]);
  free(createTable.cols);
  return CHIDB_OK;
}

int chidb_parser_CreateIndexStatement_destroyInternal(CreateIndexStatement createIndex) {
  free(createIndex.index);
  chidb_parser_Column_destroyInternal(createIndex.on);
  return CHIDB_OK;
}

int chidb_parser_Condition_destroyInternal(Condition cond) {
  chidb_parser_Column_destroyInternal(cond.op1);
  switch (cond.op2Type) {
  case OP2_COL:
    chidb_parser_Column_destroyInternal(cond.op2.col);
    break;
  case OP2_STR:
    free(cond.op2.string);
  }
  return CHIDB_OK;
}

int chidb_parser_Value_destroyInternal(Value val) {
  if(val.type == INS_STR)
    free(val.val.string);
  return CHIDB_OK;
}

int chidb_parser_Column_destroyInternal(Column col) {
  free(col.table);
  free(col.name);
  return CHIDB_OK;
}

int chidb_parser_ColumnSchema_destroyInternal(ColumnSchema cs) {
  free(cs.name);
  return CHIDB_OK;
}

/* ################ TOSTRING AND PRINTING FUNCTIONS ##################### */

int chidb_parser_appendColumn(char **s, Column *c)
{
	if (c->table) 
	{
		chidb_astrcat(s, c->table);
		chidb_astrcat(s, ".");
	}
	
	chidb_astrcat(s, c->name);

	return CHIDB_OK;
}

int chidb_parser_appendCondition(char **s, Condition *c)
{
	char *ops[] = {"=","<>","<",">","<=",">="};

	chidb_parser_appendColumn(s, &c->op1);
	if (c->op == OP_ISNULL)
		chidb_astrcat(s, " IS NULL ");
	else if (c->op == OP_ISNOTNULL)
		chidb_astrcat(s, " IS NOT NULL ");
	else
	{
		chidb_astrcat(s, " ");
		chidb_astrcat(s, ops[c->op]);
		chidb_astrcat(s, " ");
		if (c->op2Type == OP2_COL)
			chidb_parser_appendColumn(s, &c->op2.col);
		else if (c->op2Type == OP2_INT)
		{
			char *ints;
			asprintf(&ints, "%i", c->op2.integer);
			chidb_astrcat(s, ints);
			free(ints);
		}
		else if (c->op2Type == OP2_STR) {
		  	chidb_astrcat(s, "\"");
			chidb_astrcat(s, c->op2.string);
			chidb_astrcat(s, "\"");
		}
		chidb_astrcat(s, " ");
	}
		
	return CHIDB_OK;
}

int chidb_parser_appendInsertValue(char **s, Value *v)
{
	if (v->type == INS_INT)
	{
		char *ints;
		asprintf(&ints, "%i", v->val.integer);
		chidb_astrcat(s, ints);
		free(ints);
	}
	else if	(v->type == INS_STR)
	{
		chidb_astrcat(s, "\"");
		chidb_astrcat(s, v->val.string);
		chidb_astrcat(s, "\"");
	}		
	else if	(v->type == INS_NULL)
	{
		chidb_astrcat(s, "NULL"); 
	}
		
	return CHIDB_OK;
}

int chidb_parser_appendColumnSchema(char **s, ColumnSchema *colsch, bool pk)
{
	char *type;
	switch(colsch->type)
	{
		case SQL_INTEGER_1BYTE: type = "BYTE"; break;
		case SQL_INTEGER_2BYTE: type = "SMALLINT"; break;
		case SQL_INTEGER_4BYTE: type = "INTEGER"; break;
		case SQL_TEXT: type = "TEXT"; break;
	};
	char *pkstr = pk? " PRIMARY KEY": ""; 
	
	chidb_astrcat(s, colsch->name);
	chidb_astrcat(s, " ");
	chidb_astrcat(s, type);
	chidb_astrcat(s, pkstr);
	
	return CHIDB_OK;
}

char* chidb_parser_SelectToString(SQLStatement *stmt)
{
	char *s = malloc(1);
	*s = '\0';
	
	chidb_astrcat(&s, "SELECT ");
	if (stmt->query.select.select_ncols == SELECT_ALL)
		chidb_astrcat(&s, "* ");
	else
	{
		chidb_parser_appendColumn(&s, &stmt->query.select.select_cols[0]);
		for(int i=1; i<stmt->query.select.select_ncols; i++)
		{
			chidb_astrcat(&s, ", ");
			chidb_parser_appendColumn(&s, &stmt->query.select.select_cols[i]);
		}
	}
		
	chidb_astrcat(&s, " FROM ");
	chidb_astrcat(&s, stmt->query.select.from_tables[0]);
	for(int i=1; i<stmt->query.select.from_ntables; i++)
	{
		chidb_astrcat(&s, ", ");
		chidb_astrcat(&s, stmt->query.select.from_tables[i]);
	}
	chidb_astrcat(&s,  " ");

	if (stmt->query.select.where_nconds > 0)
	{
		chidb_astrcat(&s, "WHERE ");
		chidb_parser_appendCondition(&s, &stmt->query.select.where_conds[0]);
		for(int i=1; i<stmt->query.select.where_nconds; i++)
		{
			chidb_astrcat(&s, "AND ");			
			chidb_parser_appendCondition(&s, &stmt->query.select.where_conds[i]);
		}
	}
	
	return s;
}

char* chidb_parser_InsertToString(SQLStatement *stmt)
{
	char *s = malloc(1);
	*s = '\0';
	
	chidb_astrcat(&s, "INSERT INTO ");
	chidb_astrcat(&s, stmt->query.insert.table);
	chidb_astrcat(&s, " VALUES(");
	chidb_parser_appendInsertValue(&s, &stmt->query.insert.values[0]);
	for(int i=1; i<stmt->query.insert.nvalues; i++)
	{
		chidb_astrcat(&s, ", ");
		chidb_parser_appendInsertValue(&s, &stmt->query.insert.values[i]);
	}
	chidb_astrcat(&s,  ")\n");
	
	return s;
}

char* chidb_parser_CreateTableToString(SQLStatement *stmt)
{
	char *s = malloc(1);
	*s = '\0';
	
	uint8_t pk = stmt->query.createTable.pk;
	chidb_astrcat(&s,  "CREATE TABLE ");
	chidb_astrcat(&s, stmt->query.createTable.table);
	chidb_astrcat(&s, "\n(\n\t");
	chidb_parser_appendColumnSchema(&s, &stmt->query.createTable.cols[0], 0==pk);
	for(int i=1; i<stmt->query.createTable.ncols; i++)
	{
		chidb_astrcat(&s,  ",\n\t");
		chidb_parser_appendColumnSchema(&s, &stmt->query.createTable.cols[i], i==pk);
	}
	chidb_astrcat(&s,  "\n)\n");
	
	return s;
}

char* chidb_parser_CreateIndexToString(SQLStatement *stmt)
{
	char *s;
	asprintf(&s, "CREATE INDEX %s ON %s(%s)", stmt->query.createIndex.index, 
	                                               stmt->query.createIndex.on.table,
	                                               stmt->query.createIndex.on.name);
	return s;
}

char* chidb_parser_StatementToString(SQLStatement *stmt)
{
	switch(stmt->type)
	{
		case STMT_SELECT:      return chidb_parser_SelectToString(stmt);
		case STMT_INSERT:      return chidb_parser_InsertToString(stmt);
		case STMT_CREATETABLE: return chidb_parser_CreateTableToString(stmt);
		case STMT_CREATEINDEX: return chidb_parser_CreateIndexToString(stmt);
	}
}

int chidb_parser_printSelect(SQLStatement *stmt)
{
	char *s = chidb_parser_SelectToString(stmt);
	
	fprintf(stderr, "%s\n", s);
	
	free(s); 

	return CHIDB_OK;
}

int chidb_parser_printInsert(SQLStatement *stmt)
{
	char *s = chidb_parser_InsertToString(stmt);
	
	fprintf(stderr, "%s\n", s);
	
	free(s); 

	return CHIDB_OK;
}

int chidb_parser_printCreateTable(SQLStatement *stmt)
{
	char *s = chidb_parser_CreateTableToString(stmt);
	
	fprintf(stderr, "%s\n", s);
	
	free(s); 

	return CHIDB_OK;
}

int chidb_parser_printCreateIndex(SQLStatement *stmt)
{
	char *s = chidb_parser_CreateIndexToString(stmt);
	
	fprintf(stderr, "%s\n", s);
	
	free(s); 

	return CHIDB_OK;
}

