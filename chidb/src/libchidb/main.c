/*****************************************************************************
 *
 *																 chidb
 *
 * This module provides the chidb API.
 *
 * For more details on what each function does, see the chidb Architecture
 * document, or the chidb.h header file
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/


#include <stdlib.h>
#include <chidb.h>
#include <string.h>
#include "btree.h"
#include "dbm.h"
#include "parser.h"
#include "record.h"

/* This file currently provides only a very basic implementation
 * of chidb_open and chidb_close that are sufficient to test
 * the B Tree module. The rest of the functions are left
 * as an exercise to the student. chidb_open itself may need to be
 * modified if other chidb modules are implemented.
 */

int chidb_open(const char *file, chidb **db)
{
	*db = malloc(sizeof(chidb));
	if (*db == NULL)
		return CHIDB_ENOMEM;
	chidb_Btree_open(file, *db, &(*db)->bt);
	
    chidb_load_schema(*db);
    chidb_print_schema(*db);

	return CHIDB_OK;
}

void chidb_print_schema(chidb *  db) {
    int schema_row_index = 0;
    printf("=== Printing Schema ===\n");
    for (;schema_row_index < db->bt->schema_table_size;  schema_row_index++) {

       printf("Entry %d\n",schema_row_index);
       printf("%s|%s|%s|%d|%s|\n",db->bt->schema_table[schema_row_index]->item_type, db->bt->schema_table[schema_row_index]->item_name, db->bt->schema_table[schema_row_index]->assoc_table_name, db->bt->schema_table[schema_row_index]->root_page,db->bt->schema_table[schema_row_index]->sql);
       return;
    }
}   
int chidb_load_schema(chidb * db) {
    DBRecord * dbr;
    BTreeNode * btn;
    BTreeCell * cell = calloc(1,sizeof(BTreeCell));
    chidb_Btree_getNodeByPage(db->bt, 1, &btn);
    db->bt->schema_table = NULL;
    int schema_size = 0;
    int schema_row_index = 0;
    for (int i = 0; i < btn->n_cells; i++) {
        chidb_Btree_getCell(btn, i, cell);
        if (cell->type == PGTYPE_TABLE_LEAF) {
            schema_size++;
            //TODO - why does this cause memory violations in tests
            //THIS LINE CAUSES PROBLEMS IN TESTS
            db->bt->schema_table = realloc(db->bt->schema_table,schema_size*sizeof(SchemaTableRow *));
            db->bt->schema_table[schema_row_index] = calloc(1,sizeof(SchemaTableRow));

            chidb_DBRecord_unpack(&dbr,cell->fields.tableLeaf.data);
            chidb_DBRecord_getString(dbr,0,&db->bt->schema_table[schema_row_index]->item_type);
            chidb_DBRecord_getString(dbr,1,&db->bt->schema_table[schema_row_index]->item_name);
            chidb_DBRecord_getString(dbr,2,&db->bt->schema_table[schema_row_index]->assoc_table_name);
            chidb_DBRecord_getInt32(dbr,3,&db->bt->schema_table[schema_row_index]->root_page);
            chidb_DBRecord_getString(dbr,4,&db->bt->schema_table[schema_row_index]->sql);
            
            // Compensate for semicolon parser error in CREATE TABLE
            db->bt->schema_table[schema_row_index]->sql = realloc(db->bt->schema_table[schema_row_index]->sql, strlen(db->bt->schema_table[schema_row_index]->sql) + 2);
            strcat(db->bt->schema_table[schema_row_index]->sql, ";");

            
            schema_row_index++;
        }
    }
    db->bt->schema_table_size = schema_size;

    
    return CHIDB_OK;
}

int chidb_close(chidb *db)
{
	chidb_Btree_close(db->bt);

    for (int i = 0; i < db->bt->schema_table_size; i++) {
        free(db->bt->schema_table[i]);
    }
    free(db->bt->schema_table);
	free(db);
	return CHIDB_OK;
} 

int chidb_prepare(chidb *db, const char *sql, chidb_stmt **stmt)
{
    int err;

    // Call the SQL parser
    SQLStatement *sql_stmt;
    err = chidb_parser(sql, &sql_stmt);
    if(err == CHIDB_EINVALIDSQL)
        return CHIDB_EINVALIDSQL;

    // Check that the query is valid against our schema table
    int root_page;
    int ncols;
    SchemaTableRow *schema_row = NULL;
    SQLStatement *create_table_stmt;
    switch(sql_stmt->type) {
        case STMT_SELECT:
        {
            // Check that all table names are valid
            for(int i = 0; i < sql_stmt->query.select.from_ntables; i++) {
                for(int j = 0; i < db->bt->schema_table_size; j++) {
                    if(!strcmp(sql_stmt->query.select.from_tables[i], db->bt->schema_table[j]->item_name)) {
                        schema_row = db->bt->schema_table[j];
                        root_page = schema_row->root_page;
                        break;
                    }
                }
                if(schema_row)
                    break;
            }
            if(!schema_row)
                return CHIDB_EINVALIDSQL;

            // Check that all column names are valid
            chidb_parser(schema_row->sql, &create_table_stmt);
            for(int i = 0; i < sql_stmt->query.select.select_ncols; i++) {
                int match = 0;
                for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                    if(!strcmp(sql_stmt->query.select.select_cols[i].name, create_table_stmt->query.createTable.cols[j].name)) {
                        match = 1;
                        ncols = create_table_stmt->query.createTable.ncols;
                        break;
                    }
                }
                if(!match)
                    return CHIDB_EINVALIDSQL;
            }

            // Check that all column names in the WHERE clause are valid
            int match = 0;
            uint8_t type = 0;
            for(int i = 0; i < sql_stmt->query.select.where_nconds; i++) {
                for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                    if(!strcmp(sql_stmt->query.select.where_conds[i].op1.name, create_table_stmt->query.createTable.cols[j].name)) {
                        uint8_t op2t = sql_stmt->query.select.where_conds[i].op2Type;
                        if (op2t != OP2_COL && op2t == create_table_stmt->query.createTable.cols[j].type)
                            match = 1;
                        else {
                            match = 1;
                            type = create_table_stmt->query.createTable.cols[j].type;
                        }
                    }
                }
                if(!match)
                    return CHIDB_EINVALIDSQL;
                if(sql_stmt->query.select.where_conds[i].op2Type == OP2_COL) {
                    match = 0;

                    for(int i = 0; i < sql_stmt->query.select.where_nconds; i++) {
                        for(int j = 0; j < create_table_stmt->query.createTable.ncols; j++) {
                            if(!strcmp(sql_stmt->query.select.where_conds[i].op2.col.name, create_table_stmt->query.createTable.cols[j].name) && type == create_table_stmt->query.createTable.cols[j].type) {
                                match = 1;
                            }
                        }
                        if(!match)
                            return CHIDB_EINVALIDSQL;
                    }
                }
            }

            break;
        }
        case STMT_INSERT:
        {
            // Check that all table names are valid
            for(int i = 0; i < db->bt->schema_table_size; i++) {
                if(!strcmp(sql_stmt->query.insert.table, db->bt->schema_table[i]->item_name)) {
                    schema_row = db->bt->schema_table[i];
                    root_page = schema_row->root_page;
                    break;
                }
            }
            if(!schema_row)
                return CHIDB_EINVALIDSQL;

            // Check that the right number of values are being inserted
            chidb_parser(schema_row->sql, &create_table_stmt);
            if(sql_stmt->query.insert.nvalues != create_table_stmt->query.createTable.ncols)
                return CHIDB_EINVALIDSQL;

            ncols = create_table_stmt->query.createTable.ncols;
            
            // Check value types
            for(int i = 0; i < sql_stmt->query.insert.nvalues; i++) {
                if(sql_stmt->query.insert.values[i].type != create_table_stmt->query.createTable.cols[i].type)
                    return CHIDB_EINVALIDSQL;
            }
            break;
        }
    }

    // Compile the SQL statement into valid chidb statements
    *stmt = malloc(sizeof(chidb_stmt));
    (*stmt)->ins = NULL;
    (*stmt)->num_instructions = 0;
    (*stmt)->record = NULL;
    (*stmt)->db = db;
    (*stmt)->sql = sql_stmt;

    switch(sql_stmt->type) {
        case STMT_SELECT:
        {
            int numlines = 0;
            int rmax = 0;

            // Store the page number
            (*stmt)->ins = malloc(sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_INTEGER;    // Integer type
            (*stmt)->ins[numlines].P1 = root_page;               // Store the root page
            (*stmt)->ins[numlines].P2 = 0;                       // into register 0
            numlines++;

            // Open the B-Tree
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_OPENREAD;  // Open a B-Tree
            (*stmt)->ins[numlines].P1 = 0;                       // with cursor 0
            (*stmt)->ins[numlines].P2 = 0;                       // on the page in register 0
            (*stmt)->ins[numlines].P3 = ncols;                   // having ncols columns
            numlines++;

            // Rewind the B-Tree
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_REWIND;     // Rewind to the beginning of the B-Tree
            (*stmt)->ins[numlines].P1 = 0;                       // using cursor 0
            (*stmt)->ins[numlines].P2 = -1;                      // and if the table is empty, jump to CLOSE
            numlines++;

            // Select columns (0, 1, or 2) from WHERE clause
            for(int i = 0; i < sql_stmt->query.select.where_nconds; i++) {
                // Add first column of WHERE clause (always present)
                int colnum;
                for(int j = 0; j < ncols; j++) {
                    if(!strcpy(sql_stmt->query.select.where_conds[0].op1.name, create_table_stmt->query.createTable.cols[j].name)) {
                        colnum = j;
                        break;
                    }
                }
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_COLUMN;     // Get a column value
                (*stmt)->ins[numlines].P1 = 0;                       // using cursor 0
                (*stmt)->ins[numlines].P2 = colnum;                  // from column number colnum
                (*stmt)->ins[numlines].P3 = ++rmax;                  // into a new register
                numlines++;
                
                // Check if the operation is unary or binary
                if(sql_stmt->query.select.where_conds[0].op != OP_ISNULL && sql_stmt->query.select.where_conds[0].op != OP_ISNOTNULL) {
                    // Store any literals if the operation is binary
                    if(sql_stmt->query.select.where_conds[0].op2Type == OP2_INT) {
                        (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                        (*stmt)->ins[numlines].instruction = DBM_INTEGER;                                // Integer type
                        (*stmt)->ins[numlines].P1 = sql_stmt->query.select.where_conds[0].op2.integer;   // Store the integer
                        (*stmt)->ins[numlines].P2 = 1;                                                   // into register 1
                        numlines++;
                        rmax = 1;
                    } else if(sql_stmt->query.select.where_conds[0].op2Type == OP2_STR) {
                        (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                        (*stmt)->ins[numlines].instruction = DBM_STRING;                                             // String type
                        (*stmt)->ins[numlines].P1 = strlen(sql_stmt->query.select.where_conds[0].op2.string) + 1;    // Store the length
                        (*stmt)->ins[numlines].P2 = 1;                                                               // into register 1
                        (*stmt)->ins[numlines].P4 = sql_stmt->query.select.where_conds[0].op2.string;						     // and keep a ptr
                        numlines++;
                        rmax = 1;
                    } else if(sql_stmt->query.select.where_conds[0].op2Type == OP2_COL) {
                        // Store the second column, if there is one
                        for(int j = 0; j < ncols; j++) {
                            if(!strcmp(sql_stmt->query.select.where_conds[0].op2.col.name, create_table_stmt->query.createTable.cols[j].name)) {
                                colnum = j;
                                break;
                            }
                        }
                        (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                        (*stmt)->ins[numlines].instruction = DBM_COLUMN; // Get a column value
                        (*stmt)->ins[numlines].P1 = 0;                   // using cursor 0
                        (*stmt)->ins[numlines].P2 = colnum;              // into column number colnum
                        (*stmt)->ins[numlines].P3 = ++rmax;              // into a new register
                        numlines++;
                    }
                } else {
                    // Store a null if the operation is unary
                    (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                    (*stmt)->ins[numlines].instruction = DBM_NULL;    // Store a null type
                    (*stmt)->ins[numlines].P2 = ++rmax;               // into a new register
                    numlines++;
                }

                // Store the conditional jump instruction
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                switch(sql_stmt->query.select.where_conds[0].op2Type) {
                    case OP_EQ:
                    case OP_ISNULL:
                        (*stmt)->ins[numlines].instruction = DBM_NE;
                        break;
                    case OP_NE:
                    case OP_ISNOTNULL:
                        (*stmt)->ins[numlines].instruction = DBM_EQ;
                        break; 
                    case OP_LT:
                        (*stmt)->ins[numlines].instruction = DBM_GE;
                        break; 
                    case OP_GT:
                        (*stmt)->ins[numlines].instruction = DBM_LE;
                        break;
                    case OP_LTE:
                        (*stmt)->ins[numlines].instruction = DBM_GT;
                        break; 
                    case OP_GTE:
                        (*stmt)->ins[numlines].instruction = DBM_LT;
                        break;
                }
                (*stmt)->ins[numlines].P1 = rmax - 2;                // Get the register of the first operand
                (*stmt)->ins[numlines].P2 = sql_stmt->query.select.select_ncols + numlines + 1;
                                                                // Get the jump address
                (*stmt)->ins[numlines].P3 = rmax - 1;                // Get the register of the second operand
                numlines++;
            }

            // Select columns in main clause
            for(int i = 0; i < sql_stmt->query.select.select_ncols; i++) {
                // Get the column number
                int colnum;
                for(int j = 0; j < ncols; j++) {
                    if(!strcmp(sql_stmt->query.select.select_cols[i].name, create_table_stmt->query.createTable.cols[j].name)) {
                        colnum = j;
                        break;
                    }
                }
 
                // Store the column value
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_COLUMN;     // Get a column value
                (*stmt)->ins[numlines].P1 = 0;                       // using cursor 0
                (*stmt)->ins[numlines].P2 = colnum;                  // from column number colnum
                (*stmt)->ins[numlines].P3 = ++rmax;                  // into a new register
                numlines++;
            }

            // Get Result Row
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_RESULTROW;                      // Get a result row
            (*stmt)->ins[numlines].P1 = rmax - sql_stmt->query.select.select_ncols;  // Identify start register
            (*stmt)->ins[numlines].P2 = sql_stmt->query.select.select_ncols;         // Identify the number of columns in the result
            numlines++;

            // Set up a jump for multiple result rows (NEXT)
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_NEXT;       // Continue to next result row
            (*stmt)->ins[numlines].P1 = 0;                       // with cursor 0
            (*stmt)->ins[numlines].P2 = 3;                       // return to instruction 3
            numlines++;

            // Close the cursor
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_CLOSE;      // Close the cursor
            (*stmt)->ins[numlines].P1 = 0;                       // number 0

            // Set the jump for REWIND (always indexed at 2)
            (*stmt)->ins[2].P2 = numlines;
            numlines++;

            // Halt execution
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_HALT;       // Halt execution
            (*stmt)->ins[numlines].P1 = 0;                       // with return value 0
            numlines++;

            (*stmt)->num_instructions = numlines;

            break;
        }
        case STMT_INSERT:
        {
            int numlines = 0;
            int rmax = 0;

            // Store the page number
            (*stmt)->ins = malloc(sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_INTEGER;    // Integer type
            (*stmt)->ins[numlines].P1 = root_page;               // Store the root page
            (*stmt)->ins[numlines].P2 = 0;                       // into register 0
            numlines++;

            // Open the B-Tree
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_OPENWRITE;  // Open a B-Tree
            (*stmt)->ins[numlines].P1 = 0;                       // with cursor 0
            (*stmt)->ins[numlines].P2 = 0;                       // on the page in register 0
            (*stmt)->ins[numlines].P3 = ncols;                   // having ncols columns
            numlines++;

            // Store the new record contents into registers
            for(int i = 0; i < sql_stmt->query.insert.nvalues; i++) {
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                switch(sql_stmt->query.insert.values[i].type) {
                    case INS_INT:
                        (*stmt)->ins[numlines].instruction = DBM_INTEGER;    // Store an integer value
                        (*stmt)->ins[numlines].P1 = sql_stmt->query.insert.values[i].val.integer;
                        (*stmt)->ins[numlines].P2 = ++rmax;
                        break;
                    case INS_STR:
                        (*stmt)->ins[numlines].instruction = DBM_STRING;     // Store a string pointer
                        (*stmt)->ins[numlines].P1 = strlen(sql_stmt->query.insert.values[i].val.string) + 1;
                        (*stmt)->ins[numlines].P2 = ++rmax;
                        (*stmt)->ins[numlines].P4 = sql_stmt->query.insert.values[i].val.string;
                        break;
                    case INS_NULL:
                        (*stmt)->ins[numlines].instruction = DBM_NULL;       // Store a null value
                        (*stmt)->ins[numlines].P2 = ++rmax;
                        break;
                }
                numlines++;
            }
            
            // Make a new record
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_MAKERECORD;             // Create a new record
            (*stmt)->ins[numlines].P1 = 1;                                   // beginning with register 1
            (*stmt)->ins[numlines].P2 = rmax - 1;                            // with this many columns
            (*stmt)->ins[numlines].P3 = ++rmax;                              // and store the record in a new register
            numlines++;
            
            // Insert the new record
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_INSERT;                 // Insert the new record
            (*stmt)->ins[numlines].P1 = 0;                                   // using cursor 0
            (*stmt)->ins[numlines].P2 = rmax;                                // with the record in register rmax
            (*stmt)->ins[numlines].P3 = create_table_stmt->query.createTable.pk; // with primary key in column pk in this register
            numlines++;

            // Close the cursor
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_CLOSE;      // Close the cursor
            (*stmt)->ins[numlines].P1 = 0;                       // number 0
            numlines++;

            // Halt execution
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_HALT;       // Halt execution
            (*stmt)->ins[numlines].P1 = 0;                       // with return value 0
            numlines++;

            (*stmt)->num_instructions = numlines;

            break;
        }
    }

	//let the dbm know that we should initialize the dbm for statement
	(*stmt)->initialized_dbm = 0;
	return CHIDB_OK;
}

int chidb_step(chidb_stmt *stmt)
{
	if (stmt->initialized_dbm == 0) {
		//dbm needs to be initialized
		stmt->input_dbm = init_dbm(stmt, 1);
		init_lists(stmt);
        if (stmt->sql->type == STMT_INSERT) {
            for (int i = 0; i < stmt->db->bt->schema_table_size; i++) {
                if (strcmp(stmt->sql->query.insert.table,stmt->db->bt->schema_table[i]->item_name) == 0) {
                    stmt->input_dbm->table_root = stmt->db->bt->schema_table[i]->root_page;
                }
            }
        }
		stmt->initialized_dbm = 1;
	}
	//INSTRUCTION LOOP
	uint32_t result = 0;
	do {
		result = tick_dbm(stmt->input_dbm, *(stmt->ins + stmt->input_dbm->program_counter));
	} while (result == DBM_OK);
	
	if (result == DBM_HALT_STATE) {
		//check which kind of  error has occured
		//TODO: IMPLEMENT ERROR HANDING AND ERROR MESSAGE REPORTING
		return CHIDB_ECONSTRAINT;
	}
	if (result == DBM_RESULT) {
		//we have a result to put together
		generate_result_row(stmt);
		return CHIDB_ROW;
	}
}

int chidb_finalize(chidb_stmt *stmt)
{
    free(stmt->ins);
    free(stmt->sql);
    free(stmt);
	return CHIDB_OK;
}

int chidb_column_count(chidb_stmt *stmt)
{
    switch (stmt->sql->type) {
        case (STMT_SELECT):
            return stmt->sql->query.select.select_ncols;
            break;
        case (STMT_INSERT):
            return stmt->sql->query.insert.nvalues;
            break;

    }
    
	return 0;
}

int chidb_column_type(chidb_stmt *stmt, int col)
{
	return chidb_DBRecord_getType(stmt->record, col);
}

const char *chidb_column_name(chidb_stmt* stmt, int col)
{
    switch(stmt->sql->type) {
        case (STMT_SELECT):
            return (stmt->sql->query.select.select_cols + col)->name;
            break;
    }
	return "";
}

int chidb_column_int(chidb_stmt *stmt, int col)
{
    int retval;
    chidb_DBRecord_getInt32(stmt->record, col, &retval);
    return retval;
}

// TODO: Free the returned string from memory...
const char *chidb_column_text(chidb_stmt *stmt, int col)
{
    char * retval;
    chidb_DBRecord_getString(stmt->record, col, &retval);
	return retval;
}
