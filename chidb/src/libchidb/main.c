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
    int pk;
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

            pk = create_table_stmt->query.createTable.pk; 

            if(sql_stmt->query.select.select_ncols != SELECT_ALL) {
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
            } else {
                ncols = create_table_stmt->query.createTable.ncols;
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
            //if(!schema_row)
                //return CHIDB_EINVALIDSQL;

            // Check that the right number of values are being inserted
            chidb_parser(schema_row->sql, &create_table_stmt);
            
            pk = create_table_stmt->query.createTable.pk;

            //if(sql_stmt->query.insert.nvalues != create_table_stmt->query.createTable.ncols)
                //return CHIDB_EINVALIDSQL;

            ncols = create_table_stmt->query.createTable.ncols;
            
            // Check value types
            for(int i = 0; i < sql_stmt->query.insert.nvalues; i++) {
                //if(sql_stmt->query.insert.values[i].type != create_table_stmt->query.createTable.cols[i].type)
                    //return CHIDB_EINVALIDSQL;
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
    (*stmt)->create_table = create_table_stmt;

    // Initialize the table list struct
    table_l *tablelist = malloc(sizeof(table_l));
    if(sql_stmt->type == STMT_SELECT) {
        tablelist->num_tables = sql_stmt->query.select.from_ntables;
        tablelist->num_cols = 0;
        tablelist->tables = calloc(sql_stmt->query.select.from_ntables, sizeof(tabledata));

        // Add table data to the table list
        for(int i = 0; i < tablelist->num_tables; i++) {
            SchemaTableRow *sr = NULL;
            tablelist->tables[i].name = sql_stmt->query.select.from_tables[i];
            for(int j = 0; j < db->bt->schema_table_size; j++) {
                if(!strcmp(tablelist->tables[i].name, db->bt->schema_table[j]->item_name)) {
                    sr = db->bt->schema_table[j];
                    tablelist->tables[i].root = sr->root_page;
                    chidb_parser(sr->sql, &(tablelist->tables[i].create));
                    tablelist->tables[i].num_cols = tablelist->tables[i].create->query.createTable.ncols;
                    tablelist->tables[i].pk = tablelist->tables[i].create->query.createTable.pk;
                    tablelist->num_cols += tablelist->tables[i].create->query.createTable.ncols;
                    break;
                }
            }
            //if(sr)
            //    break;
        }
    }
    
    //set the table_l of the statement for use by the dbm
    (*stmt)->table_list = tablelist;

    // Sort the table struct
    // TODO

    switch(sql_stmt->type) {
        case STMT_SELECT:
        {
            int numlines = 0;
            int rmax = 0;
            (*stmt)->ins = NULL;

            for(int t = 0; t < tablelist->num_tables; t++) {
                // Store the page number
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_INTEGER;     // Integer type
                (*stmt)->ins[numlines].P1 = tablelist->tables[t].root;// Store the root page
                (*stmt)->ins[numlines].P2 = t;                        // into register t
                numlines++;

                // Open the B-Tree
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_OPENREAD;          // Open a B-Tree
                (*stmt)->ins[numlines].P1 = t;                              // with cursor t
                (*stmt)->ins[numlines].P2 = t;                              // on the page in register t
                (*stmt)->ins[numlines].P3 = tablelist->tables[t].num_cols;  // having num_cols columns
                numlines++;

                // Rewind the B-Tree
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_REWIND;     // Rewind to the beginning of the B-Tree
                (*stmt)->ins[numlines].P1 = t;                       // using cursor t
                (*stmt)->ins[numlines].P2 = -1;                      // and if the table is empty, jump to CLOSE
                numlines++;
            }

            int nextjmp = numlines;

            // Select columns (0, 1, or 2) from WHERE clause
            for(int i = 0; i < sql_stmt->query.select.where_nconds; i++) {
               int isPK = 0;

               for(int t = 0; t < tablelist->num_tables; t++) {
                    if(!strcmp(sql_stmt->query.select.where_conds[i].op1.name, tablelist->tables[t].create->query.createTable.cols[tablelist->tables[t].pk].name)) {
                        (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                        (*stmt)->ins[numlines].instruction = DBM_KEY;   // Get a primary key value
                        (*stmt)->ins[numlines].P1 = t;                  // using cursor t
                        (*stmt)->ins[numlines].P2 = ++rmax;             // into a new register
                        numlines++;
                        isPK = 1;
                    }
               }

               if(!isPK) {
                    // Add first column of WHERE clause (always present)
                    for(int t = 0; t < tablelist->num_tables; t++) {
                        for(int c = 0; c < tablelist->tables[t].num_cols; c++) {
                            if(!strcmp(sql_stmt->query.select.where_conds[i].op1.name, tablelist->tables[t].create->query.createTable.cols[c].name)) {
                                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                                (*stmt)->ins[numlines].instruction = DBM_COLUMN;    // Get a column value
                                (*stmt)->ins[numlines].P1 = t;                      // Using cursor t
                                (*stmt)->ins[numlines].P2 = c;                      // from column c
                                (*stmt)->ins[numlines].P3 = ++rmax;                 // into a new register
                                numlines++;
                                break;
                            }
                        }
                    }
               }

                // Check if the operation is unary or binary
                if(sql_stmt->query.select.where_conds[i].op != OP_ISNULL && sql_stmt->query.select.where_conds[i].op != OP_ISNOTNULL) {
                    // Store any literals if the operation is binary
                    if(sql_stmt->query.select.where_conds[i].op2Type == OP2_INT) {
                        (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                        (*stmt)->ins[numlines].instruction = DBM_INTEGER;                                // Integer type
                        (*stmt)->ins[numlines].P1 = sql_stmt->query.select.where_conds[i].op2.integer;   // Store the integer
                        (*stmt)->ins[numlines].P2 = ++rmax;                                              // into a new register
                        numlines++;
                    } else if(sql_stmt->query.select.where_conds[i].op2Type == OP2_STR) {
                        (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                        (*stmt)->ins[numlines].instruction = DBM_STRING;                                             // String type
                        (*stmt)->ins[numlines].P1 = strlen(sql_stmt->query.select.where_conds[i].op2.string) + 1;    // Store the length
                        (*stmt)->ins[numlines].P2 = ++rmax;                                                          // into a new register
                        (*stmt)->ins[numlines].P4 = sql_stmt->query.select.where_conds[i].op2.string;		         // and keep a ptr
                        numlines++;
                    } else if(sql_stmt->query.select.where_conds[i].op2Type == OP2_COL) {
                        // Store the second column, if there is one
                        for(int t = 0; t < tablelist->num_tables; t++) {
                            for(int c = 0; c < tablelist->tables[t].num_cols; c++) {
                                if(!strcmp(sql_stmt->query.select.where_conds[i].op2.col.name, tablelist->tables[t].create->query.createTable.cols[c].name)) {
                                    (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                                    (*stmt)->ins[numlines].instruction = DBM_COLUMN;    // Get a column value
                                    (*stmt)->ins[numlines].P1 = t;                      // using cursor t
                                    (*stmt)->ins[numlines].P2 = c;                      // from column c
                                    (*stmt)->ins[numlines].P3 = ++rmax;                 // into a new register
                                    numlines++;
                                    break;
                                }
                            }
                        }
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
                switch(sql_stmt->query.select.where_conds[i].op) {
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
                (*stmt)->ins[numlines].P1 = rmax - 1;                // Get the register of the first operand
                (*stmt)->ins[numlines].P2 = 0;                       // Placeholder for jump address (set later)
                (*stmt)->ins[numlines].P3 = rmax - 0;                // Get the register of the second operand
                numlines++;
            }

            // Select columns in main clause
            int num_cols;
            if(sql_stmt->query.select.select_ncols != SELECT_ALL) {
                for(int t = 0; t < tablelist->num_tables; t++) {
                    tablelist->tables[t].start_reg = rmax + 1;

                    for(int i = 0; i < sql_stmt->query.select.select_ncols; i++) {
                        if(!strcmp(sql_stmt->query.select.select_cols[i].name, tablelist->tables[t].create->query.createTable.cols[tablelist->tables[t].pk].name)) {
                            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                            (*stmt)->ins[numlines].instruction = DBM_KEY;   // Get a primary key value
                            (*stmt)->ins[numlines].P1 = t;                  // Using cursor t
                            (*stmt)->ins[numlines].P2 = ++rmax;             // into a new register
                            tablelist->tables[t].num_cols_selected++;
                            numlines++;
                            continue;
                        }

                        for(int c = 0; c < tablelist->tables[t].num_cols; c++) {
                            if(!strcmp(sql_stmt->query.select.select_cols[i].name, tablelist->tables[t].create->query.createTable.cols[c].name) && c != tablelist->tables[t].pk) {
                                // Store the column value
                                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                                (*stmt)->ins[numlines].instruction = DBM_COLUMN;    // Get a column value
                                (*stmt)->ins[numlines].P1 = t;                      // using cursor t
                                (*stmt)->ins[numlines].P2 = c;                      // from column c
                                (*stmt)->ins[numlines].P3 = ++rmax;                 // into a new register
                                tablelist->tables[t].num_cols_selected++;
                                numlines++;
                                break;
                            }
                        }
                    }
                }
            } else {
                // Handle "SELECT *"
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + tablelist->num_cols) * sizeof(chidb_instruction));
                for(int t = 0; t < tablelist->num_tables; t++) {
                    tablelist->tables[t].start_reg = rmax + 1;
                    tablelist->tables[t].num_cols_selected = tablelist->tables[t].create->query.createTable.ncols;
                    for(int c = 0; c < tablelist->tables[t].num_cols; c++) {
                        if(c == tablelist->tables[t].pk) {
                            (*stmt)->ins[numlines].instruction = DBM_KEY;   // Get a primary key value
                            (*stmt)->ins[numlines].P1 = t;                  // using cursor t
                            (*stmt)->ins[numlines].P2 = ++rmax;             // into a new register

                            numlines++;
                            continue;
                        }

                        // Store the column value
                        (*stmt)->ins[numlines].instruction = DBM_COLUMN;    // Get a column value
                        (*stmt)->ins[numlines].P1 = t;                      // using cursor t
                        (*stmt)->ins[numlines].P2 = c;                      // from column c
                        (*stmt)->ins[numlines].P3 = ++rmax;                 // into a new register
                        numlines++;
                    }
                }
            }

            for(int t = 0; t < tablelist->num_tables; t++) {
                // Get Result Row
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_RESULTROW;                 // Get a result row
                (*stmt)->ins[numlines].P1 = tablelist->tables[t].start_reg;         // Identify start register
                (*stmt)->ins[numlines].P2 = tablelist->tables[t].num_cols_selected; // Identify the number of columns in the result
                numlines++;
            }

            // Set up a jump for multiple result rows (NEXT)
            for(int t = tablelist->num_tables - 1; t >= 0; t--) {
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_NEXT;  // Continue to next result row
                (*stmt)->ins[numlines].P1 = t;                  // with cursor t
                (*stmt)->ins[numlines].P2 = nextjmp;            // return to instruction nextjmp
                numlines++;
            }

            // Update any conditional jumps to point to this previous NEXT instruction
            if(sql_stmt->query.select.where_nconds > 0) {
                for(int i = 0; i < numlines; i++) {
                    if((*stmt)->ins[i].instruction == DBM_EQ ||
                       (*stmt)->ins[i].instruction == DBM_NE ||
                       (*stmt)->ins[i].instruction == DBM_LT ||
                       (*stmt)->ins[i].instruction == DBM_LE ||
                       (*stmt)->ins[i].instruction == DBM_GT ||
                       (*stmt)->ins[i].instruction == DBM_GE) {
                        (*stmt)->ins[i].P2 = numlines - 1;
                    }
                }
            }

            // Close the cursor
            for(int t = 0; t < tablelist->num_tables; t++) {
                (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
                (*stmt)->ins[numlines].instruction = DBM_CLOSE;      // Close the cursor
                (*stmt)->ins[numlines].P1 = t;                       // number t
                numlines++;
            }

            // Set the jump for REWIND
            for(int i = 0; i < numlines; i++) {
                if((*stmt)->ins[i].instruction == DBM_REWIND)
                    (*stmt)->ins[i].P2 = numlines - 1;
            }

            // Halt execution
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_HALT;       // Halt execution
            (*stmt)->ins[numlines].P1 = 0;                       // with return value 0
            numlines++;

            (*stmt)->num_instructions = numlines;

            for(int i = 0; i < (*stmt)->num_instructions; i++) {
                printf("Instruction: %i      Arguments: %i %i %i %i\n", (*stmt)->ins[i].instruction, (*stmt)->ins[i].P1, (*stmt)->ins[i].P2, (*stmt)->ins[i].P3, (*stmt)->ins[i].P4);
            }

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
            (*stmt)->ins[numlines].P2 = rmax;                                // with this many columns
            (*stmt)->ins[numlines].P3 = ++rmax;                              // and store the record in a new register
            numlines++;
            
            // Insert the new record
            (*stmt)->ins = realloc((*stmt)->ins, (numlines + 1) * sizeof(chidb_instruction));
            (*stmt)->ins[numlines].instruction = DBM_INSERT;                 // Insert the new record
            (*stmt)->ins[numlines].P1 = 0;                                   // using cursor 0
            (*stmt)->ins[numlines].P2 = rmax;                                // with the record in register rmax
            (*stmt)->ins[numlines].P3 = create_table_stmt->query.createTable.pk+1; // with primary key in column pk in this register
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
						/*
            for(int i = 0; i < (*stmt)->num_instructions; i++) {
                printf("Instruction: %i      Arguments: %i %i %i %i\n", (*stmt)->ins[i].instruction, (*stmt)->ins[i].P1, (*stmt)->ins[i].P2, (*stmt)->ins[i].P3, (*stmt)->ins[i].P4);
            }
            */

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
		stmt->input_dbm = init_dbm(stmt, 1, 0);
		init_lists(stmt);	
		stmt->initialized_dbm = 1;
	}
	
	//DEPRECATED
	//stmt->input_dbm->create_table = stmt->create_table;
	stmt->input_dbm->table_list = stmt->table_list;
	
  if (stmt->sql->type == STMT_INSERT) {
  	for (int i = 0; i < stmt->db->bt->schema_table_size; i++) {
    	if (strcmp(stmt->sql->query.insert.table,stmt->db->bt->schema_table[i]->item_name) == 0) {
      	stmt->input_dbm->table_root = stmt->db->bt->schema_table[i]->root_page;
      }
    }
  }
	//INSTRUCTION LOOP
	uint32_t result = 0;
	do {
		result = tick_dbm(stmt->input_dbm, *(stmt->ins + stmt->input_dbm->program_counter));
	} while (result == DBM_OK);
	
	if (result == DBM_HALT_STATE) {
		uint32_t tr = stmt->input_dbm->tick_result;
		if (tr == DBM_OK) {
            return CHIDB_DONE;
    }else {
    	if (tr == CHIDB_EDUPLICATE) {
				return DBM_DUPLICATE_KEY;
			}
			if (tr == CHIDB_ENOMEM) {
				return DBM_MEMORY_ERROR;
			}
			if (tr == CHIDB_EIO) {
				return DBM_IO_ERROR;
			}
    	if (tr == DBM_INVALID_INSTRUCTION) {
    		return CHIDB_EMISUSE;
    	}
    	if (tr == DBM_IO_ERROR || tr == DBM_OPENRW_ERROR || tr == DBM_MEMORY_FREE_ERROR || tr == DBM_MEMORY_ERROR || tr == DBM_CELL_NUMBER_BOUNDS) {
    		return CHIDB_EIO;
    	}
    	if (tr == DBM_REGISTER_TYPE_MISMATCH || tr == DBM_DATA_REGISTER_LENGTH_MISMATCH || tr == DBM_INVALID_TYPE) {
    		return CHIDB_EMISMATCH;
    	}
    	if (tr == DBM_DUPLICATE_KEY) {
    		return CHIDB_ECONSTRAINT;
    	}
    	return CHIDB_ECONSTRAINT;
    }
	}
	if (result == DBM_RESULT) {
		//we have a result to put together
		generate_result_row(stmt);
		
		return CHIDB_ROW;
	}
}

int chidb_finalize(chidb_stmt *stmt)
{
	reset_dbm(stmt->input_dbm);
  clear_lists(stmt->input_dbm);
  free(stmt->input_dbm);
  free(stmt->ins);
  free(stmt->sql);
  free(stmt);
	return CHIDB_OK;
}

int chidb_column_count(chidb_stmt *stmt)
{
    switch (stmt->sql->type) {
        case (STMT_SELECT):
            return (stmt->sql->query.select.select_ncols == SELECT_ALL) ? stmt->table_list->num_cols : stmt->sql->query.select.select_ncols;
            //return (stmt->sql->query.select.select_ncols == SELECT_ALL) ? stmt->create_table->query.createTable.ncols : stmt->sql->query.select.select_ncols;
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
            if(stmt->sql->query.select.select_ncols != SELECT_ALL) {
                return (stmt->sql->query.select.select_cols + col)->name;
            } else {
                int res = 0;
                int t = 0;
                while(res <= col) {
                    if(res + stmt->table_list->tables[t].num_cols > col) {
                        return stmt->table_list->tables[t].create->query.createTable.cols[col - res].name;
                    } else {
                        res += stmt->table_list->tables[t].num_cols;
                        t++;
                    }
                }
                //return stmt->create_table->query.createTable.cols[col].name;
            }
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
    int *len = (int *)malloc(sizeof(int));
    chidb_DBRecord_getStringLength(stmt->record, col, len);
    retval = (char *)malloc(sizeof(char) * (*len));
    chidb_DBRecord_getString(stmt->record, col, &retval);
		return retval;
}
