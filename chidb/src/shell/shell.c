/*****************************************************************************
 *
 *																 chidb
 *
 * This module provides a simple chidb shell.
 *
 * This shell assumes a complete implementation of the chidb API
 * is available. If so, provides a basic SQL shell.
 *
 * 2009, 2010 Borja Sotomayor - http://people.cs.uchicago.edu/~borja/
\*****************************************************************************/

#include <histedit.h>
#include <chidb.h>

#define COL_SEPARATOR "|"

char * prompt(EditLine *e) 
{
	return "chidb> ";
}

int main(int argc, char *argv[]) 
{
	EditLine *el;
	History *hist;
	chidb *db;
	int rc;

	HistEvent ev;

	if (argc != 2)
	{
		fprintf(stderr, "ERROR: Must specify a database file.\n");
		return 1;
	}

	rc = chidb_open(argv[1], &db); 
	
	if (rc != CHIDB_OK)
	{
		fprintf(stderr, "ERROR: Could not open file %s or file is not well formed.\n", argv[1]);
		return 1;
	}

	/* Initialize EditLine */    
	el = el_init(argv[0], stdin, stdout, stderr);
	el_set(el, EL_PROMPT, &prompt);
	el_set(el, EL_EDITOR, "emacs");

	/* Initialize the history */
  	hist = history_init();
  	if (hist == 0) 
  	{
    	fprintf(stderr, "ERROR: Could not initialize history.\n");
    	return 1;
	}
	history(hist, &ev, H_SETSIZE, 100); // 100 elements in history
	el_set(el, EL_HIST, history, hist); // history callback

	while (1) 
	{
		int count;
		const char *sql;
		chidb_stmt *stmt;
		
    	sql = el_gets(el, &count);

    	if (count == 1)
    		break;
    	else
    	{
		    history(hist, &ev, H_ENTER, sql); // Add to history
		    
			rc = chidb_prepare(db, sql, &stmt);
			
    		if (rc == CHIDB_OK)
    		{
    			int numcol = chidb_column_count(stmt);
    			for(int i = 0; i < numcol; i ++)
    			{
    				printf(i==0?"":COL_SEPARATOR);
    				printf("%s", chidb_column_name(stmt,i));    				
    			}
    			printf("\n");
    			
    			while((rc = chidb_step(stmt)) == CHIDB_ROW)
    			{
    				for(int i = 0; i < numcol; i++)
    				{
    					printf(i==0?"":COL_SEPARATOR);
    					switch(chidb_column_type(stmt,i))
    					{
    						case SQL_NULL:
    							break;
    						case SQL_INTEGER_1BYTE: case SQL_INTEGER_2BYTE:	case SQL_INTEGER_4BYTE:
    							printf("%i", chidb_column_int(stmt,i));
    							break;
    						case SQL_TEXT:
    							printf("%s", chidb_column_text(stmt,i));
    							break;
    					}
    				}
    				printf("\n");
    			}
    			
    			switch(rc)
    			{
    				case CHIDB_ECONSTRAINT:
    					printf("ERROR: SQL statement failed because of a constraint violation.\n");
    					break;
    				case CHIDB_EMISMATCH:
    					printf("ERROR: Data type mismatch.\n");
    					break;
    				case CHIDB_EMISUSE:
    					printf("ERROR: API used incorrectly.\n");
    					break;
    				case CHIDB_EIO:
    					printf("ERROR: An I/O error has occurred when accessing the file.\n");
    					break;
    			}

				rc = chidb_finalize(stmt);
				if(rc == CHIDB_EMISUSE)
					printf("API used incorrectly.\n");
    		}
			else if (rc == CHIDB_EINVALIDSQL)
				printf("SQL syntax error.\n");
			else if (rc == CHIDB_ENOMEM)
				printf("ERROR: Could not allocate memory.\n");
		}
	}
  

	history_end(hist);
	el_end(el);

	return 0;
}

