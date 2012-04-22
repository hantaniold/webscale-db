%{
#include <stdio.h>
#include <string.h>
#include "sql.tab.h"
%}

%option nounput
%option noyywrap
%option yylineno
%option case-insensitive

%%

SELECT                  {return TK_SELECT;}
FROM                    {return TK_FROM;}
WHERE                   {return TK_WHERE;}

INSERT                  {return TK_INSERT;}
INTO                    {return TK_INTO;}
VALUES                  {return TK_VALUES;}

CREATE                  {return TK_CREATE;}
TABLE                   {return TK_TABLE;}
BYTE                    {return TK_BYTE;}
SMALLINT                {return TK_SMALLINT;}
INTEGER                 {return TK_INTEGER;}
TEXT                    {return TK_TEXT;}
PRIMARY                 {return TK_PRIMARY;}
KEY                     {return TK_KEY;}

INDEX                   {return TK_INDEX;}
ON                      {return TK_ON;}

EXPLAIN                 {return TK_EXPLAIN;}

\*                      {return TK_STAR;}
\(                      {return TK_LPAREN;}
\)                      {return TK_RPAREN;}
;                       {return TK_SEMICOLON;}
\.                       {return TK_DOT;}
,                       {return TK_COMMA;}

AND                     {return TK_AND;}

\<=                      {return TK_LTE;}
\>=                      {return TK_GTE;}
\<\>                      {return TK_NE;}
\>                       {return TK_GT;}
\<                       {return TK_LT;}
=                       {return TK_EQ;}
IS                       {return TK_IS;}
NOT                       {return TK_NOT;}

NULL                    {return TK_NULL;}

[a-z][a-z0-9]* 	{
		    yylval.string = (char *) strdup(yytext); 
		    return TK_ID;
		}

[0-9]+  {
          yylval.integer = atoi(yytext); 
          return TK_INT;
        }

\"[^\"]*\"	{
		    yylval.string=malloc(strlen(yytext)-1);
		    strncpy(yylval.string,yytext+1,strlen(yytext)-2); 
		    yylval.string[strlen(yytext)-2]='\0';
		    return TK_STRING;
                }

\n                      
[ \t]+                  
%%
