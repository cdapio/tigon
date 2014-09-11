/* ------------------------------------------------
Copyright 2014 AT&T Intellectual Property
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ------------------------------------------- */


/*
	MUST COMPILE WITH
		flex -PFtaParser -oftalexer.cc fta.l
	(or equivalent).
*/	

%{
/*
 * AT&T lex can't handle this lexer due to lex bugs.  It works with flex
 * 2.3.7, pclex 2.0.5, and MKS lex 3.1a.
 */

 #include "parse_fta.h"
 #include "parse_schema.h"
 #include <string.h>



#include "fta.tab.cc.h"

/*
	Some includes that flex doesn't include as standard,
	but which are needed.
*/

#include <stdlib.h>
#include <string.h>


//		Prevent flex from defining yywrap as extern "C" 

#define YY_SKIP_YYWRAP

/*		No lex lib, supply the yywrap fcn. that normally resides there
*/

int FtaParserwrap(){return(1);}

extern int FtaParserdebug;


/*
		These variables are used for error reporting:
		flex_fta_lineno : the line currently being parsed when the error occurs.
		flex_fta_ch : the character on the line where the error occurs
		flex_fta_linebuf : store the line for reporting.

		NOTE : 1) the fixed size flex_fta_linebuf buffer is dangerous.
			   2) You might get pointed to a place shortly after
			 	  where the syntax error occurs.  It is close enough
				  for now.
*/

int flex_fta_lineno = 1;
int flex_fta_ch = 0;
char flex_fta_linebuf[200000];

char *flex_fta_stringinput = NULL;
int flex_fta_stringinput_ptr = 0;
FILE *flex_fta_fileinput = NULL;
int my_FtaParser_yyinput(char *buf, int max_size);



void FtaParsererror(char *s){
	int i;
	fprintf(stderr,"On line %d, char %d: %s (token %s):\n%s\n",
				flex_fta_lineno, flex_fta_ch, s, FtaParsertext, flex_fta_linebuf );
    for(i=0;i<flex_fta_ch;i++){
		if(flex_fta_linebuf[i] == '\t'){
			fprintf(stderr,"\t");
		}else{
			fprintf(stderr," ");
		}
	}
	fprintf(stderr,"^\n");
	//	fprintf(stderr,"%*s\n",1+flex_fta_ch,"^");
}

#undef YY_INPUT
#define YY_INPUT(b, r, ms) (r = my_FtaParser_yyinput(b,ms))

%}
	/* MKS needs the next line to increase the NFA table */
%e 1200
%option noyywrap

%%

	/* literal keyword tokens */

 /*
			The actions associated with each text token are to
			keep track of the current location (for syntax error reporting)
			and to report any necessary info to the emf.y parse tree builder

			Its likely that there are a number of omissions, inconsistencies
			(some keywords do not need to be in caps), and relics
			(keywords such as BETWEEN, INDICATOR, etc., are not used
			 in emf.y)
			This parser is somewhat of a work in progress.
 */

 /*		Query keywords		*/

AND|And|and		{ flex_fta_ch+=FtaParserleng; return AND; }
AND_AGGR|And_Aggr|and_aggr		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("AND_AGGR"); return AGGR; }
OR_AGGR|Or_Aggr|or_aggr		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("OR_AGGR"); return AGGR; }
XOR_AGGR|Xor_Aggr|xor_aggr		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("XOR_AGGR"); return AGGR; }
AVG|Avg|avg		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("AVG"); return AGGR; }
MIN|Min|min		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("MIN"); return AGGR; }
MAX|Max|max		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("MAX"); return AGGR; }
SUM|Sum|sum		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("SUM"); return AGGR; }
COUNT|Count|count	{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup("COUNT"); return AGGR; }
BY|By|by		{ flex_fta_ch+=FtaParserleng; return BY; }
FROM|From|from		{ flex_fta_ch+=FtaParserleng; return FROM; }
INNER_JOIN|Inner_Join|inner_join	{ flex_fta_ch+=FtaParserleng; return INNER_JOIN; }
OUTER_JOIN|Outer_Join|outer_join	{ flex_fta_ch+=FtaParserleng; return OUTER_JOIN; }
RIGHT_OUTER_JOIN|Right_Outer_Join|right_outer_join	{ flex_fta_ch+=FtaParserleng; return RIGHT_OUTER_JOIN; }
LEFT_OUTER_JOIN|Left_Outer_Join|left_outer_join	{ flex_fta_ch+=FtaParserleng; return LEFT_OUTER_JOIN; }
FILTER_JOIN|Filter_Join|filter_join	{ flex_fta_ch+=FtaParserleng; return FILTER_JOIN; }
GROUP|Group|group	{ flex_fta_ch+=FtaParserleng; return GROUP; }
ROLLUP|Rollup|rollup	{ flex_fta_ch+=FtaParserleng; return ROLLUP; }
CUBE|Cube|cube	{ flex_fta_ch+=FtaParserleng; return CUBE; }
GROUPING_SETS|Grouping_Sets|grouping_sets	{ flex_fta_ch+=FtaParserleng; return GROUPING_SETS; }
SUPERGROUP|Supergroup|supergroup	{ flex_fta_ch+=FtaParserleng; return SUPERGROUP; }
CLEANING_WHEN|Cleaning_When|cleaning_when	{ flex_fta_ch+=FtaParserleng; return CLEANING_WHEN; }
CLEANING_BY|Cleaning_By|cleaning_by	{ flex_fta_ch+=FtaParserleng; return CLEANING_BY; }
CLOSING_WHEN|Closing_When|closing_when	{ flex_fta_ch+=FtaParserleng; return CLOSING_WHEN; }
HAVING|Having|having	{ flex_fta_ch+=FtaParserleng; return HAVING; }
AS|As|as		{ flex_fta_ch+=FtaParserleng; return AS; }
IN|In|in		{ flex_fta_ch+=FtaParserleng; return IN; }
NOT|Not|not		{ flex_fta_ch+=FtaParserleng; return NOT; }
OR|Or|or	{ flex_fta_ch+=FtaParserleng; return OR; }

SELECT|Select|select		{ flex_fta_ch+=FtaParserleng; return SELECT; }
WHERE|Where|where		{ flex_fta_ch+=FtaParserleng; return WHERE; }
SUCH|Such|such	{ flex_fta_ch+=FtaParserleng; return SUCH;}
THAT|That|that	{ flex_fta_ch+=FtaParserleng; return THAT;}
MERGE|Merge|merge	 {flex_fta_ch+=FtaParserleng; return MERGE;}
SLACK	 {flex_fta_ch+=FtaParserleng; return SLACK;}

TRUE	{ flex_fta_ch+=FtaParserleng; return TRUE_V;}
FALSE	{ flex_fta_ch+=FtaParserleng; return FALSE_V;}
TIMEVAL	{ flex_fta_ch+=FtaParserleng; return TIMEVAL_L;}
HEX	{ flex_fta_ch+=FtaParserleng; return HEX_L;}
LHEX	{ flex_fta_ch+=FtaParserleng; return LHEX_L;}
IP_VAL	{ flex_fta_ch+=FtaParserleng; return IP_L;}
IPV6_VAL|IPv6_VAL	{ flex_fta_ch+=FtaParserleng; return IPV6_L;}

DEFINE	{ flex_fta_ch+=FtaParserleng; return DEFINE_SEC;}
PARAM	{ flex_fta_ch+=FtaParserleng; return PARAM_SEC;}

[\{]	{flex_fta_ch+=FtaParserleng; return LEFTBRACE;}
[\}]	{flex_fta_ch+=FtaParserleng; return RIGHTBRACE;}

 /*
		Table definition keywords
 */		
TABLE		{ flex_fta_ch+=FtaParserleng; return TABLE; }
PROTOCOL		{ flex_fta_ch+=FtaParserleng; return PROTOCOL; }
STREAM		{ flex_fta_ch+=FtaParserleng; return STREAM; }
FTA			{ flex_fta_ch+=FtaParserleng; return FTA; }
UNPACK_FCNS			{ flex_fta_ch+=FtaParserleng; return UNPACK_FCNS; }

OPERATOR			{ flex_fta_ch+=FtaParserleng; return OPERATOR; }
OPERATOR_VIEW			{ flex_fta_ch+=FtaParserleng; return OPERATOR_VIEW; }
FIELDS			{ flex_fta_ch+=FtaParserleng; return FIELDS; }
SUBQUERIES			{ flex_fta_ch+=FtaParserleng; return SUBQUERIES; }
SELECTION_PUSHDOWN			{ flex_fta_ch+=FtaParserleng; return SELECTION_PUSHDOWN; }
;	{flex_fta_ch+=FtaParserleng; return SEMICOLON;}


	/* punctuation */
"<<"	|
">>"			{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup(yytext); return SHIFT_OP; }

"="	|
"<>" 	|
"<"	|
">"	|
"<="	|
">="		{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup(yytext); return COMPARISON; }

[%&|!+*/:(),.\[\]$@#]	{ flex_fta_ch+=FtaParserleng; return yytext[0]; }
"-"	{ flex_fta_ch+=FtaParserleng; return yytext[0]; }
"~"	{ flex_fta_ch+=FtaParserleng; return yytext[0]; }

	/* names */

[A-Za-z_][A-Za-z0-9_]*	{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup(yytext); return NAME; }

	/* numbers */

[0-9]+	|
[0-9]+UL	{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup(yytext);  return INTNUM; }

[0-9]+ULL	{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup(yytext);  return LONGINTNUM; }

[0-9]+"."[0-9]* |
"."[0-9]*	|
[0-9]+[eE][+-]?[0-9]+	|
[0-9]+"."[0-9]*[eE][+-]?[0-9]+ |
"."[0-9]*[eE][+-]?[0-9]+	{ flex_fta_ch+=FtaParserleng; FtaParserlval.strval = strdup(yytext); return APPROXNUM; }

	/* strings */

'[^'\n]*'	{
		
		int c;
		
		FtaParserlval.strval = strdup(FtaParsertext+1); 

		c = yyinput();

		unput(c);	/* just peeking */
		if(c != '\'') {
			flex_fta_ch+=FtaParserleng; 
			FtaParserlval.strval[FtaParserleng-2] = '\0';
			return STRING_TOKEN;
		} else
			yymore();
	}
		
'[^'\n]*$	{ flex_fta_ch+=FtaParserleng; FtaParsererror("Unterminated string"); }

 /*															*/
 /*		Newline : advance the error reporting line number	*/
 /*		and grab the next line into flex_fta_linebuf			*/
 /*															*/

\n.*		{flex_fta_ch=0; flex_fta_lineno++;
			   strcpy(flex_fta_linebuf,FtaParsertext+1);
			   yyless(1);
			   }

[ \t\r]+	{flex_fta_ch+=FtaParserleng; }	/* white space */

"--".*$		{flex_fta_ch+=FtaParserleng; };	/* comment */
"//".*$		{flex_fta_ch+=FtaParserleng; };	/* comment */

.|\n            {flex_fta_ch+=FtaParserleng; fprintf(stderr,"Warning: unknown token (ignored)\n");  FtaParsererror(yytext);}

%%

int my_FtaParser_yyinput(char *buf, int max_size){
	int c = 0;
	int inchar = 0;
	
	if(flex_fta_stringinput != NULL){
		while(c<max_size){
			if(flex_fta_stringinput[flex_fta_stringinput_ptr] != '\0'){
				buf[c++] = flex_fta_stringinput[flex_fta_stringinput_ptr++];
			}else{
				break;
			}
		}
		return(c);
	}
	
	if(flex_fta_fileinput != NULL){
		while(c < max_size){
			inchar = getc(flex_fta_fileinput);
			if(inchar != EOF){
				buf[c++] = inchar;
			}else{
				break;
			}
		}
		return(c);
	}
	
	return(0);
}

void FtaParser_setfileinput(FILE *f){
	FtaParserrestart(NULL);

	flex_fta_fileinput = f;
	flex_fta_stringinput = NULL;
	flex_fta_lineno = 1;
	flex_fta_ch = 0;
}

void FtaParser_setstringinput(char *s){
	FtaParserrestart(NULL);

	flex_fta_fileinput = NULL;
	flex_fta_stringinput = s;
	flex_fta_stringinput_ptr = 0;
	flex_fta_lineno = 1;
	flex_fta_ch = 0;
}
	
		


