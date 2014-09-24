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
		flex -PIfqParser -oifqlexer.cc ifq.l
	(or equivalent).
*/	

%{
/*
 * AT&T lex can't handle this lexer due to lex bugs.  It works with flex
 * 2.3.7, pclex 2.0.5, and MKS lex 3.1a.
 */

 #include "parse_fta.h"
 #include <string.h>



#include "ifq.tab.cc.h"

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

int IfqParserwrap(){return(1);}

extern int IfqParserdebug;


/*
		These variables are used for error reporting:
		flex_ifq_lineno : the line currently being parsed when the error occurs.
		flex_ifq_ch : the character on the line where the error occurs
		flex_ifq_linebuf : store the line for reporting.

		NOTE : 1) the fixed size flex_ifq_linebuf buffer is dangerous.
			   2) You might get pointed to a place shortly after
			 	  where the syntax error occurs.  It is close enough
				  for now.
*/

int flex_ifq_lineno = 1;
int flex_ifq_ch = 0;
char flex_ifq_linebuf[20000];

char *flex_ifq_stringinput = NULL;
int flex_ifq_stringinput_ptr = 0;
FILE *flex_ifq_fileinput = NULL;
int my_IfqParser_yyinput(char *buf, int max_size);



void IfqParsererror(char *s){
	int i;
	fprintf(stderr,"On line %d, char %d: %s (token %s):\n%s\n",
				flex_ifq_lineno, flex_ifq_ch, s, IfqParsertext, flex_ifq_linebuf );
    for(i=0;i<flex_ifq_ch;i++){
		if(flex_ifq_linebuf[i] == '\t'){
			fprintf(stderr,"\t");
		}else{
			fprintf(stderr," ");
		}
	}
	fprintf(stderr,"^\n");
	//	fprintf(stderr,"%*s\n",1+flex_ifq_ch,"^");
}

#undef YY_INPUT
#define YY_INPUT(b, r, ms) (r = my_IfqParser_yyinput(b,ms))

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

AND|And|and		{ flex_ifq_ch+=IfqParserleng; return AND; }
NOT|Not|not		{ flex_ifq_ch+=IfqParserleng; return NOT; }
OR|Or|or	{ flex_ifq_ch+=IfqParserleng; return OR; }

TRUE	{ flex_ifq_ch+=IfqParserleng; return TRUE_V;}
FALSE	{ flex_ifq_ch+=IfqParserleng; return FALSE_V;}

Contains|Equals|Exists	{flex_ifq_ch+=IfqParserleng; IfqParserlval.strval = strdup(yytext); return PRED; }

;		{flex_ifq_ch+=IfqParserleng; return SEMICOLON;}



[&|!+*/:(),.\[\]$]	{ flex_ifq_ch+=IfqParserleng; return yytext[0]; }
"-"	{ flex_ifq_ch+=IfqParserleng; return yytext[0]; }
"~"	{ flex_ifq_ch+=IfqParserleng; return yytext[0]; }

	/* names */

[A-Za-z_][A-Za-z0-9_]*	{ flex_ifq_ch+=IfqParserleng; IfqParserlval.strval = strdup(yytext); return NAME; }

	/* numbers */

[0-9]+	|
[0-9]+UL	{ flex_ifq_ch+=IfqParserleng; IfqParserlval.strval = strdup(yytext);  return INTNUM; }

[0-9]+ULL	{ flex_ifq_ch+=IfqParserleng; IfqParserlval.strval = strdup(yytext);  return LONGINTNUM; }

[0-9]+"."[0-9]* |
"."[0-9]*	|
[0-9]+[eE][+-]?[0-9]+	|
[0-9]+"."[0-9]*[eE][+-]?[0-9]+ |
"."[0-9]*[eE][+-]?[0-9]+	{ flex_ifq_ch+=IfqParserleng; IfqParserlval.strval = strdup(yytext); return APPROXNUM; }

	/* strings */

'[^'\n]*'	{
		
		int c;
		
		IfqParserlval.strval = strdup(IfqParsertext+1); 

		c = yyinput();

		unput(c);	/* just peeking */
		if(c != '\'') {
			flex_ifq_ch+=IfqParserleng; 
			IfqParserlval.strval[IfqParserleng-2] = '\0';
			return STRING_TOKEN;
		} else
			yymore();
	}
		
'[^'\n]*$	{ flex_ifq_ch+=IfqParserleng; IfqParsererror("Unterminated string"); }

 /*															*/
 /*		Newline : advance the error reporting line number	*/
 /*		and grab the next line into flex_ifq_linebuf			*/
 /*															*/

\n.*		{flex_ifq_ch=0; flex_ifq_lineno++;
			   strcpy(flex_ifq_linebuf,IfqParsertext+1);
			   yyless(1);
			   }

[ \t\r]+	{flex_ifq_ch+=IfqParserleng; }	/* white space */

"--".*$		{flex_ifq_ch+=IfqParserleng; };	/* comment */
"//".*$		{flex_ifq_ch+=IfqParserleng; };	/* comment */

.|\n            {flex_ifq_ch+=IfqParserleng; fprintf(stderr,"Warning: unknown token (ignored)\n");  IfqParsererror(yytext);}

%%

int my_IfqParser_yyinput(char *buf, int max_size){
	int c = 0;
	int inchar = 0;
	
	if(flex_ifq_stringinput != NULL){
		while(c<max_size){
			if(flex_ifq_stringinput[flex_ifq_stringinput_ptr] != '\0'){
				buf[c++] = flex_ifq_stringinput[flex_ifq_stringinput_ptr++];
			}else{
				break;
			}
		}
		return(c);
	}
	
	if(flex_ifq_fileinput != NULL){
		while(c < max_size){
			inchar = getc(flex_ifq_fileinput);
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

void IfqParser_setfileinput(FILE *f){
	IfqParserrestart(NULL);

	flex_ifq_fileinput = f;
	flex_ifq_stringinput = NULL;
	flex_ifq_lineno = 1;
	flex_ifq_ch = 0;
}

void IfqParser_setstringinput(char *s){
	IfqParserrestart(NULL);

	flex_ifq_fileinput = NULL;
	flex_ifq_stringinput = s;
	flex_ifq_stringinput_ptr = 0;
	flex_ifq_lineno = 1;
	flex_ifq_ch = 0;
}
	
		


