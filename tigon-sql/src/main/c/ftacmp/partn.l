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
		flex -PPartnParser -opartnlexer.cc partn.l
	(or equivalent).
*/	

%{
/*
 * AT&T lex can't handle this lexer due to lex bugs.  It works with flex
 * 2.3.7, pclex 2.0.5, and MKS lex 3.1a.
 */

 #include "parse_partn.h"
 #include <string.h>



#include "partn.tab.cc.h"

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

int PartnParserwrap(){return(1);}

extern int PartnParserdebug;


/*
		These variables are used for error reporting:
		flex_partn_lineno : the line currently being parsed when the error occurs.
		flex_partn_ch : the character on the line where the error occurs
		flex_partn_linebuf : store the line for reporting.

		NOTE : 1) the fixed size flex_partn_linebuf buffer is dangerous.
			   2) You might get pointed to a place shortly after
			 	  where the syntax error occurs.  It is close enough
				  for now.
*/

int flex_partn_lineno = 1;
int flex_partn_ch = 0;
char flex_partn_linebuf[20000];

char *flex_partn_stringinput = NULL;
int flex_partn_stringinput_ptr = 0;
FILE *flex_partn_fileinput = NULL;
int my_PartnParser_yyinput(char *buf, int max_size);



void PartnParsererror(char *s){
	int i;
	fprintf(stderr,"On line %d, char %d: %s (token %s):\n%s\n",
				flex_partn_lineno, flex_partn_ch, s, PartnParsertext, flex_partn_linebuf );
    for(i=0;i<flex_partn_ch;i++){
		if(flex_partn_linebuf[i] == '\t'){
			fprintf(stderr,"\t");
		}else{
			fprintf(stderr," ");
		}
	}
	fprintf(stderr,"^\n");
	//	fprintf(stderr,"%*s\n",1+flex_partn_ch,"^");
}

#undef YY_INPUT
#define YY_INPUT(b, r, ms) (r = my_PartnParser_yyinput(b,ms))

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

HEX	{ flex_partn_ch+=PartnParserleng; return HEX_L;}
LHEX	{ flex_partn_ch+=PartnParserleng; return LHEX_L;}
IP_VAL	{ flex_partn_ch+=PartnParserleng; return IP_L;}



	/* punctuation */

[;%&|!+*/:(),.\[\]$@#]	{ flex_partn_ch+=PartnParserleng; return yytext[0]; }
"-"	{ flex_partn_ch+=PartnParserleng; return yytext[0]; }
"~"	{ flex_partn_ch+=PartnParserleng; return yytext[0]; }

	/* names */

[A-Za-z_][A-Za-z0-9_]*	{ flex_partn_ch+=PartnParserleng; PartnParserlval.strval = strdup(yytext); return NAME; }

	/* numbers */

[0-9]+	|
[0-9]+UL	{ flex_partn_ch+=PartnParserleng; PartnParserlval.strval = strdup(yytext);  return INTNUM; }

[0-9]+ULL	{ flex_partn_ch+=PartnParserleng; PartnParserlval.strval = strdup(yytext);  return LONGINTNUM; }


	/* strings */

 /*															*/
 /*		Newline : advance the error reporting line number	*/
 /*		and grab the next line into flex_partn_linebuf			*/
 /*															*/

\n.*		{flex_partn_ch=0; flex_partn_lineno++;
			   strcpy(flex_partn_linebuf,PartnParsertext+1);
			   yyless(1);
			   }

[ \t\r]+	{flex_partn_ch+=PartnParserleng; }	/* white space */

"--".*$		{flex_partn_ch+=PartnParserleng; };	/* comment */
"//".*$		{flex_partn_ch+=PartnParserleng; };	/* comment */

.|\n            {flex_partn_ch+=PartnParserleng; fprintf(stderr,"Warning: unknown token (ignored)\n");  PartnParsererror(yytext);}

%%

int my_PartnParser_yyinput(char *buf, int max_size){
	int c = 0;
	int inchar = 0;
	
	if(flex_partn_stringinput != NULL){
		while(c<max_size){
			if(flex_partn_stringinput[flex_partn_stringinput_ptr] != '\0'){
				buf[c++] = flex_partn_stringinput[flex_partn_stringinput_ptr++];
			}else{
				break;
			}
		}
		return(c);
	}
	
	if(flex_partn_fileinput != NULL){
		while(c < max_size){
			inchar = getc(flex_partn_fileinput);
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

void PartnParser_setfileinput(FILE *f){
	PartnParserrestart(NULL);

	flex_partn_fileinput = f;
	flex_partn_stringinput = NULL;
	flex_partn_lineno = 1;
	flex_partn_ch = 0;
}

void PartnParser_setstringinput(char *s){
	PartnParserrestart(NULL);

	flex_partn_fileinput = NULL;
	flex_partn_stringinput = s;
	flex_partn_stringinput_ptr = 0;
	flex_partn_lineno = 1;
	flex_partn_ch = 0;
}
	
		


