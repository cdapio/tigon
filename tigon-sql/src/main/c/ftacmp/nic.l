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
		flex -PNicParser -oniclexer.cc nic.l
	(or equivalent).
*/	

%{
/*
 * AT&T lex can't handle this lexer due to lex bugs.  It works with flex
 * 2.3.7, pclex 2.0.5, and MKS lex 3.1a.
 */

 #include "nic_def.h"
 #include <string.h>



#include "nic.tab.cc.h"

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

int NicParserwrap(){return(1);}

extern int NicParserdebug;


/*
		These variables are used for error reporting:
		flex_nic_lineno : the line currently being parsed when the error occurs.
		flex_nic_ch : the character on the line where the error occurs
		flex_nic_linebuf : store the line for reporting.

		NOTE : 1) the fixed size flex_nic_linebuf buffer is dangerous.
			   2) You might get pointed to a place shortly after
			 	  where the syntax error occurs.  It is close enough
				  for now.
*/

int flex_nic_lineno = 1;
int flex_nic_ch = 0;
char flex_nic_linebuf[20000];

char *flex_nic_stringinput = NULL;
int flex_nic_stringinput_ptr = 0;
FILE *flex_nic_fileinput = NULL;
int my_NicParser_yyinput(char *buf, int max_size);



void NicParsererror(char *s){
	int i;
	fprintf(stderr,"On line %d, char %d: %s (token %s):\n%s\n",
				flex_nic_lineno, flex_nic_ch, s, NicParsertext, flex_nic_linebuf );
    for(i=0;i<flex_nic_ch;i++){
		if(flex_nic_linebuf[i] == '\t'){
			fprintf(stderr,"\t");
		}else{
			fprintf(stderr," ");
		}
	}
	fprintf(stderr,"^\n");
	//	fprintf(stderr,"%*s\n",1+flex_nic_ch,"^");
}

#undef YY_INPUT
#define YY_INPUT(b, r, ms) (r = my_NicParser_yyinput(b,ms))

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

OPTIONS:             { flex_nic_ch+=NicParserleng; return OPTIONS; }
FUNCTIONS:             { flex_nic_ch+=NicParserleng; return FUNC; }
TYPES:             { flex_nic_ch+=NicParserleng; return TYPES; }
UNARY_OPS:             { flex_nic_ch+=NicParserleng; return UNARY_OPS; }
BINARY_OPS:             { flex_nic_ch+=NicParserleng; return BINARY_OPS; }
MISSING_FIELDS:             { flex_nic_ch+=NicParserleng; return FIELDS; }

[;,=]	{ flex_nic_ch+=NicParserleng;  return yytext[0]; }

	/* names */

[A-Za-z_][A-Za-z0-9_]*	{ flex_nic_ch+=NicParserleng; NicParserlval.strval = strdup(yytext); return NAME; }


	/* strings */

'[^'\n]*'	{
		
		int c;
		
		NicParserlval.strval = strdup(NicParsertext+1); 

		c = yyinput();

		unput(c);	/* just peeking */
		if(c != '\'') {
			flex_nic_ch+=NicParserleng; 
			NicParserlval.strval[NicParserleng-2] = '\0';
			return STRING_TOKEN;
		} else
			yymore();
	}
		
'[^'\n]*$	{ flex_nic_ch+=NicParserleng; NicParsererror("Unterminated string"); }

 /*															*/
 /*		Newline : advance the error reporting line number	*/
 /*		and grab the next line into flex_nic_linebuf			*/
 /*															*/

\n.*		{flex_nic_ch=0; flex_nic_lineno++;
			   strcpy(flex_nic_linebuf,NicParsertext+1);
			   yyless(1);
			   }

[ \t\r]+	{flex_nic_ch+=NicParserleng; }	/* white space */

"--".*$		{flex_nic_ch+=NicParserleng; };	/* comment */
"//".*$		{flex_nic_ch+=NicParserleng; };	/* comment */

.|\n            {flex_nic_ch+=NicParserleng; fprintf(stderr,"Warning: unknown token (ignored)\n");  NicParsererror(yytext);}

%%

int my_NicParser_yyinput(char *buf, int max_size){
	int c = 0;
	int inchar = 0;
	
	if(flex_nic_stringinput != NULL){
		while(c<max_size){
			if(flex_nic_stringinput[flex_nic_stringinput_ptr] != '\0'){
				buf[c++] = flex_nic_stringinput[flex_nic_stringinput_ptr++];
			}else{
				break;
			}
		}
		return(c);
	}
	
	if(flex_nic_fileinput != NULL){
		while(c < max_size){
			inchar = getc(flex_nic_fileinput);
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

void NicParser_setfileinput(FILE *f){
	NicParserrestart(NULL);

	flex_nic_fileinput = f;
	flex_nic_stringinput = NULL;
	flex_nic_lineno = 1;
	flex_nic_ch = 0;
}

void NicParser_setstringinput(char *s){
	NicParserrestart(NULL);

	flex_nic_fileinput = NULL;
	flex_nic_stringinput = s;
	flex_nic_stringinput_ptr = 0;
	flex_nic_lineno = 1;
	flex_nic_ch = 0;
}
	
		


