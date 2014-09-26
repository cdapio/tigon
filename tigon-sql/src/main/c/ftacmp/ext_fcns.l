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
		flex -PExt_fcnsParser -oext_fcnslexer.cc ext_fcns.l
	(or equivalent).
*/	

%{
/*
 * AT&T lex can't handle this lexer due to lex bugs.  It works with flex
 * 2.3.7, pclex 2.0.5, and MKS lex 3.1a.
 */

 #include "parse_ext_fcns.h"
 #include <string.h>



#include "ext_fcns.tab.cc.h"

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

int Ext_fcnsParserwrap(){return(1);}

extern int Ext_fcnsParserdebug;


/*
		These variables are used for error reporting:
		flex_lineno : the line currently being parsed when the error occurs.
		flexch : the character on the line where the error occurs
		flex_linebuf : store the line for reporting.

		NOTE : 1) the fixed size flex_linebuf buffer is dangerous.
			   2) You might get pointed to a place shortly after
			 	  where the syntax error occurs.  It is close enough
				  for now.
*/

int flex_ext_fcns_lineno = 1;
int flex_ext_fcns_ch = 0;
char flex_ext_fcns_linebuf[20000];

//void Ext_fcnsParsererror(char *s);

void Ext_fcnsParsererror(char *s){
	fprintf(stderr,"On line %d, char %d: %s (token %s):\n%s\n",
				flex_ext_fcns_lineno, flex_ext_fcns_ch, s, Ext_fcnsParsertext, flex_ext_fcns_linebuf );
	fprintf(stderr,"%*s\n",1+flex_ext_fcns_ch,"^");
}


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

FUN		{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return FUN; }
PRED	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return PRED; }
UDAF	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return UDAF; }
EXTR	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return EXTR; }
STATE	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return STATE; }
SFUN	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return SFUN; }
HANDLE		{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return HANDLE; }
CONST		{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return CONST; }
CLASS		{ flex_ext_fcns_ch+=Ext_fcnsParserleng; return CLASS; }
;	{flex_ext_fcns_ch+=Ext_fcnsParserleng; return SEMICOLON;}

[(),\[\]]    { flex_ext_fcns_ch+=Ext_fcnsParserleng; return yytext[0]; }


	/* names */

[A-Za-z][A-Za-z0-9_]*	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; Ext_fcnsParserlval.strval = strdup(yytext); return NAME; }

	/* numbers */

[0-9]+	|
[0-9]+UL	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; Ext_fcnsParserlval.strval = strdup(yytext);  return INTNUM; }

	/* strings */

'[^'\n]*'	{
		
		int c;
		
		Ext_fcnsParserlval.strval = strdup(Ext_fcnsParsertext+1); 

		c = yyinput();

		unput(c);	/* just peeking */
		if(c != '\'') {
			flex_ext_fcns_ch+=Ext_fcnsParserleng; 
			Ext_fcnsParserlval.strval[Ext_fcnsParserleng-2] = '\0';
			return STRING_TOKEN;
		} else
			yymore();
	}
		
'[^'\n]*$	{ flex_ext_fcns_ch+=Ext_fcnsParserleng; Ext_fcnsParsererror("Unterminated string"); }



 /*															*/
 /*		Newline : advance the error reporting line number	*/
 /*		and grab the next line into flex_linebuf			*/
 /*															*/

\n.*		{flex_ext_fcns_ch=0; flex_ext_fcns_lineno++;
			   strcpy(flex_ext_fcns_linebuf,Ext_fcnsParsertext+1);
			   yyless(1);
			   }

[ \t\r]+	{flex_ext_fcns_ch+=Ext_fcnsParserleng; }	/* white space */

"--".*$		{flex_ext_fcns_ch+=Ext_fcnsParserleng; };	/* comment */
"//".*$		{flex_ext_fcns_ch+=Ext_fcnsParserleng; };	/* comment */

 /*		Parse error on anything else.		*/				
.|\n			{fprintf(stderr,"Warning: unknown token (ignored)\n"); 	Ext_fcnsParsererror(yytext);}
 

%%


