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
		flex -PxmlParser -oxmllexer.cc xml.l
	(or equivalent).
*/	

%{
/*
 * AT&T lex can't handle this lexer due to lex bugs.  It works with flex
 * 2.3.7, pclex 2.0.5, and MKS lex 3.1a.
 */

 // #include "parse_fta.h"
 #include <string.h>



#include "xml.tab.hh"

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

int xmlParserwrap(){return(1);}

extern int xmlParserdebug;


/*
		These variables are used for error reporting:
		flex_xml_lineno : the line currently being parsed when the error occurs.
		flex_xml_ch : the character on the line where the error occurs
		flex_xml_linebuf : store the line for reporting.

		NOTE : 1) the fixed size flex_xml_linebuf buffer is dangerous.
			   2) You might get pointed to a place shortly after
			 	  where the syntax error occurs.  It is close enough
				  for now.
*/

int flex_xml_lineno = 1;
int flex_xml_ch = 0;
char flex_xml_linebuf[20000];

char *flex_xml_stringinput = NULL;
int flex_xml_stringinput_ptr = 0;
FILE *flex_xml_fileinput = NULL;
int my_xmlParser_yyinput(char *buf, int max_size);



void xmlParsererror(char *s){
	int i;
	fprintf(stderr,"On line %d, char %d: %s (token %s):\n%s\n",
				flex_xml_lineno, flex_xml_ch, s, xmlParsertext, flex_xml_linebuf );
    for(i=0;i<flex_xml_ch;i++){
		if(flex_xml_linebuf[i] == '\t'){
			fprintf(stderr,"\t");
		}else{
			fprintf(stderr," ");
		}
	}
	fprintf(stderr,"^\n");
	//	fprintf(stderr,"%*s\n",1+flex_xml_ch,"^");
}

#undef YY_INPUT
#define YY_INPUT(b, r, ms) (r = my_xmlParser_yyinput(b,ms))

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




[<>=/]	{ flex_xml_ch+=xmlParserleng; return yytext[0]; }

	/* names */

[A-Za-z_][A-Za-z0-9_:\-]*	{ flex_xml_ch+=xmlParserleng; xmlParserlval.strval = strdup(yytext); return NAME; }


	/* strings */

\'[^\'\n]*\'	{
		
		int c;
		
		xmlParserlval.strval = strdup(xmlParsertext+1); 

		c = yyinput();

		unput(c);	/* just peeking */
		if(c != '\'') {
			flex_xml_ch+=xmlParserleng; 
			xmlParserlval.strval[xmlParserleng-2] = '\0';
			return STRING_TOKEN;
		} else
			yymore();
	}
		
\'[^\'\n]*$	{ flex_xml_ch+=xmlParserleng; xmlParsererror("Unterminated string"); }

 /*															*/
 /*		Newline : advance the error reporting line number	*/
 /*		and grab the next line into flex_xml_linebuf			*/
 /*															*/

\n.*		{flex_xml_ch=0; flex_xml_lineno++;
			   strcpy(flex_xml_linebuf,xmlParsertext+1);
			   yyless(1);
			   }

[ \t\r]+	{flex_xml_ch+=xmlParserleng; }	/* white space */

"--".*$		{flex_xml_ch+=xmlParserleng; };	/* comment */
"//".*$		{flex_xml_ch+=xmlParserleng; };	/* comment */
"<?xml".*$      {flex_xml_ch+=xmlParserleng; }; /* comment */

.|\n            {flex_xml_ch+=xmlParserleng; fprintf(stderr,"Warning: unknown token (ignored)\n");  xmlParsererror(yytext);}

%%

int my_xmlParser_yyinput(char *buf, int max_size){
	int c = 0;
	int inchar = 0;
	
	if(flex_xml_stringinput != NULL){
		while(c<max_size){
			if(flex_xml_stringinput[flex_xml_stringinput_ptr] != '\0'){
				buf[c++] = flex_xml_stringinput[flex_xml_stringinput_ptr++];
			}else{
				break;
			}
		}
		return(c);
	}
	
	if(flex_xml_fileinput != NULL){
		while(c < max_size){
			inchar = getc(flex_xml_fileinput);
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

void xmlParser_setfileinput(FILE *f){
	xmlParserrestart(NULL);

	flex_xml_fileinput = f;
	flex_xml_stringinput = NULL;
	flex_xml_lineno = 1;
	flex_xml_ch = 0;
}

void xmlParser_setstringinput(char *s){
	xmlParserrestart(NULL);

	flex_xml_fileinput = NULL;
	flex_xml_stringinput = s;
	flex_xml_stringinput_ptr = 0;
	flex_xml_lineno = 1;
	flex_xml_ch = 0;
}
	
		



