/* A Bison parser, made from res.y, by GNU bison 1.75.  */

/* Skeleton parser for Yacc-like parsing with Bison,
   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place - Suite 330,
   Boston, MA 02111-1307, USA.  */

/* As a special exception, when this file is copied by Bison into a
   Bison output file, you may use that output file without restriction.
   This special exception was added by the Free Software Foundation
   in version 1.24 of Bison.  */

#ifndef BISON_RES_TAB_CC_H
# define BISON_RES_TAB_CC_H

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING_TOKEN = 259
   };
#endif
#define NAME 258
#define STRING_TOKEN 259




#ifndef YYSTYPE
#line 39 "res.y"
typedef union {
	int intval;
	double floatval;
	char *strval;
	int subtok;

	/*			for FTA definition.	*/

} yystype;
/* Line 1281 of /usr/local/share/bison/yacc.c.  */
#line 58 "res.tab.cc.h"
# define YYSTYPE yystype
#endif

extern YYSTYPE ResParserlval;


#endif /* not BISON_RES_TAB_CC_H */

