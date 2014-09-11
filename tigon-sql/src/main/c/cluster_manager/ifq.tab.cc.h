/* A Bison parser, made from ifq.y, by GNU bison 1.75.  */

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

#ifndef BISON_IFQ_TAB_CC_H
# define BISON_IFQ_TAB_CC_H

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     PRED = 259,
     STRING_TOKEN = 260,
     INTNUM = 261,
     LONGINTNUM = 262,
     APPROXNUM = 263,
     OR = 264,
     AND = 265,
     NOT = 266,
     SEMICOLON = 267,
     LEFTBRACE = 268,
     RIGHTBRACE = 269,
     BY = 270,
     AS = 271,
     AGGR = 272,
     FROM = 273,
     INNER_JOIN = 274,
     OUTER_JOIN = 275,
     LEFT_OUTER_JOIN = 276,
     RIGHT_OUTER_JOIN = 277,
     GROUP = 278,
     HAVING = 279,
     IN = 280,
     SELECT = 281,
     WHERE = 282,
     SUCH = 283,
     THAT = 284,
     TRUE_V = 285,
     FALSE_V = 286,
     TIMEVAL_L = 287,
     HEX_L = 288,
     LHEX_L = 289,
     IP_L = 290,
     MERGE = 291,
     SLACK = 292,
     DEFINE_SEC = 293,
     PARAM_SEC = 294,
     PROTOCOL = 295,
     TABLE = 296,
     STREAM = 297,
     FTA = 298,
     OPERATOR = 299,
     OPERATOR_VIEW = 300,
     FIELDS = 301,
     SUBQUERIES = 302,
     SELECTION_PUSHDOWN = 303
   };
#endif
#define NAME 258
#define PRED 259
#define STRING_TOKEN 260
#define INTNUM 261
#define LONGINTNUM 262
#define APPROXNUM 263
#define OR 264
#define AND 265
#define NOT 266
#define SEMICOLON 267
#define LEFTBRACE 268
#define RIGHTBRACE 269
#define BY 270
#define AS 271
#define AGGR 272
#define FROM 273
#define INNER_JOIN 274
#define OUTER_JOIN 275
#define LEFT_OUTER_JOIN 276
#define RIGHT_OUTER_JOIN 277
#define GROUP 278
#define HAVING 279
#define IN 280
#define SELECT 281
#define WHERE 282
#define SUCH 283
#define THAT 284
#define TRUE_V 285
#define FALSE_V 286
#define TIMEVAL_L 287
#define HEX_L 288
#define LHEX_L 289
#define IP_L 290
#define MERGE 291
#define SLACK 292
#define DEFINE_SEC 293
#define PARAM_SEC 294
#define PROTOCOL 295
#define TABLE 296
#define STREAM 297
#define FTA 298
#define OPERATOR 299
#define OPERATOR_VIEW 300
#define FIELDS 301
#define SUBQUERIES 302
#define SELECTION_PUSHDOWN 303




#ifndef YYSTYPE
#line 35 "ifq.y"
typedef union {
	int intval;
	double floatval;
	char *strval;
	int subtok;

/*			for FTA definition.	*/
	literal_t *litval;
	scalarexp_t *scalarval;
	se_list_t *se_listval;
	select_list_t *select_listval;
	table_exp_t *tblp;
	predicate_t *predp;
	literal_list_t *lit_l;
	tablevar_t *table;
	tablevar_list_t *tbl_list;
	colref_t *colref;
	colref_list_t *clist;
	var_defs_t *var_defs;
	var_pair_t *var_pair;
	gb_t *gb_val;
	gb_list_t *gb_list;

	query_list_t *q_list;


} yystype;
/* Line 1281 of /usr/local/share/bison/yacc.c.  */
#line 164 "ifq.tab.cc.h"
# define YYSTYPE yystype
#endif

extern YYSTYPE IfqParserlval;


#endif /* not BISON_IFQ_TAB_CC_H */

