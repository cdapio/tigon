/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

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
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.3"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Using locations.  */
#define YYLSP_NEEDED 0

/* Substitute the variable and function names.  */
#define yyparse FtaParserparse
#define yylex   FtaParserlex
#define yyerror FtaParsererror
#define yylval  FtaParserlval
#define yychar  FtaParserchar
#define yydebug FtaParserdebug
#define yynerrs FtaParsernerrs


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING_TOKEN = 259,
     INTNUM = 260,
     LONGINTNUM = 261,
     APPROXNUM = 262,
     OR = 263,
     AND = 264,
     NOT = 265,
     COMPARISON = 266,
     SHIFT_OP = 267,
     UMINUS = 268,
     SEMICOLON = 269,
     LEFTBRACE = 270,
     RIGHTBRACE = 271,
     BY = 272,
     AS = 273,
     AGGR = 274,
     FROM = 275,
     INNER_JOIN = 276,
     FILTER_JOIN = 277,
     OUTER_JOIN = 278,
     LEFT_OUTER_JOIN = 279,
     RIGHT_OUTER_JOIN = 280,
     GROUP = 281,
     HAVING = 282,
     IN = 283,
     SELECT = 284,
     WHERE = 285,
     SUPERGROUP = 286,
     CLEANING_WHEN = 287,
     CLEANING_BY = 288,
     CLOSING_WHEN = 289,
     SUCH = 290,
     THAT = 291,
     CUBE = 292,
     ROLLUP = 293,
     GROUPING_SETS = 294,
     TRUE_V = 295,
     FALSE_V = 296,
     TIMEVAL_L = 297,
     HEX_L = 298,
     LHEX_L = 299,
     IP_L = 300,
     IPV6_L = 301,
     MERGE = 302,
     SLACK = 303,
     DEFINE_SEC = 304,
     PARAM_SEC = 305,
     PROTOCOL = 306,
     TABLE = 307,
     STREAM = 308,
     FTA = 309,
     UNPACK_FCNS = 310,
     OPERATOR = 311,
     OPERATOR_VIEW = 312,
     FIELDS = 313,
     SUBQUERIES = 314,
     SELECTION_PUSHDOWN = 315
   };
#endif
/* Tokens.  */
#define NAME 258
#define STRING_TOKEN 259
#define INTNUM 260
#define LONGINTNUM 261
#define APPROXNUM 262
#define OR 263
#define AND 264
#define NOT 265
#define COMPARISON 266
#define SHIFT_OP 267
#define UMINUS 268
#define SEMICOLON 269
#define LEFTBRACE 270
#define RIGHTBRACE 271
#define BY 272
#define AS 273
#define AGGR 274
#define FROM 275
#define INNER_JOIN 276
#define FILTER_JOIN 277
#define OUTER_JOIN 278
#define LEFT_OUTER_JOIN 279
#define RIGHT_OUTER_JOIN 280
#define GROUP 281
#define HAVING 282
#define IN 283
#define SELECT 284
#define WHERE 285
#define SUPERGROUP 286
#define CLEANING_WHEN 287
#define CLEANING_BY 288
#define CLOSING_WHEN 289
#define SUCH 290
#define THAT 291
#define CUBE 292
#define ROLLUP 293
#define GROUPING_SETS 294
#define TRUE_V 295
#define FALSE_V 296
#define TIMEVAL_L 297
#define HEX_L 298
#define LHEX_L 299
#define IP_L 300
#define IPV6_L 301
#define MERGE 302
#define SLACK 303
#define DEFINE_SEC 304
#define PARAM_SEC 305
#define PROTOCOL 306
#define TABLE 307
#define STREAM 308
#define FTA 309
#define UNPACK_FCNS 310
#define OPERATOR 311
#define OPERATOR_VIEW 312
#define FIELDS 313
#define SUBQUERIES 314
#define SELECTION_PUSHDOWN 315




/* Copy the first part of user declarations.  */
#line 8 "fta.y"



#include <stdio.h>

/*		Some addn'l includes, necessary but not included by the
		bison generated code.
*/

#include <stdlib.h>

/*		prototypes for the parser callbacks.
*/

#include "parse_fta.h"
#include "parse_schema.h"


extern fta_parse_t *fta_parse_result;
extern var_defs_t *fta_parse_defines;


#define YYDEBUG 1



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif

#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 37 "fta.y"
{
	int intval;
	double floatval;
	char *strval;
	int subtok;

	string_t *stringval;

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
	ifpref_t *ifpref;
	colref_list_t *clist;
	var_defs_t *var_defs;
	var_pair_t *var_pair;
	gb_t *gb_val;
	gb_list_t *gb_list;
	list_of_gb_list_t *list_of_gb_list;
	extended_gb_t *extended_gb;
	extended_gb_list_t *extended_gb_list;

	query_list_t *q_list;

/*		For table definition	*/
	field_entry *field_t;
	field_entry_list *field_list_t;
	table_def *table_def_t;
	table_list *table_list_schema;
	param_list *plist_t;
	name_vec  *namevec_t;
	subquery_spec *subq_spec_t;
	subqueryspec_list *subqueryspec_list_t;
	unpack_fcn	*ufcn;
	unpack_fcn_list *ufcnl;

}
/* Line 193 of yacc.c.  */
#line 295 "fta.tab.cc"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



/* Copy the second part of user declarations.  */


/* Line 216 of yacc.c.  */
#line 308 "fta.tab.cc"

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int i)
#else
static int
YYID (i)
    int i;
#endif
{
  return i;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss;
  YYSTYPE yyvs;
  };

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack)					\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack, Stack, yysize);				\
	Stack = &yyptr->Stack;						\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  57
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   708

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  80
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  59
/* YYNRULES -- Number of rules.  */
#define YYNRULES  176
/* YYNRULES -- Number of states.  */
#define YYNSTATES  377

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   315

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    74,     2,    77,    76,    19,    13,     2,
      68,    70,    17,    15,    69,    16,    71,    18,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    79,     2,
       2,     2,     2,     2,    78,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    72,     2,    73,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    12,     2,    75,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    14,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,    45,    46,    47,    48,    49,    50,    51,
      52,    53,    54,    55,    56,    57,    58,    59,    60,    61,
      62,    63,    64,    65,    66,    67
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     5,     7,    13,    17,    21,    24,    27,
      29,    33,    37,    40,    43,    45,    47,    51,    56,    60,
      65,    69,    71,    74,    78,    82,    86,    90,    96,    98,
     107,   110,   114,   118,   122,   126,   135,   137,   141,   143,
     146,   148,   152,   158,   164,   170,   172,   176,   177,   179,
     182,   183,   186,   187,   190,   191,   194,   195,   197,   200,
     204,   208,   211,   215,   217,   219,   221,   225,   230,   234,
     240,   242,   246,   250,   254,   258,   262,   266,   270,   274,
     278,   281,   284,   287,   290,   292,   295,   297,   299,   303,
     308,   313,   318,   322,   328,   334,   340,   345,   347,   351,
     355,   361,   363,   367,   369,   371,   373,   375,   377,   379,
     382,   385,   388,   391,   394,   397,   398,   402,   403,   406,
     410,   416,   418,   423,   428,   433,   435,   439,   441,   445,
     447,   451,   457,   461,   464,   469,   471,   475,   481,   485,
     489,   491,   495,   499,   501,   504,   512,   519,   536,   541,
     543,   547,   551,   553,   557,   562,   564,   567,   574,   579,
     580,   584,   586,   589,   592,   595,   599,   604,   609,   614,
     615,   619,   620,   624,   626,   630,   634
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
      81,     0,    -1,    83,    -1,   125,    -1,    61,    22,   125,
      82,    23,    -1,    85,    84,    88,    -1,    84,    85,    88,
      -1,    84,    88,    -1,    85,    88,    -1,    88,    -1,    85,
      84,    89,    -1,    84,    85,    89,    -1,    84,    89,    -1,
      85,    89,    -1,    89,    -1,    82,    -1,    83,    21,    82,
      -1,    57,    22,    86,    23,    -1,    57,    22,    23,    -1,
      56,    22,    86,    23,    -1,    56,    22,    23,    -1,    87,
      -1,    86,    87,    -1,     3,     4,    21,    -1,     3,     3,
      21,    -1,    36,    90,    91,    -1,    54,   122,    92,    -1,
      54,   122,    55,   109,    92,    -1,   110,    -1,    92,    97,
     113,   114,   102,    99,   100,   101,    -1,    27,    93,    -1,
      28,    27,    93,    -1,    30,    27,    93,    -1,    32,    27,
      93,    -1,    31,    27,    93,    -1,    29,    68,   121,    69,
       5,    70,    27,    93,    -1,    94,    -1,    93,    69,    94,
      -1,    95,    -1,    95,     3,    -1,    96,    -1,     3,    71,
      96,    -1,     3,    71,     3,    71,    96,    -1,     4,    71,
       3,    71,    96,    -1,    72,     3,    73,    71,    96,    -1,
       3,    -1,    96,    18,     3,    -1,    -1,    98,    -1,    37,
     104,    -1,    -1,    39,   104,    -1,    -1,    40,   104,    -1,
      -1,    41,   104,    -1,    -1,   103,    -1,    34,   104,    -1,
     104,     8,   104,    -1,   104,     9,   104,    -1,    10,   104,
      -1,    68,   104,    70,    -1,   105,    -1,   106,    -1,   107,
      -1,     3,    72,    73,    -1,     3,    72,   111,    73,    -1,
     109,    11,   109,    -1,   109,    35,    72,   108,    73,    -1,
     112,    -1,   108,    69,   112,    -1,   109,    15,   109,    -1,
     109,    16,   109,    -1,   109,    12,   109,    -1,   109,    17,
     109,    -1,   109,    18,   109,    -1,   109,    13,   109,    -1,
     109,    19,   109,    -1,   109,    14,   109,    -1,    15,   109,
      -1,    16,   109,    -1,    74,   109,    -1,    75,   109,    -1,
     112,    -1,    76,     3,    -1,   120,    -1,   121,    -1,    68,
     109,    70,    -1,    26,    68,    17,    70,    -1,    26,    68,
     109,    70,    -1,     3,    68,   111,    70,    -1,     3,    68,
      70,    -1,    26,    76,    68,    17,    70,    -1,    26,    76,
      68,   109,    70,    -1,     3,    76,    68,   111,    70,    -1,
       3,    76,    68,    70,    -1,   109,    -1,   109,    25,     3,
      -1,   110,    69,   109,    -1,   110,    69,   109,    25,     3,
      -1,   109,    -1,   111,    69,   109,    -1,     4,    -1,     5,
      -1,     6,    -1,     7,    -1,    47,    -1,    48,    -1,    49,
       4,    -1,    50,     4,    -1,    51,     4,    -1,    52,     4,
      -1,    53,     4,    -1,    77,     3,    -1,    -1,    33,    24,
     117,    -1,    -1,    38,   124,    -1,    68,   118,    70,    -1,
     115,    69,    68,   118,    70,    -1,   119,    -1,    45,    68,
     118,    70,    -1,    44,    68,   118,    70,    -1,    46,    68,
     115,    70,    -1,   116,    -1,   117,    69,   116,    -1,   119,
      -1,   118,    69,   119,    -1,     3,    -1,     3,    71,     3,
      -1,     3,    71,     3,    71,     3,    -1,   109,    25,     3,
      -1,    78,     3,    -1,     3,    71,    78,     3,    -1,     3,
      -1,     3,    71,     3,    -1,     3,    71,     3,    71,     3,
      -1,   121,    79,   121,    -1,   122,    79,   121,    -1,     3,
      -1,   123,    69,   123,    -1,   124,    69,   123,    -1,   126,
      -1,   125,   126,    -1,    58,     3,   136,   135,    22,   131,
      23,    -1,    60,    96,   136,    22,   131,    23,    -1,    64,
       3,    22,    63,   133,    65,    22,   131,    23,    66,    22,
     129,    23,    67,   133,    23,    -1,    62,    22,   127,    23,
      -1,   128,    -1,   127,    21,   128,    -1,     3,     3,     5,
      -1,   130,    -1,   129,    21,   130,    -1,     3,    68,   138,
      70,    -1,   132,    -1,   131,   132,    -1,     3,     3,     3,
     133,   135,    21,    -1,     3,     3,   133,    21,    -1,    -1,
      68,   134,    70,    -1,     3,    -1,     3,     3,    -1,     3,
       4,    -1,     3,     5,    -1,   134,    69,     3,    -1,   134,
      69,     3,     3,    -1,   134,    69,     3,     4,    -1,   134,
      69,     3,     5,    -1,    -1,    72,   137,    73,    -1,    -1,
      68,   137,    70,    -1,     3,    -1,   137,    69,     3,    -1,
       3,     3,   133,    -1,   138,    69,     3,     3,   133,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   189,   189,   194,   201,   215,   220,   225,   230,   234,
     237,   242,   247,   251,   255,   260,   261,   265,   266,   270,
     271,   275,   276,   280,   281,   285,   289,   291,   299,   303,
     315,   316,   317,   318,   319,   320,   324,   325,   329,   330,
     334,   335,   336,   337,   338,   342,   343,   348,   349,   353,
     357,   358,   362,   363,   367,   368,   372,   373,   377,   384,
     385,   386,   387,   388,   392,   393,   394,   395,   399,   403,
     407,   408,   414,   415,   416,   417,   418,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,   430,   431,
     432,   433,   434,   435,   436,   437,   438,   443,   444,   445,
     446,   450,   451,   455,   456,   457,   458,   459,   460,   461,
     462,   463,   464,   465,   466,   472,   473,   477,   478,   482,
     483,   487,   488,   489,   490,   494,   495,   499,   500,   504,
     505,   506,   507,   518,   519,   523,   524,   525,   529,   530,
     534,   538,   539,   554,   555,   558,   560,   562,   564,   568,
     569,   573,   577,   578,   582,   585,   586,   590,   591,   595,
     596,   600,   601,   602,   603,   604,   605,   606,   607,   611,
     612,   616,   617,   621,   622,   626,   627
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING_TOKEN", "INTNUM",
  "LONGINTNUM", "APPROXNUM", "OR", "AND", "NOT", "COMPARISON", "'|'",
  "'&'", "SHIFT_OP", "'+'", "'-'", "'*'", "'/'", "'%'", "UMINUS",
  "SEMICOLON", "LEFTBRACE", "RIGHTBRACE", "BY", "AS", "AGGR", "FROM",
  "INNER_JOIN", "FILTER_JOIN", "OUTER_JOIN", "LEFT_OUTER_JOIN",
  "RIGHT_OUTER_JOIN", "GROUP", "HAVING", "IN", "SELECT", "WHERE",
  "SUPERGROUP", "CLEANING_WHEN", "CLEANING_BY", "CLOSING_WHEN", "SUCH",
  "THAT", "CUBE", "ROLLUP", "GROUPING_SETS", "TRUE_V", "FALSE_V",
  "TIMEVAL_L", "HEX_L", "LHEX_L", "IP_L", "IPV6_L", "MERGE", "SLACK",
  "DEFINE_SEC", "PARAM_SEC", "PROTOCOL", "TABLE", "STREAM", "FTA",
  "UNPACK_FCNS", "OPERATOR", "OPERATOR_VIEW", "FIELDS", "SUBQUERIES",
  "SELECTION_PUSHDOWN", "'('", "','", "')'", "'.'", "'['", "']'", "'!'",
  "'~'", "'$'", "'#'", "'@'", "':'", "$accept", "parse_result", "gsql",
  "query_list", "params_def", "variable_def", "variable_list",
  "variable_assign", "select_statement", "merge_statement", "selection",
  "table_exp", "from_clause", "table_ref_commalist", "table_ref", "table",
  "qname", "opt_where_clause", "where_clause", "opt_cleaning_when_clause",
  "opt_cleaning_by_clause", "opt_closing_when_clause", "opt_having_clause",
  "having_clause", "search_condition", "predicate", "comparison_predicate",
  "in_predicate", "literal_commalist", "scalar_exp", "select_commalist",
  "scalar_exp_commalist", "literal", "opt_group_by_clause",
  "opt_supergroup_clause", "list_of_gb_commalist", "extended_gb",
  "extended_gb_commalist", "gb_commalist", "gb", "ifparam", "column_ref",
  "column_ref_list", "gb_ref", "gb_ref_list", "table_list", "table_def",
  "unpack_func_list", "unpack_func", "subqueryspec_list", "subq_spec",
  "field_list", "field", "opt_param_commalist", "param_commalist",
  "opt_singleparam_commalist_bkt", "opt_singleparam_commalist",
  "singleparam_commalist", "namevec_commalist", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   124,    38,   267,    43,    45,    42,    47,    37,
     268,   269,   270,   271,   272,   273,   274,   275,   276,   277,
     278,   279,   280,   281,   282,   283,   284,   285,   286,   287,
     288,   289,   290,   291,   292,   293,   294,   295,   296,   297,
     298,   299,   300,   301,   302,   303,   304,   305,   306,   307,
     308,   309,   310,   311,   312,   313,   314,   315,    40,    44,
      41,    46,    91,    93,    33,   126,    36,    35,    64,    58
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint8 yyr1[] =
{
       0,    80,    81,    81,    81,    82,    82,    82,    82,    82,
      82,    82,    82,    82,    82,    83,    83,    84,    84,    85,
      85,    86,    86,    87,    87,    88,    89,    89,    90,    91,
      92,    92,    92,    92,    92,    92,    93,    93,    94,    94,
      95,    95,    95,    95,    95,    96,    96,    97,    97,    98,
      99,    99,   100,   100,   101,   101,   102,   102,   103,   104,
     104,   104,   104,   104,   105,   105,   105,   105,   106,   107,
     108,   108,   109,   109,   109,   109,   109,   109,   109,   109,
     109,   109,   109,   109,   109,   109,   109,   109,   109,   109,
     109,   109,   109,   109,   109,   109,   109,   110,   110,   110,
     110,   111,   111,   112,   112,   112,   112,   112,   112,   112,
     112,   112,   112,   112,   112,   113,   113,   114,   114,   115,
     115,   116,   116,   116,   116,   117,   117,   118,   118,   119,
     119,   119,   119,   120,   120,   121,   121,   121,   122,   122,
     123,   124,   124,   125,   125,   126,   126,   126,   126,   127,
     127,   128,   129,   129,   130,   131,   131,   132,   132,   133,
     133,   134,   134,   134,   134,   134,   134,   134,   134,   135,
     135,   136,   136,   137,   137,   138,   138
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     1,     1,     5,     3,     3,     2,     2,     1,
       3,     3,     2,     2,     1,     1,     3,     4,     3,     4,
       3,     1,     2,     3,     3,     3,     3,     5,     1,     8,
       2,     3,     3,     3,     3,     8,     1,     3,     1,     2,
       1,     3,     5,     5,     5,     1,     3,     0,     1,     2,
       0,     2,     0,     2,     0,     2,     0,     1,     2,     3,
       3,     2,     3,     1,     1,     1,     3,     4,     3,     5,
       1,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       2,     2,     2,     2,     1,     2,     1,     1,     3,     4,
       4,     4,     3,     5,     5,     5,     4,     1,     3,     3,
       5,     1,     3,     1,     1,     1,     1,     1,     1,     2,
       2,     2,     2,     2,     2,     0,     3,     0,     2,     3,
       5,     1,     4,     4,     4,     1,     3,     1,     3,     1,
       3,     5,     3,     2,     4,     1,     3,     5,     3,     3,
       1,     3,     3,     1,     2,     7,     6,    16,     4,     1,
       3,     3,     1,     3,     4,     1,     2,     6,     4,     0,
       3,     1,     2,     2,     2,     3,     4,     4,     4,     0,
       3,     0,     3,     1,     3,     3,     5
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    15,     2,     0,     0,     9,    14,     3,   143,   135,
     103,   104,   105,   106,     0,     0,     0,   107,   108,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    97,    28,    84,    86,    87,   135,     0,     0,     0,
       0,   171,    45,   171,     0,     0,     0,     1,     0,     0,
       7,    12,     0,     8,    13,   144,     0,     0,     0,    80,
      81,     0,     0,   109,   110,   111,   112,   113,     0,    82,
      83,    85,   114,   133,     0,     0,     0,     0,     0,     0,
      25,    47,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    26,     0,    20,     0,
      21,    18,     0,     0,   169,     0,     0,     0,     0,     0,
     149,     0,    16,     6,    11,     5,    10,    92,   101,     0,
     136,     0,     0,     0,     0,     0,    88,    45,     0,     0,
      30,    36,    38,    40,     0,     0,     0,     0,     0,     0,
     115,    48,    74,    77,    79,    72,    73,    75,    76,    78,
      98,    99,   138,     0,   139,     0,     0,    19,    22,    17,
     173,     0,     0,     0,    46,     0,     0,     0,     0,   148,
     159,     0,    91,     0,   134,    96,     0,    89,    90,     0,
       0,     0,     0,     0,     0,    39,    31,     0,    32,    34,
      33,   135,     0,     0,    49,    63,    64,    65,     0,     0,
     117,     0,    27,    24,    23,     0,   172,     0,     0,     0,
       0,   155,     4,   151,   150,     0,     0,   102,   137,    95,
      93,    94,    45,    41,     0,     0,    37,     0,     0,    61,
       0,     0,     0,     0,     0,     0,     0,     0,    56,   100,
     174,   170,     0,   159,   146,   156,   161,     0,     0,     0,
       0,     0,     0,    66,     0,    62,    59,    60,    68,     0,
     129,     0,     0,     0,     0,   125,   116,   121,   140,     0,
     118,     0,    50,    57,   145,   159,     0,   162,   163,   164,
       0,   160,     0,    42,    43,    44,     0,    67,     0,    70,
       0,     0,     0,     0,     0,     0,     0,     0,    58,     0,
      52,   169,   158,   165,     0,     0,     0,    69,   130,     0,
     127,     0,     0,     0,   132,   126,   141,   142,    51,     0,
      54,     0,   166,   167,   168,     0,    35,    71,     0,     0,
     123,   122,     0,     0,   124,    53,     0,    29,   157,     0,
     131,   128,   119,     0,    55,     0,     0,     0,     0,   152,
     120,     0,     0,     0,     0,     0,   153,   159,   159,     0,
     154,     0,   175,     0,   147,   159,   176
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    10,    11,    12,    13,    14,   109,   110,    15,    16,
      40,    90,    91,   140,   141,   142,   143,   150,   151,   310,
     330,   347,   282,   283,   204,   205,   206,   207,   298,   208,
      42,   129,    43,   210,   248,   323,   275,   276,   319,   320,
      44,    45,    48,   279,   280,    17,    18,   119,   120,   358,
     359,   220,   221,   226,   257,   173,   114,   171,   365
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -247
static const yytype_int16 yypact[] =
{
     121,   535,    76,     6,    60,    85,   115,   111,   132,   123,
     165,  -247,   147,    -9,   117,  -247,  -247,   178,  -247,    87,
    -247,  -247,  -247,  -247,   535,   535,   -24,  -247,  -247,   207,
     219,   223,   235,   242,   535,   535,   535,   198,   222,   266,
     261,   554,   230,  -247,  -247,  -247,   243,   238,   360,   104,
     107,   252,  -247,    18,   178,   319,   329,  -247,   -14,   163,
    -247,  -247,   163,  -247,  -247,  -247,   200,    12,   295,  -247,
    -247,   279,   301,  -247,  -247,  -247,  -247,  -247,   294,  -247,
    -247,  -247,  -247,  -247,    13,   366,   326,   373,   374,   397,
    -247,   359,   535,   535,   535,   535,   535,   535,   535,   535,
     423,   535,   424,    76,   535,    76,  -247,    63,  -247,   113,
    -247,  -247,   153,   425,   362,   432,   414,   422,   435,    30,
    -247,   377,  -247,  -247,  -247,  -247,  -247,  -247,   689,   197,
     370,   439,   355,   383,   327,   369,  -247,   385,   386,   451,
     390,  -247,   464,   450,    13,    76,    13,    13,    13,   445,
     436,  -247,   486,   395,   318,   191,   191,  -247,  -247,  -247,
    -247,   645,  -247,   624,  -247,   449,   452,  -247,  -247,  -247,
    -247,   210,   425,   455,  -247,   469,   458,   478,   319,  -247,
     419,   535,  -247,   485,  -247,  -247,   228,  -247,  -247,   420,
     616,   488,   511,   416,    13,  -247,   390,   446,   390,   390,
     390,   247,   445,   445,   205,  -247,  -247,  -247,   126,   492,
     479,   515,  -247,  -247,  -247,   521,  -247,   -19,   469,   522,
     161,  -247,  -247,  -247,  -247,   523,   463,   689,  -247,  -247,
    -247,  -247,   460,   450,   472,   473,  -247,   524,   459,  -247,
      11,   246,   445,   445,   535,   474,   144,   527,   513,  -247,
    -247,  -247,   164,    15,  -247,  -247,   268,   233,   526,   115,
     115,   115,   475,  -247,    59,  -247,   540,  -247,   689,    25,
      43,   489,   490,   491,   659,  -247,   493,  -247,  -247,   494,
     505,   445,   537,  -247,  -247,   419,   539,  -247,  -247,  -247,
     574,  -247,   469,   450,   450,   450,   551,  -247,    93,  -247,
      23,   549,   549,   512,   578,   144,   527,   527,   205,   445,
     550,   362,  -247,   345,   221,    13,    25,  -247,   216,   255,
    -247,   296,   549,   298,  -247,  -247,  -247,  -247,   205,   445,
     548,   570,  -247,  -247,  -247,   528,   390,  -247,   589,   549,
    -247,  -247,   308,   525,  -247,   205,   445,  -247,  -247,   573,
     675,  -247,  -247,   549,   205,   601,   310,   538,   177,  -247,
    -247,   602,   601,   541,   604,   313,  -247,   419,   419,   611,
    -247,   592,  -247,   613,  -247,   419,  -247
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -247,  -247,   -37,  -247,   605,   607,   568,   -71,    50,   110,
    -247,  -247,   -38,  -142,   427,  -247,    -5,  -247,  -247,  -247,
    -247,  -247,  -247,  -247,  -194,  -247,  -247,  -247,  -247,    -1,
    -247,  -121,  -232,  -247,  -247,  -247,   317,  -247,  -233,  -234,
    -247,     1,  -247,    92,  -247,   590,    -4,  -247,   467,  -247,
     284,  -193,  -206,  -246,  -247,   336,   595,   477,  -247
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -138
static const yytype_int16 yytable[] =
{
      41,    53,   196,    47,   198,   199,   200,   286,   239,   240,
     106,   186,   277,    65,   255,   130,   137,   138,   285,   242,
     243,   122,     1,    69,    70,   252,   318,     1,    49,    20,
      21,    22,    23,    78,    79,    80,   115,   299,   168,   311,
       2,   168,     3,     4,    71,     2,   255,     3,   266,   267,
     215,   178,    72,   179,   251,  -135,  -135,  -135,  -135,  -135,
    -135,  -135,  -135,    60,    63,   128,   165,   166,  -135,   321,
     134,   277,    27,    28,    29,    30,    31,    32,    33,    46,
     176,   265,    50,   225,   337,   139,   113,   308,    51,   342,
     131,   152,   153,   154,   155,   156,   157,   158,   159,   314,
     161,   131,    38,   163,   162,   351,   164,   107,   255,   123,
     107,    66,   125,    65,   300,   328,   107,   264,    52,    68,
     356,   371,   372,    61,    64,   212,    56,   108,   181,   376,
     111,   128,   297,    54,   190,   345,   167,   244,    92,    93,
      94,    95,    96,    97,    98,    99,   197,   270,    20,    21,
      22,    23,   354,     1,    55,    66,   107,     1,    67,    24,
      25,   245,   316,    68,   219,    57,   317,   219,    58,   124,
      26,     2,   126,   336,     4,     2,   169,     3,     4,     5,
     227,     6,     7,     8,   254,     9,   233,   284,   271,   272,
     273,    27,    28,    29,    30,    31,    32,    33,   362,     1,
     363,    81,   241,    19,    20,    21,    22,    23,    97,    98,
      99,    73,    34,   242,   243,    24,    25,     2,    35,    36,
      37,    38,    39,    74,   219,    82,    26,    75,  -136,  -136,
    -136,  -136,  -136,  -136,  -136,  -136,     5,   128,     6,    76,
       8,  -136,     9,   268,   335,   274,    77,    27,    28,    29,
      30,    31,    32,    33,   293,   294,   295,   244,    92,    93,
      94,    95,    96,    97,    98,    99,   181,   182,    34,    83,
     127,   287,   288,   289,    35,    36,    37,    38,    39,   215,
     216,   245,    19,    20,    21,    22,    23,   338,    84,    85,
      86,    87,    88,    89,    24,    25,   133,   181,   229,   101,
     274,   274,   290,   291,   274,    26,    92,    93,    94,    95,
      96,    97,    98,    99,   102,    66,   136,   103,    67,   238,
     113,   274,   118,    68,   339,   340,    27,    28,    29,    30,
      31,    32,    33,    95,    96,    97,    98,    99,   274,    92,
      93,    94,    95,    96,    97,    98,    99,    34,   332,   333,
     334,   121,   274,    35,    36,    37,    38,    39,    19,    20,
      21,    22,    23,   132,   136,   339,   341,   343,   344,   135,
      24,    25,    19,    20,    21,    22,    23,   339,   352,   339,
     360,    26,   369,   370,    24,    25,   189,    84,    85,    86,
      87,    88,    89,   144,   145,    26,   149,   188,   326,   327,
     146,   147,    27,    28,    29,    30,    31,    32,    33,    94,
      95,    96,    97,    98,    99,   104,    27,    28,    29,    30,
      31,    32,    33,    34,   148,   185,   160,   130,   170,    35,
      36,    37,    38,    39,   172,   174,   175,    34,   177,   105,
     180,   183,   184,    35,    36,    37,    38,    39,   201,    20,
      21,    22,    23,   187,   193,   202,   191,   192,     1,   194,
      24,    25,    19,    20,    21,    22,    23,   195,   115,   209,
     213,    26,   219,   214,    24,    25,     2,   218,     3,     4,
       5,   222,     6,   223,     8,    26,     9,   225,   228,   235,
     230,   232,    27,    28,    29,    30,    31,    32,    33,    93,
      94,    95,    96,    97,    98,    99,    27,    28,    29,    30,
      31,    32,    33,   203,   234,   237,   246,   247,   249,    35,
      36,    37,    38,    39,   250,   253,   256,    34,   258,   262,
     278,   259,   263,    35,    36,    37,    38,    39,    19,    20,
      21,    22,    23,   260,   261,   296,   269,   281,   292,   243,
      24,    25,   270,    20,    21,    22,    23,   301,   302,   303,
     312,    26,   305,   306,    24,    25,    92,    93,    94,    95,
      96,    97,    98,    99,   307,    26,   309,   313,   315,   100,
     322,   324,    27,    28,    29,    30,    31,    32,    33,   346,
     329,   348,   350,   353,   349,   355,    27,    28,    29,    30,
      31,    32,    33,    34,   357,   364,   361,   368,   367,    35,
      36,    37,    38,    39,   373,   374,   375,    34,   112,    62,
      59,   236,   325,    35,    36,    37,    38,    39,    92,    93,
      94,    95,    96,    97,    98,    99,    92,    93,    94,    95,
      96,    97,    98,    99,   117,   224,   366,   331,   116,   217,
       0,    84,    85,    86,    87,    88,    89,    92,    93,    94,
      95,    96,    97,    98,    99,     0,     0,     0,     0,     0,
     211,    92,    93,    94,    95,    96,    97,    98,    99,     0,
       0,     0,     0,     0,   304,     0,   231,  -137,  -137,  -137,
    -137,  -137,  -137,  -137,  -137,     0,     0,     0,     0,     0,
    -137,    92,    93,    94,    95,    96,    97,    98,    99
};

static const yytype_int16 yycheck[] =
{
       1,     6,   144,     2,   146,   147,   148,   253,   202,   203,
      48,   132,   246,    17,   220,     3,     3,     4,     3,     8,
       9,    58,    36,    24,    25,   218,     3,    36,    22,     4,
       5,     6,     7,    34,    35,    36,    18,   269,   109,   285,
      54,   112,    56,    57,    68,    54,   252,    56,   242,   243,
      69,    21,    76,    23,    73,    12,    13,    14,    15,    16,
      17,    18,    19,    13,    14,    66,     3,     4,    25,   302,
      71,   305,    47,    48,    49,    50,    51,    52,    53,     3,
     117,    70,    22,    68,   316,    72,    68,   281,     3,   322,
      78,    92,    93,    94,    95,    96,    97,    98,    99,   292,
     101,    78,    77,   104,   103,   339,   105,     3,   314,    59,
       3,    68,    62,   117,    71,   309,     3,   238,     3,    76,
     353,   367,   368,    13,    14,   163,     3,    23,    69,   375,
      23,   132,    73,    22,   135,   329,    23,    11,    12,    13,
      14,    15,    16,    17,    18,    19,   145,     3,     4,     5,
       6,     7,   346,    36,    22,    68,     3,    36,    71,    15,
      16,    35,    69,    76,     3,     0,    73,     3,    21,    59,
      26,    54,    62,   315,    57,    54,    23,    56,    57,    58,
     181,    60,    61,    62,    23,    64,   191,    23,    44,    45,
      46,    47,    48,    49,    50,    51,    52,    53,    21,    36,
      23,     3,   203,     3,     4,     5,     6,     7,    17,    18,
      19,     4,    68,     8,     9,    15,    16,    54,    74,    75,
      76,    77,    78,     4,     3,     3,    26,     4,    12,    13,
      14,    15,    16,    17,    18,    19,    58,   238,    60,     4,
      62,    25,    64,   244,    23,   246,     4,    47,    48,    49,
      50,    51,    52,    53,   259,   260,   261,    11,    12,    13,
      14,    15,    16,    17,    18,    19,    69,    70,    68,     3,
      70,     3,     4,     5,    74,    75,    76,    77,    78,    69,
      70,    35,     3,     4,     5,     6,     7,    71,    27,    28,
      29,    30,    31,    32,    15,    16,    17,    69,    70,    69,
     301,   302,    69,    70,   305,    26,    12,    13,    14,    15,
      16,    17,    18,    19,    71,    68,    70,    79,    71,    72,
      68,   322,     3,    76,    69,    70,    47,    48,    49,    50,
      51,    52,    53,    15,    16,    17,    18,    19,   339,    12,
      13,    14,    15,    16,    17,    18,    19,    68,     3,     4,
       5,    22,   353,    74,    75,    76,    77,    78,     3,     4,
       5,     6,     7,    68,    70,    69,    70,    69,    70,    68,
      15,    16,     3,     4,     5,     6,     7,    69,    70,    69,
      70,    26,    69,    70,    15,    16,    17,    27,    28,    29,
      30,    31,    32,    27,    68,    26,    37,    70,   306,   307,
      27,    27,    47,    48,    49,    50,    51,    52,    53,    14,
      15,    16,    17,    18,    19,    55,    47,    48,    49,    50,
      51,    52,    53,    68,    27,    70,     3,     3,     3,    74,
      75,    76,    77,    78,    72,     3,    22,    68,     3,    79,
      63,    71,     3,    74,    75,    76,    77,    78,     3,     4,
       5,     6,     7,    70,     3,    10,    71,    71,    36,    69,
      15,    16,     3,     4,     5,     6,     7,     3,    18,    33,
      21,    26,     3,    21,    15,    16,    54,    22,    56,    57,
      58,    23,    60,     5,    62,    26,    64,    68,     3,    73,
      70,     3,    47,    48,    49,    50,    51,    52,    53,    13,
      14,    15,    16,    17,    18,    19,    47,    48,    49,    50,
      51,    52,    53,    68,     3,    69,    24,    38,     3,    74,
      75,    76,    77,    78,     3,     3,     3,    68,    65,     5,
       3,    71,    73,    74,    75,    76,    77,    78,     3,     4,
       5,     6,     7,    71,    71,    70,    72,    34,    22,     9,
      15,    16,     3,     4,     5,     6,     7,    68,    68,    68,
      21,    26,    69,    69,    15,    16,    12,    13,    14,    15,
      16,    17,    18,    19,    69,    26,    39,     3,    27,    25,
      68,     3,    47,    48,    49,    50,    51,    52,    53,    41,
      40,    21,     3,    68,    66,    22,    47,    48,    49,    50,
      51,    52,    53,    68,     3,     3,    68,     3,    67,    74,
      75,    76,    77,    78,     3,    23,     3,    68,    50,    14,
      13,   194,   305,    74,    75,    76,    77,    78,    12,    13,
      14,    15,    16,    17,    18,    19,    12,    13,    14,    15,
      16,    17,    18,    19,    54,   178,   362,   311,    53,   172,
      -1,    27,    28,    29,    30,    31,    32,    12,    13,    14,
      15,    16,    17,    18,    19,    -1,    -1,    -1,    -1,    -1,
      25,    12,    13,    14,    15,    16,    17,    18,    19,    -1,
      -1,    -1,    -1,    -1,    25,    -1,    70,    12,    13,    14,
      15,    16,    17,    18,    19,    -1,    -1,    -1,    -1,    -1,
      25,    12,    13,    14,    15,    16,    17,    18,    19
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,    36,    54,    56,    57,    58,    60,    61,    62,    64,
      81,    82,    83,    84,    85,    88,    89,   125,   126,     3,
       4,     5,     6,     7,    15,    16,    26,    47,    48,    49,
      50,    51,    52,    53,    68,    74,    75,    76,    77,    78,
      90,   109,   110,   112,   120,   121,     3,   121,   122,    22,
      22,     3,     3,    96,    22,    22,     3,     0,    21,    85,
      88,    89,    84,    88,    89,   126,    68,    71,    76,   109,
     109,    68,    76,     4,     4,     4,     4,     4,   109,   109,
     109,     3,     3,     3,    27,    28,    29,    30,    31,    32,
      91,    92,    12,    13,    14,    15,    16,    17,    18,    19,
      25,    69,    71,    79,    55,    79,    92,     3,    23,    86,
      87,    23,    86,    68,   136,    18,   136,   125,     3,   127,
     128,    22,    82,    88,    89,    88,    89,    70,   109,   111,
       3,    78,    68,    17,   109,    68,    70,     3,     4,    72,
      93,    94,    95,    96,    27,    68,    27,    27,    27,    37,
      97,    98,   109,   109,   109,   109,   109,   109,   109,   109,
       3,   109,   121,   109,   121,     3,     4,    23,    87,    23,
       3,   137,    72,   135,     3,    22,    82,     3,    21,    23,
      63,    69,    70,    71,     3,    70,   111,    70,    70,    17,
     109,    71,    71,     3,    69,     3,    93,   121,    93,    93,
      93,     3,    10,    68,   104,   105,   106,   107,   109,    33,
     113,    25,    92,    21,    21,    69,    70,   137,    22,     3,
     131,   132,    23,     5,   128,    68,   133,   109,     3,    70,
      70,    70,     3,    96,     3,    73,    94,    69,    72,   104,
     104,   109,     8,     9,    11,    35,    24,    38,   114,     3,
       3,    73,   131,     3,    23,   132,     3,   134,    65,    71,
      71,    71,     5,    73,   111,    70,   104,   104,   109,    72,
       3,    44,    45,    46,   109,   116,   117,   119,     3,   123,
     124,    34,   102,   103,    23,     3,   133,     3,     4,     5,
      69,    70,    22,    96,    96,    96,    70,    73,   108,   112,
      71,    68,    68,    68,    25,    69,    69,    69,   104,    39,
      99,   133,    21,     3,   131,    27,    69,    73,     3,   118,
     119,   118,    68,   115,     3,   116,   123,   123,   104,    40,
     100,   135,     3,     4,     5,    23,    93,   112,    71,    69,
      70,    70,   118,    69,    70,   104,    41,   101,    21,    66,
       3,   119,    70,    68,   104,    22,   118,     3,   129,   130,
      70,    68,    21,    23,     3,   138,   130,    67,     3,    69,
      70,   133,   133,     3,    23,     3,   133
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      yytoken = YYTRANSLATE (yychar);				\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (YYLEX_PARAM)
#else
# define YYLEX yylex ()
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  yy_symbol_value_print (yyoutput, yytype, yyvaluep);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *bottom, yytype_int16 *top)
#else
static void
yy_stack_print (bottom, top)
    yytype_int16 *bottom;
    yytype_int16 *top;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; bottom <= top; ++bottom)
    YYFPRINTF (stderr, " %d", *bottom);
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, int yyrule)
#else
static void
yy_reduce_print (yyvsp, yyrule)
    YYSTYPE *yyvsp;
    int yyrule;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      fprintf (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       		       );
      fprintf (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, Rule); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep)
#else
static void
yydestruct (yymsg, yytype, yyvaluep)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
#endif
{
  YYUSE (yyvaluep);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */

#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */



/* The look-ahead symbol.  */
int yychar;

/* The semantic value of the look-ahead symbol.  */
YYSTYPE yylval;

/* Number of syntax errors so far.  */
int yynerrs;



/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void)
#else
int
yyparse ()

#endif
#endif
{
  
  int yystate;
  int yyn;
  int yyresult;
  /* Number of tokens to shift before error messages enabled.  */
  int yyerrstatus;
  /* Look-ahead token as an internal (translated) token number.  */
  int yytoken = 0;
#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

  /* Three stacks and their tools:
     `yyss': related to states,
     `yyvs': related to semantic values,
     `yyls': related to locations.

     Refer to the stacks thru separate pointers, to allow yyoverflow
     to reallocate them elsewhere.  */

  /* The state stack.  */
  yytype_int16 yyssa[YYINITDEPTH];
  yytype_int16 *yyss = yyssa;
  yytype_int16 *yyssp;

  /* The semantic value stack.  */
  YYSTYPE yyvsa[YYINITDEPTH];
  YYSTYPE *yyvs = yyvsa;
  YYSTYPE *yyvsp;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  YYSIZE_T yystacksize = YYINITDEPTH;

  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;


  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY;		/* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */

  yyssp = yyss;
  yyvsp = yyvs;

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;


	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),

		    &yystacksize);

	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss);
	YYSTACK_RELOCATE (yyvs);

#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;


      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     look-ahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to look-ahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a look-ahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid look-ahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  if (yyn == YYFINAL)
    YYACCEPT;

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the look-ahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token unless it is eof.  */
  if (yychar != YYEOF)
    yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:
#line 189 "fta.y"
    {
		fta_parse_result->parse_tree_list = (yyvsp[(1) - (1)].q_list);
		fta_parse_result->tables = NULL;
		fta_parse_result->parse_type = QUERY_PARSE;
	;}
    break;

  case 3:
#line 194 "fta.y"
    {
		fta_parse_result->parse_tree_list = NULL;
		fta_parse_result->fta_parse_tree = NULL;
		fta_parse_result->tables = (yyvsp[(1) - (1)].table_list_schema);
		fta_parse_result->parse_type = TABLE_PARSE;
	;}
    break;

  case 4:
#line 201 "fta.y"
    {
		fta_parse_result->parse_tree_list = NULL;
		fta_parse_result->fta_parse_tree = (yyvsp[(4) - (5)].tblp);
		fta_parse_result->tables = (yyvsp[(3) - (5)].table_list_schema);
		fta_parse_result->parse_type = STREAM_PARSE;
	;}
    break;

  case 5:
#line 215 "fta.y"
    {
				(yyvsp[(3) - (3)].tblp)->add_nmap((yyvsp[(1) - (3)].var_defs));			// Memory leak : plug it.
				(yyvsp[(3) - (3)].tblp)->add_param_list((yyvsp[(2) - (3)].var_defs));		// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(3) - (3)].tblp);
			;}
    break;

  case 6:
#line 220 "fta.y"
    {
				(yyvsp[(3) - (3)].tblp)->add_nmap((yyvsp[(2) - (3)].var_defs));			// Memory leak : plug it.
				(yyvsp[(3) - (3)].tblp)->add_param_list((yyvsp[(1) - (3)].var_defs));		// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(3) - (3)].tblp);
			;}
    break;

  case 7:
#line 225 "fta.y"
    {
				(yyvsp[(2) - (2)].tblp)->add_nmap(NULL);			// Memory leak : plug it.
				(yyvsp[(2) - (2)].tblp)->add_param_list((yyvsp[(1) - (2)].var_defs));		// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(2) - (2)].tblp);
			;}
    break;

  case 8:
#line 230 "fta.y"
    {
				(yyvsp[(2) - (2)].tblp)->add_nmap((yyvsp[(1) - (2)].var_defs));			// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(2) - (2)].tblp);
			;}
    break;

  case 9:
#line 234 "fta.y"
    {
				(yyval.tblp) = (yyvsp[(1) - (1)].tblp);
			;}
    break;

  case 10:
#line 237 "fta.y"
    {
				(yyvsp[(3) - (3)].tblp)->add_nmap((yyvsp[(1) - (3)].var_defs));			// Memory leak : plug it.
				(yyvsp[(3) - (3)].tblp)->add_param_list((yyvsp[(2) - (3)].var_defs));		// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(3) - (3)].tblp);
			;}
    break;

  case 11:
#line 242 "fta.y"
    {
				(yyvsp[(3) - (3)].tblp)->add_nmap((yyvsp[(2) - (3)].var_defs));			// Memory leak : plug it.
				(yyvsp[(3) - (3)].tblp)->add_param_list((yyvsp[(1) - (3)].var_defs));		// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(3) - (3)].tblp);
			;}
    break;

  case 12:
#line 247 "fta.y"
    {
				(yyvsp[(2) - (2)].tblp)->add_param_list((yyvsp[(1) - (2)].var_defs));		// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(2) - (2)].tblp);
			;}
    break;

  case 13:
#line 251 "fta.y"
    {
				(yyvsp[(2) - (2)].tblp)->add_nmap((yyvsp[(1) - (2)].var_defs));			// Memory leak : plug it.
				(yyval.tblp) = (yyvsp[(2) - (2)].tblp);
			;}
    break;

  case 14:
#line 255 "fta.y"
    {
				(yyval.tblp) = (yyvsp[(1) - (1)].tblp);
			;}
    break;

  case 15:
#line 260 "fta.y"
    {(yyval.q_list) = new query_list_t((yyvsp[(1) - (1)].tblp));;}
    break;

  case 16:
#line 261 "fta.y"
    {(yyval.q_list) = (yyvsp[(1) - (3)].q_list)->append((yyvsp[(3) - (3)].tblp));;}
    break;

  case 17:
#line 265 "fta.y"
    {(yyval.var_defs)=(yyvsp[(3) - (4)].var_defs);;}
    break;

  case 18:
#line 266 "fta.y"
    {(yyval.var_defs)=NULL;;}
    break;

  case 19:
#line 270 "fta.y"
    {(yyval.var_defs)=(yyvsp[(3) - (4)].var_defs);fta_parse_defines=(yyvsp[(3) - (4)].var_defs);;}
    break;

  case 20:
#line 271 "fta.y"
    {(yyval.var_defs)=NULL;fta_parse_defines = NULL;;}
    break;

  case 21:
#line 275 "fta.y"
    {(yyval.var_defs) = new var_defs_t((yyvsp[(1) - (1)].var_pair));;}
    break;

  case 22:
#line 276 "fta.y"
    {(yyval.var_defs) = (yyvsp[(1) - (2)].var_defs)->add_var_pair((yyvsp[(2) - (2)].var_pair));;}
    break;

  case 23:
#line 280 "fta.y"
    {(yyval.var_pair) = new var_pair_t((yyvsp[(1) - (3)].strval),(yyvsp[(2) - (3)].strval));;}
    break;

  case 24:
#line 281 "fta.y"
    {(yyval.var_pair) = new var_pair_t((yyvsp[(1) - (3)].strval),(yyvsp[(2) - (3)].strval));;}
    break;

  case 25:
#line 285 "fta.y"
    {(yyval.tblp) = (yyvsp[(3) - (3)].tblp)->add_selection((yyvsp[(2) - (3)].select_listval));;}
    break;

  case 26:
#line 290 "fta.y"
    {(yyval.tblp) = new table_exp_t((yyvsp[(2) - (3)].clist),(yyvsp[(3) - (3)].tbl_list));;}
    break;

  case 27:
#line 292 "fta.y"
    {(yyval.tblp) = new table_exp_t((yyvsp[(2) - (5)].clist),(yyvsp[(4) - (5)].scalarval),(yyvsp[(5) - (5)].tbl_list));;}
    break;

  case 28:
#line 299 "fta.y"
    { (yyval.select_listval) = (yyvsp[(1) - (1)].select_listval);;}
    break;

  case 29:
#line 311 "fta.y"
    {(yyval.tblp)=new table_exp_t((yyvsp[(1) - (8)].tbl_list),(yyvsp[(2) - (8)].predp),(yyvsp[(3) - (8)].extended_gb_list),(yyvsp[(4) - (8)].clist),(yyvsp[(5) - (8)].predp),(yyvsp[(6) - (8)].predp),(yyvsp[(7) - (8)].predp), (yyvsp[(8) - (8)].predp));;}
    break;

  case 30:
#line 315 "fta.y"
    {(yyval.tbl_list) = (yyvsp[(2) - (2)].tbl_list); (yyval.tbl_list)->set_properties(-1);;}
    break;

  case 31:
#line 316 "fta.y"
    {(yyval.tbl_list) = (yyvsp[(3) - (3)].tbl_list); (yyval.tbl_list)->set_properties(INNER_JOIN_PROPERTY);;}
    break;

  case 32:
#line 317 "fta.y"
    {(yyval.tbl_list) = (yyvsp[(3) - (3)].tbl_list); (yyval.tbl_list)->set_properties(OUTER_JOIN_PROPERTY);;}
    break;

  case 33:
#line 318 "fta.y"
    {(yyval.tbl_list) = (yyvsp[(3) - (3)].tbl_list); (yyval.tbl_list)->set_properties(RIGHT_OUTER_JOIN_PROPERTY);;}
    break;

  case 34:
#line 319 "fta.y"
    {(yyval.tbl_list) = (yyvsp[(3) - (3)].tbl_list); (yyval.tbl_list)->set_properties(LEFT_OUTER_JOIN_PROPERTY);;}
    break;

  case 35:
#line 320 "fta.y"
    {(yyval.tbl_list) = (yyvsp[(8) - (8)].tbl_list); (yyval.tbl_list)->set_properties(FILTER_JOIN_PROPERTY); (yyval.tbl_list)->set_colref((yyvsp[(3) - (8)].colref)); (yyval.tbl_list)->set_temporal_range((yyvsp[(5) - (8)].strval));;}
    break;

  case 36:
#line 324 "fta.y"
    {(yyval.tbl_list) = new tablevar_list_t((yyvsp[(1) - (1)].table));;}
    break;

  case 37:
#line 325 "fta.y"
    {(yyval.tbl_list)= (yyvsp[(1) - (3)].tbl_list)->append_table((yyvsp[(3) - (3)].table));;}
    break;

  case 38:
#line 329 "fta.y"
    { (yyval.table) = (yyvsp[(1) - (1)].table);;}
    break;

  case 39:
#line 330 "fta.y"
    { (yyval.table)= (yyvsp[(1) - (2)].table)->set_range_var((yyvsp[(2) - (2)].strval));;}
    break;

  case 40:
#line 334 "fta.y"
    {(yyval.table) = new tablevar_t((yyvsp[(1) - (1)].stringval)->c_str());;}
    break;

  case 41:
#line 335 "fta.y"
    {(yyval.table) = new tablevar_t((yyvsp[(1) - (3)].strval),(yyvsp[(3) - (3)].stringval)->c_str(),0);;}
    break;

  case 42:
#line 336 "fta.y"
    {(yyval.table) = new tablevar_t((yyvsp[(1) - (5)].strval),(yyvsp[(3) - (5)].strval),(yyvsp[(5) - (5)].stringval)->c_str());;}
    break;

  case 43:
#line 337 "fta.y"
    {(yyval.table) = new tablevar_t((yyvsp[(1) - (5)].strval),(yyvsp[(3) - (5)].strval),(yyvsp[(5) - (5)].stringval)->c_str());;}
    break;

  case 44:
#line 338 "fta.y"
    {(yyval.table) = new tablevar_t((yyvsp[(2) - (5)].strval),(yyvsp[(5) - (5)].stringval)->c_str(),1);;}
    break;

  case 45:
#line 342 "fta.y"
    {(yyval.stringval) = new string_t((yyvsp[(1) - (1)].strval));;}
    break;

  case 46:
#line 343 "fta.y"
    {(yyval.stringval) = (yyval.stringval)->append("/",(yyvsp[(3) - (3)].strval));;}
    break;

  case 47:
#line 348 "fta.y"
    {(yyval.predp)=NULL;;}
    break;

  case 48:
#line 349 "fta.y"
    {(yyval.predp)=(yyvsp[(1) - (1)].predp);;}
    break;

  case 49:
#line 353 "fta.y"
    {(yyval.predp) = (yyvsp[(2) - (2)].predp);;}
    break;

  case 50:
#line 357 "fta.y"
    {(yyval.predp)=NULL;;}
    break;

  case 51:
#line 358 "fta.y"
    {(yyval.predp)=(yyvsp[(2) - (2)].predp); ;}
    break;

  case 52:
#line 362 "fta.y"
    {(yyval.predp)=NULL;;}
    break;

  case 53:
#line 363 "fta.y"
    {(yyval.predp)=(yyvsp[(2) - (2)].predp); ;}
    break;

  case 54:
#line 367 "fta.y"
    {(yyval.predp)=NULL;;}
    break;

  case 55:
#line 368 "fta.y"
    {(yyval.predp)=(yyvsp[(2) - (2)].predp); ;}
    break;

  case 56:
#line 372 "fta.y"
    {(yyval.predp)=NULL;;}
    break;

  case 57:
#line 373 "fta.y"
    {(yyval.predp)=(yyvsp[(1) - (1)].predp);;}
    break;

  case 58:
#line 377 "fta.y"
    {(yyval.predp) = (yyvsp[(2) - (2)].predp);;}
    break;

  case 59:
#line 384 "fta.y"
    {(yyval.predp)=new predicate_t("OR",(yyvsp[(1) - (3)].predp),(yyvsp[(3) - (3)].predp));;}
    break;

  case 60:
#line 385 "fta.y"
    {(yyval.predp)=new predicate_t("AND",(yyvsp[(1) - (3)].predp),(yyvsp[(3) - (3)].predp));;}
    break;

  case 61:
#line 386 "fta.y"
    {(yyval.predp) = new predicate_t("NOT", (yyvsp[(2) - (2)].predp) );;}
    break;

  case 62:
#line 387 "fta.y"
    {(yyval.predp) = (yyvsp[(2) - (3)].predp);;}
    break;

  case 63:
#line 388 "fta.y"
    {(yyval.predp) = (yyvsp[(1) - (1)].predp);;}
    break;

  case 64:
#line 392 "fta.y"
    { (yyval.predp)=(yyvsp[(1) - (1)].predp);;}
    break;

  case 65:
#line 393 "fta.y"
    { (yyval.predp) = (yyvsp[(1) - (1)].predp);;}
    break;

  case 66:
#line 394 "fta.y"
    {(yyval.predp) = predicate_t::make_paramless_fcn_predicate((yyvsp[(1) - (3)].strval)); ;}
    break;

  case 67:
#line 395 "fta.y"
    {(yyval.predp) = new predicate_t((yyvsp[(1) - (4)].strval), (yyvsp[(3) - (4)].se_listval)->get_se_list()); ;}
    break;

  case 68:
#line 399 "fta.y"
    {(yyval.predp) = new predicate_t((yyvsp[(1) - (3)].scalarval),(yyvsp[(2) - (3)].strval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 69:
#line 403 "fta.y"
    { (yyval.predp) = new predicate_t((yyvsp[(1) - (5)].scalarval),(yyvsp[(4) - (5)].lit_l)); ;}
    break;

  case 70:
#line 407 "fta.y"
    {(yyval.lit_l) = new literal_list_t((yyvsp[(1) - (1)].litval));;}
    break;

  case 71:
#line 408 "fta.y"
    {(yyval.lit_l) = (yyvsp[(1) - (3)].lit_l)->append_literal((yyvsp[(3) - (3)].litval));;}
    break;

  case 72:
#line 414 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("+",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 73:
#line 415 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("-",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 74:
#line 416 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("|",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 75:
#line 417 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("*",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 76:
#line 418 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("/",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 77:
#line 419 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("&",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 78:
#line 420 "fta.y"
    { (yyval.scalarval)=new scalarexp_t("%",(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 79:
#line 421 "fta.y"
    { (yyval.scalarval)=new scalarexp_t((yyvsp[(2) - (3)].strval),(yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].scalarval));;}
    break;

  case 80:
#line 422 "fta.y"
    { (yyval.scalarval) = new scalarexp_t("+",(yyvsp[(2) - (2)].scalarval));;}
    break;

  case 81:
#line 423 "fta.y"
    { (yyval.scalarval) = new scalarexp_t("-",(yyvsp[(2) - (2)].scalarval));;}
    break;

  case 82:
#line 424 "fta.y"
    { (yyval.scalarval) = new scalarexp_t("!",(yyvsp[(2) - (2)].scalarval));;}
    break;

  case 83:
#line 425 "fta.y"
    { (yyval.scalarval) = new scalarexp_t("~",(yyvsp[(2) - (2)].scalarval));;}
    break;

  case 84:
#line 426 "fta.y"
    { (yyval.scalarval)= new scalarexp_t((yyvsp[(1) - (1)].litval));;}
    break;

  case 85:
#line 427 "fta.y"
    {(yyval.scalarval) = scalarexp_t::make_param_reference((yyvsp[(2) - (2)].strval));;}
    break;

  case 86:
#line 428 "fta.y"
    {(yyval.scalarval) = scalarexp_t::make_iface_param_reference((yyvsp[(1) - (1)].ifpref));;}
    break;

  case 87:
#line 429 "fta.y"
    { (yyval.scalarval) = new scalarexp_t((yyvsp[(1) - (1)].colref));;}
    break;

  case 88:
#line 430 "fta.y"
    {(yyval.scalarval) = (yyvsp[(2) - (3)].scalarval);;}
    break;

  case 89:
#line 431 "fta.y"
    { (yyval.scalarval) = scalarexp_t::make_star_aggr((yyvsp[(1) - (4)].strval)); ;}
    break;

  case 90:
#line 432 "fta.y"
    { (yyval.scalarval) = scalarexp_t::make_se_aggr((yyvsp[(1) - (4)].strval),(yyvsp[(3) - (4)].scalarval)); ;}
    break;

  case 91:
#line 433 "fta.y"
    {(yyval.scalarval) = new scalarexp_t((yyvsp[(1) - (4)].strval), (yyvsp[(3) - (4)].se_listval)->get_se_list()); ;}
    break;

  case 92:
#line 434 "fta.y"
    {(yyval.scalarval) = scalarexp_t::make_paramless_fcn((yyvsp[(1) - (3)].strval)); ;}
    break;

  case 93:
#line 435 "fta.y"
    { (yyval.scalarval) = scalarexp_t::make_star_aggr((yyvsp[(1) - (5)].strval)); (yyval.scalarval)->set_superaggr(true); ;}
    break;

  case 94:
#line 436 "fta.y"
    { (yyval.scalarval) = scalarexp_t::make_se_aggr((yyvsp[(1) - (5)].strval),(yyvsp[(4) - (5)].scalarval)); (yyval.scalarval)->set_superaggr(true); ;}
    break;

  case 95:
#line 437 "fta.y"
    {(yyval.scalarval) = new scalarexp_t((yyvsp[(1) - (5)].strval), (yyvsp[(4) - (5)].se_listval)->get_se_list()); (yyval.scalarval)->set_superaggr(true); ;}
    break;

  case 96:
#line 438 "fta.y"
    {(yyval.scalarval) = scalarexp_t::make_paramless_fcn((yyvsp[(1) - (4)].strval)); (yyval.scalarval)->set_superaggr(true); ;}
    break;

  case 97:
#line 443 "fta.y"
    {	(yyval.select_listval)= new select_list_t((yyvsp[(1) - (1)].scalarval)); ;}
    break;

  case 98:
#line 444 "fta.y"
    {	(yyval.select_listval)= new select_list_t((yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].strval)); ;}
    break;

  case 99:
#line 445 "fta.y"
    { (yyval.select_listval)=(yyvsp[(1) - (3)].select_listval)->append((yyvsp[(3) - (3)].scalarval)); ;}
    break;

  case 100:
#line 446 "fta.y"
    { (yyval.select_listval)=(yyvsp[(1) - (5)].select_listval)->append((yyvsp[(3) - (5)].scalarval),(yyvsp[(5) - (5)].strval)); ;}
    break;

  case 101:
#line 450 "fta.y"
    {	(yyval.se_listval)= new se_list_t((yyvsp[(1) - (1)].scalarval)); ;}
    break;

  case 102:
#line 451 "fta.y"
    { (yyval.se_listval)=(yyvsp[(1) - (3)].se_listval)->append((yyvsp[(3) - (3)].scalarval)); ;}
    break;

  case 103:
#line 455 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(1) - (1)].strval),LITERAL_STRING);;}
    break;

  case 104:
#line 456 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(1) - (1)].strval),LITERAL_INT);;}
    break;

  case 105:
#line 457 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(1) - (1)].strval),LITERAL_LONGINT);;}
    break;

  case 106:
#line 458 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(1) - (1)].strval),LITERAL_FLOAT);;}
    break;

  case 107:
#line 459 "fta.y"
    {(yyval.litval) = new literal_t("TRUE",LITERAL_BOOL);;}
    break;

  case 108:
#line 460 "fta.y"
    {(yyval.litval) = new literal_t("FALSE",LITERAL_BOOL);;}
    break;

  case 109:
#line 461 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(2) - (2)].strval),LITERAL_TIMEVAL);;}
    break;

  case 110:
#line 462 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(2) - (2)].strval),LITERAL_HEX);;}
    break;

  case 111:
#line 463 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(2) - (2)].strval),LITERAL_LONGHEX);;}
    break;

  case 112:
#line 464 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(2) - (2)].strval),LITERAL_IP);;}
    break;

  case 113:
#line 465 "fta.y"
    {(yyval.litval) = new literal_t((yyvsp[(2) - (2)].strval),LITERAL_IPV6);;}
    break;

  case 114:
#line 466 "fta.y"
    {(yyval.litval) = literal_t::make_define_literal((yyvsp[(2) - (2)].strval),fta_parse_defines);;}
    break;

  case 115:
#line 472 "fta.y"
    {(yyval.extended_gb_list) = NULL;;}
    break;

  case 116:
#line 473 "fta.y"
    {(yyval.extended_gb_list) = (yyvsp[(3) - (3)].extended_gb_list);;}
    break;

  case 117:
#line 477 "fta.y"
    {(yyval.clist) = NULL;;}
    break;

  case 118:
#line 478 "fta.y"
    {(yyval.clist) = (yyvsp[(2) - (2)].clist);;}
    break;

  case 119:
#line 482 "fta.y"
    { (yyval.list_of_gb_list) = new list_of_gb_list_t((yyvsp[(2) - (3)].gb_list));;}
    break;

  case 120:
#line 483 "fta.y"
    {(yyval.list_of_gb_list) = (yyvsp[(1) - (5)].list_of_gb_list)->append((yyvsp[(4) - (5)].gb_list));;}
    break;

  case 121:
#line 487 "fta.y"
    {(yyval.extended_gb) = extended_gb_t::create_from_gb((yyvsp[(1) - (1)].gb_val));;}
    break;

  case 122:
#line 488 "fta.y"
    {(yyval.extended_gb) = extended_gb_t::extended_create_from_rollup((yyvsp[(3) - (4)].gb_list));;}
    break;

  case 123:
#line 489 "fta.y"
    {(yyval.extended_gb) = extended_gb_t::extended_create_from_cube((yyvsp[(3) - (4)].gb_list));;}
    break;

  case 124:
#line 490 "fta.y"
    {(yyval.extended_gb) = extended_gb_t::extended_create_from_gsets((yyvsp[(3) - (4)].list_of_gb_list));;}
    break;

  case 125:
#line 494 "fta.y"
    { (yyval.extended_gb_list) = new extended_gb_list_t((yyvsp[(1) - (1)].extended_gb));;}
    break;

  case 126:
#line 495 "fta.y"
    { (yyval.extended_gb_list)=(yyvsp[(1) - (3)].extended_gb_list)->append((yyvsp[(3) - (3)].extended_gb));;}
    break;

  case 127:
#line 499 "fta.y"
    { (yyval.gb_list) = new gb_list_t((yyvsp[(1) - (1)].gb_val));;}
    break;

  case 128:
#line 500 "fta.y"
    { (yyval.gb_list)=(yyvsp[(1) - (3)].gb_list)->append((yyvsp[(3) - (3)].gb_val));;}
    break;

  case 129:
#line 504 "fta.y"
    {(yyval.gb_val) = new gb_t((yyvsp[(1) - (1)].strval)); ;}
    break;

  case 130:
#line 505 "fta.y"
    {(yyval.gb_val) = new gb_t((yyvsp[(1) - (3)].strval),(yyvsp[(3) - (3)].strval)); ;}
    break;

  case 131:
#line 506 "fta.y"
    {(yyval.gb_val) = new gb_t((yyvsp[(1) - (5)].strval),(yyvsp[(3) - (5)].strval),(yyvsp[(5) - (5)].strval)); ;}
    break;

  case 132:
#line 507 "fta.y"
    {(yyval.gb_val) = new gb_t((yyvsp[(1) - (3)].scalarval),(yyvsp[(3) - (3)].strval)); ;}
    break;

  case 133:
#line 518 "fta.y"
    {(yyval.ifpref) = new ifpref_t((yyvsp[(2) - (2)].strval));;}
    break;

  case 134:
#line 519 "fta.y"
    {(yyval.ifpref) = new ifpref_t((yyvsp[(1) - (4)].strval), (yyvsp[(4) - (4)].strval));;}
    break;

  case 135:
#line 523 "fta.y"
    {(yyval.colref) = new colref_t((yyvsp[(1) - (1)].strval)); ;}
    break;

  case 136:
#line 524 "fta.y"
    {(yyval.colref) = new colref_t((yyvsp[(1) - (3)].strval),(yyvsp[(3) - (3)].strval)); ;}
    break;

  case 137:
#line 525 "fta.y"
    {(yyval.colref) = new colref_t((yyvsp[(1) - (5)].strval),(yyvsp[(3) - (5)].strval),(yyvsp[(5) - (5)].strval)); ;}
    break;

  case 138:
#line 529 "fta.y"
    {(yyval.clist)=new colref_list_t((yyvsp[(1) - (3)].colref)); (yyval.clist)->append((yyvsp[(3) - (3)].colref));;}
    break;

  case 139:
#line 530 "fta.y"
    {(yyval.clist) = (yyvsp[(1) - (3)].clist)->append((yyvsp[(3) - (3)].colref));;}
    break;

  case 140:
#line 534 "fta.y"
    {(yyval.colref) = new colref_t((yyvsp[(1) - (1)].strval)); ;}
    break;

  case 141:
#line 538 "fta.y"
    {(yyval.clist)=new colref_list_t((yyvsp[(1) - (3)].colref)); (yyval.clist)->append((yyvsp[(3) - (3)].colref));;}
    break;

  case 142:
#line 539 "fta.y"
    {(yyval.clist) = (yyvsp[(1) - (3)].clist)->append((yyvsp[(3) - (3)].colref));;}
    break;

  case 143:
#line 554 "fta.y"
    {(yyval.table_list_schema) = new table_list((yyvsp[(1) - (1)].table_def_t));;}
    break;

  case 144:
#line 555 "fta.y"
    {(yyval.table_list_schema) = (yyvsp[(1) - (2)].table_list_schema)->append_table((yyvsp[(2) - (2)].table_def_t));;}
    break;

  case 145:
#line 558 "fta.y"
    {
					(yyval.table_def_t)=new table_def((yyvsp[(2) - (7)].strval),(yyvsp[(3) - (7)].plist_t),(yyvsp[(4) - (7)].plist_t), (yyvsp[(6) - (7)].field_list_t), PROTOCOL_SCHEMA); delete (yyvsp[(6) - (7)].field_list_t);;}
    break;

  case 146:
#line 560 "fta.y"
    {
					(yyval.table_def_t)=new table_def((yyvsp[(2) - (6)].stringval)->c_str(),(yyvsp[(3) - (6)].plist_t),NULL,(yyvsp[(5) - (6)].field_list_t), STREAM_SCHEMA); delete (yyvsp[(5) - (6)].field_list_t);;}
    break;

  case 147:
#line 562 "fta.y"
    {
				(yyval.table_def_t) = new table_def((yyvsp[(2) - (16)].strval), (yyvsp[(5) - (16)].plist_t), (yyvsp[(8) - (16)].field_list_t), (yyvsp[(12) - (16)].subqueryspec_list_t), (yyvsp[(15) - (16)].plist_t)); ;}
    break;

  case 148:
#line 564 "fta.y"
    { (yyval.table_def_t) = new table_def((yyvsp[(3) - (4)].ufcnl)); ;}
    break;

  case 149:
#line 568 "fta.y"
    {(yyval.ufcnl) = new unpack_fcn_list((yyvsp[(1) - (1)].ufcn));;}
    break;

  case 150:
#line 569 "fta.y"
    {(yyval.ufcnl) = (yyvsp[(1) - (3)].ufcnl) -> append((yyvsp[(3) - (3)].ufcn));;}
    break;

  case 151:
#line 573 "fta.y"
    {(yyval.ufcn) = new unpack_fcn((yyvsp[(1) - (3)].strval),(yyvsp[(2) - (3)].strval),(yyvsp[(3) - (3)].strval));;}
    break;

  case 152:
#line 577 "fta.y"
    {(yyval.subqueryspec_list_t) = new subqueryspec_list((yyvsp[(1) - (1)].subq_spec_t));;}
    break;

  case 153:
#line 578 "fta.y"
    {(yyval.subqueryspec_list_t) = (yyvsp[(1) - (3)].subqueryspec_list_t)->append((yyvsp[(3) - (3)].subq_spec_t));;}
    break;

  case 154:
#line 582 "fta.y"
    {(yyval.subq_spec_t)=new subquery_spec((yyvsp[(1) - (4)].strval), (yyvsp[(3) - (4)].namevec_t)); delete (yyvsp[(3) - (4)].namevec_t);;}
    break;

  case 155:
#line 585 "fta.y"
    {(yyval.field_list_t) = new field_entry_list((yyvsp[(1) - (1)].field_t));;}
    break;

  case 156:
#line 586 "fta.y"
    {(yyval.field_list_t) = (yyvsp[(1) - (2)].field_list_t)->append_field((yyvsp[(2) - (2)].field_t));;}
    break;

  case 157:
#line 590 "fta.y"
    {(yyval.field_t) = new field_entry((yyvsp[(1) - (6)].strval),(yyvsp[(2) - (6)].strval),(yyvsp[(3) - (6)].strval),(yyvsp[(4) - (6)].plist_t),(yyvsp[(5) - (6)].plist_t));;}
    break;

  case 158:
#line 591 "fta.y"
    {(yyval.field_t) = new field_entry((yyvsp[(1) - (4)].strval),(yyvsp[(2) - (4)].strval),"",(yyvsp[(3) - (4)].plist_t),NULL);;}
    break;

  case 159:
#line 595 "fta.y"
    {(yyval.plist_t) = NULL;;}
    break;

  case 160:
#line 596 "fta.y"
    {(yyval.plist_t) = (yyvsp[(2) - (3)].plist_t);;}
    break;

  case 161:
#line 600 "fta.y"
    {(yyval.plist_t) = new param_list((yyvsp[(1) - (1)].strval));;}
    break;

  case 162:
#line 601 "fta.y"
    {(yyval.plist_t) = new param_list((yyvsp[(1) - (2)].strval),(yyvsp[(2) - (2)].strval));;}
    break;

  case 163:
#line 602 "fta.y"
    {(yyval.plist_t) = new param_list((yyvsp[(1) - (2)].strval),(yyvsp[(2) - (2)].strval));;}
    break;

  case 164:
#line 603 "fta.y"
    {(yyval.plist_t) = new param_list((yyvsp[(1) - (2)].strval),(yyvsp[(2) - (2)].strval));;}
    break;

  case 165:
#line 604 "fta.y"
    {(yyval.plist_t) = (yyvsp[(1) - (3)].plist_t)->append((yyvsp[(3) - (3)].strval));;}
    break;

  case 166:
#line 605 "fta.y"
    {(yyval.plist_t) = (yyvsp[(1) - (4)].plist_t)->append((yyvsp[(3) - (4)].strval),(yyvsp[(4) - (4)].strval));;}
    break;

  case 167:
#line 606 "fta.y"
    {(yyval.plist_t) = (yyvsp[(1) - (4)].plist_t)->append((yyvsp[(3) - (4)].strval),(yyvsp[(4) - (4)].strval));;}
    break;

  case 168:
#line 607 "fta.y"
    {(yyval.plist_t) = (yyvsp[(1) - (4)].plist_t)->append((yyvsp[(3) - (4)].strval),(yyvsp[(4) - (4)].strval));;}
    break;

  case 169:
#line 611 "fta.y"
    {(yyval.plist_t) = NULL;;}
    break;

  case 170:
#line 612 "fta.y"
    {(yyval.plist_t) = (yyvsp[(2) - (3)].plist_t);;}
    break;

  case 171:
#line 616 "fta.y"
    {(yyval.plist_t) = NULL;;}
    break;

  case 172:
#line 617 "fta.y"
    {(yyval.plist_t) = (yyvsp[(2) - (3)].plist_t);;}
    break;

  case 173:
#line 621 "fta.y"
    {(yyval.plist_t) = new param_list((yyvsp[(1) - (1)].strval));;}
    break;

  case 174:
#line 622 "fta.y"
    {(yyval.plist_t) = (yyvsp[(1) - (3)].plist_t)->append((yyvsp[(3) - (3)].strval));;}
    break;

  case 175:
#line 626 "fta.y"
    {(yyval.namevec_t) = new name_vec((yyvsp[(1) - (3)].strval),(yyvsp[(2) - (3)].strval),(yyvsp[(3) - (3)].plist_t));;}
    break;

  case 176:
#line 627 "fta.y"
    {(yyval.namevec_t) = (yyvsp[(1) - (5)].namevec_t)->append((yyvsp[(3) - (5)].strval),(yyvsp[(4) - (5)].strval), (yyvsp[(5) - (5)].plist_t));;}
    break;


/* Line 1267 of yacc.c.  */
#line 2843 "fta.tab.cc"
      default: break;
    }
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;


  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (YY_("syntax error"));
#else
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (yymsg);
	  }
	else
	  {
	    yyerror (YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse look-ahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse look-ahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (yyn != YYPACT_NINF)
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;


      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  if (yyn == YYFINAL)
    YYACCEPT;

  *++yyvsp = yylval;


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#ifndef yyoverflow
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEOF && yychar != YYEMPTY)
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval);
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}


#line 630 "fta.y"



