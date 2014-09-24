/* A Bison parser, made from ext_fcns.y, by GNU bison 1.75.  */

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

/* Written by Richard Stallman by simplifying the original so called
   ``semantic'' parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON	1

/* Pure parsers.  */
#define YYPURE	0

/* Using locations.  */
#define YYLSP_NEEDED 0

/* If NAME_PREFIX is specified substitute the variables and functions
   names.  */
#define yyparse Ext_fcnsParserparse
#define yylex   Ext_fcnsParserlex
#define yyerror Ext_fcnsParsererror
#define yylval  Ext_fcnsParserlval
#define yychar  Ext_fcnsParserchar
#define yydebug Ext_fcnsParserdebug
#define yynerrs Ext_fcnsParsernerrs


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING_TOKEN = 259,
     INTNUM = 260,
     SEMICOLON = 261,
     HANDLE = 262,
     CONST = 263,
     CLASS = 264,
     FUN = 265,
     PRED = 266,
     EXTR = 267,
     UDAF = 268,
     STATE = 269,
     SFUN = 270
   };
#endif
#define NAME 258
#define STRING_TOKEN 259
#define INTNUM 260
#define SEMICOLON 261
#define HANDLE 262
#define CONST 263
#define CLASS 264
#define FUN 265
#define PRED 266
#define EXTR 267
#define UDAF 268
#define STATE 269
#define SFUN 270




/* Copy the first part of user declarations.  */
#line 8 "ext_fcns.y"



#include <stdio.h>

/*		Some addn'l includes, necessary but not included by the
		bison generated code.
*/

#include <stdlib.h>

/*		prototypes for the parser callbacks.
*/

#include "parse_ext_fcns.h"

/*		Interface to ext_fcns parser	*/
	int yyparse();
	void yyerror(char *s);
	int yylex();
extern int flex_ext_fcns_lineno, flex_ext_fcns_ch;

/*		Return value	*/
extern ext_fcn_list *Ext_fcns;

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

#ifndef YYSTYPE
#line 40 "ext_fcns.y"
typedef union {
	char* strval;
	ext_fcn_list *ext_fcn_list_t;
	ext_fcn_def *ext_fcn_t;
	ext_fcn_param_list *plist_t;
	param_list *tplist_t;
	ext_fcn_param *param_t;
	ext_fcn_modifier_list *modif_t;
	} yystype;
/* Line 193 of /usr/local/share/bison/yacc.c.  */
#line 151 "ext_fcns.tab.cc"
# define YYSTYPE yystype
# define YYSTYPE_IS_TRIVIAL 1
#endif

#ifndef YYLTYPE
typedef struct yyltype
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} yyltype;
# define YYLTYPE yyltype
# define YYLTYPE_IS_TRIVIAL 1
#endif

/* Copy the second part of user declarations.  */


/* Line 213 of /usr/local/share/bison/yacc.c.  */
#line 172 "ext_fcns.tab.cc"

#if ! defined (yyoverflow) || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# if YYSTACK_USE_ALLOCA
#  define YYSTACK_ALLOC alloca
# else
#  ifndef YYSTACK_USE_ALLOCA
#   if defined (alloca) || defined (_ALLOCA_H)
#    define YYSTACK_ALLOC alloca
#   else
#    ifdef __GNUC__
#     define YYSTACK_ALLOC __builtin_alloca
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning. */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
# else
#  if defined (__STDC__) || defined (__cplusplus)
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   define YYSIZE_T size_t
#  endif
#  define YYSTACK_ALLOC malloc
#  define YYSTACK_FREE free
# endif
#endif /* ! defined (yyoverflow) || YYERROR_VERBOSE */


#if (! defined (yyoverflow) \
     && (! defined (__cplusplus) \
	 || (YYLTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  short yyss;
  YYSTYPE yyvs;
  };

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAX (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (short) + sizeof (YYSTYPE))				\
      + YYSTACK_GAP_MAX)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  register YYSIZE_T yyi;		\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];	\
	}					\
      while (0)
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
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAX;	\
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (0)

#endif

#if defined (__STDC__) || defined (__cplusplus)
   typedef signed char yysigned_char;
#else
   typedef short yysigned_char;
#endif

/* YYFINAL -- State number of the termination state. */
#define YYFINAL  11
#define YYLAST   136

/* YYNTOKENS -- Number of terminals. */
#define YYNTOKENS  21
/* YYNNTS -- Number of nonterminals. */
#define YYNNTS  10
/* YYNRULES -- Number of rules. */
#define YYNRULES  39
/* YYNRULES -- Number of states. */
#define YYNSTATES  117

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   270

#define YYTRANSLATE(X) \
  ((unsigned)(X) <= YYMAXUTOK ? yytranslate[X] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const unsigned char yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
      18,    19,     2,     2,    20,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    16,     2,    17,     2,     2,     2,     2,     2,     2,
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
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const unsigned char yyprhs[] =
{
       0,     0,     3,     5,     7,    10,    22,    31,    41,    48,
      61,    71,    85,    96,   109,   119,   124,   125,   127,   129,
     132,   136,   141,   143,   147,   151,   155,   159,   164,   169,
     172,   173,   177,   179,   182,   185,   188,   192,   197,   202
};

/* YYRHS -- A `-1'-separated list of the rules' RHS. */
static const yysigned_char yyrhs[] =
{
      22,     0,    -1,    23,    -1,    24,    -1,    23,    24,    -1,
       3,    29,    10,    16,    26,    17,     3,    18,    25,    19,
       6,    -1,     3,    29,    10,     3,    18,    25,    19,     6,
      -1,    11,    16,    26,    17,     3,    16,    25,    17,     6,
      -1,    11,     3,    16,    25,    17,     6,    -1,     3,    29,
      13,    16,    26,    17,     3,     3,    18,    25,    19,     6,
      -1,     3,    29,    13,     3,     3,    18,    25,    19,     6,
      -1,     3,    29,    12,    16,    26,    17,     3,     3,     3,
      18,    25,    19,     6,    -1,     3,    29,    12,     3,     3,
       3,    18,    25,    19,     6,    -1,     3,    29,    15,    16,
      26,    17,     3,     3,    18,    25,    19,     6,    -1,     3,
      29,    15,     3,     3,    18,    25,    19,     6,    -1,     3,
      14,     3,     6,    -1,    -1,    27,    -1,     3,    -1,     3,
       3,    -1,    26,    20,     3,    -1,    26,    20,     3,     3,
      -1,    28,    -1,    27,    20,    28,    -1,     3,    29,     7,
      -1,     3,    29,     8,    -1,     3,    29,     9,    -1,     3,
      29,     7,     9,    -1,     3,    29,     8,     9,    -1,     3,
      29,    -1,    -1,    18,    30,    19,    -1,     3,    -1,     3,
       3,    -1,     3,     4,    -1,     3,     5,    -1,    30,    20,
       3,    -1,    30,    20,     3,     3,    -1,    30,    20,     3,
       4,    -1,    30,    20,     3,     5,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const unsigned char yyrline[] =
{
       0,    75,    75,    78,    79,    82,    84,    86,    88,    90,
      92,    94,    96,    98,   100,   102,   107,   109,   112,   114,
     115,   116,   119,   121,   124,   126,   127,   128,   129,   130,
     134,   136,   139,   141,   142,   143,   144,   145,   146,   147
};
#endif

#if YYDEBUG || YYERROR_VERBOSE
/* YYTNME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals. */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING_TOKEN", "INTNUM", 
  "SEMICOLON", "HANDLE", "CONST", "CLASS", "FUN", "PRED", "EXTR", "UDAF", 
  "STATE", "SFUN", "'['", "']'", "'('", "')'", "','", "$accept", "result", 
  "fcn_list", "fcn_def", "opt_param_commalist", "modifier_list", 
  "param_commalist", "param", "opt_type_param_commalist", 
  "type_param_commalist", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const unsigned short yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,    91,    93,    40,    41,
      44
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const unsigned char yyr1[] =
{
       0,    21,    22,    23,    23,    24,    24,    24,    24,    24,
      24,    24,    24,    24,    24,    24,    25,    25,    26,    26,
      26,    26,    27,    27,    28,    28,    28,    28,    28,    28,
      29,    29,    30,    30,    30,    30,    30,    30,    30,    30
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const unsigned char yyr2[] =
{
       0,     2,     1,     1,     2,    11,     8,     9,     6,    12,
       9,    13,    10,    12,     9,     4,     0,     1,     1,     2,
       3,     4,     1,     3,     3,     3,     3,     4,     4,     2,
       0,     3,     1,     2,     2,     2,     3,     4,     4,     4
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const unsigned char yydefact[] =
{
       0,    30,     0,     0,     2,     3,     0,     0,     0,     0,
       0,     1,     4,     0,    32,     0,     0,     0,     0,     0,
      16,    18,     0,    15,    33,    34,    35,    31,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    30,     0,    17,
      22,    19,     0,     0,    36,    16,     0,     0,     0,     0,
       0,     0,     0,    29,     0,     0,     0,    20,    37,    38,
      39,     0,     0,     0,     0,    16,     0,    16,     0,    24,
      25,    26,     8,    23,    16,    21,     0,     0,    16,     0,
       0,     0,     0,     0,    27,    28,     0,     6,    16,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    10,
      16,    14,    16,     7,     0,    12,    16,     0,     0,     5,
       0,     0,     0,     0,     9,    13,    11
};

/* YYDEFGOTO[NTERM-NUM]. */
static const yysigned_char yydefgoto[] =
{
      -1,     3,     4,     5,    38,    22,    39,    40,     8,    15
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -46
static const yysigned_char yypact[] =
{
       8,    -5,    -2,     7,     8,   -46,    25,    35,    11,     9,
      50,   -46,   -46,    48,    41,    22,    -1,     0,     1,     2,
      53,    55,    10,   -46,   -46,   -46,   -46,   -46,    56,    42,
      50,    59,    50,    60,    50,    61,    50,    47,    49,    51,
     -46,   -46,    64,    65,    44,    53,    14,    66,    15,    52,
      19,    54,    20,    43,    67,    53,    58,    72,   -46,   -46,
     -46,    57,    74,    62,    75,    53,    76,    53,    78,    73,
      77,   -46,   -46,   -46,    53,   -46,    79,    69,    53,    80,
      70,    81,    71,    85,   -46,   -46,    82,   -46,    53,    83,
      88,    86,    87,    89,    90,    91,    84,    92,    93,   -46,
      53,   -46,    53,   -46,    94,   -46,    53,    95,    96,   -46,
      97,    98,   100,   101,   -46,   -46,   -46
};

/* YYPGOTO[NTERM-NUM].  */
static const yysigned_char yypgoto[] =
{
     -46,   -46,   -46,   105,   -45,   -24,   -46,    38,    99,   -46
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, parse error.  */
#define YYTABLE_NINF -1
static const unsigned char yytable[] =
{
      61,     9,    29,    31,    33,    35,    46,    11,    48,     6,
      50,     1,    52,     7,    10,    30,    32,    34,    36,     2,
      80,    16,    82,    17,    18,    20,    19,    42,    13,    86,
      43,    62,    64,    89,    43,    43,    66,    68,    14,    43,
      43,    27,    28,    96,    24,    25,    26,    58,    59,    60,
      69,    70,    71,    21,    23,   107,    37,   108,    41,    44,
      45,   110,    47,    49,    51,     7,    54,    56,    57,    63,
      65,    55,    67,    72,    74,    75,    76,    77,    79,    81,
      78,    83,    84,    90,    92,    87,    85,    88,    94,    91,
      93,    98,    99,    73,     0,   101,     0,   103,   105,    95,
     109,     0,    97,   104,   114,   100,   115,   116,   102,    12,
       0,   106,     0,     0,   111,   112,   113,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    53
};

static const yysigned_char yycheck[] =
{
      45,     3,     3,     3,     3,     3,    30,     0,    32,    14,
      34,     3,    36,    18,    16,    16,    16,    16,    16,    11,
      65,    10,    67,    12,    13,    16,    15,    17,     3,    74,
      20,    17,    17,    78,    20,    20,    17,    17,     3,    20,
      20,    19,    20,    88,     3,     4,     5,     3,     4,     5,
       7,     8,     9,     3,     6,   100,     3,   102,     3,     3,
      18,   106,     3,     3,     3,    18,    17,     3,     3,     3,
      18,    20,    18,     6,    16,     3,    19,     3,     3,     3,
      18,     3,     9,     3,     3,     6,     9,    18,     3,    19,
      19,     3,     6,    55,    -1,     6,    -1,     6,     6,    17,
       6,    -1,    19,    19,     6,    18,     6,     6,    18,     4,
      -1,    18,    -1,    -1,    19,    19,    19,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    37
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const unsigned char yystos[] =
{
       0,     3,    11,    22,    23,    24,    14,    18,    29,     3,
      16,     0,    24,     3,     3,    30,    10,    12,    13,    15,
      16,     3,    26,     6,     3,     4,     5,    19,    20,     3,
      16,     3,    16,     3,    16,     3,    16,     3,    25,    27,
      28,     3,    17,    20,     3,    18,    26,     3,    26,     3,
      26,     3,    26,    29,    17,    20,     3,     3,     3,     4,
       5,    25,    17,     3,    17,    18,    17,    18,    17,     7,
       8,     9,     6,    28,    16,     3,    19,     3,    18,     3,
      25,     3,    25,     3,     9,     9,    25,     6,    18,    25,
       3,    19,     3,    19,     3,    17,    25,    19,     3,     6,
      18,     6,    18,     6,    19,     6,    18,    25,    25,     6,
      25,    19,    19,    19,     6,     6,     6
};

#if ! defined (YYSIZE_T) && defined (__SIZE_TYPE__)
# define YYSIZE_T __SIZE_TYPE__
#endif
#if ! defined (YYSIZE_T) && defined (size_t)
# define YYSIZE_T size_t
#endif
#if ! defined (YYSIZE_T)
# if defined (__STDC__) || defined (__cplusplus)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# endif
#endif
#if ! defined (YYSIZE_T)
# define YYSIZE_T unsigned int
#endif

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		-2
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrlab1

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
      yychar1 = YYTRANSLATE (yychar);				\
      YYPOPSTACK;						\
      goto yybackup;						\
    }								\
  else								\
    { 								\
      yyerror ("syntax error: cannot back up");			\
      YYERROR;							\
    }								\
while (0)

#define YYTERROR	1
#define YYERRCODE	256

/* YYLLOC_DEFAULT -- Compute the default location (before the actions
   are run).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)           \
  Current.first_line   = Rhs[1].first_line;      \
  Current.first_column = Rhs[1].first_column;    \
  Current.last_line    = Rhs[N].last_line;       \
  Current.last_column  = Rhs[N].last_column;
#endif

/* YYLEX -- calling `yylex' with the right arguments.  */

#define YYLEX	yylex ()

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
} while (0)
# define YYDSYMPRINT(Args)			\
do {						\
  if (yydebug)					\
    yysymprint Args;				\
} while (0)
/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YYDSYMPRINT(Args)
#endif /* !YYDEBUG */

/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   SIZE_MAX < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#if YYMAXDEPTH == 0
# undef YYMAXDEPTH
#endif

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined (__GLIBC__) && defined (_STRING_H)
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
static YYSIZE_T
#   if defined (__STDC__) || defined (__cplusplus)
yystrlen (const char *yystr)
#   else
yystrlen (yystr)
     const char *yystr;
#   endif
{
  register const char *yys = yystr;

  while (*yys++ != '\0')
    continue;

  return yys - yystr - 1;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined (__GLIBC__) && defined (_STRING_H) && defined (_GNU_SOURCE)
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
#   if defined (__STDC__) || defined (__cplusplus)
yystpcpy (char *yydest, const char *yysrc)
#   else
yystpcpy (yydest, yysrc)
     char *yydest;
     const char *yysrc;
#   endif
{
  register char *yyd = yydest;
  register const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

#endif /* !YYERROR_VERBOSE */



#if YYDEBUG
/*-----------------------------.
| Print this symbol on YYOUT.  |
`-----------------------------*/

static void
#if defined (__STDC__) || defined (__cplusplus)
yysymprint (FILE* yyout, int yytype, YYSTYPE yyvalue)
#else
yysymprint (yyout, yytype, yyvalue)
    FILE* yyout;
    int yytype;
    YYSTYPE yyvalue;
#endif
{
  /* Pacify ``unused variable'' warnings.  */
  (void) yyvalue;

  if (yytype < YYNTOKENS)
    {
      YYFPRINTF (yyout, "token %s (", yytname[yytype]);
# ifdef YYPRINT
      YYPRINT (yyout, yytoknum[yytype], yyvalue);
# endif
    }
  else
    YYFPRINTF (yyout, "nterm %s (", yytname[yytype]);

  switch (yytype)
    {
      default:
        break;
    }
  YYFPRINTF (yyout, ")");
}
#endif /* YYDEBUG. */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
#if defined (__STDC__) || defined (__cplusplus)
yydestruct (int yytype, YYSTYPE yyvalue)
#else
yydestruct (yytype, yyvalue)
    int yytype;
    YYSTYPE yyvalue;
#endif
{
  /* Pacify ``unused variable'' warnings.  */
  (void) yyvalue;

  switch (yytype)
    {
      default:
        break;
    }
}



/* The user can define YYPARSE_PARAM as the name of an argument to be passed
   into yyparse.  The argument should have type void *.
   It should actually point to an object.
   Grammar actions can access the variable by casting it
   to the proper pointer type.  */

#ifdef YYPARSE_PARAM
# if defined (__STDC__) || defined (__cplusplus)
#  define YYPARSE_PARAM_ARG void *YYPARSE_PARAM
#  define YYPARSE_PARAM_DECL
# else
#  define YYPARSE_PARAM_ARG YYPARSE_PARAM
#  define YYPARSE_PARAM_DECL void *YYPARSE_PARAM;
# endif
#else /* !YYPARSE_PARAM */
# define YYPARSE_PARAM_ARG
# define YYPARSE_PARAM_DECL
#endif /* !YYPARSE_PARAM */

/* Prevent warning if -Wstrict-prototypes.  */
#ifdef __GNUC__
# ifdef YYPARSE_PARAM
int yyparse (void *);
# else
int yyparse (void);
# endif
#endif


/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Number of parse errors so far.  */
int yynerrs;


int
yyparse (YYPARSE_PARAM_ARG)
     YYPARSE_PARAM_DECL
{
  
  register int yystate;
  register int yyn;
  int yyresult;
  /* Number of tokens to shift before error messages enabled.  */
  int yyerrstatus;
  /* Lookahead token as an internal (translated) token number.  */
  int yychar1 = 0;

  /* Three stacks and their tools:
     `yyss': related to states,
     `yyvs': related to semantic values,
     `yyls': related to locations.

     Refer to the stacks thru separate pointers, to allow yyoverflow
     to reallocate them elsewhere.  */

  /* The state stack.  */
  short	yyssa[YYINITDEPTH];
  short *yyss = yyssa;
  register short *yyssp;

  /* The semantic value stack.  */
  YYSTYPE yyvsa[YYINITDEPTH];
  YYSTYPE *yyvs = yyvsa;
  register YYSTYPE *yyvsp;



#define YYPOPSTACK   (yyvsp--, yyssp--)

  YYSIZE_T yystacksize = YYINITDEPTH;

  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;


  /* When reducing, the number of symbols on the RHS of the reduced
     rule.  */
  int yylen;

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
     have just been pushed. so pushing a state here evens the stacks.
     */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyssp >= yyss + yystacksize - 1)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack. Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	short *yyss1 = yyss;


	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow ("parser stack overflow",
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),

		    &yystacksize);

	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyoverflowlab;
# else
      /* Extend the stack our own way.  */
      if (yystacksize >= YYMAXDEPTH)
	goto yyoverflowlab;
      yystacksize *= 2;
      if (yystacksize > YYMAXDEPTH)
	yystacksize = YYMAXDEPTH;

      {
	short *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyoverflowlab;
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

      if (yyssp >= yyss + yystacksize - 1)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

/* Do appropriate processing given the current state.  */
/* Read a lookahead token if we need one and don't already have one.  */
/* yyresume: */

  /* First try to decide what to do without reference to lookahead token.  */

  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* yychar is either YYEMPTY or YYEOF
     or a valid token in external form.  */

  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  /* Convert token to internal form (in yychar1) for indexing tables with.  */

  if (yychar <= 0)		/* This means end of input.  */
    {
      yychar1 = 0;
      yychar = YYEOF;		/* Don't call YYLEX any more.  */

      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yychar1 = YYTRANSLATE (yychar);

      /* We have to keep this `#if YYDEBUG', since we use variables
	 which are defined only if `YYDEBUG' is set.  */
      YYDPRINTF ((stderr, "Next token is "));
      YYDSYMPRINT ((stderr, yychar1, yylval));
      YYDPRINTF ((stderr, "\n"));
    }

  /* If the proper action on seeing token YYCHAR1 is to reduce or to
     detect an error, take that action.  */
  yyn += yychar1;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yychar1)
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

  /* Shift the lookahead token.  */
  YYDPRINTF ((stderr, "Shifting token %d (%s), ",
	      yychar, yytname[yychar1]));

  /* Discard the token being shifted unless it is eof.  */
  if (yychar != YYEOF)
    yychar = YYEMPTY;

  *++yyvsp = yylval;


  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  yystate = yyn;
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



#if YYDEBUG
  /* We have to keep this `#if YYDEBUG', since we use variables which
     are defined only if `YYDEBUG' is set.  */
  if (yydebug)
    {
      int yyi;

      YYFPRINTF (stderr, "Reducing via rule %d (line %d), ",
		 yyn - 1, yyrline[yyn]);

      /* Print the symbols being reduced, and their result.  */
      for (yyi = yyprhs[yyn]; yyrhs[yyi] >= 0; yyi++)
	YYFPRINTF (stderr, "%s ", yytname[yyrhs[yyi]]);
      YYFPRINTF (stderr, " -> %s\n", yytname[yyr1[yyn]]);
    }
#endif
  switch (yyn)
    {
        case 2:
#line 75 "ext_fcns.y"
    {Ext_fcns = yyvsp[0].ext_fcn_list_t;}
    break;

  case 3:
#line 78 "ext_fcns.y"
    {yyval.ext_fcn_list_t = new ext_fcn_list(yyvsp[0].ext_fcn_t);}
    break;

  case 4:
#line 79 "ext_fcns.y"
    {yyval.ext_fcn_list_t = yyvsp[-1].ext_fcn_list_t->append_ext_fcn_def(yyvsp[0].ext_fcn_t);}
    break;

  case 5:
#line 82 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-10].strval,yyvsp[-9].tplist_t,yyvsp[-6].modif_t,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-6].modif_t; delete yyvsp[-2].plist_t; }
    break;

  case 6:
#line 84 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-7].strval,yyvsp[-6].tplist_t,NULL,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-2].plist_t; }
    break;

  case 7:
#line 86 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-6].modif_t,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-6].modif_t; delete yyvsp[-2].plist_t; }
    break;

  case 8:
#line 88 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(NULL,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-2].plist_t; }
    break;

  case 9:
#line 90 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-11].strval,yyvsp[-10].tplist_t,yyvsp[-7].modif_t,yyvsp[-5].strval,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-7].modif_t; delete yyvsp[-2].plist_t; }
    break;

  case 10:
#line 92 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-8].strval,yyvsp[-7].tplist_t,NULL,yyvsp[-5].strval,yyvsp[-4].strval, yyvsp[-2].plist_t); delete yyvsp[-2].plist_t; }
    break;

  case 11:
#line 94 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-12].strval,yyvsp[-11].tplist_t,yyvsp[-8].modif_t,yyvsp[-6].strval,yyvsp[-5].strval, yyvsp[-4].strval, yyvsp[-2].plist_t); delete yyvsp[-8].modif_t; delete yyvsp[-2].plist_t; }
    break;

  case 12:
#line 96 "ext_fcns.y"
    {
					yyval.ext_fcn_t=new ext_fcn_def(yyvsp[-9].strval,yyvsp[-8].tplist_t,NULL,yyvsp[-6].strval,yyvsp[-5].strval, yyvsp[-4].strval, yyvsp[-2].plist_t); delete yyvsp[-2].plist_t; }
    break;

  case 13:
#line 98 "ext_fcns.y"
    {
					yyval.ext_fcn_t=ext_fcn_def::make_sfun_def(yyvsp[-11].strval,yyvsp[-10].tplist_t,yyvsp[-7].modif_t,yyvsp[-5].strval,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-2].plist_t; }
    break;

  case 14:
#line 100 "ext_fcns.y"
    {
					yyval.ext_fcn_t=ext_fcn_def::make_sfun_def(yyvsp[-8].strval,yyvsp[-7].tplist_t,NULL,yyvsp[-5].strval,yyvsp[-4].strval,yyvsp[-2].plist_t); delete yyvsp[-2].plist_t; }
    break;

  case 15:
#line 102 "ext_fcns.y"
    {
					yyval.ext_fcn_t=ext_fcn_def::make_state_def(yyvsp[-3].strval,yyvsp[-1].strval); }
    break;

  case 16:
#line 108 "ext_fcns.y"
    {yyval.plist_t = NULL;}
    break;

  case 17:
#line 109 "ext_fcns.y"
    {yyval.plist_t = yyvsp[0].plist_t;}
    break;

  case 18:
#line 113 "ext_fcns.y"
    {yyval.modif_t = new ext_fcn_modifier_list(yyvsp[0].strval);}
    break;

  case 19:
#line 114 "ext_fcns.y"
    {yyval.modif_t = new ext_fcn_modifier_list(yyvsp[-1].strval, yyvsp[0].strval);}
    break;

  case 20:
#line 115 "ext_fcns.y"
    {yyval.modif_t = yyvsp[-2].modif_t->append(yyvsp[0].strval);	}
    break;

  case 21:
#line 116 "ext_fcns.y"
    {yyval.modif_t = yyvsp[-3].modif_t->append(yyvsp[-1].strval, yyvsp[0].strval);	}
    break;

  case 22:
#line 120 "ext_fcns.y"
    {yyval.plist_t = new ext_fcn_param_list(yyvsp[0].param_t);}
    break;

  case 23:
#line 121 "ext_fcns.y"
    {yyval.plist_t = yyvsp[-2].plist_t->append(yyvsp[0].param_t);}
    break;

  case 24:
#line 125 "ext_fcns.y"
    { yyval.param_t = new ext_fcn_param(yyvsp[-2].strval,yyvsp[-1].tplist_t,1,0,0);}
    break;

  case 25:
#line 126 "ext_fcns.y"
    { yyval.param_t = new ext_fcn_param(yyvsp[-2].strval,yyvsp[-1].tplist_t,0,1,0);}
    break;

  case 26:
#line 127 "ext_fcns.y"
    { yyval.param_t = new ext_fcn_param(yyvsp[-2].strval,yyvsp[-1].tplist_t,0,0,1);}
    break;

  case 27:
#line 128 "ext_fcns.y"
    { yyval.param_t = new ext_fcn_param(yyvsp[-3].strval,yyvsp[-2].tplist_t,1,0,1);}
    break;

  case 28:
#line 129 "ext_fcns.y"
    { yyval.param_t = new ext_fcn_param(yyvsp[-3].strval,yyvsp[-2].tplist_t,0,1,1);}
    break;

  case 29:
#line 131 "ext_fcns.y"
    { yyval.param_t = new ext_fcn_param(yyvsp[-1].strval,yyvsp[0].tplist_t,0,0,0);}
    break;

  case 30:
#line 135 "ext_fcns.y"
    {yyval.tplist_t = NULL;}
    break;

  case 31:
#line 136 "ext_fcns.y"
    {yyval.tplist_t = yyvsp[-1].tplist_t;}
    break;

  case 32:
#line 140 "ext_fcns.y"
    {yyval.tplist_t = new param_list(yyvsp[0].strval);}
    break;

  case 33:
#line 141 "ext_fcns.y"
    {yyval.tplist_t = new param_list(yyvsp[-1].strval,yyvsp[0].strval);}
    break;

  case 34:
#line 142 "ext_fcns.y"
    {yyval.tplist_t = new param_list(yyvsp[-1].strval,yyvsp[0].strval);}
    break;

  case 35:
#line 143 "ext_fcns.y"
    {yyval.tplist_t = new param_list(yyvsp[-1].strval,yyvsp[0].strval);}
    break;

  case 36:
#line 144 "ext_fcns.y"
    {yyval.tplist_t = yyvsp[-2].tplist_t->append(yyvsp[0].strval);}
    break;

  case 37:
#line 145 "ext_fcns.y"
    {yyval.tplist_t = yyvsp[-3].tplist_t->append(yyvsp[-1].strval,yyvsp[0].strval);}
    break;

  case 38:
#line 146 "ext_fcns.y"
    {yyval.tplist_t = yyvsp[-3].tplist_t->append(yyvsp[-1].strval,yyvsp[0].strval);}
    break;

  case 39:
#line 147 "ext_fcns.y"
    {yyval.tplist_t = yyvsp[-3].tplist_t->append(yyvsp[-1].strval,yyvsp[0].strval);}
    break;


    }

/* Line 1016 of /usr/local/share/bison/yacc.c.  */
#line 1272 "ext_fcns.tab.cc"

  yyvsp -= yylen;
  yyssp -= yylen;


#if YYDEBUG
  if (yydebug)
    {
      short *yyssp1 = yyss - 1;
      YYFPRINTF (stderr, "state stack now");
      while (yyssp1 != yyssp)
	YYFPRINTF (stderr, " %d", *++yyssp1);
      YYFPRINTF (stderr, "\n");
    }
#endif

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
#if YYERROR_VERBOSE
      yyn = yypact[yystate];

      if (YYPACT_NINF < yyn && yyn < YYLAST)
	{
	  YYSIZE_T yysize = 0;
	  int yytype = YYTRANSLATE (yychar);
	  char *yymsg;
	  int yyx, yycount;

	  yycount = 0;
	  /* Start YYX at -YYN if negative to avoid negative indexes in
	     YYCHECK.  */
	  for (yyx = yyn < 0 ? -yyn : 0;
	       yyx < (int) (sizeof (yytname) / sizeof (char *)); yyx++)
	    if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	      yysize += yystrlen (yytname[yyx]) + 15, yycount++;
	  yysize += yystrlen ("parse error, unexpected ") + 1;
	  yysize += yystrlen (yytname[yytype]);
	  yymsg = (char *) YYSTACK_ALLOC (yysize);
	  if (yymsg != 0)
	    {
	      char *yyp = yystpcpy (yymsg, "parse error, unexpected ");
	      yyp = yystpcpy (yyp, yytname[yytype]);

	      if (yycount < 5)
		{
		  yycount = 0;
		  for (yyx = yyn < 0 ? -yyn : 0;
		       yyx < (int) (sizeof (yytname) / sizeof (char *));
		       yyx++)
		    if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
		      {
			const char *yyq = ! yycount ? ", expecting " : " or ";
			yyp = yystpcpy (yyp, yyq);
			yyp = yystpcpy (yyp, yytname[yyx]);
			yycount++;
		      }
		}
	      yyerror (yymsg);
	      YYSTACK_FREE (yymsg);
	    }
	  else
	    yyerror ("parse error; also virtual memory exhausted");
	}
      else
#endif /* YYERROR_VERBOSE */
	yyerror ("parse error");
    }
  goto yyerrlab1;


/*----------------------------------------------------.
| yyerrlab1 -- error raised explicitly by an action.  |
`----------------------------------------------------*/
yyerrlab1:
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      /* Return failure if at end of input.  */
      if (yychar == YYEOF)
        {
	  /* Pop the error token.  */
          YYPOPSTACK;
	  /* Pop the rest of the stack.  */
	  while (yyssp > yyss)
	    {
	      YYDPRINTF ((stderr, "Error: popping "));
	      YYDSYMPRINT ((stderr,
			    yystos[*yyssp],
			    *yyvsp));
	      YYDPRINTF ((stderr, "\n"));
	      yydestruct (yystos[*yyssp], *yyvsp);
	      YYPOPSTACK;
	    }
	  YYABORT;
        }

      YYDPRINTF ((stderr, "Discarding token %d (%s).\n",
		  yychar, yytname[yychar1]));
      yydestruct (yychar1, yylval);
      yychar = YYEMPTY;
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */

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

      YYDPRINTF ((stderr, "Error: popping "));
      YYDSYMPRINT ((stderr,
		    yystos[*yyssp], *yyvsp));
      YYDPRINTF ((stderr, "\n"));

      yydestruct (yystos[yystate], *yyvsp);
      yyvsp--;
      yystate = *--yyssp;


#if YYDEBUG
      if (yydebug)
	{
	  short *yyssp1 = yyss - 1;
	  YYFPRINTF (stderr, "Error: state stack now");
	  while (yyssp1 != yyssp)
	    YYFPRINTF (stderr, " %d", *++yyssp1);
	  YYFPRINTF (stderr, "\n");
	}
#endif
    }

  if (yyn == YYFINAL)
    YYACCEPT;

  YYDPRINTF ((stderr, "Shifting error token, "));

  *++yyvsp = yylval;


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
/*----------------------------------------------.
| yyoverflowlab -- parser overflow comes here.  |
`----------------------------------------------*/
yyoverflowlab:
  yyerror ("parser stack overflow");
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  return yyresult;
}


#line 152 "ext_fcns.y"

 

