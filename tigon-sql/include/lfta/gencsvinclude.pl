#! /usr/bin/perl

# ------------------------------------------------
#   Copyright 2014 AT&T Intellectual Property
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# -------------------------------------------

open(I,"cat ../packet.h | grep '#define CSVELEMENTS' | cut -f 3 -d ' ' |");
$num=<I>;
close(I);
chomp $num;
printf("#ifndef CSV_MACRO_H\n#define CSV_MACRO_H\n");
for($x=1;$x<=$num;$x++) {
	printf "#define get_csv_uint_pos$x(X,Y) get_csv_uint((X),(Y),$x)\n";
    printf "#define get_csv_ullong_pos$x(X,Y) get_csv_ullong((X),(Y),$x)\n";
    printf "#define get_csv_ip_pos$x(X,Y) get_csv_ip((X),(Y),$x)\n";
    printf "#define get_csv_ipv6_pos$x(X,Y) get_csv_ipv6((X),(Y),$x)\n";
    printf "#define get_csv_string_pos$x(X,Y) get_csv_string((X),(Y),$x)\n";
    printf "#define get_csv_v_str_pos$x(X,Y) get_csv_string((X),(Y),$x)\n";
    printf "#define get_csv_bool_pos$x(X,Y) get_csv_bool((X),(Y),$x)\n";
    printf "#define get_csv_int_pos$x(X,Y) get_csv_int((X),(Y),$x)\n";
    printf "#define get_csv_llong_pos$x(X,Y) get_csv_llong((X),(Y),$x)\n";
    printf "#define get_csv_float_pos$x(X,Y) get_csv_float((X),(Y),$x)\n";
}
print("#endif\n");
