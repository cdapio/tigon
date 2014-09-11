Running and developing test suites.

To run the test suite, cd to Streaming/test and execute run_test.pl
The result will be placed in a file
	test_results_<yyyy>_<mm>_<dd>.txt

The test suite contains a collection of tests; each test can contain a
collection of subtests; subtests may contain subsubtests and so on.
A test is places in a directory the same name, subtests are placed in
subdirectories with corresponding names, and so on.

For example, at the time of this writing the STREAMING/test directory
contains two tests, TestJoins and TestTypes, in directories
STREAMING/test/TestJoins and STREAMING/test/TestTypes respectively.  The
TestJoins test contains subtests inner_join, ..., right_outer_join in
subdirectories STREAMING/test/TestJoins/inner_join through
STREAMING/test/TestJoins/right_outer_join respectively.

To create a new test, create a subdirectory off of STREAMING/test ith the
test's name.  Put a file run_test.pl in the sibdirectory.  The script
STREAMING/test/run_test.pl will discover all subdirectories and invoke
run_test.pl to eecute each test.

You may structure subtests, subsubtests, and so on in the manner of your
choosing, and you make execute them in the manner of your choosing.  However
we recommend using a separate run_test.pl script in each subdirectory.

A test or test suite may need to make use of a specific set of schemas.
Please copy the schema file into
	STREAMING/cfg/packet_schema_test.txt
to build a test, run
	STREAMING/bin/buildit_test.pl
in the test or subtest directory.  This buildit script will use
packet_schema_test.txt as the schema file.

Each test should follow a set of guidelines to ensure that tests can be
readily composed and their results readily parsed.

Output:
Each test, subtest, etc., should emit results to stdout.  This output will be
captured by the parent run_test in STREAMING/test

1) The output of each test, subtest, etc. should start with the line

Starting: <test_name>

where <test_name> is the structured name of the test.  For example, the
TestJoins test has structured name TestJoin and its inner_join subtest has the
structured name TestJoins/inner_join.  Please note that the structured name of
the test is the suffix of the directory of the test under STREAMING/test

This line should precede all other output related to the test.
For example, the TestJoins test starts with

Starting: <TestJoins>

and is soon followed by

Starting: <TestJoins/inner_join>


2) The output of each test, subtest should end with the line

Ending: <test_name> <result>


where <test_name> is the structured name of the test, and <result> is one of
	Success 	Failure		SystemError
where Success means, the test passed, Failure means, the test ran but did not
produce the expected results, and SystemError means that some system problem
prevented the test from running.

The Ending: line should follow all output text for a particular test.
For example, all output related to a successful inner_join test should be
bracketed by the lines

Starting: <TestJoins/inner_join>
Ending: <TestJoins/inner_join> <Success>

3) test summaries
A test, subtest, etc. should return
	0	for a successful test
	1 	for a failed test
	2	for a test that could not run due to a system error.
That is, if the <result> is Success, exit with 0; Failure, exit with 1;
SystemError, exit with 2.

If a test has subtests, then the <result> is
	SystemError	: if any subtest returned 2
	Else, Failure : if any subtest returned 1
	Else Success

4) Supporting information.
A test, subtest, etc.  may emit supporting information to the log, as long as
it falls between the Starting: and Ending: lines.  For a failed test or a
system error, the test should emit enough information to identify the source
of the error.

Parsing output

By following these guidelines, tests and subtests can be composed and
automatically invoked and summarized.

Failed tests can be found by grep'ing the output file.  The information
related to a particular failed test is always contained within the Starting:
and Ending: lines, so that information can be readily extracted.
