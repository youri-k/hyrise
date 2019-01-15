#!/usr/bin/expect -d

set timeout 1
set red "\x1B\[31m"
set color_reset "\x1B\[0m"

proc assert { text } {
	expect {
		$text {}
		default {
			send_error "\nFailed - expected \"$text\"\n"
			exit 1
		}
	}
}

set build_dir [lindex $argv 0]
set build_type [lindex $argv 1]

spawn "./${build_dir}/hyriseConsole"

assert "HYRISE SQL Interface"
assert "Type 'help' for more information."
assert ""
assert "Hyrise is running a"
assert "${red}\(${build_type}\)${color_reset}\> watt"

#send "help\r"
#assert "HYRISE SQL Interface"

#
# Test that tables in varying formats can be loaded
#
send "load resources/test_data/tbl/int.tbl t1\r"
assert "Loading resources/test_data/tbl/int.tbl into table \"t1\" ..."

send "load resources/test_data/csv/float.csv t2\r"
assert "Loading resources/test_data/csv/float.csv into table \"t2\" ..."

send "load resources/test_data/bin/float.bin t3\r"
assert "Loading resources/test_data/bin/float.bin into table \"t3\" ..."

#
# Test that an error message is given when loading table files that do not exist
#
send "load doesnotexist.tbl doesnotexist\r"
assert "Could not find file"

#
# Test that an invalid load command gives an error message
#
send "load invalid.tbl\r"
assert "Usage:"
assert "  load FILEPATH TABLENAME"