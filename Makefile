unittest:
	ant test
	#ant runtest -Dtest=HeapFileReadTest
	ant runsystest -Dtest=ScanTest

watch:
	watchr unittest.watch
