awk -F ns/op '
! /Benchmark/ {print $0}
/Benchmark/ {
	printf $1"ns/op"
	idx=split($1,a,"\t");
	if (a[idx]+1-1 > 0) {
		printf " ("1000000000/a[idx]" op/s) \t"
	}
	print $2
}
'
