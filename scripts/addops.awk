#!/usr/bin/awk -F ns/op -f
! /Benchmark/ {print $0}
/Benchmark/ {
	printf $1"ns/op"
	idx=split($1,a,"\t");
	printf " ("1000000000/a[idx]" op/s) \t"
	print $2
}
