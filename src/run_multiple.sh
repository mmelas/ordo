#!/bin/bash

allRuns=(8)
for j in ${allRuns[@]}; do 
	for l in {1..10}; do
		for ((k=1; k <= j; k++)) do
			echo "HI ${j} ${k}"
			sed -i "s/.*RUN.*/pub const RUN : i64 = ${k}${l};/" params.rs;
			if [ $k -eq $j ]; then
				cargo run --release ${j}${k}
				sleep 60
			else 
				cargo run --release ${j}${k}&
			fi
			#done;
		done;
	done;
done;
