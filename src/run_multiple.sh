#!/bin/bash

j=4
for l in {1..10}; do
	for ((k=1; k <= j; k++)) do
		echo "HI ${j} ${k}"
		sed -i "s/.*RUN.*/pub const RUN : i64 = ${k}${l};/" params.rs;
		if [ $k -eq $j ]; then
			echo "ENDING RUN"
			cargo run --release ${k}
			sleep 100
			sudo /cm/shared/package/utils/bin/drop_caches
		else 
			cargo run --release ${k} &
		fi
		#done;
	done;
done;
