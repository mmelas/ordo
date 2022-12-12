#!/bin/bash

#for ((i=32; i<=64; i*=2)); do
	#sed -i "s/.*PRODUCERS.*/pub const PRODUCERS : i64 = ${i};/" params.rs;
for j in {1..10}; do 
	sed -i "s/.*RUN.*/pub const RUN : i64 = ${j};/" params.rs;
	sudo /cm/shared/package/utils/bin/drop_caches;
	cargo run --release latencies; #done;
	done;
#	done;
done;
