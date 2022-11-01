#!/bin/bash

for ((i=4; i<=256; i*=2)); do
	sed -i "s/.*PRODUCERS.*/pub const PRODUCERS : i64 = ${i};/" params.rs;
	for j in {1..10}; do 
		sudo /cm/shared/package/utils/bin/drop_caches;
		sed -i "s/.*RUN.*/pub const RUN : i64 = ${j};/" params.rs;
		cargo run --release single; #done;
		done;
done;
