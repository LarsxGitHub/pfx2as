#/bin/bash
loop_date=$1

let j=0
while [[ "$loop_date" != "$2" ]]; do
        loop_date=`date   -j -v+${j}d  -f "%Y-%m-%d" "$1" +"%Y-%m-%d"`
        echo $loop_date
        let j=j+1
done
