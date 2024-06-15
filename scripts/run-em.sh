#!/bin/bash

num_confs=5

source ./scripts/misc/icons.sh

# files=(\
# "bursty-overlapping-i6d30s300_2" \
# "bursty-overlapping-i6d30s350_2" \
# "bursty-overlapping-i6d30s300_3" \
# "bursty-overlapping-i6d30s350_3" \
# "bursty-overlapping-i6d30s400_4" \
# )
files=(\
"bursty-spaced-ii10bl60bb5sf550d1800_2"
"bursty-overlapping-i8bl60bb5sf550d1800_4"
"bursty-spaced-ii8bl60bb5sf550d1800_4"
"bursty-spaced-long-i8bl90bb35sf550d1800_4"
"constant-i10qd60sf550d1800_2"
"constant-i4qd60sf550d1800_4"
"constant-mixed-i18l60s550d1800_4"
"mixed-bi16bl60bb5bi2cl90ci10sf550d1800_4"
)

# files=(\
# "bursty-spaced-i10d60s550_2"
# "bursty-overlapping-i10d60s550_2"
# "bursty-spaced-i6d60s600_3"
# "bursty-overlapping-i6d60s600_3"
# "bursty-spaced-i6d60s500_4"
# "bursty-overlapping-i6d60s500_4"
# "bursty-spaced-i10d60s600_4"
# "bursty-overlapping-i10d60s600_4"
# )

modes=(\
"static"
"shared"
)

# ./scripts/image.sh

for mode in "${modes[@]}"
do
    for ((i=0; i<${#files[@]}; i++)); do
        num_apps=$(echo ${files[$i]} | grep -oP '(?<=_)\d+')
        echo "$ICN_WAIT Starting ${files[$i]} with $num_apps apps in mode $mode"
        ./scripts/run-experiment.sh $mode $num_apps ${files[$i]} 45
        echo 
        echo 
        echo "================================================================================"
    done
done
