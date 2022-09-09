MAX_SIZE=100

for num_to_merge in $(seq 92 $MAX_SIZE);
do {
  echo "----------------------------------------";
  echo "Running for number to merge $num_to_merge";
  echo "----------------------------------------";
  R_MAX_VSIZE=160000000000 mprof run --output mprof_run_for_num_to_merge_${num_to_merge} Rscript merge.R $num_to_merge
  echo "----------------------------------------";
  echo "FINISHED running for number to merge $num_to_merge";
  echo "----------------------------------------";
}
done;