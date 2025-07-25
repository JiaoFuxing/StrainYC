#!/bin/bash
#Render high respect to CSOL-Jacky14.
if [ "$#" -ne 1 ]; then  
    echo "Usage: $0 <bam>"  
    exit 1  
fi  
BAM=$1  
output="./output/"

del_data() {
 depth=$(samtools depth $BAM | head | awk '{depth[$3]++} END {for (d in depth) print d, depth[d]}' | sort -k2nr | head -1 | awk '{print $1}')
 if [ "$depth" -le 100 ]; then
   ErrorModel=0
    ./library/MOST-2.sh $BAM 10
 else
   ErrorModel=1
    ./library/MOST-2.sh $BAM 50
 fi
 ./library/RUST/jf_df/target/release/jf_df "$ErrorModel" & PID=$!
 ./library/RUST/RefBuild/target/release/RefBuild
 ./library/RUST/jf_score/target/release/jf_score 20 > ./output/2.txt
 wait $PID
 Cluster_abu
 echo -e "\nCluster_abundance"
 awk 'NR > 1 {if($2 > 0.5 && $5 != 0)print $1"\t"$5;else print $1"\t"$6}' DF-result-3.txt |awk '{if($2 < 0.005) next; print $0}'
}

Cluster_abu() {
 ./library/RUST/snp_filter_repeat/target/release/snp_filter_repeat ./DB/2kadd.snp   ./output/2.txt  ./output/poc.txt  ./output/out2 
 ./library/RUST/special-site/target/release/special-site  ./output/out2
 ./library/RUST/df_check2_rs/target/release/df_check2_rs filter-result-with-counts.txt ./output/2.txt DF-result-1.txt 
 cat DF-result-1.txt
 #2-----DFregionsMatchRate------#
 echo -e "\nagain2(DFregions)"
 awk 'BEGIN{print "address"} NR>1 && $2>=0.7 && $3>10 {print $1}'  DF-result-1.txt > ./output/3.txt
 ./library/RUST/snp_filter_repeat/target/release/snp_filter_repeat ./output/out2   ./output/3.txt  ./output/poc.txt  ./output/out3
 ./library/RUST/special-site/target/release/special-site  ./output/out3
 ./library/RUST/df_check2_rs/target/release/df_check2_rs filter-result-with-counts.txt ./output/3.txt DF-result-2.txt
 cat DF-result-2.txt
 #3----WholeGenomeMatchRate-----#
 echo -e "\nagain3(WholeGenome)"
 awk 'BEGIN{print "address"} NR>1 && $2>=0.8 {print $1}'  DF-result-2.txt > ./output/4.txt
 ./library/RUST/snp_filter_repeat/target/release/snp_filter_repeat ./DB/2kadd.snp   ./output/4.txt  ./DB/2k-num.add ./output/out4
 ./library/RUST/special-site/target/release/special-site  ./output/out4
 ./library/RUST/df_check2_rs/target/release/df_check2_rs all-stats.txt ./output/4.txt DF-result-3.txt
 cat DF-result-3.txt
}

del_data