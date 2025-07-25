#!/bin/bash

if [ "$#" -ne 2 ]; then  
    echo "Usage: $0 <bam> <t>"  
    exit 1  
fi  

input_bam=$1
num_segments=$2

#total_length=$(awk 'NR==2 {print length($0)}' /Stor/home/danielgroup/jiaofuxing/20240425-vp/2vp-ref/hebing2.fa)
#kp
#total_length=$(awk 'NR==2 {print length($0)}' /Stor/home/danielgroup/jiaofuxing/DB/MOST-KP-JD-ZhangTY/ref/Kp_reference.fasta)
total_length=6000000


segment_length=$((total_length / num_segments))

output_prefix="./output/"
ref_name="RIMD_2210633_NCBI"
#ref_name="AP006725.1"  #Kp
#ref_name="hi"
#ref_name="Efm"
mkdir -p "$output_prefix"

for ((i=1; i<=num_segments; i++))
do
    (
        start=$(( (i - 1) * segment_length + 1 ))
        end=$(( i * segment_length ))
        output_file="${output_prefix}${i}.bam"
        add_file="${output_prefix}${i}.add"
        
        # 切片BAM文件
        sambamba -q slice "$input_bam" "$ref_name:$start-$end" > "$output_file"
        #python /Stor/home/danielgroup/jiaofuxing/YC/library/slice.py  "$input_bam" "$ref_name:$start-$end" > "$output_file"

        awk -v start="$start" -v end="$end" '{if ($2 >= start && $2 <= end) print}' \
            /Stor/home/danielgroup/jiaofuxing/YC/DB/2k.add > "$add_file"
        samtools mpileup -q 20 -Q 20 --no-output-ends --no-output-del --no-output-del  --no-output-ins --no-output-ins -A -a -l "$add_file" "$output_file" > "${output_prefix}${i}-out" 2> "${output_prefix}${i}-err.log" && rm "${output_prefix}${i}-err.log"


        #awk -v start="$start" -v end="$end" '{if ($2 >= start && $2 <= end) print}' \
            #/Stor/home/danielgroup/jiaofuxing/20240425-vp/ychao-data/2/newStr/all+RIMD.add > "$add_file"
        # 运行mpileup并输出结果
        #samtools mpileup -q 30 -Q 0  --no-output-ends --no-output-del --no-output-del  --no-output-ins --no-output-ins -A -a  --output-QNAME  -l "$add_file" "$output_file" > "${output_prefix}${i}-out" 2> "${output_prefix}${i}-err.log" && rm "${output_prefix}${i}-err.log"

        #/Stor/home/danielgroup/jiaofuxing/20240425-vp/ychao-data/2/12-str.add
    ) &
done
wait

# 合并输出结果
seq 1 "$num_segments" | xargs -I{} cat "${output_prefix}{}-out" | tee >(awk '{print $2 "\t" $5}' > "${output_prefix}2lie-mp-samtool") | awk '{if ($4 <= 3) {print $2 "\t" "*"} else {print $2 "\t" $5}}' > "${output_prefix}hi"
#seq 1 30 | xargs -I{} cat ./output/{}-out | awk '{print $5 "\t" $7}' > hebing
echo "BAM analysis is OK"