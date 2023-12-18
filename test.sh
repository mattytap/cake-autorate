#!/bin/bash

logfile="/var/log/cake-autorate.primary.log"
records_per_batch=55
num_splits=7
num_fields=31
timestamp_index=3
reflector_index=10
skip_records=3

declare -a records
declare -A ip_arrays
declare -A dodgy_records
declare -a sum_array
declare -a count_array

should_keep_record() {
    local index="$1"
    local record="${records[index]}"
    local field13=$(echo "$record" | awk -F'; ' '{print $13}')
    local start_index=$((index - 3))
    local end_index=$((index + 3))
    local all_values=()

    for ((i = start_index; i < end_index; i++)); do
        local value=$(echo "${records[i]}" | awk -F'; ' '{print $13}')
        all_values+=("$value")
    done

    # Calculate median
    local sorted_values=($(printf "%s\n" "${all_values[@]}" | sort -n))
    local median_index=$(((${#sorted_values[@]} - 1) / 2))
    local median=${sorted_values[$median_index]}

    if ((${#sorted_values[@]} % 2 == 0)); then
        local next_index=$((median_index + 1))
        median=$(((median + sorted_values[next_index]) / 2))
    fi

    ((field13 > 2 * median)) && return 1 || return 0
}

debug_output_array() {
    local array_name="$1"
    local -n array="${array_name}"
    echo "Debug output for $array_name:"
    for key in "${!array[@]}"; do
        echo "Index: $key, Record: ${array[$key]}"
    done
    echo "==============================="
}
while true; do
    mapfile -t logfile_records < <(tail -n "$records_per_batch" "$logfile" | awk -F'; ' -v nf="$num_fields" '{OFS=FS; NF=nf; print}')
    records=()
    ip_arrays=()
    dodgy_records=()
    epoch=$(echo "${logfile_records[0]}" | awk -F'; ' '{print int($3)}')
    echo "epoch=$epoch"

    for i in "${!logfile_records[@]}"; do
        record="${logfile_records[i]}"
        timestamp=$(echo "$record" | awk -F'; ' '{print $3}')
        timestamp100=$(echo "scale=6; ($timestamp - $epoch) * 100" | bc)
        records[i]="${record}; $timestamp100"

        if ((i >= 3 && i < $((${#logfile_records[@]} - 3)))); then
            trimmed_records[i]="${records[i]}"
        fi
    done
    for index in "${!trimmed_records[@]}"; do
        record="${records[index]}"
        timestamp=$(echo "$record" | awk -F'; ' '{print $3}')
        timestamp100=$(echo "$record" | awk -F'; ' '{print $32}')
        reflector=$(echo "$record" | awk -F'; ' '{print $10}')

        should_keep_record "$index" && ip_arrays["$reflector"]+=" $index" || dodgy_records["$reflector"]+=" $index"
    done

    for reflector in "${!ip_arrays[@]}"; do
        filtered_records=()
        read -r -a record_array <<<"${ip_arrays[$reflector]}"

        for index in "${record_array[@]}"; do
            filtered_records["$index"]="${records[$index]}"
        done

        sum_array=()
        count_array=()

        for ((j = 0; j < num_fields + 1; j++)); do
            sum_array[$j]=0
            count_array[$j]=0
        done

        for record in "${filtered_records[@]}"; do
            fields=($(echo "$record" | awk -F'; ' '{ for (i=1; i<=NF; i++) print $i }'))

            for ((j = 0; j < num_fields + 1; j++)); do
                if [[ "${fields[$j]}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                    integer_value=$(echo "${fields[$j]} * 1000000" | awk '{printf "%.0f", $1}')

                    if [ "$j" -eq 2 ]; then
                        timestamp=$integer_value
                    fi

                    ((sum_array[$j] += integer_value))
                    ((count_array[$j]++))
                fi
            done
        done

        processed_line=""

        for ((j = 0; j < num_fields + 1; j++)); do
            if [ "${count_array[$j]}" -gt 0 ]; then
                average=$((100 * sum_array[$j] / count_array[$j]))
            else
                case $((j + 1)) in
                1) processed_line+="DATA; " ;;
                2) processed_line+="$(date -d "@$((timestamp))" "+%Y-%m-%d-%H:%M:%S"); " ;;
                10) processed_line+="$reflector; " ;;
                *) processed_line+="Field $((j + 1)); " ;;
                esac
                continue
            fi

            processed_line+="$((average / 100)); "
        done

        #echo "$processed_line"
        line=$(echo "$processed_line" | awk -v OFS=':' -F'; ' '{
        $28=(substr($28,length($28)-2)=="_bb")?10:0
        $29=(substr($29,length($29)-2)=="_bb")?10:0
        print $11,$5,$6,$7,$8,$12,$13+$14-$15,$13,$14,$15,$16,$17,$18+$19-$20,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31
    }')
        echo "PUTVAL \"$HOSTNAME/autorate-$reflector/autorate\" interval=$INTERVAL N:$line"
        echo "PUTVAL \"$HOSTNAME/autorate-all/autorate\" interval=$INTERVAL N:$line"
    done

    #sleep 1
done
