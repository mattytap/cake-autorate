#!/bin/bash

logfile="/var/log/cake-autorate.primary.log"
records_per_batch=56
num_splits=7
num_fields=31 # Adjust based on your logfile structure
field10_index=10

# Function to check if a record should be kept based on conditions
should_keep_record() {
    local record="$1"
    local field13=$(echo "$record" | awk -F'; ' '{print $13}')
    local field18=$(echo "$record" | awk -F'; ' '{print $18}')

    # Extract values of the three previous and three following records
    local previous_values=($(echo "$last_records" | awk -F'; ' '{print $13}' | tail -n 3))
    local following_values=($(echo "$last_records" | awk -F'; ' '{print $13}' | head -n 3))

    # Concatenate all values for median calculation
    local all_values=("${previous_values[@]}" "$field13" "${following_values[@]}")

    # Sort values
    IFS=$'\n' sorted_values=($(sort -n <<<"${all_values[*]}"))
    unset IFS

    # Calculate median
    local median_index=$(((${#sorted_values[@]} - 1) / 2))
    local median=${sorted_values[$median_index]}

    # Check conditions and return result
    if [ "$field13" -gt "$((2 * median))" ] || [ "$field18" -gt "$((2 * median))" ]; then
        return 1 # Record should be filtered out
    else
        return 0 # Record can be kept
    fi
}

while true; do
    # Read the last 56 lines from the logfile
    mapfile -t last_records < <(tail -n "$records_per_batch" "$logfile")

    # Initialize arrays to store records for each IP address
    ip_arrays=()

    # Distribute records based on field10 (assumed to be an IP address)
    for record in "${last_records[@]}"; do
        # Extract field10
        field10=$(echo "$record" | awk '{print $'$field10_index'}')

        # Determine the index based on field10
        index=$(($(printf '%d' "'$field10") % num_splits))

        # Add a null field to ensure a trailing '; ' in the last field
        record_with_null_field="$record; "

        # Add the record to the corresponding IP array
        ip_arrays[$index]+="$record_with_null_field"
    done

    # Process each IP array separately and calculate the running average
    for ((i = 0; i < num_splits; i++)); do
        echo "Processing records for IP ${ip_arrays[$i]}"

        # Pre-process each record and filter out based on conditions
        declare -a filtered_records
        for record in "${ip_arrays[$i]}"; do
            if should_keep_record "$record"; then
                filtered_records+=("$record")
            fi
        done

        # Calculate running average for each field based on available records
        declare -a sum_array
        declare -a count_array

        for ((j = 0; j < num_fields; j++)); do
            sum_array[$j]=0
            count_array[$j]=0
        done

        for record in "${filtered_records[@]}"; do
            fields=($(echo "$record" | awk -F'; ' '{ for (i=1; i<=NF; i++) print $i }'))
            for ((j = 0; j < num_fields; j++)); do
                # Exclude fields that are not numeric (you can add more conditions if needed)
                if [[ "${fields[$j]}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                    # Multiply floating-point fields by 1,000,000 and convert to integer
                    integer_value=$(echo "${fields[$j]} * 1000000" | awk '{printf "%.0f", $1}')
                    sum_array[$j]=$((sum_array[$j] + integer_value))
                    count_array[$j]=$((count_array[$j] + 1))
                fi
            done
        done

        for ((j = 0; j < num_fields; j++)); do
            if [ "${count_array[$j]}" -gt 0 ]; then
                average=$(printf "%.2f" "$((100 * sum_array[$j] / count_array[$j]))e-2")
                echo "Running Average for Field $((j + 1)): $average"
            else
                echo "No records for Field $((j + 1))"
            fi
        done

    done
    # Add a sleep to avoid high resource usage
    sleep 1
done
exit

# Process the log file line by line
echo "$log_data" | while IFS= read -r line; do
    reflector=$(echo "$line" | cut -d';' -f10 | sed 's/^.//')
    line=$(echo "$line" | awk -v OFS=':' -F'; ' '{
        $28=(substr($28,length($28)-2)=="_bb")?10:0
        $29=(substr($29,length($29)-2)=="_bb")?10:0
        print $11,$5,$6,$7,$8,$12,$13+$14-$15,$13,$14,$15,$16,$17,$18+$19-$20,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31
    }')

    echo "PUTVAL \"$HOSTNAME/autorate-$reflector/autorate\" interval=$INTERVAL N:$line"
    echo "PUTVAL \"$HOSTNAME/autorate-all/autorate\" interval=$INTERVAL N:$line"
done



SEQUENCE
DL_ACHIEVED_RATE_KBPS
UL_ACHIEVED_RATE_KBPS
DL_LOAD_PERCENT
UL_LOAD_PERCENT

DL_OWD_BASELINE
DL_OWD_US
DL_OWD_DELTA_EWMA_US
DL_OWD_DELTA_US
DL_ADJ_DELAY_THR
UL_OWD_BASELINE
UL_OWD_US
UL_OWD_DELTA_EWMA_US
UL_OWD_DELTA_US
UL_ADJ_DELAY_THR
DL_SUM_DELAYS
DL_AVG_OWD_DELTA_US
DL_ADJ_AVG_OWD_DELTA_THR_US
UL_SUM_DELAYS
UL_AVG_OWD_DELTA_US
UL_ADJ_AVG_OWD_DELTA_THR_US
DL_LOAD_CONDITION
UL_LOAD_CONDITION
CAKE_DL_RATE_KBPS
CAKE_UL_RATE_KBPS
