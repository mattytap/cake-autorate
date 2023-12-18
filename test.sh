#!/bin/bash

# Set DEBUG to true to enable debug output
DEBUG=true

logfile="/var/log/cake-autorate.primary.log"
records_per_batch=55
num_splits=7
num_fields=31 # Adjust based on your logfile structure
timestamp_index=3
reflector_index=10
skip_records=3

declare -a records
declare -a trimmed_records
declare -A ip_arrays
declare -A dodgy_records
declare -a sum_array
declare -a count_array
declare -a filtered_records

# Function to check if a record should be kept based on conditions
should_keep_record() {
    local index="$1"
    local record=("${records[@]:index:1}")
    local field13=$(echo "$record" | awk -F'; ' '{print $13}')
    local field18=$(echo "$record" | awk -F'; ' '{print $18}')

    if [ "$DEBUG" = true ]; then
        echo "index: $index"
        echo "record: $record"
    fi

    # Extract three records before and after the specified index
    start_index=$((index - 3))
    end_index=$((index + 3))

    # Extract the desired range of records
    previous_records=("${records[@]:start_index:3}")
    following_records=("${records[@]:index+1:3}")

    if [ "$DEBUG" = true ]; then
        # Print the results
        echo ""
        echo "Previous Records:"
        for i in "${!previous_records[@]}"; do
            echo "Index: $((start_index + i)), Record: ${previous_records[$i]}"
        done
        echo "record:"
        echo "Index: $index, Record: $record"
        echo "Following Records:"
        for i in "${!following_records[@]}"; do
            echo "Index: $((index + i + 1)), Record: ${following_records[$i]}"
        done
    fi

    # Extract values of the three previous and three following records
    local previous_values=()
    for record in "${previous_records[@]}"; do
        local value=$(echo "$record" | awk -F'; ' '{print $13}')
        previous_values+=("$value")
    done

    local following_values=()
    for record in "${following_records[@]}"; do
        local value=$(echo "$record" | awk -F'; ' '{print $13}')
        following_values+=("$value")
    done

    # Concatenate surrounding values for median calculation
    local all_values=($(echo "${previous_values[@]}" "${following_values[@]}" | tr ' ' '\n' | sort -n))

    if [ "$DEBUG" = true ]; then
        echo ""
        echo "all values"
        for i in ${all_values[@]}; do
            echo -n $i" "
        done
        echo ""
    fi
    # Calculate median
    local median_index=$(((${#all_values[@]} - 1) / 2))
    local median=${all_values[$median_index]}

    if ((${#all_values[@]} % 2 == 0)); then
        # If the number of elements is even, calculate the mean of the two middle values
        local next_index=$((median_index + 1))
        local next_value=${all_values[$next_index]}
        median=$(((median + next_value) / 2))
    fi

    # Calculate the mean of all values in the all_values array
    local sum=0
    for value in "${all_values[@]}"; do
        sum=$((sum + value))
    done

    local mean=$((sum / ${#all_values[@]}))
    if [ "$DEBUG" = true ]; then

        echo ""
        echo "field13: $field13"
        echo " median: $median"
        echo "   mean: $mean"
    fi

    # Check conditions and return result
    if [[ "$field13" -gt "$((2 * median))" ]] || [[ "$field18" -gt "$((2 * median))" ]]; then
        return 1 # Record should be filtered out
    else
        return 0 # Record can be kept
    fi
}

# Function to output array debug information
debug_output_array() {
    local array_name="$1"
    local -n array="${array_name}"

    echo "Debug output for $array_name:"
    for i in "${!array[@]}"; do
        echo "Index: $i, Record: ${array[$i]}"
    done
    echo "==============================="
}

while true; do
    # Read the last 55 lines from the logfile and extract the first 20 (num_fields) fields
    mapfile -t logfile_records < <(tail -n "$records_per_batch" "$logfile" | awk -F'; ' -v nf="$num_fields" '{OFS=FS; NF=nf; print}')

    # Reset arrays
    records=()
    trimmed_records=()
    ip_arrays=()
    dodgy_records=()

    # epoch is defined here as timestamp of the first record in batch
    epoch=$(echo "${logfile_records[0]}" | awk -F'; ' '{print int($3)}')
    echo "epoch=$epoch"

    # Populate arrays for the last 55 records and middle 49 records
    for i in "${!logfile_records[@]}"; do
        record="${logfile_records[$i]}"
        timestamp=$(echo "$record" | awk -F'; ' '{print $3}')

        # Calculate timestamp100
        timestamp100=$(awk -v timestamp="$timestamp" -v epoch="$epoch" 'BEGIN { printf "%.0f", timestamp * 100 - epoch * 100 }')

        # Add a null field and the new column at the end of each record
        record_with_null_field="${record}; ${timestamp100}"

        # For the last 55 records
        records["$i"]="$record_with_null_field"

        # For the middle 49 records
        if ((i >= 3 && i < $((${#logfile_records[@]} - 3)))); then
            trimmed_records["$i"]="$record_with_null_field"
        fi
    done

    if [ "$DEBUG" = true ]; then
        debug_output_array "records"
        debug_output_array "trimmed_records"
    fi

    # Initialize an array to store records to later be averaged by IP address based on reflector
    # Distribute records based on timestamp in hundredths of a second from epoch (no reason other than it's just easier to read!)

    # Loop through each index in trimmed_records
    for index in "${!trimmed_records[@]}"; do
        # Extract timestamp etc
        record="${records[$index]}"
        timestamp=$(echo "${records["$index"]}" | awk -F'; ' '{print $3}')
        timestamp100=$(echo "${records["$index"]}" | awk -F'; ' -v num_fields="$num_fields" '{print $(num_fields + 1)}')
        reflector=$(echo "${records["$index"]}" | awk -F'; ' '{print $10}')

        if [ "$DEBUG" = true ]; then
            echo "==============================="
            echo ""
            echo "   timestamp: $timestamp"
            echo "timestamp100: $timestamp100"
            echo "   reflector: $reflector"
        fi

        # Check conditions and filter accordingly
        if should_keep_record "$index"; then
            # Append a new field (binary) indicating to keep the record
            ip_arrays["$reflector"]+=" ${index}"
        else
            # Append a new field (binary) indicating to discard the record
            dodgy_records["$reflector"]+=" ${index}"
        fi
    done

    if [ "$DEBUG" = true ]; then
        debug_output_array "ip_arrays"
        debug_output_array "dodgy_records"
        debug_output_array "records"
        debug_output_array "trimmed_records"

        echo "# ============================================================================================================"
    # ============================================================================================================
    fi

    # Process each IP array separately and calculate the running average
    for reflector in "${!ip_arrays[@]}"; do
        # Translate selected record indesese
        filtered_records=()
        IFS=' ' read -r -a record_array <<<"${ip_arrays[$reflector]}"
        unset IFS
        if [ "$DEBUG" = true ]; then
            debug_output_array "ip_arrays"
            debug_output_array "record_array"
        fi
        # Output each record on a new line
        for i in "${!record_array[@]}"; do
            index="${record_array[$i]}"
            record="${records[$index]}"
            # Append the record to an indexed array using index as the key
            filtered_records["$index"]="$record"
        done

        if [ "$DEBUG" = true ]; then
            echo ""
            echo "Calculate Running Average for IP $reflector. Records:"
            debug_output_array "filtered_records"
            echo ""
            echo "Running Average:"
        fi

        # Calculate running average for each field based on available records
        sum_array=()
        count_array=()

        for ((j = 0; j < num_fields + 1; j++)); do
            sum_array[$j]=0
            count_array[$j]=0
        done

        for record in "${filtered_records[@]}"; do
            fields=($(echo "$record" | awk -F'; ' '{ for (i=1; i<=NF; i++) print $i }'))
            for ((j = 0; j < num_fields + 1; j++)); do
                # Exclude fields that are not numeric (you can add more conditions if needed)
                if [[ "${fields[$j]}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                    # Multiply floating-point fields by 1,000,000 and convert to integer
                    integer_value=$(echo "${fields[$j]} * 1000000" | awk '{printf "%.0f", $1}')
                    if [ "$j" -eq 2 ]; then
                        timestamp=$integer_value
                    fi
                    sum_array[$j]=$((sum_array[$j] + integer_value))
                    count_array[$j]=$((count_array[$j] + 1))
                fi
            done
        done

        # Print the processed data in the desired format with rounded integer values
        processed_line=""
        for ((j = 0; j < num_fields + 1; j++)); do
            if [ "${count_array[$j]}" -gt 0 ]; then
                average=$((100 * sum_array[$j] / count_array[$j]))
            else
                case $((j + 1)) in
                1) processed_line+="DATA; " ;;
                2) processed_line+="$(date -d "@$(printf "%.0f" "$timestamp")" "+%Y-%m-%d-%H:%M:%S"); " ;;
                10) processed_line+="$reflector; " ;;
                *) processed_line+="Field $((j + 1)); " ;;
                esac
                continue
            fi

            processed_line+="$((average / 100)); "
        done
        # final script output
        echo "$processed_line"
    done

    exit # temp exit to debug a single pass of main loop
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
