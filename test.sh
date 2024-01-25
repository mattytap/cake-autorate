#!/bin/bash

logfile="/var/log/cake-autorate.primary.log"
records_per_batch=55
num_fields=31

declare -a records
declare -a trimmed_records
declare -A ip_arrays
declare -A dodgy_records
declare -a sum_array
declare -a count_array
declare -a preprocessed_data

# Function to calculate median
calculate_median() {
    local values=("$@")
    local length=${#values[@]}
    local sorted_values=($(printf "%s\n" "${values[@]}" | sort -n))
    local mid=$((length / 2))
    if ((length % 2)); then
        echo "${sorted_values[$mid]}"
    else
        echo $(((sorted_values[mid - 1] + sorted_values[mid]) / 2))
    fi
}

debug_output_array() {
    local key
    local array_name="$1"
    local -n disarray="${array_name}"
    echo "Debug output for $array_name:"
    for key in "${!disarray[@]}"; do
        echo "Index: $key, Record: ${disarray[$key]}"
    done
    echo "==============================="
}

while true; do
    start_time=${EPOCHREALTIME/./}

    # Read and filter the log file records
    mapfile -t logfile_records < <(tail -n "$records_per_batch" "$logfile" | awk -F'; ' -v nf="$num_fields" '$1=="DATA" {OFS=FS; NF=nf; print}')
    records=()
    trimmed_records=()
    ip_arrays=()
    dodgy_records=()
    epoch=$(echo "${logfile_records[0]}" | awk -F'; ' '{print int($3)}')
    #echo "epoch=$epoch"

    # Populate records and trimmed_records arrays
    for i in "${!logfile_records[@]}"; do
        records[i]="${logfile_records[i]}; "
        if ((i >= 3 && i < $((${#logfile_records[@]} - 3)))); then
            trimmed_records[i]="${logfile_records[i]}; "
        fi
    done
    #debug_output_array "records"
    #debug_output_array "trimmed_records"

    end_time=${EPOCHREALTIME/./}
    execution_time=$(echo "($end_time - $start_time) / 1000" | bc)
    #echo "1 Execution time: $execution_time ms"
    start_time=${EPOCHREALTIME/./}

    # Preprocess the data
    preprocessed_data=()

    # Concatenate the records array into a single string separated by newlines
    records_string=$(printf "%s\n" "${records[@]}")

    # Use awk to process the entire dataset at once
    IFS=$'\n' read -rd '' -a preprocessed_data < <(echo "$records_string" | awk -F'; ' '{print $13 ";" $18 ";" $3 ";" $10}')

    # Optional: Uncomment to debug
    #debug_output_array "preprocessed_data"

    end_time=${EPOCHREALTIME/./}
    execution_time=$(echo "($end_time - $start_time) / 1000" | bc)
    #echo "2b Execution time: $execution_time ms"
    start_time=${EPOCHREALTIME/./}

    # Batch processing
    for index in "${!trimmed_records[@]}"; do
        IFS=';' read -r field13 field18 timestamp reflector <<<"${preprocessed_data[index]}"
        start_index=$((index - 3))
        end_index=$((index + 1 + 3))
        surrounding_values13=()

        for ((i = start_index; i < end_index; i++)); do
            if [ "$i" -eq "$index" ]; then
                continue # Skip this iteration if i equals index
            fi

            IFS=';' read -r value _ <<<"${preprocessed_data[i]}"
            surrounding_values13+=("$value")
        done
        #debug_output_array "surrounding_values13"

        median13=$(calculate_median "${surrounding_values13[@]}")

        if ((field13 > 3 * median13)); then
            dodgy_records["$reflector"]+=" $index"
        else
            ip_arrays["$reflector"]+=" $index"
        fi
    done

    #debug_output_array "ip_arrays"
    #debug_output_array "dodgy_records"

    end_time=${EPOCHREALTIME/./}
    execution_time=$(echo "($end_time - $start_time) / 1000" | bc)
    #echo "3 Execution time: $execution_time ms"
    start_time=${EPOCHREALTIME/./}

    # Generate a random number between 0 and 6 (inclusive)
    selected_iteration=$((RANDOM % 7))

    # Initialize a counter
    counter=0

    # Iterate over the ip array
    for reflector in "${!ip_arrays[@]}"; do
        filtered_records=()
        read -r -a record_array <<<"${ip_arrays[$reflector]}"

        for index in "${record_array[@]}"; do
            filtered_records["$index"]="${records[$index]}"
        done
        #debug_output_array "filtered_records"

        sum_array=()
        count_array=()

        # Initialize sum and count arrays for the fields of interest
        for field in 11 5 6 12 13 15 16 17 18 20 21 28 29 30 31; do
            sum_array[field]=0
            count_array[field]=0
        done

        # Process only the fields of interest
        for record in "${filtered_records[@]}"; do
            # Extract all fields including the transformed 28 and 29
            fields=($(echo "$record" | awk -F'; ' '{
            $28=(substr($28,length($28)-2)=="_bb")?10:0  # Adjusted index for $28
            $29=(substr($29,length($29)-2)=="_bb")?10:0  # Adjusted index for $29
            print $11, $5,$6,$12,$13,$15,$16,$17,$18,$20,$21,$28,$29,$30,$31
            }'))
            #debug_output_array "fields"

            index=0
            for field in 11 5 6 12 13 15 16 17 18 20 21 28 29 30 31; do
                if [[ "${fields[index]}" =~ ^[0-9]+$ ]]; then
                    ((sum_array[field] += "${fields[index]}"))
                    ((count_array[field]++))
                fi
                ((index++))
            done
        done

        line=""
        for field in 11 5 6 12 13 15 16 17 18 20 21 28 29 30 31; do
            if [ "${count_array[field]}" -gt 0 ]; then
                average=$((100 * sum_array[field] / count_array[field]))
                line+="$((average / 100)):"
            else
                line+="0:" # Default value if no data available
            fi
        done

        # Remove the trailing colon
        line=${line%:}
        #echo $line

        # Output the final line
        echo "PUTVAL \"$HOSTNAME/autorate-$reflector/autorate\" interval=$INTERVAL N:$line"
        if [ "$counter" -eq "$selected_iteration" ]; then
            echo "PUTVAL \"$HOSTNAME/autorate-all/autorate\" interval=$INTERVAL N:$line"
        fi

        # Increment the counter
        ((counter++))
    done

    end_time=${EPOCHREALTIME/./}
    execution_time=$(echo "($end_time - $start_time) / 1000" | bc)
    #echo "4 Execution time: $execution_time ms"

    #sleep 1
done
end_time=${EPOCHREALTIME/./}
execution_time=$(echo "($end_time - $start_time) / 1000" | bc)
echo "4 Execution time: $execution_time ms"
start_time=${EPOCHREALTIME/./}
