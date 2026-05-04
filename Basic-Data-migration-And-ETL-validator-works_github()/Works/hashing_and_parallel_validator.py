import pandas as pd
from joblib import Parallel, delayed
from datasketch import MinHash, MinHashLSH
import numpy as np
from tqdm import tqdm
from pathlib import Path
import os


class validate:

    differences={}
     
    def validate_csv_differences(original_file, new_file, primary_key=None):

        #Define column sets upfront (used in both branches)
        original_cols, new_cols = set(original_file.columns), set(new_file.columns)
        added_columns = list(new_cols - original_cols)
        deleted_columns = list(original_cols - new_cols)
        common_columns = list(original_cols.intersection(new_cols))
        
        # Branch: No primary key specified
        
        if primary_key == None:

            print(f"\nAnalyzing differences between files...   (With No Primary Key)")
            original_array = original_file[common_columns].fillna('').astype(str).values
            new_array = new_file[common_columns].fillna('').astype(str).values

            # 2. OPTIMIZED: Much faster fingerprinting using numpy vectorization
            def create_fingerprints_vectorized(array):
                with tqdm(total=1, desc="Creating fingerprints", unit="batch", leave=False) as progress_bar:
                    fingerprints = np.array([hash(''.join(row)) for row in array])
                    progress_bar.update(1)
                return fingerprints

            # Vectorized fingerprinting - much faster
            original_fingerprints = create_fingerprints_vectorized(original_array)
            new_fingerprints = create_fingerprints_vectorized(new_array)

            # 3. Find exact matches using vectorized operations
            matched_orig_indices = set()
            matched_new_indices = set()
            exact_matches = {}

            # OPTIMIZED: Process in chunks for efficient memory use
            chunk_size = 10000
            with tqdm(total=len(new_fingerprints), desc="Finding exact matches", unit="rows", leave=False) as progress_bar:
                for chunk_start in range(0, len(new_fingerprints), chunk_size):
                    chunk_end = min(chunk_start + chunk_size, len(new_fingerprints))
                    chunk_size_actual = chunk_end - chunk_start
                    
                    # Create a lookup dictionary for this chunk of fingerprints
                    orig_fp_dict = {}
                    for i, fp in enumerate(original_fingerprints):
                        if i not in matched_orig_indices:  # Only consider unmatched rows
                            if fp in orig_fp_dict:
                                orig_fp_dict[fp].append(i)
                            else:
                                orig_fp_dict[fp] = [i]
                    
                    # Find matches for this chunk
                    for i in range(chunk_start, chunk_end):
                        if i in matched_new_indices:
                            continue
                            
                        fp = new_fingerprints[i]
                        if fp in orig_fp_dict:
                            for j in orig_fp_dict[fp]:
                                if j not in matched_orig_indices:
                                    if np.array_equal(new_array[i], original_array[j]):
                                        exact_matches[i] = j
                                        matched_orig_indices.add(j)
                                        matched_new_indices.add(i)
                                        break
                    
                    progress_bar.update(chunk_size_actual)

            # 4. Handle remaining rows with aggressive optimizations
            remaining_orig_indices = list(set(range(len(original_array))) - matched_orig_indices)
            remaining_new_indices = list(set(range(len(new_array))) - matched_new_indices)

            # OPTIMIZATION: Pre-compute string representations for comparison
            remaining_orig_rows = original_array[remaining_orig_indices]
            remaining_new_rows = new_array[remaining_new_indices]

            fuzzy_matches = {}
            fuzzy_threshold = 0.2857142857142857 # Minimum similarity threshold

            # Compute MinHash signatures for all remaining rows
            def compute_minhash_batch(rows, num_perm=64):
                results = []
                for row in rows:
                    mh = MinHash(num_perm=num_perm)
                    for val in row:
                        mh.update(val.encode('utf8'))
                    results.append(mh)
                return results

            # Pre-compute all MinHash values in parallel
            original_hashes = {}
            new_hashes = {}

            # Process original rows in parallel
            if remaining_orig_indices:
                cpu_count = os.cpu_count() or 2
                batch_size = max(50, len(remaining_orig_rows) // (cpu_count * 2))
                batches = [remaining_orig_rows[i:i+batch_size] for i in range(0, len(remaining_orig_rows), batch_size)]
                
                with tqdm(total=len(batches), desc="Computing MinHash (original)", unit="batch", leave=False) as progress_bar:
                    orig_hash_list = []
                    for batch in batches:
                        batch_result = Parallel(n_jobs=-1, backend="threading")(
                            delayed(compute_minhash_batch)([row]) for row in batch
                        )
                        orig_hash_list.extend(batch_result)
                        progress_bar.update(1)
                
                original_hashes = {remaining_orig_indices[i]: hash_list[0] for i, hash_list in enumerate(orig_hash_list)}

            # Process new rows in parallel
            if remaining_new_indices:
                cpu_count = os.cpu_count() or 2
                batch_size = max(50, len(remaining_new_rows) // (cpu_count * 2))
                batches = [remaining_new_rows[i:i+batch_size] for i in range(0, len(remaining_new_rows), batch_size)]
                
                with tqdm(total=len(batches), desc="Computing MinHash (new)", unit="batch", leave=False) as progress_bar:
                    new_hash_list = []
                    for batch in batches:
                        batch_result = Parallel(n_jobs=-1, backend="threading")(
                            delayed(compute_minhash_batch)([row]) for row in batch
                        )
                        new_hash_list.extend(batch_result)
                        progress_bar.update(1)
                
                new_hashes = {remaining_new_indices[i]: hash_list[0] for i, hash_list in enumerate(new_hash_list)}

            # Use LSH for faster matching
            lsh = MinHashLSH(threshold=0.2, num_perm=64)

            # Insert original rows into LSH index
            with tqdm(total=len(original_hashes), desc="Building LSH index", unit="row", leave=False) as progress_bar:
                N = max(1, len(original_hashes) // 100)
                count = 0
                
                for idx in original_hashes:
                    lsh.insert(f"orig_{idx}", original_hashes[idx])
                    count += 1
                    if count % N == 0:
                        progress_bar.update(N)
                
                # Update remaining items
                remaining = count % N
                if remaining > 0:
                    progress_bar.update(remaining)

            # Query LSH with new rows
            with tqdm(total=len(new_hashes), desc="Querying LSH index", unit="row", leave=False) as progress_bar:
                N = max(1, len(new_hashes) // 100)
                count = 0
                
                for idx in new_hashes:
                    matches = lsh.query(new_hashes[idx])
                    if matches:
                        best_match_idx = None
                        best_similarity = fuzzy_threshold
                        
                        for match_key in matches:
                            orig_idx = int(match_key.split('_')[1])
                            if orig_idx not in matched_orig_indices:
                                # Calculate similarity directly
                                orig_row = original_array[orig_idx]
                                new_row = new_array[idx]
                                
                                # Count exact matches - faster than Jaccard for short rows
                                match_count = np.count_nonzero(orig_row == new_row)
                                similarity = match_count / len(common_columns)
                                
                                if similarity >= best_similarity:
                                    best_similarity = similarity
                                    best_match_idx = orig_idx
                        
                        if best_match_idx is not None:
                            fuzzy_matches[idx] = best_match_idx
                            matched_orig_indices.add(best_match_idx)
                    
                    count += 1
                    if count % N == 0:
                        progress_bar.update(N)
                
                # Update remaining items
                remaining = count % N
                if remaining > 0:
                    progress_bar.update(remaining)

            # 7. Combine exact and fuzzy matches
            all_matches = {**exact_matches, **fuzzy_matches}

            # 8. Find updated rows and changes - process in chunks
            updated_rows_details = []

            # Process in batches for better memory management
            batch_size = 1000
            all_matches_items = list(all_matches.items())
            with tqdm(total=len(all_matches_items), desc="Finding updated rows", unit="row", leave=False) as progress_bar:
                for batch_start in range(0, len(all_matches_items), batch_size):
                    batch_end = min(batch_start + batch_size, len(all_matches_items))
                    batch = all_matches_items[batch_start:batch_end]
                    batch_size_actual = len(batch)
                    
                    for new_idx, orig_idx in batch:
                        orig_row = original_array[orig_idx]
                        new_row = new_array[new_idx]
                        
                        # Find differences - vectorized comparison
                        diff_mask = orig_row != new_row
                        if np.any(diff_mask):
                            changes = {}
                            for i, col in enumerate(common_columns):
                                if diff_mask[i]:
                                    changes[col] = {"old": orig_row[i], "new": new_row[i]}
                            
                            updated_rows_details.append({
                                "row_index": new_idx,
                                "changes": changes,
                                "row_data": dict(zip(common_columns, new_row))
                            })
                    
                    progress_bar.update(batch_size_actual)

            # 9. Find added and deleted rows
            added_indices = set(range(len(new_array))) - set(all_matches.keys())
            deleted_indices = set(range(len(original_array))) - matched_orig_indices

            # Process in batches
            added_rows = []
            batch_size = 500
            added_indices_list = list(added_indices)
            with tqdm(total=len(added_indices_list), desc="Processing added rows", unit="row", leave=False) as progress_bar:
                for i in range(0, len(added_indices_list), batch_size):
                    batch_indices = added_indices_list[i:i+batch_size]
                    batch_size_actual = len(batch_indices)
                    added_rows.extend([dict(zip(common_columns, new_array[idx])) for idx in batch_indices])
                    progress_bar.update(batch_size_actual)

            deleted_rows = []
            deleted_indices_list = list(deleted_indices)
            with tqdm(total=len(deleted_indices_list), desc="Processing deleted rows", unit="row", leave=False) as progress_bar:
                for i in range(0, len(deleted_indices_list), batch_size):
                    batch_indices = deleted_indices_list[i:i+batch_size]
                    batch_size_actual = len(batch_indices)
                    deleted_rows.extend([dict(zip(common_columns, original_array[idx])) for idx in batch_indices])
                    progress_bar.update(batch_size_actual)

            # 10. Find duplicates with vectorized approach
            def find_duplicates(df, columns):
                with tqdm(total=2, desc=f"Finding duplicates", unit="step", leave=False) as progress_bar:
                    # Much faster fingerprinting
                    df_str = df[columns].fillna('').astype(str)
                    
                    # Vectorized fingerprint calculation
                    fingerprints = np.array([hash(tuple(row)) for row in df_str.to_numpy()])
                    progress_bar.update(1)
                    
                    # Find duplicates
                    fingerprint_series = pd.Series(fingerprints)
                    dupe_mask = fingerprint_series.duplicated(keep=False)
                    dupe_fingerprints = fingerprint_series[dupe_mask]
                    progress_bar.update(1)
                    
                    return df[dupe_mask].copy()

            with tqdm(total=2, desc="Processing duplicates", unit="file", leave=False) as progress_bar:
                duplicate_rows_in_source = find_duplicates(original_file, common_columns)
                progress_bar.update(1)
                
                duplicate_rows_in_target = find_duplicates(new_file, common_columns)
                progress_bar.update(1)

            validate.differences = {
                "added_columns": added_columns,
                "deleted_columns": deleted_columns,
                "added_rows": added_rows,
                "deleted_rows": deleted_rows,
                "updated_rows_details": updated_rows_details,
                "duplicate_rows_in_source": duplicate_rows_in_source.to_dict(orient='records'),
                "duplicate_rows_in_target": duplicate_rows_in_target.to_dict(orient='records'),
                "summary": {
                    "added_columns_count": len(added_columns),
                    "deleted_columns_count": len(deleted_columns),
                    "added_rows_count": len(added_rows),
                    "deleted_rows_count": len(deleted_rows),
                    "updated_rows_count": len(updated_rows_details),
                    "duplicate_rows_in_source_count": len(duplicate_rows_in_source),
                    "duplicate_rows_in_target_count": len(duplicate_rows_in_target),
                }
            }

        else:

            print(f"\nAnalyzing differences between files...   (With Primary Key as {primary_key})")
                      
            # Use sets for faster lookup
            original_ids = set(original_file[primary_key])
            new_ids = set(new_file[primary_key])
            # Directly compute added/deleted rows using set operations
            added_ids = new_ids - original_ids
            deleted_ids = original_ids - new_ids
            common_ids = original_ids.intersection(new_ids)


            total_steps = int(7 + (len(list(common_ids))/1000))
            # progress_bar = tqdm(total=total_steps, desc="Processing", unit="step")

            with tqdm(total=total_steps, desc="Analyzing.......", unit="iteration", leave= False) as progress_bar:

                progress_bar.update(1)

                # Use indexing for faster dataframe operations
                original_key_index = {k: i for i, k in enumerate(original_file[primary_key])}
                new_key_index = {k: i for i, k in enumerate(new_file[primary_key])}

                progress_bar.update(1)

                # Sequential processing
                added_rows = new_file[new_file[primary_key].isin(added_ids)].to_dict(orient='records')
                deleted_rows = original_file[original_file[primary_key].isin(deleted_ids)].to_dict(orient='records')

                progress_bar.update(1)

                duplicate_source_ids = original_file[original_file.duplicated(subset=[primary_key], keep=False)][primary_key].tolist()
                duplicate_target_ids = new_file[new_file.duplicated(subset=[primary_key], keep=False)][primary_key].tolist()

                progress_bar.update(1)

                # Get duplicate rows
                duplicate_rows_in_source = original_file[original_file[primary_key].isin(duplicate_source_ids)].to_dict(orient='records')
                duplicate_rows_in_target = new_file[new_file[primary_key].isin(duplicate_target_ids)].to_dict(orient='records')

                progress_bar.update(1)

                updated_rows = []
                updated_rows_details = []
                # Batch processing for updates
                batch_size = min(1000, len(common_ids))
                common_ids_list = list(common_ids)

                progress_bar.update(1)

                N=max(1,(len(common_ids_list)/batch_size)//1000)

                for i in range(0, len(common_ids_list), batch_size):
                    batch = common_ids_list[i:i+batch_size]

                    batch_updates = []
                    batch_details = []
                    for pk_value in batch:
                        orig_row_idx = original_key_index.get(pk_value)
                        new_row_idx = new_key_index.get(pk_value)
                        
                        if orig_row_idx is not None and new_row_idx is not None:
                            orig_row = original_file.iloc[orig_row_idx]
                            new_row = new_file.iloc[new_row_idx]
                            
                            changes = {}
                            for column in orig_row.index:
                                if column in new_row.index and orig_row[column] != new_row[column]:
                                    changes[column] = {'old': orig_row[column], 'new': new_row[column]}
                            
                            if changes:
                                batch_updates.append(new_row.to_dict())
                                batch_details.append({
                                    primary_key: pk_value,
                                    "row_data": new_row.to_dict(),
                                    "changes": changes
                                })
                    updated_rows.extend(batch_updates)
                    updated_rows_details.extend(batch_details)
                    
                    if i % N == 0:
                        progress_bar.update(N) 


                # Structure results consistently with expected format
                validate.differences = {
                    "added_columns": added_columns,
                    "deleted_columns": deleted_columns,
                    "added_rows": added_rows,
                    "deleted_rows": deleted_rows,
                    "updated_rows": updated_rows,
                    "updated_rows_details": updated_rows_details,
                    "duplicate_rows_in_source": duplicate_rows_in_source,
                    "duplicate_rows_in_target": duplicate_rows_in_target,
                    "summary": {
                        "added_columns_count": len(added_columns),
                        "deleted_columns_count": len(deleted_columns),
                        "added_rows_count": len(added_rows),
                        "deleted_rows_count": len(deleted_rows),
                        "updated_rows_count": len(updated_rows),
                        "duplicate_rows_in_source_count": len(duplicate_rows_in_source),
                        "duplicate_rows_in_target_count": len(duplicate_rows_in_target)
                    }
                }
                progress_bar.update(1)

        return validate.differences

    def print_formatted_output(differences):
        """Prints the differences in a clean, readable format with limits on rows displayed."""     
        
        GREEN = "\033[92m"
        RED = "\033[91m"
        BLUE = "\033[94m"
        YELLOW = "\033[93m"
        BOLD = "\033[1m"
        END = "\033[0m"
        flag = False

        print(f"\n{BOLD}========== CSV DIFFERENCES ANALYSIS =========={END}")
        
        print(f"\n{BOLD}SUMMARY:{END}")

        Addedcol = differences['summary']['added_columns_count']
        Deletedcol = differences['summary']['deleted_columns_count']
        Addedrows= differences['summary']['added_rows_count']
        Deletedrows= differences['summary']['deleted_rows_count']
        Updatedrows= differences['summary']['updated_rows_count']
        DuplicateIDsource = differences['summary']['duplicate_rows_in_source_count']
        DuplicateIDtarget = differences['summary']['duplicate_rows_in_target_count']
        
        print(f"  • Added columns: {GREEN}{Addedcol}{END}")
        print(f"  • Deleted columns: {RED}{Deletedcol}{END}")
        print(f"  • Added rows: {GREEN}{Addedrows}{END}")
        print(f"  • Deleted rows: {RED}{Deletedrows}{END}")
        print(f"  • Updated rows: {BLUE}{Updatedrows}{END}")
        print(f"  • Duplicate rows source: {YELLOW}{DuplicateIDsource}{END}")
        print(f"  • Duplicate rows target: {YELLOW}{DuplicateIDtarget}{END}")
        
        print(f"\n{BOLD}COLUMN CHANGES:{END}")

        # Display added and deleted columns side by side
        has_added = bool(differences['added_columns'])
        has_deleted = bool(differences['deleted_columns'])

        if has_added or has_deleted:
            # Print headers side by side
            # Calculate max lengths for proper header alignment
            max_added_header_len = len(f"  {GREEN}Added columns:{END}")

            # Calculate appropriate padding between headers based on column content
            max_added_len = max([len(str(col)) for col in differences['added_columns']] or [0]) + 10  # +10 for "    • " and some spacing
            header_padding = max(max_added_len - max_added_header_len + 20, 10)

            # Print headers with dynamic padding
            print(f"  {GREEN}Added columns:{END}" + " " * header_padding + f"{RED}Deleted columns:{END}")
            
            # Calculate max number of columns to display (from either added or deleted)
            max_lines = max(len(differences['added_columns'] or []), 
                            len(differences['deleted_columns'] or []))
            
            # Find the maximum length of any column name for proper padding

            # Display columns side by side with proper alignment
            for i in range(max_lines):
                if not has_added and i == 0:
                    added_col = f"    • {GREEN}None          {END}"
                else:
                    added_col = f"    • {GREEN}{differences['added_columns'][i]}{END}" if i < len(differences['added_columns']) else ""
                
                if not has_deleted and i == 0:
                    deleted_col = f"    • {RED}None          {END}"
                else:
                    deleted_col = f"    • {RED}{differences['deleted_columns'][i]}{END}" if i < len(differences['deleted_columns']) else ""
                
                # Padding based on maximum length of column names plus some extra space
                padding = max(max_added_len - len(added_col) + 20, 10) if added_col else max_added_len + 11
                print(added_col + " " * padding + deleted_col)
                
        else:
            print(f"  {GREEN}Added columns: None{END}" + " " * 15 + f"{RED}Deleted columns: None{END}")

        print(f"\n{BOLD}ROW CHANGES:{END}")


        # Modified to show only 5 rows for added rows
        if differences['added_rows']:
            total_rows = len(differences['added_rows'])
            print(f"  {GREEN}Added rows ({total_rows}):{END}")
            for i, row_id in enumerate(differences['added_rows'][:5]):
                print(f"    • {row_id}")
            if total_rows > 5:
                print(f"    {YELLOW}... and {total_rows - 5} more. Try using Excel as output to see full details of analysis.{END}")
                flag=True
        else:
            print(f"  {GREEN}Added rows: None{END}")


        # Modified to show only 5 rows for deleted rows
        if differences['deleted_rows']:
            total_rows = len(differences['deleted_rows'])
            print(f"  {RED}Deleted rows ({total_rows}):{END}")
            for i, row_id in enumerate(differences['deleted_rows'][:5]):
                print(f"    • {row_id}")
            if total_rows > 5:
                print(f"    {YELLOW}... and {total_rows - 5} more. Try using Excel as output to see full details of analysis.{END}")
                flag=True
        else:
            print(f"  {RED}Deleted rows: None{END}")
        
        # Modified to show only 5 rows for updated rows
        if differences['updated_rows_details']:
            total_rows = len(differences['updated_rows_details'])
            print(f"  {BLUE}Updated rows ({total_rows}):{END}")

            for i, update in enumerate(differences['updated_rows_details'][:5]):
                row_data = update["row_data"] 
                changes = update["changes"]

                print(f"    • {row_data}") 

                for col, vals in changes.items():
                    print(f"      - {col}: {RED}{vals['old']}{END} → {GREEN}{vals['new']}{END}")
                    
            if total_rows > 5:
                print(f"    {YELLOW}... and {total_rows - 5} more. Try using Excel as output to see full details of analysis.{END}")
                flag=True
        else:
            print(f"  {BLUE}Updated rows: None{END}")

        # Modified to show only 5 rows for duplicate rows
        for key, label in [('duplicate_rows_in_source', 'DUPLICATE ROWs in Source'), ('duplicate_rows_in_target', 'DUPLICATE ROWs in Target')]:
            duplicates = differences[key]
            total_duplicates = len(duplicates)
            print(f"\n{BOLD}{label}:{END} {YELLOW}\n    • Found {total_duplicates} duplicate IDs:{END}")
            
            if duplicates:
                for dup in duplicates[:4]:
                    print(f"      - {dup}")
                if total_duplicates > 4:
                    print(f"    {YELLOW}... and {total_duplicates - 4} more. Try using Excel as output to see full details of analysis.{END}")
                    flag=True
            else:
                print("      - None")

        
        if flag :
            print("\n\nWant to switch to .xlsx as output form \nenter: yes/exit")
            a=input()
            if a=="yes":
                return a

    def get_dataframe_results(original_file, new_file, differences):
        """
        Returns detailed differences as DataFrames for further analysis.
        
        Parameters:
        original_file (pandas.DataFrame): Original DataFrame
        new_file (pandas.DataFrame): New DataFrame for comparison
        differences (dict): Dictionary containing identified differences
        
        Returns:
        dict: Dictionary containing DataFrames with detailed differences
        """
        result_dfs = {}
        
        # DataFrame for added columns
        result_dfs['added_columns_df'] = pd.DataFrame({'column_name': differences['added_columns']}) if differences['added_columns'] else pd.DataFrame(columns=['column_name'])
        
        # DataFrame for deleted columns
        result_dfs['deleted_columns_df'] = pd.DataFrame({'column_name': differences['deleted_columns']}) if differences['deleted_columns'] else pd.DataFrame(columns=['column_name'])
        
        # DataFrame for added rows
        result_dfs['added_rows_df'] = pd.DataFrame(differences['added_rows']) if differences['added_rows'] else pd.DataFrame(columns=new_file.columns)
        
        # DataFrame for deleted rows
        result_dfs['deleted_rows_df'] = pd.DataFrame(differences['deleted_rows']) if differences['deleted_rows'] else pd.DataFrame(columns=original_file.columns)
        

        # DataFrame for updated rows
        if differences['updated_rows_details']:
            updated_rows_data = []
            for update in differences['updated_rows_details']:
                row_data = update['row_data'].copy()
                for column, values in update['changes'].items():
                    row_data[column] = f"{values['old']} → {values['new']}"
                updated_rows_data.append(row_data)
            result_dfs['updated_rows_df'] = pd.DataFrame(updated_rows_data)
        else:
            result_dfs['updated_rows_df'] = pd.DataFrame(columns=new_file.columns)
        
        # DataFrame for duplicate rows
        result_dfs['duplicate_rows_in_source_df'] = pd.DataFrame(differences['duplicate_rows_in_source']) if differences['duplicate_rows_in_source'] else pd.DataFrame(columns=original_file.columns)
        result_dfs['duplicate_rows_in_target_df'] = pd.DataFrame(differences['duplicate_rows_in_target']) if differences['duplicate_rows_in_target'] else pd.DataFrame(columns=new_file.columns)
        
        # Summary DataFrame
        summary_data = {
            "Details": [
                "Rows_only_in_target",
                "Rows_only_in_source",
                "Mismatch_rows_from_source_to_target",
                "Matched_rows_from_source_to_target",
                "Total_rows_in_source",
                "Total_rows_in_target"
            ],
            "Count": [
                differences['summary']['added_rows_count'],
                differences['summary']['deleted_rows_count'],
                differences['summary']['updated_rows_count'],
                (original_file.shape[0] - (differences['summary'].get('updated_rows_count', 0) + (differences['summary'].get('deleted_rows_count', 0) + differences['summary'].get('added_rows_count', 0) ))),
                original_file.shape[0],
                new_file.shape[0]
            ],
            "Percentage":[
                f"{(differences['summary']['added_rows_count']/original_file.shape[0])*100:.4f}%",
                f"{(differences['summary']['deleted_rows_count']/original_file.shape[0])*100:.4f}%",
                f"{(differences['summary']['updated_rows_count']/original_file.shape[0])*100:.4f}%",
                f"{((original_file.shape[0] - (differences['summary'].get('updated_rows_count', 0) + (differences['summary'].get('deleted_rows_count', 0) + differences['summary'].get('added_rows_count', 0) )))/original_file.shape[0])*100:.4f}%",
                f"{(original_file.shape[0]/original_file.shape[0])*100:.4f}%",
                f"{(new_file.shape[0]/original_file.shape[0])*100:.4f}%"
            ]
        }
        result_dfs['analysis_summary_df'] = pd.DataFrame(summary_data)        
        return result_dfs

    def print_df_output(differences, method=None):
        added_columns_df = differences['added_columns_df']
        deleted_columns_df = differences['deleted_columns_df']
        added_rows_df = differences['added_rows_df']
        deleted_rows_df = differences['deleted_rows_df']
        duplicate_rows_in_source_df = differences['duplicate_rows_in_source_df']
        duplicate_rows_in_target_df = differences['duplicate_rows_in_target_df']
        updated_rows_df = differences['updated_rows_df']
        analysis_summary_df = differences['analysis_summary_df']


        # Pre-process dataframes to optimize column width calculations
        all_dfs = {
            "columns_only_in_target": added_columns_df,
            "columns_only_in_source": deleted_columns_df,
            "rows_only_in_target": added_rows_df,
            "rows_only_in_source": deleted_rows_df,
            "duplicate_rows_in_source": duplicate_rows_in_source_df,
            "duplicate_rows_in_target": duplicate_rows_in_target_df,
            "missmatched_rows": updated_rows_df,
            "summary": analysis_summary_df
        }

        total =  6 + (len(updated_rows_df) + 1) + (len(analysis_summary_df) +1 ) +((len(all_dfs))*2)

        with tqdm(total=total, desc="Loading data into xl file", unit="iteration", leave=False) as progress_bar:
            
            BASE_DIR = Path(__file__).resolve().parent

            if method == None:
                output_file = BASE_DIR/"Validator_output"/"results_with_no_key.xlsx"
            elif method == "assume":
                output_file = BASE_DIR/"Validator_output"/"results_with_assumed_key.xlsx"
            elif method == "key":
                output_file = BASE_DIR/"Validator_output"/"results_with_key.xlsx"

            progress_bar.update(1)

        
            with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                workbook = writer.book
                
                pie_chart = workbook.add_chart({'type': 'pie'})
                progress_bar.update(1)

                # Create formats once
                bold_format = workbook.add_format({'bold': True})
                red_format = workbook.add_format({'font_color': 'red', 'bold': True})
                green_format = workbook.add_format({'font_color': 'green', 'bold': True})
                blue_format = workbook.add_format({'font_color': '#00aeff', 'bold': True})
                progress_bar.update(1)

                def save_dataframe(df, sheet_name):
                    """Helper function to write data or a 'No records' message."""
                    if df.empty:
                        empty_msg = pd.DataFrame({"Message": [f"There are no records in {sheet_name}"]})
                        empty_msg.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    worksheet = writer.sheets[sheet_name]
                    
                    # Apply pre-calculated column widths
                    if sheet_name in column_widths:
                        for col_num, width in column_widths[sheet_name].items():
                            worksheet.set_column(col_num, col_num, width)

                def format_updated_rows(worksheet, updated_rows_df, red_format, green_format):
                    """Formats old and new values in 'updated rows' with different colors."""
                    # Pre-process all cells needing special formatting
                    cells_to_format = []
                    N=max(1,(len(updated_rows_df))//100) 
                    i = 0 
                    for row in range(1, len(updated_rows_df) + 1):
                        for col, column_name in enumerate(updated_rows_df.columns):
                            cell_value = str(updated_rows_df.iloc[row - 1, col])
                            if " → " in cell_value:
                                parts = cell_value.split(" → ")
                                if len(parts) == 2:
                                    cells_to_format.append((row, col, parts[0], parts[1]))
                        i +=1
                        if i % N == 0:
                            progress_bar.update(N)
                        
                    
                    # Apply formatting in batch
                    
                    for row, col, old_val, new_val in cells_to_format:
                        worksheet.write_rich_string(
                            row, col, red_format, old_val, " → ", green_format, new_val
                        )

                def format_analysis_summary(worksheet, analysis_summary_df, bold_format, red_format, green_format, blue_format):
                    """Formats the summary sheet with colors based on count values."""
                    threshold = (5/100) * analysis_summary_df.iloc[4, 1]
                    
                    # Pre-process all cells needing formatting
                    format_cells = []
                    
                    # First section (rows 1-4)
                    for row in range(1, len(analysis_summary_df) - 1):
                        count_value = analysis_summary_df.iloc[row - 1, 1]
                        percent_value = analysis_summary_df.iloc[row - 1, 2]
                        name_value = analysis_summary_df.iloc[row - 1, 0]
                        
                        # Column name in bold
                        format_cells.append((row, 0, name_value, bold_format))
                        
                        # Count value with conditional formatting
                        if count_value > threshold:
                            format_cells.append((row, 1, count_value, red_format))
                            format_cells.append((row, 2, percent_value, red_format))
                        elif count_value == 0:
                            format_cells.append((row, 1, count_value, blue_format))
                            format_cells.append((row, 2, percent_value, blue_format))
                        else:
                            format_cells.append((row, 1, count_value, green_format))
                            format_cells.append((row, 2, percent_value, green_format))

                        progress_bar.update(1)                        
                

                    # Second section (rows 5+)
                    for row in range(5, len(analysis_summary_df) + 1):
                        count_value = analysis_summary_df.iloc[row - 1, 1]
                        percent_value = analysis_summary_df.iloc[row - 1, 2]
                        name_value = analysis_summary_df.iloc[row - 1, 0]
                        
                        format_cells.append((row, 0, name_value, bold_format))
                        format_cells.append((row, 1, count_value, blue_format))
                        format_cells.append((row, 2, percent_value, blue_format))

                        progress_bar.update(1)
                        
                    
                    # Apply all formatting in one batch
                    for row, col, value, fmt in format_cells:
                        worksheet.write(row, col, value, fmt)

                progress_bar.update(1)
                

                # Calculate column widths in advance
                column_widths = {}
                for sheet_name, df in all_dfs.items():
                    if not df.empty:
                        column_widths[sheet_name] = {}
                        for col_num, col_name in enumerate(df.columns):
                            # Pre-calculate max length for each column
                            #max_length = max(df[col_name].astype(str).map(len).max(), len(col_name)) + 2
                            max_length = max(df[col_name].apply(lambda x: len(str(x)) if pd.notna(x) else 0).max(),len(str(col_name))) + 2
                            column_widths[sheet_name][col_num] = max_length

                    progress_bar.update(1)

                # Write all dataframes to sheets first (batch operation)
                
                for sheet_name, df in all_dfs.items():
                    save_dataframe(df, sheet_name)

                progress_bar.update(1)

                # Apply formatting only if data is present

                if not analysis_summary_df.empty:
                    worksheet = writer.sheets["summary"]
                    format_analysis_summary(worksheet, analysis_summary_df, bold_format, red_format, green_format, blue_format)

                    pie_chart.add_series({
                        'name': 'Row Distribution',
                        'categories': [sheet_name, 1, 0, 4, 0],  # Categories in column A, rows 1-4 (after header)
                        'values': [sheet_name, 1, 1, 4, 1],      # Values in column B, rows 1-4 (after header)
                        'data_labels': {'percentage': True, 'value': True},
                        'points': [
                            {'fill': {'color': '#92D050'}},  # Rows only in target - green
                            {'fill': {'color': '#FFC000'}},  # Rows only in source - yellow
                            {'fill': {'color': 'red'}},      # Mismatched rows - red
                            {'fill': {'color': '#00aeff'}}   # Matched rows - blue
                        ]
                    })
                    
                    pie_chart.set_title({'name': 'ETL Row Comparison'})
                    pie_chart.set_style(10)
    
                    # Insert the chart into the worksheet
                    worksheet.insert_chart('I7', pie_chart, {'x_scale': 1.5, 'y_scale': 1.5})
                                     
      
                progress_bar.update(1)                

                if not updated_rows_df.empty:
                    worksheet = writer.sheets["missmatched_rows"]
                    format_updated_rows(worksheet, updated_rows_df, red_format, green_format)

                progress_bar.update(1)


        print(f"\rLoading data into xl file....", end="", flush=True)        
        print(f"\rResults saved successfully!", end="\n", flush=True)
        print(f"File Path : {output_file}")

    def value(differences):
    
            Addedcol = differences['summary']['added_columns_count']
            Deletedcol = differences['summary']['deleted_columns_count']
            Addedrows= differences['summary']['added_rows_count']
            Deletedrows= differences['summary']['deleted_rows_count']
            Updatedrows= differences['summary']['updated_rows_count']
            DuplicateIDsource = differences['summary']['duplicate_rows_in_source_count']
            DuplicateIDtarget = differences['summary']['duplicate_rows_in_target_count']
            return Addedcol+Addedrows+Deletedrows+Deletedcol+Updatedrows+DuplicateIDsource+DuplicateIDtarget

    def check_diff(original_file, new_file, primary_key=None):

        if validate.differences == {} :
            differences = validate.validate_csv_differences(original_file, new_file, primary_key)
        elif validate.differences != {}:
            differences=validate.differences
        return differences
