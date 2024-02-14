import json
import os
import re
import pandas as pd
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from functools import partial

def create_keyword_pattern(keywords):
    """
    Create a regex pattern for keyword matching.
    """
    pattern = r'(?:(?<=\W)|(?<=^))(' + '|'.join(map(re.escape, keywords)) + r')(?=\W|$)'
    return re.compile(pattern, re.IGNORECASE)

def remove_latex_commands(s):
    """
    Remove LaTeX commands from a string.
    
    Parameters:
        s (str): The input string.
        
    Returns:
        str: The string with LaTeX commands removed.
    """
    s = re.sub(r'\\[nrt]|[\n\r\t]', ' ', s)
    s = re.sub(r'\\[a-zA-Z]+', '', s)
    s = re.sub(r'\\.', '', s)
    s = re.sub(r'\\begin\{.*?\}.*?\\end\{.*?\}', '', s, flags=re.DOTALL)
    s = re.sub(r'\$.*?\$', '', s)
    s = re.sub(r'\\[.*?\\]', '', s)
    s = re.sub(r'\\\(.*?\\\)', '', s)
    s = re.sub(r'\\\[.*?\\\]', '', s)
    s = re.sub(r'(?<=\W)\\|\\(?=\W)', '', s)
    return s.strip()

def process_file(file_path, auroc_pattern, auprc_pattern, metadata_keys, remove_latex):
    """
    Process a single JSONL file to search for texts mentioning either AUROC or AUPRC, or both.
    """
    output_data = []
    total_texts = 0

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            total_texts += 1
            try:
                entry = json.loads(line)
                text = entry['text']
                if remove_latex:
                    text = remove_latex_commands(text)
                meta_data = entry.get('meta', {})

                contains_auroc = auroc_pattern.search(text) is not None
                contains_auprc = auprc_pattern.search(text) is not None

                if contains_auroc or contains_auprc:
                    row_data = {key: meta_data.get(key, None) for key in metadata_keys}
                    row_data['text'] = text
                    row_data['contains_auroc'] = contains_auroc
                    row_data['contains_auprc'] = contains_auprc

                    output_data.append(row_data)

            except json.JSONDecodeError as e:
                print(f"Error loading line in {file_path}: {line}. Error: {e}")

    return output_data, total_texts

def jsonl_folder_filtering(input_folder_path, auroc_search_terms, auprc_search_terms, metadata_keys=[], output_folder_path=None, remove_latex=True, save_file=True, filename="filtered_data.json", total_texts_filename="total_texts.txt"):
    """
    Filter and process all JSONL files in a folder for AUROC and AUPRC related texts.
    """
    auroc_pattern = create_keyword_pattern(auroc_search_terms)
    auprc_pattern = create_keyword_pattern(auprc_search_terms)

    file_paths = [os.path.join(input_folder_path, file_name) for file_name in os.listdir(input_folder_path) if file_name.endswith(".jsonl")]

    process_partial = partial(process_file, auroc_pattern=auroc_pattern, auprc_pattern=auprc_pattern, metadata_keys=metadata_keys, remove_latex=remove_latex)
    with Pool(cpu_count()) as p:
        results = p.map(process_partial, file_paths)

    output_data = [item for sublist, _ in results for item in sublist]
    total_texts = sum(total for _, total in results)

    df_output = pd.DataFrame(output_data)

    # Assigning a unique text_id for each unique text
    df_output['text_id'] = pd.factorize(df_output['text'])[0]
    
    # Specifying the column order
    keyword_columns = ['contains_auroc', 'contains_auprc']
    column_order = ['text', 'text_id'] + metadata_keys + keyword_columns
    df_output = df_output[column_order]

    # Save data
    if save_file and output_folder_path is not None:
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)
        with open(os.path.join(output_folder_path, total_texts_filename), 'w') as f:
            f.write(str(total_texts))

        df_output.to_csv(os.path.join(output_folder_path, filename), index=False)
    elif save_file:
        print("Warning: Output folder path is not provided. The DataFrame is not saved to a file.")

    return df_output