import json
import openai
import time
from tqdm import tqdm
import pandas as pd
from typing import List, Tuple
import re

from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def get_context_windows(text, compiled_regexes, window_size):
    """
    Extract context windows around matches found by any of the compiled regexes in the text,
    ensuring that overlapping windows are merged into one.
    
    Parameters:
        text (str): The text to search through.
        compiled_regexes (List[re.Pattern]): A list of compiled regex objects used to find matches.
        window_size (int): The number of words around the match to include in the context window.
        
    Returns:
        List[str]: A list of context windows around the matches.
    """
    context_windows = []
    
    # Combine matches from all compiled regexes
    all_matches = []
    for compiled_regex in compiled_regexes:
        all_matches.extend(list(compiled_regex.finditer(text)))
    
    # Sort matches by their start position
    all_matches.sort(key=lambda match: match.start())

    # Merge overlapping or adjacent matches
    merged_matches = []
    i = 0
    while i < len(all_matches):
        current_match = all_matches[i]
        start_pos = current_match.start()
        end_pos = current_match.end()

        while i + 1 < len(all_matches) and all_matches[i + 1].start() - end_pos <= window_size * 2 * len(' '):  # Estimate space size
            i += 1
            end_pos = all_matches[i].end()

        merged_matches.append((start_pos, end_pos))
        i += 1

    # Extract context windows for merged matches
    for start_pos, end_pos in merged_matches:
        words = text.split()
        start_word_pos = len(text[:start_pos].split()) - 1
        end_word_pos = len(text[:end_pos].split())
        context_words = words[max(0, start_word_pos - window_size):min(len(words), end_word_pos + window_size)]
        context_str = ' '.join(context_words)
        context_windows.append(context_str)

    return context_windows


def extract_context_windows_df(df, text_column, compiled_regexes, window_size):
    """
    Extract context windows for each text in the specified column of a DataFrame,
    and return a new DataFrame with each context window as a row, along with the original metadata.
    
    Parameters:
        df (pd.DataFrame): The original DataFrame.
        text_column (str): The name of the column containing text to search through.
        compiled_regexes (List[re.Pattern]): A list of compiled regex objects used to find matches.
        window_size (int): The number of words around the match to include in the context window.
        
    Returns:
        pd.DataFrame: A new DataFrame where each row is a context window, with original metadata.
    """
    context_rows = []

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        text = row[text_column]
        context_windows = get_context_windows(text, compiled_regexes, window_size)
        
        # For each context window, create a new row with the same metadata
        for window in context_windows:
            new_row = row.to_dict()  # Convert the row to a dictionary
            new_row[text_column] = window  # Replace the text column with the context window
            context_rows.append(new_row)
    
    # Create a new DataFrame from the list of context rows
    context_df = pd.DataFrame(context_rows)

    return context_df


import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import openai


def process_with_gpt_with_retries(context_window, model, system_prompt, openai_api_key, max_retries=5):
    retry_delay = 0.5  # Reduced initial delay in seconds for retries
    max_retry_delay = 16  # Maximum delay, to avoid long waits
    for attempt in range(max_retries):
        try:
            openai.api_key = openai_api_key  # Set the API key here
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": context_window}
                ]
            )
            return response.choices[0].message.content
        except openai.error.RateLimitError:
            print(f"Rate limit reached, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff with max limit
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt == max_retries - 1:
                return f"Error: {str(e)}"
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff with max limit
    return "Error: Max retries exceeded."

def process_all_context_windows(new_df, model, system_prompt, openai_api_key, texts_before_pause=1000, pause_duration=5, max_workers= 6):
    responses = {}
    processed_texts = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:  # Adjust max_workers based on your environment
        future_to_idx = {executor.submit(process_with_gpt_with_retries, row['context_window'], model, system_prompt, openai_api_key): idx for idx, row in new_df.iterrows()}

        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                response = future.result()
            except Exception as exc:
                print(f'Context window at index {idx} generated an exception: {exc}')
                response = "Error: Exception in processing"
            
            responses[idx] = response
            processed_texts += 1

            # Indicator for how many texts have been processed
            if processed_texts % texts_before_pause == 0:
                print(f"Processed {processed_texts}/{len(new_df)} texts; pausing for {pause_duration} seconds...")
                time.sleep(pause_duration)
    
    # Update the DataFrame with the responses using .loc
    for idx, response in responses.items():
        if idx in new_df.index:
            new_df.loc[idx, 'gpt_response'] = response

    return new_df