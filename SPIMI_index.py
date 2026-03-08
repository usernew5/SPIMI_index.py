#-------------------------------------------------------------
# AUTHOR: Parth Gajjar
# FILENAME: SPIMI_index.py
# SPECIFICATION: simplified SPIMI-based inverted index construction
# FOR: CS 5180- Assignment #2
# TIME SPENT: 2d
#-----------------------------------------------------------*/

# importing required libraries
import pandas as pd
import heapq
from sklearn.feature_extraction.text import CountVectorizer
import os

# -----------------------------
# PARAMETERS
# -----------------------------
INPUT_PATH = "corpus/corpus.tsv"
BLOCK_SIZE = 100
NUM_BLOCKS = 10
READ_BUFFER_LINES_PER_FILE = 100
WRITE_BUFFER_LINES = 500

# ---------------------------------------------------------
# 1) READ FIRST BLOCK OF 100 DOCUMENTS USING PANDAS
# ---------------------------------------------------------
# Use pandas.read_csv with chunksize=100.
# Each chunk corresponds to one memory block.
# Convert docIDs like "D0001" to integers.
# ---------------------------------------------------------
def docid_to_int(docid_str):
    # strip non-digits and convert to int
    # handles formats like "D0001"
    digits = ''.join(ch for ch in str(docid_str) if ch.isdigit())
    return int(digits) if digits != '' else 0

# ---------------------------------------------------------
# 2) BUILD PARTIAL INDEX (SPIMI STYLE) FOR CURRENT BLOCK
# ---------------------------------------------------------
# - Use CountVectorizer(stop_words='english')
# - Fit and transform the 100 documents
# - Reconstruct binary postings lists from the sparse matrix
# - Store postings in a dictionary: term -> set(docIDs)
# ---------------------------------------------------------*/
def build_partial_index(doc_ids, texts):
    vectorizer = CountVectorizer(stop_words='english', binary=True)
    X = vectorizer.fit_transform(texts)  # shape (n_docs, n_terms)
    terms = vectorizer.get_feature_names_out()
    partial_index = {}
    # iterate over terms (columns)
    # For each term, find nonzero rows (documents)
    X_csc = X.tocsc()
    for term_idx, term in enumerate(terms):
        col = X_csc.getcol(term_idx)
        doc_indices = col.indices  # zero-based indices into texts/doc_ids
        if doc_indices.size > 0:
            postings = {doc_ids[i] for i in doc_indices}
            partial_index[term] = postings
    return partial_index

# ---------------------------------------------------------
# 3) FLUSH PARTIAL INDEX TO DISK
# ---------------------------------------------------------
# - Sort terms lexicographically
# - Sort postings lists (ascending docID)
# - Write to: block_1.txt, block_2.txt, ..., block_10.txt
# - Format: term:docID1,docID2,docID3
# ---------------------------------------------------------
def flush_block_to_disk(partial_index, block_number, output_dir="."):
    filename = os.path.join(output_dir, f"block_{block_number}.txt")
    with open(filename, 'w', encoding='utf-8') as f:
        for term in sorted(partial_index.keys()):
            postings = sorted(partial_index[term])
            postings_str = ",".join(str(d) for d in postings)
            line = f"{term}:{postings_str}\n"
            f.write(line)

# ---------------------------------------------------------
# 4) REPEAT STEPS 1–3 FOR ALL 10 BLOCKS
# ---------------------------------------------------------
# - Continue reading next 100-doc chunks
# - After processing each block, flush to disk
# - Do NOT keep previous blocks in memory
# ---------------------------------------------------------
def create_blocks(input_path, block_size=BLOCK_SIZE, num_blocks=NUM_BLOCKS, output_dir="."):
# Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    reader = pd.read_csv(input_path, sep='\t', header=None, names=['docid', 'text'],
                         encoding='utf-8', chunksize=block_size, dtype=str)
    block_num = 0
    for chunk in reader:
        block_num += 1
        # convert docids to ints
        chunk['docid_int'] = chunk['docid'].apply(docid_to_int)
        # sort by docid_int to ensure ascending docID order within block
        chunk = chunk.sort_values('docid_int')
        doc_ids = chunk['docid_int'].tolist()
        texts = chunk['text'].fillna('').tolist()
        partial_index = build_partial_index(doc_ids, texts)
        flush_block_to_disk(partial_index, block_num, output_dir=output_dir)
        # free memory by deleting partial_index (will be garbage collected)
        del partial_index
        if block_num >= num_blocks:
            break

# ---------------------------------------------------------
# 5) FINAL MERGE PHASE
# ---------------------------------------------------------
# After all block files are created:
# - Open block_1.txt ... block_10.txt simultaneously
# ---------------------------------------------------------

# ---------------------------------------------------------
# 6) INITIALIZE READ BUFFERS
# ---------------------------------------------------------
# For each block file:
# - Read up to READ_BUFFER_LINES_PER_FILE lines
# - Parse each line into (term, postings_list)
# - Store in a per-file read buffer
# ---------------------------------------------------------
def parse_block_line(line):
    line = line.rstrip('\n')
    if ':' not in line:
        return None, []
    term, postings_str = line.split(':', 1)
    postings = []
    if postings_str:
        postings = [int(x) for x in postings_str.split(',') if x != '']
    return term, postings
def read_lines_from_file(f, max_lines):
    lines = []
    for _ in range(max_lines):
        line = f.readline()
        if not line:
            break
        term, postings = parse_block_line(line)
        if term is not None:
            lines.append((term, postings))
    return lines

# ---------------------------------------------------------
# 7) INITIALIZE MIN-HEAP (OR SORTED STRUCTURE)
# ---------------------------------------------------------
# - Push the first term from each read buffer into a min-heap
# - Heap elements: (term, file_index)
# ---------------------------------------------------------

# ---------------------------------------------------------
# 8) MERGE LOOP
# ---------------------------------------------------------
# While min-heap is not empty:
#   1. Pop the min-heap root (smallest term)
#   2. Keep popping the min-heap root while the current term equals the previous term
#   3. Collect all read buffers whose current term matches
#   4. Merge postings lists associated with this term (sorted + deduplicated)
#   5. Advance corresponding read buffer pointers
#   6. If a read buffer is exhausted, read next 100 lines from the corresponding block (if available)
#   7. For each read buffer whose pointer advanced, push its new pointed term into the heap (if available).
# ---------------------------------------------------------
def multiway_merge(block_count=NUM_BLOCKS,
                   read_buffer_lines=READ_BUFFER_LINES_PER_FILE,
                   write_buffer_limit=WRITE_BUFFER_LINES,
                   blocks_dir=".",
                   final_index_path="final_index.txt"):
# Open all block files
    files = []
    for i in range(1, block_count + 1):
        filename = os.path.join(blocks_dir, f"block_{i}.txt")
        f = open(filename, 'r', encoding='utf-8')
        files.append(f)
# Initialize per-file read buffers and pointers
    read_buffers = []  # list of lists of (term, postings)
    pointers = []      # current index into each read buffer
    for f in files:
        buf = read_lines_from_file(f, read_buffer_lines)
        read_buffers.append(buf)
        pointers.append(0)
    # Initialize heap with first term from each buffer (if available)
    heap = []
    for idx, buf in enumerate(read_buffers):
        if pointers[idx] < len(buf):
            term = buf[pointers[idx]][0]
            heapq.heappush(heap, (term, idx))
    # Prepare final index file for writing
    final_f = open(final_index_path, 'w', encoding='utf-8')
    write_buffer = []  # list of lines to write to final index
    # Merge loop
    while heap:
        term, file_idx = heapq.heappop(heap)
        current_term = term
        merged_postings_set = set()
        # Process the popped entry and any other entries with same term
        # We'll process the popped file first
        def process_file_for_term(fi):
            nonlocal merged_postings_set
            ptr = pointers[fi]
            if ptr < len(read_buffers[fi]):
                t, postings = read_buffers[fi][ptr]
                if t == current_term:
                    merged_postings_set.update(postings)
                    pointers[fi] += 1
                    # If buffer exhausted, attempt to refill
                    if pointers[fi] >= len(read_buffers[fi]):
                        # refill
                        more = read_lines_from_file(files[fi], read_buffer_lines)
                        read_buffers[fi] = more
                        pointers[fi] = 0
                    # After advancing (and possibly refilling), if there's a new current term, push it
                    if pointers[fi] < len(read_buffers[fi]):
                        new_term = read_buffers[fi][pointers[fi]][0]
                        heapq.heappush(heap, (new_term, fi))
        # process the first popped file
        process_file_for_term(file_idx)
        # Now process any other heap entries that have the same term
        while heap and heap[0][0] == current_term:
            _, fi = heapq.heappop(heap)
            process_file_for_term(fi)
        # After collecting all postings for current_term, sort and deduplicate
        merged_postings = sorted(merged_postings_set)
        postings_str = ",".join(str(d) for d in merged_postings)
        line = f"{current_term}:{postings_str}\n"
        write_buffer.append(line)
        # Flush write buffer if needed
        if len(write_buffer) >= write_buffer_limit:
            final_f.writelines(write_buffer)
            write_buffer = []
    # After loop ends, flush remaining write buffer
    if write_buffer:
        final_f.writelines(write_buffer)
        write_buffer = []
    # Cleanup: close all files
    final_f.close()
    for f in files:
        f.close()

# ---------------------------------------------------------
# 9) WRITE BUFFER MANAGEMENT
# ---------------------------------------------------------
# - Append merged term-line to write buffer
# - If write buffer reaches WRITE_BUFFER_LINES:
#       flush (append) to final_index.txt
# - After merge loop ends:
#       flush remaining write buffer
# ---------------------------------------------------------
# (Handled inside multiway_merge)

# ---------------------------------------------------------
# 10) CLEANUP
# ---------------------------------------------------------
# - Close all open block files
# - Ensure final_index.txt is properly written
# ---------------------------------------------------------
# (Handled inside multiway_merge)

if name == "main":
    # Create blocks directory (use current directory)
    blocks_output_dir = "."
    # Step 1-4: create block files from corpus
    create_blocks(INPUT_PATH, block_size=BLOCK_SIZE, num_blocks=NUM_BLOCKS, output_dir=blocks_output_dir)
    # Step 5-9: merge block files into final_index.txt
    final_index_file = "final_index.txt"
    multiway_merge(block_count=NUM_BLOCKS,
                   read_buffer_lines=READ_BUFFER_LINES_PER_FILE,
write_buffer_limit=WRITE_BUFFER_LINES,
                   blocks_dir=blocks_output_dir,
                   final_index_path=final_index_file)
    print("SPIMI indexing complete. Final index written to", final_index_file)
