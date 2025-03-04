import json
import zlib
import redis
import time
import pymupdf4llm
from io import BytesIO
import fitz

# Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Queue key
PDF_QUEUE = "test:markdown:queue"

def process_queue_forever():
    """Process items from the PDF queue indefinitely"""
    print(f"Starting PDF processing service - waiting for items on queue {PDF_QUEUE}")
    
    while True:
        try:
            # BRPOP blocks until an item is available, then returns [key, value]
            # This ensures only one consumer gets each item
            result = redis_client.spop(PDF_QUEUE)
            
            if not result:
                # Timeout with no items
                print("Queue empty, waiting...")
                time.sleep(5)
                continue
            
            # Process the item
            process_queue_item(result)
            
        except Exception as e:
            print(f"Error processing queue item: {e}")
            time.sleep(5)  # Brief delay before retrying

def process_queue_item(item_data):
    """Process a single item from the queue"""
    try:
        # Parse the JSON string
        item = json.loads(item_data)
        
        # Extract metadata and binary
        metadata = item['metadata']
        compressed_binary_hex = item['binary']

        print(f"Processing page {metadata['page_number']} of PDF {metadata['pdf_id']} - {metadata['pdf_name']}")
        
        # Convert binary from hex back to bytes
        compressed_binary = bytes.fromhex(compressed_binary_hex)
        
        # Decompress
        page_binary = zlib.decompress(compressed_binary)
        
        print(f"Processing page {metadata['page_number']} of PDF {metadata['pdf_name']}")
        
        # Create PDF document from bytes
        doc = fitz.open(stream=BytesIO(page_binary), filetype="pdf")
        
        if doc.page_count == 0:
            raise Exception("Failed to load PDF page")
        
        # Convert to markdown
        start_time = time.time()
        markdown_text = pymupdf4llm.to_markdown(doc, show_progress=False)
        processing_time = time.time() - start_time
        
        # Close the document
        doc.close()
        
        with open(f"extracted_page_{metadata['page_number']}.txt", "w", encoding="utf-8") as f:
            f.write(markdown_text + "\n")
        
        print(f"Successfully processed page {metadata['page_number']} in {processing_time:.2f}s")
        
        # # Optional: track in a set of processed pages
        # redis_client.sadd(f"pdf:processed:{metadata['pdf_id']}", metadata['page_number'])
        
    except Exception as e:
        print(f"Error processing item: {e}")

if __name__ == "__main__":
    process_queue_forever()