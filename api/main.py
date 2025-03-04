import os
os.environ["TOKENIZERS_PARALLELISM"] = "true"
import time, uuid, json
from fastapi import FastAPI, File, UploadFile, Form
from tika import parser  # Apache Tika
from PyPDF2 import PdfReader  # PyPDF2
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import pymupdf4llm
from unstructured.partition.auto import partition
from unstructured.chunking.basic import chunk_elements
import fitz
from multiprocessing import Pool
import multiprocessing
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import markdownify
from io import BytesIO
from redis_conn import RedisConnection
import zlib

redis = RedisConnection()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specific frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads/"
os.makedirs(UPLOAD_DIR, exist_ok=True)

CHUNK_STORAGE: Dict[str, set] = {}  # To track received chunks

@app.post("/upload")
async def upload_chunk(
    file: UploadFile = File(...),
    chunk_index: int = Form(...),
    file_id: str = Form(...)
):
    chunk_path = os.path.join(UPLOAD_DIR, f"{file_id}_part{chunk_index}")

    try:
        # Save the received chunk
        with open(chunk_path, "wb") as f:
            f.write(await file.read())

        # with open(chunk_path, "rb") as fobj:
        #     extract_text_pypdf(fobj)
        #     extract_text_pymupdf4llm(fobj)
        #     extract_text_tika(fobj)

        # Track chunks
        if file_id not in CHUNK_STORAGE:
            CHUNK_STORAGE[file_id] = set()
        CHUNK_STORAGE[file_id].add(chunk_index)

        print(f"Chunk {chunk_index} uploaded")

        return {"message": f"Chunk {chunk_index} uploaded"}
    
    except Exception as e:
        print(f"Error uploading chunk {chunk_index}: {e}")
        return {"error": f"Failed to upload chunk {chunk_index}"}

@app.post("/upload_complete")
async def complete_upload(data: dict):
    file_id = data["file_id"]
    final_file_path = os.path.join(UPLOAD_DIR, file_id)

    # Ensure all chunks are received
    expected_chunks = sorted(CHUNK_STORAGE.get(file_id, []))
    if not expected_chunks:
        return {"error": "No chunks received"}

    start_time = time.time()
    # Merge chunks in order
    with open(final_file_path, "wb") as final_file:
        for i in expected_chunks:
            chunk_path = os.path.join(UPLOAD_DIR, f"{file_id}_part{i}")
            
            with open(chunk_path, "rb") as chunk_file:
                final_file.write(chunk_file.read())

            os.remove(chunk_path)  # Cleanup

    del CHUNK_STORAGE[file_id]  # Remove tracking
    end_time = time.time()

    total_time = end_time - start_time
    print(f"combination time: {total_time:.4f} seconds")

    # ---------------------PREPROCESSING--------------------
    extract_text_tika(final_file_path)

    start_time_par_extraction = time.time()
    extract_per_page_and_send_to_redis(final_file_path)

    end_time_par_extraction = time.time()
    total_time_par_extraction = end_time_par_extraction - start_time_par_extraction

    print(f"\npar extraction time: {total_time_par_extraction:.4f} seconds")
    

    # ------------------------------------------------

    # start_time = time.time()  # Start timing the partition process
    # elements = partition(filename=filename, content_type="application/pdf")
    # end_time = time.time()  # End timing the partition process
    # time_lapse = end_time - start_time  # Calculate the time lapse duration
    # print(f"Time taken for partitioning: {time_lapse:.2f} seconds")  # Print the time lapse
    
    # #print the extracted elements
    # with open("extracted_elements.txt", "a", encoding="utf-8") as f:
    #     for index, e in enumerate(elements):
    #         f.write(f"Index: {index}\nCategory: {e.category}\n{e}\n")

    # # extracted_text_pypdf, time_pypdf = extract_text_pypdf(chunk_path, chunk_index)
    # # extracted_text_tika, time_tika = extract_text_tika(chunk_path, chunk_index)

    # start_time = time.time()  # Start timing the chunking process
    # chunks = chunk_elements(elements=elements)
    # end_time = time.time()  # End timing the chunking process
    # time_lapse = end_time - start_time  # Calculate the time lapse duration
    # print(f"Time taken for chunking: {time_lapse:.2f} seconds")  # Print the time lapse

    # with open("extracted_chunks_output.txt", "a", encoding="utf-8") as f:
    #     for chunk in chunks:
    #         f.write(str(chunk) + "\n---------------\n")

    print("File successfully reconstructed")
    return {"message": "File successfully reconstructed"}


def extract_page_text(args):
    pdf_bytes, page_num, filename = args
    extract_text_pypdf(pdf_bytes, page_num, is_new=True)
    # extract_text_pymupdf4llm(filename, page_num, is_new=True)
    extract_text_tika(filename)

def extract_per_page_and_send_to_redis(file_path):
    """Process PDF file, extract text with Tika, and send pages to Redis"""
    # Open the PDF
    doc = fitz.open(file_path)
    total_pages = doc.page_count
    pdf_id = str(uuid.uuid4())
    pdf_filename = os.path.basename(file_path)

    print(f"Processing PDF with {total_pages} pages")

    # Process each page and send to Redis
    for page_num in range(total_pages):
        # Extract page as bytes
        page_bytes = page_to_bytes(doc, page_num)

        if page_bytes:
            # Save to verify content
            with open("extracted_page.pdf", "wb") as f:
                f.write(page_bytes)
            print(f"Extracted page saved, size: {len(page_bytes)} bytes")
        else:
            print("Failed to extract page")


        # Compress the page bytes using zlib
        compressed_page_bytes = zlib.compress(page_bytes, level=6)

        # Generate a unique ID for this page
        page_id = f"{pdf_id}_{page_num}"

        # Create metadata
        metadata = {
            'pdf_id': pdf_id,
            'pdf_name': pdf_filename,
            'page_id': page_id,
            'page_number': page_num,
            'total_pages': total_pages,
            'timestamp': time.time()
        }

        payload = {
            "metadata": metadata,
            "binary": compressed_page_bytes.hex()
        }
        
        
        with redis.redis_connection_pipeline() as pipe:
            pipe.sadd("test:markdown:queue", json.dumps(payload))
            pipe.execute()
            
            print(f"Page {page_num+1}/{total_pages} sent to Redis")
    
    doc.close()
    print("PDF processing complete")

def page_to_bytes(doc, page_num):
    """Extract a single page as bytes from a PDF document"""
    # Create a new PDF with just this page
    new_doc = fitz.open()
    
    new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
    
    # Save to bytes buffer
    buffer = BytesIO()
    new_doc.save(buffer)
    new_doc.close()
    
    # Reset buffer position to start
    buffer.seek(0)
    
    # Get bytes data
    pdf_bytes = buffer.getvalue()
    
    # Verify we have actual content
    if len(pdf_bytes) == 0:
        print("Error: Generated PDF has zero bytes")
        return None
        
    return pdf_bytes

def extract_text_pypdf(file_path, page_num, is_new=False):
    """Extract text using PyPDF2 for a specific page number"""
    start_time = time.time()
    
    doc = fitz.open("pdf", file_path)
    page = doc.load_page(page_num)
    print(type(page))
    exit(0)
    text = page.get_text()
    doc.close()
    
    end_time = time.time()
    
    total_time = end_time - start_time
    print(f"extraction_time pypdf: {total_time:.4f} seconds")
    
    # Write extracted text to file
    with open(f"extracted_text_pypdf{is_new}.txt", "a", encoding="utf-8") as f:
        f.write(f"{text} \n------------{page_num}-----------\n" if text else "No readable text\n")

    mtext = pymupdf4llm.to_markdown(page, show_progress=False)

    with open(f"extracted_text_mtext.txt", "a", encoding="utf-8") as f:
        f.write(mtext + "\n------------" + str(page_num) + "-----------\n" if mtext else "No markdown text\n")

def extract_text_pypdf2(file_path, page_num, is_new=False):
    """Extract text using PyPDF2 for a specific page number"""
    start_time = time.time()
    
    doc = fitz.open("pdf", file_path)
    page = doc.load_page(page_num)
    text = page.get_text()
    doc.close()
    
    end_time = time.time()
    
    total_time = end_time - start_time
    print(f"extraction_time pypdf: {total_time:.4f} seconds")
    
    # Write extracted text to file
    with open(f"extracted_text_pypdf{is_new}.txt", "a", encoding="utf-8") as f:
        f.write(f"{text} \n------------{page_num}-----------\n" if text else "No readable text\n")
    
    return text, end_time - start_time

def extract_text_tika(file_path):
    """Extract text using Apache Tika"""
    start_time = time.time()
    
    headers = {
        "X-Tika-PDFOcrStrategy": "no_ocr"
    }
    parsed = parser.from_file(filename=file_path, xmlContent=True, requestOptions={'headers': headers, 'timeout': 300})
    text = parsed['content']

    xhtml_data = BeautifulSoup(text, 'html.parser')

    print("Parsing extracted text...")
    pdf_pages = xhtml_data.find_all('div', attrs={'class': 'page'})
    
    for page in pdf_pages:
        page_text = page.get_text()

        with open(f"extracted_text_tika.txt", "a", encoding="utf-8") as f:
            f.write(f"{page_text} \n------------{pdf_pages.index(page)}-----------\n" if page_text else "No readable text\n")
    
    end_time = time.time()

    total_time = end_time - start_time
    print(f"extraction_time tika: {total_time:.4f} seconds")
    
    
    return text, end_time - start_time


def extract_text_pymupdf4llm(file_path, page_num, is_new=False):
    """Extract text using pymupdf4llm for a specific page number"""
    start_time = time.time()

    # Extract text for the specified page number
    text = pymupdf4llm.to_markdown(file_path, pages=[page_num],show_progress=False)
    
    end_time = time.time()

    total_time = end_time - start_time
    print(f"extraction_time pymupdf: {total_time:.4f} seconds")
    
    # Write extracted text to file
    with open(f"extracted_text_pymupdf{is_new}.txt", "a", encoding="utf-8") as f:
        f.write(f"{text} \n------------{page_num}-----------\n" if text else "No readable text\n")
    
    return text, end_time - start_time
