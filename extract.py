import time
from tika import parser  # Apache Tika
from PyPDF2 import PdfReader  # PyPDF2
import pymupdf4llm

# PDF_FILE = "files/sparks.pdf"  # Replace with your PDF file
PDF_FILE = "files/moral_living.pdf"

def extract_text_pypdf(file_path):
    """Extract text using PyPDF2"""
    start_time = time.time()
    
    reader = PdfReader(file_path)
    text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
    
    end_time = time.time()
    
    # Write extracted text to file
    with open("extracted_text_pypdf.txt", "a", encoding="utf-8") as f:
        f.write(text if text else "No readable text\n")
    
    return text, end_time - start_time

def extract_text_tika(file_path):
    """Extract text using Apache Tika"""
    start_time = time.time()
    
    parsed = parser.from_file(file_path)
    text = parsed.get("content", "").strip()
    
    end_time = time.time()
    
    # Write extracted text to file
    with open("extracted_text_tika.txt", "a", encoding="utf-8") as f:
        f.write(text if text else "No readable text\n")
    
    return text, end_time - start_time

def extract_text_pymupdf4llm(file_path):
    """Extract text using pymupdf4llm"""
    start_time = time.time()

    text = pymupdf4llm.to_markdown(file_path, page_chunks=False)
    
    end_time = time.time()
    
    # Write extracted text to file
    with open("extracted_text_pymupdf4llm.txt", "a", encoding="utf-8") as f:
        f.write(text if text else "No readable text\n")
    
    return text, end_time - start_time

# Run Extraction and Measure Time
text_pypdf, time_pypdf = extract_text_pypdf(PDF_FILE)
text_tika, time_tika = extract_text_tika(PDF_FILE)
text_pymupdf4llm, time_pymupdf4llm = extract_text_pymupdf4llm(PDF_FILE)

# Print Results
print(f"PyPDF2 Extraction Time: {time_pypdf:.4f} seconds")
print(f"Tika Extraction Time: {time_tika:.4f} seconds")
print(f"pymupdf4llm Extraction Time: {time_pymupdf4llm:.4f} seconds")
