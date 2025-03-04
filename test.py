# import requests

# API_URL = "http://localhost:8000/upload"
# CHUNK_SIZE = 5 * 1024 * 1024  # 5MB chunks
# FILE_PATH = "files/sparks.pdf"

# def upload_file(file_path):
#     file_id = "some_unique_id"  # This should be unique per file upload
#     with open(file_path, "rb") as f:
#         chunk_index = 0
#         while chunk := f.read(CHUNK_SIZE):
#             files = {"file": (f"{file_id}_part{chunk_index}", chunk)}
#             data = {"chunk_index": chunk_index, "total_chunks": -1, "file_id": file_id}
#             response = requests.post(API_URL, files=files, data=data)
#             print(response.json())
#             chunk_index += 1

#     # Notify the server that all chunks are uploaded
#     requests.post("http://localhost:8000/upload_complete", json={"file_id": file_id})

# upload_file(FILE_PATH)


import markdownify

text = """
<div class="page"><p></p>
<p>World War I
drishtiias.com/printpdf/world-war-i-1
</p>
<p>World War I (WW I), also known as the Great War, lasted from 28 July 1914 to 11
November 1918.
WW I was fought between the Allied Powers and the Central Powers.
</p>
<p>The main members of the Allied Powers were France, Russia, and Britain.
The United States also fought on the side of the Allies after 1917.
The main members of the Central Powers were Germany, Austria-Hungary,
the Ottoman Empire, and Bulgaria.
</p>
<p>Causes of the War
</p>
<p>There was no single event that led to World War I. The war happened because of several
different events that took place in the years building up to 1914.
</p>
<p>The new international expansionist policy of Germany: In 1890 the new
emperor of Germany, Wilhelm II, began an international policy that sought to turn
his country into a world power. Germany was seen as a threat by the other powers
and destabilized the international situation.
Mutual Defense Alliances: Countries throughout Europe made mutual defence
agreements. These treaties meant that if one country was attacked, allied countries
were bound to defend them.
</p>
<p>The Triple Alliance-1882 linking Germany with Austria-Hungary and Italy.
The Triple Entente, which was made up of Britain, France, and Russia,
concluded by 1907.
Thus, there were two rival groups in Europe.
</p>
<p>1/6</p>
<p></p>
<div class="annotation"><a href="https://www.drishtiias.com/printpdf/world-war-i-1">https://www.drishtiias.com/printpdf/world-war-i-1</a></div>
</div>
"""

markdown_text = markdownify.markdownify(text, heading_style="ATX")
with open("output_markdown.txt", "a", encoding="utf-8") as f:
    f.write(markdown_text)