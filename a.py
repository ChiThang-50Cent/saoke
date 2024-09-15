import multiprocessing as mp
import pdfplumber
import datetime
import re

PDF_SETTING = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "text",
    "snap_y_tolerance": 4.5,
    "intersection_x_tolerance": 50,
}

def _get_current_table_groups(table):
    current_group = []
    total_group = []
    for line in table[3:]:
        if any(line) and re.match(r'\d{2}/\d{2}/\d{4}',line[0]):
            if current_group:
                total_group.append(current_group)
            current_group = [line]
        else:
            current_group.append(line)
    if current_group:
        total_group.append(current_group)
    
    return total_group

def _get_data_from_group(group):
    date_code = ' '.join([row[0] for row in group]).strip()
    date_code_split = date_code.split(' ')
    date = date_code_split[0] if len(date_code_split) >= 2 else date_code
    code = date_code_split[1] if len(date_code_split) >= 2 else date_code
    amount = ' '.join([row[2] for row in group]).strip()
    content = ' '.join([row[4] for row in group]).strip()

    return date, code, amount, content

def _write_data_in_csv(path, date, code, amount, content):
    with open(path, 'a+') as csv:
        csv.write(f'{date},{code},{amount},{content}\n')

def _logging(path, data):
    with open(path, 'a+') as log:
        log.write(f'{data}\n')

def _process_page(pdf, page_num):
    try:
        page = pdf.pages[page_num]
        table = page.extract_table(PDF_SETTING)
        total_group = _get_current_table_groups(table)
        for group in total_group:
            date, code, amount, content = _get_data_from_group(group)
            _write_data_in_csv(csv_path, date, code, amount, content)
        print('Success get and write at: ', page_num)

    except Exception as e:
        _logging(log_path, f'Error at page number {page_num}, err: {str(e)}, group: {group}')
    finally:
        page.flush_cache()
        del page

def _process_chunk(start, end, pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in range(start, end):
            if page_num >= len(pdf.pages):
                break
            _process_page(pdf, page_num)


if __name__ == '__main__':
    num_processes = mp.cpu_count() - 1
    chunk_size = 3000

    csv_path = './saoke_1.csv'
    log_path = './log.txt'

    pdf_path = './danh-sach-ung-ho-1726197796019953464538.pdf'

    start_time = datetime.datetime.now()

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)

    chunks = [(i, min(i + chunk_size, total_pages), pdf_path) for i in range(0, total_pages, chunk_size)]

    with mp.Pool(processes=num_processes) as pool:
        pool.starmap(_process_chunk, chunks)

    print(f"All pages processed. Time execute: {datetime.datetime.now() - start_time}")
