from resource import *
import psutil
from collections import defaultdict

# FILE_IN_PATH = 'K3241.K03200DV.D00422.L00001'
FILE_IN_PATH = '/Users/cadu/Downloads/datasets/dadosabertos_CNPjs/K3241.K03200DV.D00422.L00001'
FILE_OUT_PATH = 'output.txt'

# head
# posicao de inicio da linha e TAMANHO de registros
# ini  = acc(todas pos anteriores) + pos_initial
# position 1 is '0' for python strings, qtd fica como é.
LINE_HEAD_0_METRICS = ((0), (1, 16, 11, 8, 8, 1155, 1))  # tuple pos_initial, tamanho
# dados cadastrais
LINE_DETAL_1_METRICS = ((0), (
    1, 1, 1, 14, 1, 150, 55, 2, 8, 2, 55, 3, 70, 4, 8, 7, 20, 60, 6, 156,
    50, 8, 2, 4, 50, 12, 4, 8, 12, 4, 8, 12, 4, 8, 115, 2, 14, 2, 1, 8, 8,
    1, 23, 8, 243, 1)
)
# dados socios
LINE_SOCIO_2_METRICS = ((0), (1, 1, 1, 14, 1, 150, 14, 2, 5, 8, 3, 70, 11, 60, 2, 855, 1))
# dados socios
LINE_CNAES_6_METRICS = ((0), (1, 1, 1, 14, 693, 489, 1))

ROWTYPE_HEAD = '0'
ROWTYPE_DACAD = '1'
ROWTYPE_SOCIO = '2'
ROWTYPE_CNAES = '6'

SELECT_CASE_ROWTYPE = defaultdict(
    lambda: [], {
        (ROWTYPE_HEAD, LINE_HEAD_0_METRICS),
        (ROWTYPE_DACAD, LINE_DETAL_1_METRICS),
        (ROWTYPE_SOCIO, LINE_SOCIO_2_METRICS),
        (ROWTYPE_CNAES, LINE_CNAES_6_METRICS),
    }
)


def get_row_data_list(row):

    def _recorta(row, row_metrics):
        acc = row_metrics[0]
        row_columns = []
        index = 0
        for i in row_metrics[1]:
            row_columns.append((row[acc:acc + row_metrics[1][index]]).strip())
            acc = acc + row_metrics[1][index]
            index += 1

        return (";".join(row_columns)) + '\n'

    return _recorta(row, SELECT_CASE_ROWTYPE[row[0]])

def main():

    linhas_lidas = 0
    linhas_gravadas = 0
    rows = list()
    with open(FILE_IN_PATH, 'rt', encoding="latin-1") as f:
        for row in f:
            linhas_lidas += 1
            rows.append(get_row_data_list(row))

    # bulk save output file

    with open(FILE_OUT_PATH, 'w+') as fo:
        fo.writelines(rows)
    linhas_gravadas = len(rows)
    print(f'Linha lidas: {linhas_lidas}')
    print(f'Linha gravadas: {linhas_gravadas}')


if __name__ == "__main__":
    import time
    start_time = time.time()
    main()
    print(f'----------- ESTATS---------------------------')
    print(f'Python seconds: {time.time()-start_time}')
    print(f'Python CPU: {getrusage(RUSAGE_SELF)}')
    print(f'PSUTIL CPU: {psutil.cpu_percent()}')
    print(f'PSUTIL VitMem: {psutil.virtual_memory()[2]}')
    print(f'----------- ESTATS END-----------------------')


'''
(juPy3)  cadu@kduMcP  ~/projs/cnpj_cad_pub   master  time python3 parser_cnpj_raw_files.py
Linha lidas: 4451225
Linha gravadas: 0
----------- ESTATS---------------------------
Python seconds: 102.01295185089111
Python CPU: resource.struct_rusage(ru_utime=88.570654, ru_stime=6.273981, ru_maxrss=1397960704, ru_ixrss=0, ru_idrss=0, ru_isrss=0, ru_minflt=451742, ru_majflt=87, ru_nswap=0, ru_inblock=0, ru_oublock=0, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0, ru_nvcsw=158, ru_nivcsw=100374)
PSUTIL CPU: 46.2
PSUTIL VitMem: 58.1
----------- ESTATS END-----------------------

python3 parser_cnpj_raw_files.py  88.58s user 6.28s system 92% cpu 1:42.16 total

(juPy3)  cadu@kduMcP  ~/projs/cnpj_cad_pub   master  time python3 parser_cnpj_raw_files.py
Linha lidas: 4451225
Linha gravadas: 4451225
----------- ESTATS---------------------------
Python seconds: 113.33112096786499
Python CPU: resource.struct_rusage(ru_utime=90.818354, ru_stime=6.470985, ru_maxrss=1398063104, ru_ixrss=0, ru_idrss=0, ru_isrss=0, ru_minflt=442122, ru_majflt=87, ru_nswap=0, ru_inblock=0, ru_oublock=0, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0, ru_nvcsw=140, ru_nivcsw=105640)
PSUTIL CPU: 51.6
PSUTIL VitMem: 56.9
----------- ESTATS END-----------------------

python3 parser_cnpj_raw_files.py  90.83s user 6.47s system 85% cpu 1:53.46 total

(juPy3)  cadu@kduMcP  ~/projs/cnpj_cad_pub   master  time python3 parser_cnpj_raw_files.py
Linha lidas: 4451225
Linha gravadas: 4451225
----------- ESTATS---------------------------
Python seconds: 129.0470609664917
Python CPU: resource.struct_rusage(ru_utime=98.683839, ru_stime=7.93907, ru_maxrss=1397706752, ru_ixrss=0, ru_idrss=0, ru_isrss=0, ru_minflt=529189, ru_majflt=87, ru_nswap=0, ru_inblock=0, ru_oublock=0, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0, ru_nvcsw=218, ru_nivcsw=163853)
PSUTIL CPU: 57.8
PSUTIL VitMem: 58.3
----------- ESTATS END-----------------------
python3 parser_cnpj_raw_files.py  98.69s user 7.94s system 82% cpu 2:09.19 total

(juPy3)  ✘ cadu@kduMcP  ~/projs/cnpj_cad_pub   master  time python3 parser_cnpj_raw_files.py
Linha lidas: 4451225
Linha gravadas: 4451225
----------- ESTATS---------------------------
Python seconds: 104.40437197685242
Python CPU: resource.struct_rusage(ru_utime=89.547603, ru_stime=8.325446, ru_maxrss=1389268992, ru_ixrss=0, ru_idrss=0, ru_isrss=0, ru_minflt=684927, ru_majflt=95, ru_nswap=0, ru_inblock=0, ru_oublock=0, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0, ru_nvcsw=297, ru_nivcsw=85287)
PSUTIL CPU: 45.3
PSUTIL VitMem: 56.0
----------- ESTATS END-----------------------
python3 parser_cnpj_raw_files.py  89.55s user 8.33s system 93% cpu 1:44.56 total
'''

