from resource import *
import psutil
from collections import defaultdict

# FILE_IN_PATH = 'K3241.K03200DV.D00422.L00001'
FILE_IN_PATH = '/Users/cadu/Downloads/datasets/dadosabertos_CNPjs/K3241.K03200DV.D00422.L00001'
FILE_OUT_PATH = 'output.txt'

LINE_HEAD_0 = (
    (0, 1), (1, 17), (17, 28), (28, 36), (36, 44), (44, 1199), (1199, 1200)
)
LINE_DETAL_1 = (
    (0, 1), (1, 2), (2, 3), (3, 17), (17, 18), (18, 168), (168, 223),
    (223, 225), (225, 233), (233, 235), (235, 290), (290, 293), (293, 363),
    (363, 367), (367, 375), (375, 382), (382, 402), (402, 462), (462, 468),
    (468, 624), (624, 674), (674, 682), (682, 684), (684, 688), (688, 738),
    (738, 750), (750, 762), (762, 774), (774, 889), (889, 891), (891, 905),
    (905, 907), (907, 908), (908, 916), (916, 924), (924, 925), (925, 948),
    (948, 956), (956, 1199), (1199, 1200)
)
LINE_SOCIO_2 = (
    (0, 1), (1, 2), (2, 3), (3, 17), (17, 18), (18, 168), (168, 182),
    (182, 184), (184, 189), (189, 197), (197, 200), (200, 270), (270, 281),
    (281, 341), (341, 343), (343, 1198), (1199, 1200)
)
LINE_CNAES_6 = (
    (0, 1), (1, 2), (2, 3), (3, 17), (17, 710), (710, 1199), (1199, 1200)
)

ROWTYPE_HEAD = '0'
ROWTYPE_DACAD = '1'
ROWTYPE_SOCIO = '2'
ROWTYPE_CNAES = '6'

SELECT_CASE_ROWTYPE = defaultdict(
    lambda: [], {
        (ROWTYPE_HEAD, LINE_HEAD_0),
        (ROWTYPE_DACAD, LINE_DETAL_1),
        (ROWTYPE_SOCIO, LINE_SOCIO_2),
        (ROWTYPE_CNAES, LINE_CNAES_6),
    }
)


def get_row_data_list(row):

    def _recorta(row, row_metrics):

        row_columns = []
        for i in row_metrics:
            row_columns.append((row[i[0]:i[1]]).strip())

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
(juPy3)  ✘ cadu@kduMcP  ~/projs/cnpj_cad_pub   master  time python3 parser_cnpj_raw_iter.py
Linha lidas: 4451225
Linha gravadas: 4451225
----------- ESTATS---------------------------
Python seconds: 70.65792727470398
Python CPU: resource.struct_rusage(ru_utime=59.423764, ru_stime=7.535882, ru_maxrss=1381871616, ru_ixrss=0, ru_idrss=0, ru_isrss=0, ru_minflt=602131, ru_majflt=0, ru_nswap=0, ru_inblock=0, ru_oublock=0, ru_msgsnd=0, ru_msgrcv=0, ru_nsignals=0, ru_nvcsw=66, ru_nivcsw=70267)
PSUTIL CPU: 45.8
PSUTIL VitMem: 56.7
----------- ESTATS END-----------------------
python3 parser_cnpj_raw_iter.py  59.43s user 7.54s system 94% cpu 1:10.77 total
'''

