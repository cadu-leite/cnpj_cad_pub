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


def get_row_data_list(row):
    ROWTYPE_HEAD = '0'
    ROWTYPE_DACAD = '1'
    ROWTYPE_SOCIO = '2'
    ROWTYPE_CNAES = '6'

    l = []
    def _recorta(row, row_metrics):
        acc = row_metrics[0]
        l = []
        index = 0
        for i in row_metrics[1]:

            l.append((row[acc:acc + row_metrics[1][index]]).strip())
            acc = acc + row_metrics[1][index]

            #print(f'--> {row[acc:(acc + row_metrics[1][index])]}')
            index += 1

        return (";".join(l)) + '\n'

    if row[0] == ROWTYPE_HEAD:
        return _recorta(row, LINE_HEAD_0_METRICS)
    if row[0] == ROWTYPE_DACAD:
        return _recorta(row, LINE_DETAL_1_METRICS)
    if row[0] == ROWTYPE_SOCIO:
        return _recorta(row, LINE_SOCIO_2_METRICS)
    if row[0] == ROWTYPE_CNAES:
        return _recorta(row, LINE_CNAES_6_METRICS)


def main():

    linhas_lidas = 0
    linhas_gravadas = 0
    l=list()
    with open(FILE_IN_PATH, 'rt',encoding="latin-1") as f, open(FILE_OUT_PATH, 'w') as fo:
        for row in f:
            linhas_lidas += 1
            try:
                fo.write(get_row_data_list(row))
                linhas_gravadas += 1
            except:
                print (f'FAIL at |{row}|')

    print(f'Linha lidas: {linhas_lidas}')
    print(f'Linha gravadas: {linhas_gravadas}')

#     with open(FILE_OUT_PATH, 'w') as f, :
#         print(l)
#         f.writelines(l)

if __name__ == "__main__":
    import time
    start_time = time.time()
    main()
    print(f'----------- ESTATS-----------------------')
    print(f'Python seconds: {time.time()-start_time}')
    print(f'Python CPU: {getrusage(RUSAGE_SELF)}')
    print (f'PSUTIL CPU: {psutil.cpu_percent()}')
    print (f'PSUTIL VitMem: {psutil.virtual_memory()[2]}')
    print(f'----------- ESTATS END-----------------------')