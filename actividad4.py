#Dataset: Contractor, D. (2022). Marvel Comic Books Dataset [Data set].


import pandas as pd
import multiprocessing

#Se crea una función procesar_datos() que será ejecutada por cada proceso. Esta función recibe una parte de los datos y la lista compartida resultados_compartidos.
def procesar_datos(datos, resultados):
    
    # Carga los datos en un DataFrame
    df = pd.DataFrame(datos, columns=['comic_name', 'active_years', 'issue_title', 'publish_date',
                                      'issue_description', 'penciler', 'writer', 'cover_artist',
                                      'Imprint', 'Format', 'Rating', 'Price'])

    # Realiza el cálculo estadístico
    conteo_formatos = df['Format'].value_counts()

    # Agrega los resultados a la lista compartida
    resultados.append(conteo_formatos)

if __name__ == '__main__':
  

    #Se carga el archivo CSV y se divide en partes para procesar en paralelo. La cantidad de partes se determina según el número de núcleos de la CPU disponible

    dataframe = pd.read_csv('Marvel_Comics.csv')

    # Divide los datos en partes para procesar en paralelo
    num_procesos = multiprocessing.cpu_count()  # Obtiene el número de núcleos de la CPU
    tamaño_parte = len(dataframe) // num_procesos
    partes = [dataframe[i:i+tamaño_parte] for i in range(0, len(dataframe), tamaño_parte)]

    #Se crea una lista compartida resultados_compartidos utilizando multiprocessing.Manager(). Esta lista será utilizada para almacenar los resultados de cada proceso.
    manager = multiprocessing.Manager()
    resultados_compartidos = manager.list()

    #Se crean los procesos paralelos utilizando multiprocessing.Process(). Cada proceso ejecutará la función procesar_datos() con una parte específica de los datos y la lista compartida resultados_compartidos.
    procesos = []
    for parte in partes:
        proceso = multiprocessing.Process(target=procesar_datos, args=(parte, resultados_compartidos))
        procesos.append(proceso)

    # Se inician los procesos utilizando proceso.start().
    for proceso in procesos:
        proceso.start()

    #Se espera a que los procesos terminen utilizando proceso.join().
    for proceso in procesos:
        proceso.join()

    #Finalmente, se concatenan y agrupan los resultados obtenidos de la lista compartida resultados_compartidos.
    conteo_total = pd.concat(resultados_compartidos).groupby(level=0).sum()

    # Imprime los resultados
    print("Analisis Estadistico")
    print(conteo_total)
