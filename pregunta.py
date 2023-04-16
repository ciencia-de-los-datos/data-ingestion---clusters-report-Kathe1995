"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
from warnings import filterwarnings
filterwarnings("ignore")


def ingest_data():
    df=pd.read_fwf('clusters_report.txt',
    widths=[7, 16, 16, 80], index_col=False).drop([0,1],axis=0).reset_index(drop=True)
    df.columns = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']
    lista_index_notnull=list(df[df['cluster'].isnull()==False].index)
    #uniendo los parrafos
    for i in lista_index_notnull[:-1]:
        stringlist= df['principales_palabras_clave'][i]
        for j in range(i+1,lista_index_notnull[lista_index_notnull.index(i)+1]):
            stringlist+=' ' + df['principales_palabras_clave'][j]
        df['principales_palabras_clave'][i]=stringlist
    w=lista_index_notnull[-1]
    stringlistf= df['principales_palabras_clave'][w]
    for k in range(w+1,len(df.index)):
        stringlistf+=' ' + df['principales_palabras_clave'][k]
    df['principales_palabras_clave'][w]=stringlistf
    #eliminando las lineas que ya agregamos en un solo parrafo
    df.drop(list(df[df['cluster'].isnull()==True].index),axis=0,inplace=True)
    df.reset_index(drop=True,inplace=True)
    #convirtiendo a entero las columnas cluster y cantidad de palabras clave
    for k in ['cluster', 'cantidad_de_palabras_clave']:
        df[k]=df[k].astype(int)
    #convirtiendo a float la columna palabras clave y quitandoles el punto al final a las que lo tienen
    df['porcentaje_de_palabras_clave']=df['porcentaje_de_palabras_clave'].apply(lambda x: float(x[:4].replace(',','.')))
    df['principales_palabras_clave']=df['principales_palabras_clave'].apply(lambda x: x[:-1] if '.' in x else x)
    # eliminando espacios en los parrafos, primero separando cada frase, luego cada palabra, ordenando
    # y volviendolas a unir
    for m in range(len(df['principales_palabras_clave'])):
        lista_phrases=[]
        lista_words=[]
        for i in df['principales_palabras_clave'][m].split(', '):
            lista_phrases.append(i.strip())
        for j  in lista_phrases:
            lista_words.append(j.split(' '))
        lista_words_total=[]
        for k in lista_words:
            DIC_words={}
            words=[]
            if '' in k:
                for w in sorted(list(set(k)))[1:]:
                    DIC_words[k.index(w)]=w
                for s in sorted(DIC_words.keys()):
                     words.append(DIC_words[s])
            else: 
                for n in k:
                     words.append(n)
            lista_words_total.append(' '.join(words))
        df['principales_palabras_clave'][m]=', '.join(lista_words_total)
    return df