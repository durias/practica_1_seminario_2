from datetime import datetime

from pyspark import SparkContext
import plotly.offline as py
import plotly.graph_objs as go

sc = SparkContext ("local","practica")
texto= sc.textFile("C:/Archivos/killings.csv")


###############  Primer reporte #############################################


rdd=texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[3]!=""))\
.filter(lambda linea:(linea[3]!="Victim's race"))\
.map(lambda linea:(linea[3],1))\
.reduceByKey(lambda x,y:x+y)\
.sortBy(lambda linea:linea[1],ascending=False).take(3)

for races in rdd:
    print("Raza: "+races[0]+" | Total: "+str(races[1]))

ejex=[]
ejey=[]

for elemento in rdd:
    ejex.append(elemento[0])
    ejey.append(elemento[1])

fig = go.Figure(go.Bar(
    x=ejex,
    y=ejey,
    text=ejey,
    textposition='auto',
))

fig.update_layout(
    title="Top 3 de la raza de las víctimas",
    xaxis_title="Raza",
    yaxis_title="Víctimas",
    font=dict(
        family="Courier New, monospace",
        size=18,
        color="#7f7f7f"
    )
)

py.plot(figure_or_data=fig,filename="reporte3_a.html")


##################### Segundo reporte ############################################

rdd=texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[5]!="Date of Incident (month/day/year)"))\
.filter(lambda linea:(linea[5]!=""))\
.map(lambda linea:(str(datetime.strptime(linea[5], '%d/%m/%Y').date().year),1))\
.reduceByKey(lambda x,y:x+y)\
.sortBy(lambda linea:linea[1],ascending=False).take(5)

for valor in rdd:
    print("Año: "+str(valor[0])+"| Total incidentes: "+str(valor[1]))

ejex=[]
ejey=[]

for elemento in rdd:
    ejex.append("["+elemento[0]+"]")
    ejey.append(elemento[1])

fig = go.Figure(go.Bar(
    x=ejex,
    y=ejey,
    text=ejey,
    textposition='auto',
))

fig.update_layout(
    title="Años con más incidentes",
    xaxis_title="Años",
    yaxis_title="Incidentes",

    font=dict(
        family="Courier New, monospace",
        size=18,
        color="#7f7f7f"
    )
)


py.plot(figure_or_data=fig,filename="reporte3_b.html")

