from datetime import datetime
from pyspark import SparkContext
import plotly.offline as py
import plotly.graph_objs as go

sc = SparkContext ("local","practica")
texto= sc.textFile("C:/Archivos/ventas.csv")

rdd=texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[0]!="Region"))\
.filter(lambda linea:(linea[0]!=""))\
.map(lambda linea:(linea[0],float(linea[11])))\
.reduceByKey(lambda x,y:x+y)\
.sortBy(lambda linea:linea[1],ascending=False)


for valor in rdd.collect():
    print("Region: "+valor[0]+"| Total: "+str(round(valor[1],2)))

etiquetas=[]
valores=[]

for elemento in rdd.collect():
    etiquetas.append(elemento[0])
    valores.append(elemento[1])

fig = go.Figure(go.Pie(
    labels=etiquetas,
    values=valores,
    text=etiquetas,
    textposition='auto',
))

fig.update_layout(
    title="Total de ingresos por región",
    font=dict(
        family="Courier New, monospace",
        size=18,
        color="#7f7f7f"
    )
)

py.plot(figure_or_data=fig,filename="reporte2_a.html")

##################### Segundo reporte ############################################

rdd=texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[1]=="Guatemala"))\
.map(lambda linea:(str(datetime.strptime(linea[5], '%d/%m/%Y').date().year),int(linea[8])))\
.reduceByKey(lambda x,y:x+y)\
.sortBy(lambda linea:linea[1],ascending=False)


for valor in rdd.collect():
    print("Año: "+str(valor[0])+"| Unidades vendidas: "+str(valor[1]))
print(rdd.collect())

ejex=[]
ejey=[]

for elemento in rdd.collect():
    ejex.append("["+elemento[0]+"]")
    ejey.append(elemento[1])

fig = go.Figure(go.Bar(
    x=ejex,
    y=ejey,
    text=ejey,
    textposition='auto',
))

fig.update_layout(
    title="Año con más ventas en Guatemala",
    xaxis_title="Años",
    yaxis_title="Ventas",

    font=dict(
        family="Courier New, monospace",
        size=18,
        color="#7f7f7f"
    )
)


py.plot(figure_or_data=fig,filename="reporte2_b.html")


##################### Tercer reporte ############################################

ejex=[]
ejey=[]

rdd=texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[3]=="Online"))\
.filter(lambda linea:(linea[5]!="Order Date"))\
.filter(lambda linea:(str(datetime.strptime(linea[5], '%d/%m/%Y').date().year)=="2010"))\
.map(lambda linea:("Ingresos",float(linea[11])))\
.reduceByKey(lambda x,y:x+y)+\
\
texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[3]=="Online"))\
.filter(lambda linea:(linea[5]!="Order Date"))\
.filter(lambda linea:(str(datetime.strptime(linea[5], '%d/%m/%Y').date().year)=="2010"))\
.map(lambda linea:("Costos",float(linea[12])))\
.reduceByKey(lambda x,y:x+y)+\
\
texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[3]=="Online"))\
.filter(lambda linea:(linea[5]!="Order Date"))\
.filter(lambda linea:(str(datetime.strptime(linea[5], '%d/%m/%Y').date().year)=="2010"))\
.map(lambda linea:("Ganancia",float(linea[13])))\
.reduceByKey(lambda x,y:x+y)


for valor in rdd.collect():
    print(str(valor[0])+": "+str(round(valor[1],2)))

ejex=[]
ejey=[]

for elemento in rdd.collect():
    ejex.append(elemento[0])
    ejey.append(round(elemento[1],2))

fig = go.Figure(go.Bar(
    x=ejex,
    y=ejey,
    text=ejey,
    textposition='auto',
))

fig.update_layout(
    title="Analisis de ingresos,costos,ganancias",

    font=dict(
        family="Courier New, monospace",
        size=18,
        color="#7f7f7f"
    )
)

py.plot(figure_or_data=fig,filename="reporte2_c.html")
