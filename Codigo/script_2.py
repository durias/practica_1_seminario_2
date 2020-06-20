from pyspark import SparkContext
import plotly.offline as py
import plotly.graph_objs as go

sc = SparkContext ("local","practica")
texto= sc.textFile("C:/Archivos/ventas_video_juegos.csv")

rdd=texto.map(lambda linea:linea.split("|"))\
.filter(lambda linea:(linea[4]!="Genre"))\
.filter(lambda linea:(linea[4]!=""))\
.filter(lambda linea:(linea[4]=="Action" or linea[4]=="Sports" or linea[4]=="Fighting" or linea[4]=="Shooter" or linea[4]=="Racing" or linea[4]=="Adventure" or linea[4]=="Strategy"))\
.map(lambda linea:(linea[4],float(linea[10])))\
.reduceByKey(lambda x,y:x+y)\
.sortBy(lambda linea:linea[1],ascending=False)


for categorias in rdd.collect():
    print("Categoria: "+categorias[0]+"| Ventas globales: "+str(round(categorias[1],2)))

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
    title="Ventas globales",
    xaxis_title="Categorias",
    yaxis_title="Ventas",
    font=dict(
        family="Courier New, monospace",
        size=18,
        color="#7f7f7f"
    )
)
py.plot(figure_or_data=fig,filename="reporte2_a.html")
