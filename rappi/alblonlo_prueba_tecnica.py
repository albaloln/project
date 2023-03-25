# *****************************************************************************************************
#                 RAPPIPAY
# SCRIPT ID           : alblonlo_prueba_tecnica
# PROCESO             : TRANSFORMACION Y CARGA
# TABLA(S) DESTINO    :  
# AUTOR               : Alba Mey Londoño Londoño
# BITACORA DE CAMBIOS :
# VER.  FECHA       HECHO POR            COMENTARIOS
# --------------------------------------------------------------------------------------------------
# 1.0   2022/03/23  Alba Mery Londoño Londoño                      Version inicial
# --------------------------------------------------------------------------------------------------
# MODO DE EJECUCION :
#     Esta script es invocada por lambda manualmente
#*****************************************************************************************************
import json
from xml.dom import minidom
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Constante():
    """
    Clase que permite declarar las variables constantes del proceso 
    """
    BUCKET_NAME_RAW = "dllo-raw-prueba-tecnica"
    BUCKET_NAME_ANALYTICS = "dllo-analytics-prueba-tecnica"
    DIM_CLIENTE = "dim_cliente"
    DIM_PRODUCTO = "dim_producto"
    FACT_VENTA = "fact_venta"
    NAME_BD = "database_prueba_tecnica"
    NOMBRE_ARCHIVO_PT = "PruebaTecnica.xml"
    NOMBRE_VENTAS_PT = "Ventas.csv"
    PREFIX_ANALYTICS = "financiera"

class Xml():
    """
    Clase que permite declarar las funciones relacionadas con XML
    """
    def get_raiz_tag(self, archivo):
        """
        get_raiz_tag: función que obtener la información de la raíz 
        Parameters 
        ----------
            archivo string:
                Nombre del archivo  
        Returns
        -------
            raiz_tag string:
                Información del XML 
        """
        raiz_tag = ""
        if archivo == Constante.NOMBRE_ARCHIVO_PT:
            raiz_tag = "Informe"
        return raiz_tag
    
    def get_archivos(self):
        """
        get_archivos: función que obtener la información de los archivos a cargar
        Parameters 
        ----------
        Returns
        -------
            list:
                Nombre de los archivos de XML
        """
        return [Constante.NOMBRE_ARCHIVO_PT]
            
    def get_tag(self, archivo):
        """
        get_tag: función que obtener la información de los tag y sus elementos
        Parameters 
        ----------
            archivo string:
                Nombre del archivo  
        Returns
        -------
            procesar_tag dict:
                La llave es el tag y los elementos los valores 
        """
        procesar_tag = {}
        if archivo == Constante.NOMBRE_ARCHIVO_PT:
            procesar_tag = {
                    "NaturalNacional": ["nombres", "primerApellido", "segundoApellido"],
                    "Identificacion": ["ciudad", "departamento", "numero"],
                    "Edad": ["Edad"]
                }
        return procesar_tag
            
    def get_int_column(self, archivo):
        """
        get_int_column: función que obtener la información de los elementos que son enteros
        Parameters 
        ----------
            archivo string:
                Nombre del archivo  
        Returns
        -------
            int_columns list:
                Listas con los elementos que son enteros
        """
        int_columns=[]
        if archivo == Constante.NOMBRE_ARCHIVO_PT:
            int_columns = ["codSeguridad", "Edad"]
        return int_columns
    
    def get_str_int_column(self, archivo):
        """
        get_str_int_column: función que obtiene la información de los elementos que son enteros y string
        esto se hace ya que la información de csv tiene un 0 adelante y se convierte a int para que quite 
        este 0
        Parameters 
        ----------
            archivo string:
                Nombre del archivo  
        Returns
        -------
            str_int_columns list:
                Listas con los elementos que son enteros y string
        """
        str_int_columns=[]
        if archivo == Constante.NOMBRE_ARCHIVO_PT:
            str_int_columns = ["numero"]
        return str_int_columns
    
    def get_child_column(self, archivo):
        """
        get_child_column: función que obtener la información de los elementos son child en este caso 
        la edad que no es un elemento del tag sino la información 
        Parameters 
        ----------
            archivo string:
                Nombre del archivo  
        Returns
        -------
            child_columns list:
                Listas con los elementos que tiene child
        """
        child_columns=[]
        if archivo == Constante.NOMBRE_ARCHIVO_PT:
            child_columns = ["Edad"]
        return child_columns

    def load_xml(self, bucket_name, filename):
        """
        load_xml: función que permite cargar la información de un xml
        Parameters 
        ----------
            bucket_name string:
                Nombre del bucket
            filename string:
                Nombre del archivo junto con la llave     
        Returns
        -------
            xmldoc:
                Información del XML 
        """
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket_name, filename)
        file_data = obj.get()['Body'].read()
        xmldoc = minidom.parseString(file_data)
        return xmldoc
   
    def get_attribute(self, registro, tag, attributes, archivo):
        """
        get_attribute: función que permite obtener los atributos del XML 
        Parameters 
        ----------
            registro string:
                Registro que se está procesando
            tag string:
                Información que se va obtener 
            attributes list:
                Atributos relacionados con el tag
            archivo string:
                Nombre del archivo
        Returns
        -------
            list_valor list:
                Información del registro
        """
        registro_tag = registro.getElementsByTagName(tag)[0]
        list_valor = []
        for attribute in attributes:
            if attribute in self.get_child_column(archivo):
                valor = int(registro_tag.firstChild.nodeValue)
            else:
                valor = registro_tag.getAttribute(attribute)
            if attribute in self.get_int_column(archivo):
                valor = int(valor)
            
            if attribute in self.get_str_int_column(archivo):
                valor = str(int(valor))
            
            list_valor.append(valor)
            
        print("list_valor", list_valor)
        return list_valor
        
    def information_xml(self, archivo):
        """
        information_xml: función que permite obtener los atributos del XML 
        Parameters 
        ----------
            archivo string:
                Nombre del archivo
        Returns
        -------
            list_valor list:
                Información del registro
            columns list:
                Columnas de la información 
        """
        xmldoc = self.load_xml(constante.BUCKET_NAME_RAW, "clientes/{0}".format(archivo))
        
        registros = xmldoc.getElementsByTagName(self.get_raiz_tag(archivo))
        list_valor = []
        for registro in registros:
            valor_registro = []
            columns = ["codSeguridad"]
            valor = registro.getAttribute("codSeguridad")
            valor_registro.append(valor)
            for tag, attribute in self.get_tag(archivo).items():
                valor = self.get_attribute (registro, tag, attribute, archivo)    
                valor_registro.extend(valor)
                columns.extend(attribute)
                
            list_valor.append(valor_registro)

        print("list_valor", list_valor)
        print("columns", columns)
        return list_valor, columns

class Util():
    """
    Clase que permite declarar las funciones relacionadas con pyspark y lo que se puede cargar con este csv
    """
    def __init__(self):
        self.client = boto3.client('s3')
    
    def get_archivos(self):
        """
        get_archivos: función que obtener la información de los archivos a cargar
        Parameters 
        ----------
        Returns
        -------
            list:
                Nombre de los archivos que se van a cargar con pyspark
        """
        return [Constante.NOMBRE_VENTAS_PT]
    
    def load_informacion_pyspark(self, formato, path, opciones):
        """
        load_informacion_pyspark: función que obtener la información de los archivos a cargar
        Parameters 
        ----------
            formato string:
                Formato de archivo.
            path string:
                Ruta del archivo que se va cargar.
            opciones dict:
                Diferentes opciones para cargar la informaciòn 
        Returns
        -------
            df dataframe.pyspark:
                Nombre de los archivos que se van a cargar con pyspark
        """
        df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format=formato,
           format_options = opciones,
        ).toDF()
    
        return df
        
    def save_information_s3(self, bucket_name, prefijo, df_table, tablename, arg_mode):
        """
        save_information_s3: funcion que guardar la información en s3
        Parameters 
        ----------
            bucket_name string:
                Nombre del bucket donde se va a guardar la información
            prefijo string:
                Nombre del prefijo 
            df_table dataframe.pyspark:
                Dataframe de la información a guardar
            tablename string:
                Nombre de la base de datos
            arg_mode string:
                Cómo se va a guardar la información 
        Returns
        -------
        """
        ruta = "s3://{0}/{1}/{2}".format(bucket_name, prefijo, tablename)
        df_table.repartition(1).write.mode(arg_mode).option('header', 'true').parquet(ruta)  
        
    def execute_query(self, query):
        """
        execute_query: función que permite ejecutar los queries a la base de datos de sql 
        Parameters 
        ----------
            query string:
                Query que se va a ejecutar
        Returns
        -------
            pyspark.dataframe:
                El dataframe con la información de query 
        """
        print("query", query)
        dfps_information_query = spark.sql(query)
        return dfps_information_query
        
    def create_or_replace_temp_view (self, dfp_view, view):
        """
        create_or_replace_temp_view: Crea o reemplaza una vista temporal en spark sql
        Parameters 
        ----------
            dfp_view: pyspark.dataframe
                La dateframe pyspark que se va a convertir en una vista
            view: string
                Nombre como se va a llamar la vista
        Returns
        -------
        """
        dfp_view.createOrReplaceTempView(view)
    
    def create_pyspark_data_frame(self, list_valor, columns):
        """
        create_pyspark_data_frame: función que permite crear un dataframe de pyspark
        Parameters 
        ----------
            list_valor list:
                Información con los valores
            columns list:
                Columnas 
        Returns
        -------
        pyspark.dataframe
            El pyspark.dataframe de la información de las columnas y la estructura
        """
        return spark.createDataFrame(list_valor, columns)
    
    def listar_objetos (self, bucket_name, prefix_analytics):
        """
        listar_objetos: función que permite listar los archivos de un bucket
        Parameters 
        ----------
            bucket_name string:
                Información con los valores
            prefix_analytics string:
                El prefijo del bucket  
        Returns
        -------
            objetos list:
            Informaciòn de los archivos parquet en el bucket
        """
        objetos = []
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix_analytics
        }
        objs = self.client.list_objects_v2(**params)['Contents']
        objetos = []
        for obj in objs:
            key = obj['Key']
            if "parquet" in key:
                objetos.append(key)
        return objetos
                
    def borrar_objetos (self, bucket_name, prefix_analytics):
        """
        borrar_objetos: función que borrar la información del bucket 
        Parameters 
        ----------
            bucket_name string:
                Información con los valores
            prefix_analytics string:
                El prefijo del bucket  
        Returns
        -------
        """
        objetos = self.listar_objetos(bucket_name, prefix_analytics)
        for obj in objetos:
            self.client.delete_object(Bucket=bucket_name, Key=obj)
            
    
class Dim():
    """
    Clase que permite declarar las funciones relacionadas con Dim
    """
    def get_info_dim(self, table):
        """
        get_info_dim: función que permite obtener la información de la DIM
        Parameters 
        ----------
            table string:
                Nombre de la tabla
        Returns
            query_tabla_final string:
                Query de la información para guardar en la base de datos 
        -------
        """
        if table == constante.DIM_CLIENTE:
            query_tabla_final = """
                SELECT COALESCE (t.cliente_id, 
                    SUM (CASE
                                WHEN	t.cliente_id IS NULL  THEN 1
                                ELSE	0
                        END)
                    OVER( ORDER	BY d.codSeguridad ROWS UNBOUNDED PRECEDING) + (case when Maximo.MaxId = -1 then 0 else  Maximo.MaxId end)) AS cliente_id,
                    d.codSeguridad cod_seguridad, d.numero, d.nombres nombre, d.primerApellido primer_apellido, d.segundoApellido segundo_apellido, d.ciudad, d.departamento, d.edad, coalesce(t.fecha_ejecucion, now()) fecha_ejecucion
                            FROM (
                                SELECT COALESCE(max(cliente_id), 0) as MaxId FROM {0}.{1}
                            ) Maximo, 
                            cliente d
                            LEFT OUTER JOIN {0}.{1} t ON d.codSeguridad = t.cod_seguridad
                """.format(constante.NAME_BD, table)
        elif table == constante.DIM_PRODUCTO:
            
            query_tabla_final = """
                SELECT COALESCE (t.producto_id, 
                    SUM (CASE
                                WHEN	t.producto_id IS NULL  THEN 1
                                ELSE	0
                        END)
                    OVER( ORDER	BY d.ARTICULO ROWS UNBOUNDED PRECEDING) + Maximo.MaxId) AS producto_id,
                    d.ARTICULO desc_producto, coalesce(t.fecha_ejecucion, now()) fecha_ejecucion
                            FROM (
                                SELECT COALESCE(max(producto_id), 0) as MaxId FROM {0}.{1} 
                            ) Maximo, 
                            ventas d
                            LEFT OUTER JOIN {0}.{1} t ON d.ARTICULO = t.desc_producto
                """.format(constante.NAME_BD, table)
                
        return query_tabla_final
    
    def get_tables_dim(self):
        """
        get_tables_dim: función que permite obtener el nombre de las dimensiones
        Parameters 
        ----------
        Returns
            list:
                Nombre de las tablas de dimensiones
        -------
        """
        return [constante.DIM_CLIENTE, constante.DIM_PRODUCTO]
            
class Fact():
    """
    Clase que permite declarar las funciones relacionadas con Fact
    """
    def get_info_fact(self, table):
        """
        get_info_fact: función que permite obtener la información de la Fact
        Parameters 
        ----------
            table string:
                Nombre de la tabla
        Returns
            query_tabla_final string:
                Query de la información para guardar en la base de datos 
        -------
        """
        if table == constante.FACT_VENTA:
            query_tabla_final = """
                SELECT COALESCE(p.producto_id, -1) producto_id, COALESCE(c.cliente_id, -1) cliente_id, cast(d.cantidad as decimal(10, 2)) cantidad, 
                    cast(d.VALOR_UNITARIO as decimal(16, 6)) valor, now() fecha_ejecucion
                    FROM ventas d
                    LEFT OUTER JOIN {0} c ON COALESCE(d.ID_CLIENTE, "-3") = c.numero
                    LEFT OUTER JOIN {1} p ON COALESCE(d.ARTICULO, "-3") = p.desc_producto
                """.format(constante.DIM_CLIENTE, constante.DIM_PRODUCTO)
                
        return query_tabla_final
            
    def get_tables_fact(self):
        """
        get_tables_fact: función que permite obtener el nombre de las facts
        Parameters 
        ----------
        Returns
            list:
                Nombre de las tablas de facts
        -------
        """
        return [constante.FACT_VENTA]

class Main():
    """
    Clase que permite declarar las funciones relacionadas con el procesamiento 
    """
    xml  = Xml()
    util  = Util()
    dim  = Dim()
    fact  = Fact()
    
    def process_information(self):
        """
        process_information: función que permite orquestar la información
        Parameters 
        ----------
        Returns
        -------
        """
        #XML
        for archivo in self.xml.get_archivos():
            ls_list_valor, columns = self.xml.information_xml(archivo)
            df_xml = self.pyspark.create_pyspark_data_frame (ls_list_valor, columns)
            self.pyspark.create_or_replace_temp_view(df_xml, "cliente")     
        
        df_xml.show()
        #csv
        for archivo in self.pyspark.get_archivos():
            path = "s3://{0}/clientes/{1}".format(constante.BUCKET_NAME_RAW, archivo)
            formato = "csv"
            format_options={"withHeader": True}
            df_csv = self.pyspark.load_informacion_pyspark(formato, path, format_options)
            self.pyspark.create_or_replace_temp_view(df_csv, "ventas")
        
        self.pyspark.borrar_objetos(constante.BUCKET_NAME_ANALYTICS, constante.PREFIX_ANALYTICS)
        
        for dim in self.dim.get_tables_dim():
            query_tabla_final = self.dim.get_info_dim(dim)
            df_table = self.pyspark.execute_query (query_tabla_final)
            self.pyspark.create_or_replace_temp_view(df_table, dim)
            self.pyspark.save_information_s3(constante.BUCKET_NAME_ANALYTICS, "financiera", df_table, dim,  "append")
        
        for fact in self.fact.get_tables_fact():
            query_tabla_final = self.fact.get_info_fact(fact)
            df_table = self.pyspark.execute_query (query_tabla_final)
            self.pyspark.save_information_s3(constante.BUCKET_NAME_ANALYTICS, "financiera", df_table, fact,  "append")
            
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
constante = Constante()
main = Main()
main.process_information()

