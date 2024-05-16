import pandas as pd 

#Importación de archivos a analizar
baseDatosRaw = pd.read_excel(r"C:\Users\HP\Documents\Manuel_Angulo\Python\reportMovBascEx (1).xls",sheet_name="MovimientoBascSALIDA",header=2)
baseDatosInside = pd.read_csv(r"C:\Users\HP\Documents\Manuel_Angulo\Python\RegistrosRien (1).xls",sep="\t")

#Tratamiento de datos en archivo de báscula
noInterest = ["Numero Tiquete",'Documento','Contenedor','Eje','Producto','Observaciones','Conductor','Cedula',"Compañia","Peso Neto"]
filtDB = baseDatosRaw.drop(noInterest,axis=1)
SinNaN = filtDB.dropna(axis=1)
NuevaDB = SinNaN.replace({'BASCULA MAMONAL ZOFRANCA S.A. ENTRADA': 'B1374','BASCULA MAMONAL ZOFRANCA S.A. SALIDA':'B1373'})
NuevaDB['Hora'] = pd.to_datetime(NuevaDB['Hora']).dt.time
NuevaDB['Hora.1'] = pd.to_datetime(NuevaDB['Hora.1']).dt.time

#Tratamiento de datos en archivo de INSIDE
noImportant = ["INGRESOID", "FECHAING","BASCULAENTRADA","BASCULASALIDA","ENTURNAMIENTO","CAUSALID","TIPO_OPERACION","TIPO_OPERACION_USO","SALIDA_TERMINAL","PESAJE_ENTRADA",
               "PESAJE_SALIDA","ENTRADA_TERMINAL","OBSERVACION","NOMBREEMPRESA"]
filtered = baseDatosInside.drop(noImportant,axis=1)
filtered = filtered.dropna(how='all',axis="columns")
filtered = filtered.dropna(axis=0)
filtered['NUMMANIFIESTOCARGA'] = filtered['NUMMANIFIESTOCARGA'].astype('int64')
filtered['EMPRESA_TRANSPORTADORA_NIT'] = filtered['EMPRESA_TRANSPORTADORA_NIT'].astype('int64')
forClose = filtered[filtered['ESTADO']!='US']
forClose = forClose.rename(columns={'VEHICULO_NUMPLACA':'vehiculoNumPlaca'})
NuevaDB = NuevaDB.rename(columns={'Placa':'vehiculoNumPlaca'})

#Comparacion de las dos bases de datos - compara los que entraron y que tienen cita abierta 
entraron_porcerrar = pd.merge(forClose,NuevaDB, on="vehiculoNumPlaca",how='inner')
entraron_porcerrar = entraron_porcerrar.drop("ESTADO",axis=1)

#Comparación de las dos base de datos - compara los que tienen cita y no han ingresado A n B
citas_pendientes = pd.merge(forClose,NuevaDB, on="vehiculoNumPlaca",how='left')
#Se obtienen las citas asignadas que no tienen ingreso en báscula A-B
citas_pendientes = citas_pendientes[citas_pendientes['Hora'].isnull()]

#Transformación de tabla para cerrar
entraron_porcerrar = entraron_porcerrar.rename(columns={'EMPRESA_TRANSPORTADORA_NIT':'empresaTransportadoraNit',
                                                        'CONDUCTOR_NUMIDTERCERO':'conductorCedulaCiudadania',
                                                        'Peso Kg.1':'pesajeSalida',
                                                        'Peso Kg':'pesajeEntrada',
                                                        'NUMMANIFIESTOCARGA':'numManifiestoCarga',
                                                        'FECHA_OFERTA_SOLICITUD':'fechaOfertaSolicitud',
                                                        'Bascula Ingreso':'basculaEntrada',
                                                        'Bascula Repeso':'basculaSalida'})

#Da formato requerido para INSIDE a la fecha de oferta
entraron_porcerrar['fechaOfertaSolicitud'] = pd.to_datetime(entraron_porcerrar['fechaOfertaSolicitud'])
entraron_porcerrar['Hora'] = pd.to_datetime(entraron_porcerrar['Hora'],format='%H:%M:%S').dt.time
entraron_porcerrar['Hora.1'] = pd.to_datetime(entraron_porcerrar['Hora.1'],format='%H:%M:%S').dt.time
entraron_porcerrar['fechaOfertaSolicitud'] = entraron_porcerrar['fechaOfertaSolicitud'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

#Combina las columnas de fecha con la columna de hora
entraron_porcerrar['Fecha.1'] = entraron_porcerrar.apply(lambda row: pd.Timestamp.combine(row['Fecha.1'], row['Hora.1']), axis=1)
entraron_porcerrar = entraron_porcerrar.drop('Hora.1',axis=1)

entraron_porcerrar['Fecha'] = entraron_porcerrar.apply(lambda row: pd.Timestamp.combine(row['Fecha'], row['Hora']), axis=1)
entraron_porcerrar = entraron_porcerrar.drop('Hora',axis=1)

#Cambio nombres de la columna por requeridos en INSIDE y asigna formato YYYY-MM-DDTHH:MM:SSZ
entraron_porcerrar = entraron_porcerrar.rename(columns={'Fecha':'entradaTerminal', 'Fecha.1':'salidaTerminal'})
entraron_porcerrar['entradaTerminal'] = entraron_porcerrar['entradaTerminal'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
entraron_porcerrar['salidaTerminal'] = entraron_porcerrar['salidaTerminal'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

#Le da el orden correcto a la tabla
entraron_porcerrar["fecha"] = entraron_porcerrar["fechaOfertaSolicitud"]
orden = entraron_porcerrar.pop("fecha")
entraron_porcerrar.insert(1,"fecha",orden)

terminalPortuariaNit = 8060126542
entraron_porcerrar.insert(0,"terminalPortuariaNit",terminalPortuariaNit)

sistemaEnturamientoId = 1430
entraron_porcerrar.insert(1,"sistemaEnturnamientoId",sistemaEnturamientoId)

tipoOperacion = 2
entraron_porcerrar.insert(2,"tipoOperacionId",tipoOperacion)

orden = entraron_porcerrar.pop("empresaTransportadoraNit")
entraron_porcerrar.insert(3,"empresaTransportadoraNit",orden)

entraron_porcerrar = entraron_porcerrar[['terminalPortuariaNit', 'sistemaEnturnamientoId', 'tipoOperacionId', "empresaTransportadoraNit",
                                         "vehiculoNumPlaca","conductorCedulaCiudadania","numManifiestoCarga","fechaOfertaSolicitud",
                                         "fecha","entradaTerminal","pesajeEntrada","basculaEntrada","salidaTerminal","pesajeSalida","basculaSalida"]]

#Crea el archivo plano de cierre de citas
print(entraron_porcerrar)
entraron_porcerrar.to_csv('archivoPlano.txt',sep="\t",index=False)