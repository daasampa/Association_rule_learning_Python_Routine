class ARL_ETL:
    def __init__(self, days, age, day_lag, month_lag):
        self.days = days #cantidad de días en los que se busca el cambio en las variables#
        self.age = age #edad en la que se hace el corte para la variable categórica, e.g. 60 años.#
        self.day_lag = day_lag #cantidad de días para buscar la última ingestión de datos de las tablas nativas de la LZ.#
        self.month_lag = month_lag #cantidad de meses en los que se buscan datos de fraude en SIIFRA.#
    def arl_enumeracion(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_enumeracion purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_enumeracion stored as parquet as \
                             with \
                             temp_1 as ( \
                              select documento, trim(ip) as ip, \
                                     max(cast(concat(substr(cast(fecha_enumeracion as string), 1, 4), '-', \
                                                     substr(cast(fecha_enumeracion as string), 5, 2), '-', \
                                                     substr(cast(fecha_enumeracion as string), 7, 2)) as timestamp)) \
                                     as fecha_ultima_enumeracion \
                                     from proceso_seguridad_externa.arl_enumeracion_cruda \
                                     group by documento, ip), \
                             temp_2 as ( \
                              select t1.documento, t1.ip, t1.fecha_ultima_enumeracion, \
                                     if(t2.ip is null, 0, 1) as indicador_ip_riesgosa \
                                     from temp_1 as t1 \
                                     left join proceso_seguridad_externa.ips_riesgosas_rsa as t2 \
                                               on t1.ip = t2.ip), \
                             temp_3 as ( \
                              select documento, max(fecha_ultima_enumeracion) as fecha_ultima_enumeracion, \
                                     if(sum(indicador_ip_riesgosa) > 0, 1, 0) as ip_riesgosa \
                                     from temp_2 \
                                     group by documento) \
                             select *, \
                                    if(now() - interval " + str(self.days) + " days < fecha_ultima_enumeracion > 0, 1, 0) \
                                     as enumeracion \
                                    from temp_3")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_cambio_otp(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_cambio_otp purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_cambio_otp stored as parquet as \
                           with \
                           temp_1 as ( \
                            select documento, \
                                   cast(concat(substr(cast(fecha_max as string), 1, 4), '-', \
                                               substr(cast(fecha_max as string), 5, 2), '-', \
                                               substr(cast(fecha_max as string), 7, 2)) as timestamp) \
                                    as fecha_ultimo_cambio_otp \
                                    from proceso_seguridad_externa.arl_cambio_otp_cruda) \
                           select t1.*, \
                                  if(now() - interval " + str(self.days) + " days < fecha_ultimo_cambio_otp > 0, 1, 0) \
                                         as cambio_otp \
                                  from temp_1 as t1")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_evidente_riesgoso(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_evidente_riesgoso purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_evidente_riesgoso stored as parquet as \
                           with \
                           temp_1 as ( \
                            select documento, \
                                   max(cast(concat(substr(cast(fechorevid as string), 1, 4), '-', \
                                                   substr(cast(fechorevid as string), 5, 2), '-', \
                                                   substr(cast(fechorevid as string), 7, 2)) as timestamp)) \
                                    as fecha_ultimo_evidente_riesgoso \
                                   from proceso_seguridad_externa.arl_evidente_riesgoso_cruda \
                                   group by documento) \
                           select documento, fecha_ultimo_evidente_riesgoso, \
                                  if(now() - interval " + str(self.days) + " days < fecha_ultimo_evidente_riesgoso, 1, 0) \
                                   as evidente_riesgoso \
                                  from temp_1")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_topes(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_topes purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_topes stored as parquet as \
                           with \
                           temp_1 as ( \
                            select nroid as documento, cast(concat(substr(cast(max(fecha) as string), 1, 4), '-', \
                                                                   substr(cast(max(fecha) as string), 5, 2), '-', \
                                                                   substr(cast(max(fecha) as string), 7, 2)) as timestamp) \
                                                        as fecha_ultimo_cambio_topes \
                                  from s_canales.pcc_pcclibramd_pccffppcli \
                                  where year = year(now()) and ingestion_month = month(now()) and \
                                               ingestion_day = day(now()) - " + str(self.day_lag) + \
                                               " group by nroid) \
                           select documento, fecha_ultimo_cambio_topes, \
                                  if(now() - interval " + str(self.days) + " days < fecha_ultimo_cambio_topes > 0, 1, 0) \
                                   as cambio_topes \
                                 from temp_1")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_edad_clientes(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_edad_clientes purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_edad_clientes stored as parquet as \
                           with \
                           temp_1 as ( \
                            select cinamk as llave_nombre, cast(cidtbr as string) as fecha_nacimiento \
                                   from s_clientes.bvclegados_visionr_cindv \
                                   where year = year(now()) and month = month(now()) and ingestion_day = day(now()) - " +\
                                   str(self.day_lag) + "), \
                           temp_2 as ( \
                            select llave_nombre, datediff(now(), \
                                                          cast(concat(substr(fecha_nacimiento, 1, 4), '-', \
                                                                      substr(fecha_nacimiento, 5, 2), '-', \
                                                                      substr(fecha_nacimiento, 7, 2)) as timestamp)) / 365.25 \
                                                  as edad \
                                   from temp_1) \
                           select *, if(edad > " + str(self.age) + ", 1, 0) as edad_cliente \
                                  from temp_2")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_actualizacion_datos(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_actualizacion_datos purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_actualizacion_datos stored as parquet as \
                           with \
                           temp_1 as ( \
                            select cnnamk as llave_nombre, cast(cnnoss as bigint) as documento, \
                                   cast(cncdti as bigint) as tipo_doc, \
                                   ifnull(cndtlm, 0) as fecha_actualizacion_datos \
                                   from s_clientes.bvclegados_visionr_cname \
                                   where year = year(now()) and ingestion_month = month(now()) and \
                                         ingestion_day = day(now()) - " + str(self.day_lag) + " and cncdst = '1'), \
                           temp_2 as ( \
                            select cast(dclllave as bigint) as llave_nombre, cast(dclnumdoc as bigint) as documento, \
                                   ifnull(max(dclfecgra), 0) as fecha_ultima_actualizacion_datos \
                                   from s_clientes.bvclegados_bvclibramd_bvcffacdcl \
                                   where year = year(now()) and trim(lower(dcldescr)) not in ('area calidad de datos') \
                                   group by dclnumdoc, dclllave), \
                           temp_3 as ( \
                            select t1.llave_nombre, t1.documento, t1.tipo_doc, \
                                   cast(if(t1.fecha_actualizacion_datos > t2.fecha_ultima_actualizacion_datos, \
                                   t1.fecha_actualizacion_datos, t2.fecha_ultima_actualizacion_datos) as string) \
                                    as fecha_ultima_actualizacion_datos \
                                   from temp_1 as t1 \
                                   left join temp_2 as t2 \
                                        on t1.llave_nombre = t2.llave_nombre \
                                   where t2.fecha_ultima_actualizacion_datos is not null), \
                           temp_4 as ( \
                            select llave_nombre, documento, \
                                   max(cast(concat(substr(fecha_ultima_actualizacion_datos, 1, 4), '-', \
                                                   substr(fecha_ultima_actualizacion_datos, 5, 2), '-', \
                                                   substr(fecha_ultima_actualizacion_datos, 7, 2)) as timestamp)) \
                                    as fecha_ultima_actualizacion_datos \
                                    from temp_3 \
                                    group by llave_nombre, documento) \
                           select *, if(now() - interval " + str(self.days) + " days < fecha_ultima_actualizacion_datos, 1, 0) \
                                      as actualizacion_datos \
                                  from temp_4")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_regeneracion_clave(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_regeneracion_clave purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_regeneracion_clave stored as parquet as \
                           with \
                           temp_1 as ( \
                            select documento, cast(max(fecha) as string) as fecha_max \
                                   from proceso_seguridad_externa.cambio_regeneracion_clave \
                                   where descripcion like '%regenera%' \
                                   group by documento), \
                           temp_2 as ( \
                            select documento, cast(concat(substr(fecha_max, 1, 4), '-', \
                                                          substr(fecha_max, 5, 2), '-', \
                                                          substr(fecha_max, 7, 2)) as timestamp) \
                                               as fecha_ultima_regeneracion_clave \
                                               from temp_1) \
                          select documento, fecha_ultima_regeneracion_clave, \
                                 if(now() - interval " + str(self.days) + " days < fecha_ultima_regeneracion_clave, 1, 0) \
                                  as regeneracion_clave \
                                 from temp_2")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_clientes_activos(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_clientes_activos purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_clientes_activos stored as parquet as \
                           with \
                           temp_1 as ( \
                            select cnnamk as llave_nombre, cast(cnnoss as bigint) as documento, cncdbi as codigo_segmento \
                                   from s_clientes.bvclegados_visionr_cname \
                                   where year = year(now()) and ingestion_month = month(now()) and \
                                                ingestion_day = day(now()) - " + str(self.day_lag) + " and cncdst = '1'), \
                           temp_2 as ( \
                            select distinct xfmlcd as codigo_segmento, lower(trim(xfdesc)) as segmento \
                                   from s_apoyo_corporativo.seg_visionspar_xtcod \
                                   where year = year(now()) and lower(xfldnm) = 'cncdbi') \
                           select t1.llave_nombre, t1.documento, \
                                  if(regexp_like(t2.segmento, '&'), 'mi negocio', t2.segmento) as segmento \
                                  from temp_1 as t1 \
                                  left join temp_2 as t2 \
                                       on trim(t1.codigo_segmento) = trim(t2.codigo_segmento)")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_base_siifra(self):
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_base_siifra purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_base_siifra stored as parquet as \
                           with \
                           temp_1 as ( \
                            select documento, max(cast(concat(substr(cast(fecha as string), 1, 4), '-', \
                                                              substr(cast(fecha as string), 5, 2), '-', \
                                                              substr(cast(fecha as string), 7, 2)) as timestamp)) as fecha_ultimo_fraude, \
                                               count(*) as total_fraudes \
                                               from s_apoyo_corporativo.dsc_reportes_siifra_transacciones_itc \
                                               group by documento) \
                           select * \
                                  from temp_1 \
                                  where fecha_ultimo_fraude > now() - interval " + str(self.month_lag) + " months")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def arl_matriz_variables():
        import time, pyodbc as odbc
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        cursor_lz = cnxn_lz.cursor()
        start = time.time()
        cursor_lz.execute("drop table if exists proceso_seguridad_externa.arl_matriz_variables purge")
        cursor_lz.execute("create table if not exists proceso_seguridad_externa.arl_matriz_variables stored as parquet as \
                           with \
                           temp_1 as ( \
                            select t1.documento, t1.segmento as segmento, ifnull(t2.cambio_otp, 0) as cambio_otp, \
                                   datediff(t9.fecha_ultimo_fraude, t2.fecha_ultimo_cambio_otp) as t1, \
                                   ifnull(t3.cambio_topes, 0) as  cambio_topes, \
                                   datediff(t9.fecha_ultimo_fraude, t3.fecha_ultimo_cambio_topes) as t2, \
                                   ifnull(t4.ip_riesgosa, 0) as ip_riesgosa, \
                                   ifnull(t4.enumeracion, 0) as enumeracion, \
                                   datediff(t9.fecha_ultimo_fraude, t4.fecha_ultima_enumeracion) as t3, \
                                   ifnull(t5.actualizacion_datos, 0) as actualizacion_datos, \
                                   datediff(t9.fecha_ultimo_fraude, t5.fecha_ultima_actualizacion_datos) as t4, \
                                   ifnull(t6.edad_cliente, 0) as edad_cliente, \
                                   ifnull(t7.evidente_riesgoso, 0) as evidente_riesgoso, \
                                   datediff(t9.fecha_ultimo_fraude, t7.fecha_ultimo_evidente_riesgoso) as t5, \
                                   ifnull(t8.regeneracion_clave, 0) as regeneracion_clave, \
                                   datediff(t9.fecha_ultimo_fraude, t8.fecha_ultima_regeneracion_clave) as t6, \
                                   if(t9.documento is null, 'NO_FRAUDE', 'FRAUDE') as fraude \
                                   from      proceso_seguridad_externa.arl_clientes_activos as t1 \
                                   left join proceso_seguridad_externa.arl_cambio_otp as t2 \
                                        on t1.documento = t2.documento \
                                   left join proceso_seguridad_externa.arl_topes as t3 \
                                        on t1.documento = t3.documento \
                                   left join proceso_seguridad_externa.arl_enumeracion as t4 \
                                        on t1.documento = t4.documento \
                                   left join proceso_seguridad_externa.arl_actualizacion_datos as t5 \
                                        on t1.llave_nombre = t5.llave_nombre \
                                   left join proceso_seguridad_externa.arl_edad_clientes as t6 \
                                        on t1.llave_nombre = t6.llave_nombre \
                                   left join proceso_seguridad_externa.arl_evidente_riesgoso as t7 \
                                        on t1.documento = t7.documento \
                                   left join proceso_seguridad_externa.arl_regeneracion_clave as t8 \
                                        on t1.documento = t8.documento \
                                   left join proceso_seguridad_externa.arl_base_siifra as t9 \
                                        on t1.documento = t9.documento) \
                           select distinct * from temp_1")
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
    def check(table):
        import time, pyodbc as odbc, pandas as pd
        cnxn_lz = odbc.connect("DSN=LZ", autocommit=True)
        start = time.time()
        sql = "with \
               temp_1 as ( \
                select 1 as llave, count(*) as total_registros from proceso_seguridad_externa." + table + ")," \
              "temp_2 as ( \
                select 1 as llave, count(distinct documento) as total_clientes from proceso_seguridad_externa." + table + ")" \
              "select t1.total_registros, t2.total_clientes, \
                      if(t1.total_registros = t2.total_clientes, 'CORRECTO', 'ERROR') as indicador_calidad, \
                      abs(t1.total_registros - t2.total_clientes) as diferencia_registros\
                      from temp_1 as t1 \
                      join temp_2 as t2 \
                           on t1.llave = t2.llave"
        check = pd.read_sql(sql = sql, con = cnxn_lz)
        end = time.time()
        print(str(round((end - start)/60, 2)) + " min. elapsed")
        return check