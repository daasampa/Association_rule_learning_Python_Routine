def parameters(segmento, num_features):
    readline.parse_and_bind("tab: complete")
    def segmentos(segmento):
        return """select documento, segmento,
                         cambio_otp as c1, cambio_topes as c2, ip_riesgosa as c3, enumeracion as c4,
                         actualizacion_datos as c5, edad_cliente as c6, evidente_riesgoso as c7,
                         regeneracion_clave as c8, fraude
                         from proceso_seguridad_externa.arl_matriz_variables
                         where fraude = 'FRAUDE' and segmento = """ + "'" + segmento + "'"
    return ARL(segmentos(segmento), num_features)