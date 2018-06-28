def parameters(segment, num_features):
    def segments(segment):
        return """select documento, segmento,
                         cambio_otp as c1, cambio_topes as c2, ip_riesgosa as c3, enumeracion as c4,
                         actualizacion_datos as c5, edad_cliente as c6, evidente_riesgoso as c7,
                         regeneracion_clave as c8, if(fraude = 'FRAUDE', 1, 0) as fraude
                         from proceso_seguridad_externa.arl_matriz_variables
                         where segmento = """ + "'" + segment + "'"
    return ARL(segments(segment), num_features)
