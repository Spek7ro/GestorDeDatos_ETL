##!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------
# Archivo: txt_transformer.py
# Capitulo: Flujo de Datos
# Autor(es): Perla Velasco & Yonathan Mtz. & Jorge Solís
# Version: 1.0.0 Noviembre 2022
# Descripción:
#
#   Este archivo define un procesador de datos que se encarga de transformar
#   y formatear el contenido de un archivo TXT
#-------------------------------------------------------------------------
from src.extractors.txt_extractor import TXTExtractor
from os.path import join
import luigi, os, json

class TXTTransformer(luigi.Task):

    def requires(self):
        return TXTExtractor()

    def run(self):
        result = []
        for file in self.input():
            with file.open() as txt_file:
                txt_file.readline() # omitir la primera linea (cabecera)
                for linea in txt_file: # se lee la segunda linea que contiene los registros
                    linea = linea.strip()
                    registros = linea.split(';') # los registros estan separados por un ;
                    for registro in registros:
                        if registro:
                            # print("Registro:", registro)
                            parts = registro.split(',')
                            description = parts[2]
                            quantity = int(parts[3])
                            price = float(parts[5])
                            total = quantity * price
                            invoice = parts[1]
                            provider = parts[6]
                            country = parts[7]
                            
                            # print("description:", description)
                            # print("quantity:", quantity)
                            # print("price:", price)
                            # print("total:", total)
                            # print("invoice:", invoice)
                            # print("provider:", provider)
                            # print("country:", country)
                            # print('----------')

                            result.append({
                                'description': description,
                                'quantity': quantity,
                                'price': price,
                                'total': total,
                                'invoice': invoice,
                                'provider': provider,
                                'country': country
                            }) 
        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))
