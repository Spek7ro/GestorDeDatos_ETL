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
                # Leer el archivo de texto y transformar los datos
                header = txt_file.readline().strip().split(',')
                for line in txt_file:
                    parts = line.strip().split(',')
                    result.append({
                        'description': parts[2],
                        'quantity': int(parts[3]),
                        'price': float(parts[5]),
                        'total': float(float(parts[3])) * float(parts[5]),
                        'invoice': int(parts[0]),
                        'provider': int(parts[6]),
                        'country': parts[7].strip()
                    })
           
        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))