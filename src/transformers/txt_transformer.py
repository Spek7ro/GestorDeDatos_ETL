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
                for line in txt_file:
                    # Split the line by comma
                    line_parts = line.strip().split(',')

                    # Check if the line has the expected number of parts
                    if len(line_parts) != 8:
                        continue

                    # Extract the values and format them
                    fatura_num = line_parts[0].strip()
                    inventario_cod = line_parts[1].strip()
                    descricao = line_parts[2].strip()
                    montante = line_parts[3].strip()
                    data_fatura = line_parts[4].strip()
                    preco_unit = line_parts[5].strip()
                    cliente_id = line_parts[6].strip()
                    pais = line_parts[7].strip()

                    # Create the result dictionary
                    result.append(
                        {
                            "description": descricao,
                            "quantity": montante,
                            "price": preco_unit,
                            "total": float(montante) * float(preco_unit),
                            "invoice": fatura_num,
                            "provider": cliente_id,
                            "country": pais
                        }
                    )
           
        with self.output().open('w') as out:
            out.write(json.dumps(result, indent=4))

    def output(self):
        project_dir = os.path.dirname(os.path.abspath("loader.py"))
        result_dir = join(project_dir, "result")
        return luigi.LocalTarget(join(result_dir, "txt.json"))