import time
import random
import numpy as np
from datetime import datetime

class EstacionMeteorologica:
    """Simula una estación meteorológica con múltiples sensores"""
    
    def __init__(self, estacion_id, temp_media=25.0, temp_std=10.0, 
                 humedad_media=50.0, humedad_std=15.0):
        """
        Inicializa la estación meteorológica
        
        Args:
            estacion_id: Identificador único de la estación
            temp_media: Temperatura media en °C
            temp_std: Desviación estándar de temperatura
            humedad_media: Humedad media en %
            humedad_std: Desviación estándar de humedad
        """
        self.estacion_id = estacion_id
        self.temp_media = temp_media
        self.temp_std = temp_std
        self.humedad_media = humedad_media
        self.humedad_std = humedad_std
        self.direcciones_viento = ['N', 'NO', 'O', 'SO', 'S', 'SE', 'E', 'NE']
    
    def leer_temperatura(self):
        """
        Simula lectura de sensor de temperatura
        Usa distribución normal limitada para mantener valores en rango [0, 110]
        
        Returns:
            float: Temperatura en °C con 2 decimales
        """
        temp = np.random.normal(self.temp_media, self.temp_std)
        # limitar valores fuera del rango
        temp = max(0.0, min(110.0, temp))
        return round(temp, 2)
    
    def leer_humedad(self):
        """
        Simula lectura de sensor de humedad relativa
        Usa distribución normal limitada para mantener valores en rango [0, 100]
        
        Returns:
            int: Humedad relativa en %
        """
        humedad = np.random.normal(self.humedad_media, self.humedad_std)
        # limitar valores fuera del rango
        humedad = max(0, min(100, humedad))
        return int(humedad)
    
    def leer_direccion_viento(self):
        """
        Simula lectura de sensor de dirección del viento
        
        Returns:
            str: Dirección del viento (N, NO, O, SO, S, SE, E, NE)
        """
        return random.choice(self.direcciones_viento)
    
    def generar_lectura(self):
        """
        Genera una lectura completa de todos los sensores
        
        Returns:
            dict: Diccionario con todas las mediciones y metadatos
        """
        lectura = {
            "estacion_id": self.estacion_id,
            "timestamp": datetime.now().isoformat(),
            "temperatura": self.leer_temperatura(),
            "humedad": self.leer_humedad(),
            "direccion_viento": self.leer_direccion_viento()
        }
        return lectura

if __name__ == "__main__":
    # Crear estación meteorológica
    estacion = EstacionMeteorologica(
        estacion_id= "EstacionPrueba",
        temp_media= 25.0,   # Temperatura media: 25°C
        temp_std= 10.0,        # Desviación estándar: ±10°C
        humedad_media= 60.0,   # Humedad media: 60%
        humedad_std= 15.0     # Desviación estándar: ±15%
    )
    
    print(f"Generando lecturas cada 15 a 30 segundos...")
    
    try:
        while True:
            # Generar lectura
            lectura = estacion.generar_lectura()
            print(f"Lectura generada: {lectura}")
            
            # Esperar antes de la siguiente lectura
            timeSleep = random.randint(15, 30)
            print(f"Pausa {timeSleep}s")
            time.sleep(timeSleep)
            
    except KeyboardInterrupt:
        print("\n\nDeteniendo productor...")