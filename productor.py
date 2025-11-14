"""
Laboratorio 9 - Redes
Productor de datos de estación meteorológica
Simula sensores de temperatura, humedad y dirección del viento
"""

import json
import time
import random
from kafka import KafkaProducer
from SimulaciónEstación import EstacionMeteorologica


class ProductorKafka:
    """Maneja el envío de datos a Apache Kafka"""
    
    def __init__(self, bootstrap_server, topic):
        """
        Inicializa el productor de Kafka
        
        Args:
            bootstrap_server: servidor Kafka 
            topic: Nombre del topic donde se enviarán los mensajes
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        print(f"Productor Kafka conectado a {bootstrap_server}")
        print(f"Enviando mensajes al topic: {topic}")
    
    def enviar_lectura(self, lectura):
        """
        Envía una lectura al topic de Kafka
        
        Args:
            lectura: Diccionario con los datos de la lectura
        """
        try:
            # Enviar mensaje con la estación_id como key para particionamiento
            future = self.producer.send(
                self.topic,
                key=lectura['estacion_id'].encode('utf-8'),
                value=lectura
            )
            # Esperar confirmación
            record_metadata = future.get(timeout=10)
            
            print(f"[{lectura['timestamp']}] Enviado -> "
                  f"Temp: {lectura['temperatura']}°C, "
                  f"Humedad: {lectura['humedad']}%, "
                  f"Viento: {lectura['direccion_viento']} "
                  f"(partition: {record_metadata.partition})")
            
        except Exception as e:
            print(f"✗ Error al enviar mensaje: {e}")
    
    def cerrar(self):
        """Cierra la conexión del productor"""
        self.producer.flush()
        self.producer.close()
        print("\nProductor cerrado correctamente")


def main():
    """Función principal para ejecutar el productor"""
    
    # CONFIGURACIÓN 
    KAFKA_SERVER = ["iot.redesuvg.cloud:9092"]
    KAFKA_TOPIC = '22787' # Carné de un integrante
    ESTACION_ID = 'estacion1'
    
    # Parámetros de la distribución gaussiana
    TEMP_MEDIA = 25.0      # Temperatura media: 25°C
    TEMP_STD = 10.0        # Desviación estándar: ±10°C
    HUMEDAD_MEDIA = 60.0   # Humedad media: 60%
    HUMEDAD_STD = 15.0     # Desviación estándar: ±15%
    
    print("PRODUCTOR IoT - ESTACIÓN METEOROLÓGICA")
    
    # Crear estación meteorológica
    estacion = EstacionMeteorologica(
        estacion_id=ESTACION_ID,
        temp_media=TEMP_MEDIA,
        temp_std=TEMP_STD,
        humedad_media=HUMEDAD_MEDIA,
        humedad_std=HUMEDAD_STD
    )
    
    # Crear productor de Kafka
    productor = ProductorKafka(bootstrap_server=KAFKA_SERVER, topic=KAFKA_TOPIC)
    
    print("\nGenerando lecturas cada 15 a 30 segundos...")
    print(f"Distribución Temperatura: N(μ={TEMP_MEDIA}°C, σ={TEMP_STD})")
    print(f"Distribución Humedad: N(μ={HUMEDAD_MEDIA}%, σ={HUMEDAD_STD})")
    
    try:
        while True:
            # Generar y enviar lectura
            lectura = estacion.generar_lectura()
            productor.enviar_lectura(lectura)
            
            # Esperar antes de la siguiente lectura
            timeSleep = random.randint(15, 30)
            time.sleep(timeSleep)
            
    except KeyboardInterrupt:
        print("\n\nDeteniendo productor...")
    finally:
        productor.cerrar()


if __name__ == "__main__":
    main()