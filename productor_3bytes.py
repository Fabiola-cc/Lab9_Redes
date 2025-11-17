"""
Laboratorio 9 - Redes
Productor con restricción de 3 bytes
"""

import time
import random
from kafka import KafkaProducer
from SimulaciónEstación import EstacionMeteorologica
from codificador import CodificadorIoT

# ============ CONFIGURACIÓN ============
KAFKA_SERVER = ["iot.redesuvg.cloud:9092"]
KAFKA_TOPIC = '22787'  # Tu carné
ESTACION_ID = 'estacion1'

# Parámetros de distribución
TEMP_MEDIA = 25.0
TEMP_STD = 10.0
HUMEDAD_MEDIA = 60.0
HUMEDAD_STD = 15.0

print("=" * 60)
print("PRODUCTOR IoT - RESTRICCIÓN 3 BYTES (24 bits)")
print("=" * 60)

# Crear estación
estacion = EstacionMeteorologica(
    estacion_id=ESTACION_ID,
    temp_media=TEMP_MEDIA,
    temp_std=TEMP_STD,
    humedad_media=HUMEDAD_MEDIA,
    humedad_std=HUMEDAD_STD
)

# Crear producer (sin serializer, enviamos bytes directos)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER
)

print(f"✓ Conectado a Kafka: {KAFKA_SERVER}")
print(f"✓ Topic: {KAFKA_TOPIC}")
print(f"✓ Restricción: 3 bytes (24 bits)\n")

contador = 0

try:
    while True:
        # Generar lectura
        lectura = estacion.generar_lectura()
        temp = lectura['temperatura']
        hum = lectura['humedad']
        viento = lectura['direccion_viento']
        
        # CODIFICAR en 3 bytes
        datos_codificados = CodificadorIoT.encode(temp, hum, viento)
        
        # Enviar a Kafka
        producer.send(KAFKA_TOPIC, value=datos_codificados)
        
        contador += 1
        
        # Mostrar info
        print(f"[Mensaje #{contador}] {lectura['timestamp']}")
        print(f"  Original: Temp={temp}°C, Hum={hum}%, Viento={viento}")
        print(f"  Codificado: {len(datos_codificados)} bytes -> {datos_codificados.hex()}")
        print(f"  ✓ Enviado\n")
        
        # Esperar 15-30 segundos
        time.sleep(random.randint(15, 30))
        
except KeyboardInterrupt:
    print("\nDeteniendo productor...")
finally:
    producer.close()
    print("✓ Cerrado correctamente")