"""
Laboratorio 9 - Redes
Codificador/Decodificador para restricción de 3 bytes (24 bits)
"""

class CodificadorIoT:
    """
    Codifica y decodifica datos de sensores en exactamente 3 bytes (24 bits)
    
    Distribución de bits:
    - Temperatura: 14 bits (0-13)    -> 0 a 11000 (temp * 100)
    - Humedad:      7 bits (14-20)   -> 0 a 100
    - Dirección:    3 bits (21-23)   -> 0 a 7
    """
    
    # Mapeo de direcciones a números
    DIRECCIONES_A_NUM = {
        'N': 0, 'NO': 1, 'O': 2, 'SO': 3,
        'S': 4, 'SE': 5, 'E': 6, 'NE': 7
    }
    
    NUM_A_DIRECCIONES = {v: k for k, v in DIRECCIONES_A_NUM.items()}
    
    @staticmethod
    def encode(temperatura, humedad, direccion_viento):
        """
        Codifica los datos de sensores en 3 bytes
        
        Args:
            temperatura: float 0-110.00
            humedad: int 0-100
            direccion_viento: str (N, NO, O, SO, S, SE, E, NE)
        
        Returns:
            bytes: 3 bytes codificados
        """
        # Validar rangos
        if not (0 <= temperatura <= 110):
            raise ValueError(f"Temperatura fuera de rango: {temperatura}")
        if not (0 <= humedad <= 100):
            raise ValueError(f"Humedad fuera de rango: {humedad}")
        if direccion_viento not in CodificadorIoT.DIRECCIONES_A_NUM:
            raise ValueError(f"Dirección inválida: {direccion_viento}")
        
        # 1. Convertir temperatura a entero (multiplicar por 100)
        temp_int = int(round(temperatura * 100))  # 25.45 -> 2545
        
        # 2. Convertir dirección a número
        dir_num = CodificadorIoT.DIRECCIONES_A_NUM[direccion_viento]
        
        # 3. Empaquetar en 24 bits
        # Temperatura: bits 0-13 (14 bits)
        # Humedad:     bits 14-20 (7 bits)
        # Dirección:   bits 21-23 (3 bits)
        
        packed = 0
        packed |= (temp_int & 0x3FFF)        # 14 bits: 0x3FFF = 0011111111111111
        packed |= (humedad & 0x7F) << 14     # 7 bits:  0x7F   = 01111111
        packed |= (dir_num & 0x07) << 21     # 3 bits:  0x07   = 00000111
        
        # 4. Convertir a 3 bytes
        byte1 = (packed >> 16) & 0xFF  # Byte más significativo
        byte2 = (packed >> 8) & 0xFF   # Byte medio
        byte3 = packed & 0xFF          # Byte menos significativo
        
        return bytes([byte1, byte2, byte3])
    
    @staticmethod
    def decode(data_bytes):
        """
        Decodifica 3 bytes a datos de sensores
        
        Args:
            data_bytes: bytes de longitud 3
        
        Returns:
            dict: {'temperatura': float, 'humedad': int, 'direccion_viento': str}
        """
        if len(data_bytes) != 3:
            raise ValueError(f"Se esperan 3 bytes, se recibieron {len(data_bytes)}")
        
        # 1. Reconstruir el entero de 24 bits
        packed = (data_bytes[0] << 16) | (data_bytes[1] << 8) | data_bytes[2]
        
        # 2. Extraer cada campo
        temp_int = packed & 0x3FFF          # Bits 0-13
        humedad = (packed >> 14) & 0x7F     # Bits 14-20
        dir_num = (packed >> 21) & 0x07     # Bits 21-23
        
        # 3. Convertir temperatura de vuelta a float
        temperatura = temp_int / 100.0
        
        # 4. Convertir número a dirección
        direccion_viento = CodificadorIoT.NUM_A_DIRECCIONES[dir_num]
        
        return {
            'temperatura': temperatura,
            'humedad': humedad,
            'direccion_viento': direccion_viento
        }
    
    @staticmethod
    def test():
        """Prueba el codificador/decodificador"""
        print("=== TEST DE CODIFICADOR ===\n")
        
        # Casos de prueba
        casos = [
            {'temperatura': 25.45, 'humedad': 60, 'direccion_viento': 'NO'},
            {'temperatura': 0.00, 'humedad': 0, 'direccion_viento': 'N'},
            {'temperatura': 110.00, 'humedad': 100, 'direccion_viento': 'NE'},
            {'temperatura': 37.89, 'humedad': 78, 'direccion_viento': 'S'},
        ]
        
        for i, caso in enumerate(casos, 1):
            print(f"Caso {i}:")
            print(f"  Original: {caso}")
            
            # Codificar
            encoded = CodificadorIoT.encode(
                caso['temperatura'],
                caso['humedad'],
                caso['direccion_viento']
            )
            print(f"  Codificado: {len(encoded)} bytes -> {encoded.hex()}")
            
            # Decodificar
            decoded = CodificadorIoT.decode(encoded)
            print(f"  Decodificado: {decoded}")
            
            # Verificar
            match = (
                abs(decoded['temperatura'] - caso['temperatura']) < 0.01 and
                decoded['humedad'] == caso['humedad'] and
                decoded['direccion_viento'] == caso['direccion_viento']
            )
            print(f"  ✓ Match: {match}\n")


if __name__ == "__main__":
    CodificadorIoT.test()