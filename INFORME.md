# 1. Introducción

En el presente trabajo se implementaron mecanismos de coordinación para comunicar los distintos nodos pertenecientes a un sistema de control de stock de una verdulería, con el objetivo de aprovechar nuevas réplicas de los controles si hubiese.

A continuación, se detalla la forma en la que se coordinan en particular las instancias de los nodos Sum y Aggregator.


# 2. Coordinación entre Sums

Por el diseño del esqueleto provisto del sistema, todos los nodos Sum consumen de una misma cola de datos, provocando que los datos de los clientes sean repartidos al estilo round-robin entre todos los nodos Sum. 

Los mensajes que pueden venir de la cola de datos son: mensajes de datos, ó mensajes de End of File (EOF) para un cliente. Cada mensaje viene con el ID del cliente asociado al mensaje. Por un lado, para tratar los mensajes de datos, cada Sum simplemente almacena y suma los pares (fruta, valor) asociados al ID del cliente. Por otro lado, para tratar los mensajes de End of File (EOF), en cambio, el procedimiento es más complejo pues de alguna forma se necesita comunicar este EOF al resto de nodos, y así puedan avanzar todos los datos del cliente a la siguiente etapa.

El mensaje EOF viene con la cantidad total de mensajes enviados para ese cliente, es decir, la totalidad de mensajes que, entre todos los nodos Sum, se debieron haber procesado.

El nodo Sum que recibe el EOF “original” desde la cola de datos automáticamente se convierte en el coordinador para ese cliente. Cuando recibe el EOF, primero envía los datos que éste tenga del cliente hacia la siguiente etapa (ver siguiente sección), y luego realiza un broadcasting del mensaje EOF hacia el resto de nodos. Este broadcast se realiza usando un Exchange específico para comunicar mensajes de control, como el EOF. A su vez, cada nodo Sum cuenta también con una cola de control, enlazada al exchange de control mencionado, para recibir tales mensajes de control.

Por ejemplo, la estructura de comunicación con 3 nodos Sums sería:
<p align="center">
  <img width="1107" height="1042" alt="Captura de pantalla 2026-10-01 141713" src="https://github.com/user-attachments/assets/96c47110-2d10-46c1-98c5-c9922911a7da" />
</p>

Cuando otro nodo Sum reciba este EOF propagado / broadcasteado, al igual que el coordinador, inmediatamente envía los datos del cliente que tenga disponibles en el momento a la siguiente etapa y responde al coordinador, a través del exchange de control, con un mensaje de tipo “reporte” que contiene la cantidad de mensajes totales que procesó hasta ahora para ese cliente. Si posteriormente a recibir el EOF propagado procesa un nuevo mensaje de datos del cliente, el nodo tendrá que flushear este dato nuevamente a la siguiente etapa, y reportar al coordinador. Este comportamiento se aplica a cada uno de los demás nodos que no son el coordinador para el cliente en cuestión, los “seguidores” ó “followers” del coordinador.

El nodo coordinador, quien recibe todos estos reportes a través de la cola de control, compara si la cantidad total de mensajes enviados para ese cliente coincide con la cantidad total de mensajes procesados para ese cliente hasta el momento. Cuando finalmente se terminaron de procesar todos los mensajes, el coordinador envía un mensaje de tipo “ack” ó confirmación hacia todos los demás nodos, haciéndoles saber que es seguro avanzar a la siguiente etapa.

Con esto, todos los nodos Sum proceden a enviar mensajes EOF a todos los nodos Aggregator, y se da por finalizada la etapa de Sum para este cliente.

En resumen, la secuencia general de ejecución ante la llegada de un mensaje EOF con, por ejemplo, 3 nodos Sum es:
<p align="center">
  <img width="790" height="945" alt="Captura de pantalla 2026-10-01 144300" src="https://github.com/user-attachments/assets/c04021e1-56b2-48a5-bc83-91b56edfefe2" />
</p>

Algunas consideraciones:
* Dentro de un nodo Sum, viven dos go routines:
  * Una se encarga de escuchar en la cola de datos, procesando mensajes de datos y mensajes EOF “originales” provenientes del gateway.
  * La otra go routine escucha en la cola de control, procesando mensajes como el EOF propagado, reportes y acks.


# 3. Siguiente etapa: Aggregation

Los nodos Sum envían los datos de los clientes hacia los nodos Aggregator a través de un exchange (diferente al exchange de control), utilizando enrutamiento por hash.

Para evitar que la carga de los mensajes vaya a parar a sólo unos pocos nodos Aggregator si hay pocos clientes ó si hay pocas frutas, la clave de enrutamiento está formada por la tupla ID cliente y fruta. Esto quiere decir que, para futuros mensajes, si el ID del cliente y la fruta son las mismas, estos mensajes irán a parar al mismo nodo Aggregator. De esta manera, también se reparte la carga de datos que envían los nodos Sum, evitando que todos los nodos Aggregator procesen todos los datos.

A medida que los nodos Aggregator reciben los mensajes de los datos, éstos también guardan los pares (fruta, valor) vinculados por ID del cliente que reciben de los Sums. Para avanzar a la siguiente etapa, el nodo Aggregator necesita recibir la misma cantidad de EOFs que de nodos Sums. Así, el Aggregator sabe que todos los Sums ya pasaron todos los datos que tenían de ese cliente, y se evita que debido a un delay de conexión el Aggregator pase a la siguiente etapa con la información incompleta.

Cuando finalmente recibe todos los EOFs, el nodo Aggregator calcula el top parcial para el cliente, y lo envía junto con su propio mensaje EOF a la siguiente etapa con el Join. Este comportamiento se aplica para todos los nodos Aggregator.


# 4. Etapa final: Join

El nodo Join sirve para juntar los resultados de los distintos nodos Aggregator, y armar el top global para cada cliente. Similarmente al nodo Aggregator, el Join recibe y guarda, a través de una cola, los pares (fruta, valor) vinculados por ID del cliente, y cuando se recibe la misma cantidad de EOFs que de nodos Aggregator para un determinado cliente, se calcula su top global. Este top luego es comunicado a través de una cola (que viene en el esqueleto del sistema por default) hacia el gateway para responder al cliente.




